package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/crypto/bcrypt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

// Command-line flags.
var (
	primaryKubeconfig    = flag.String("primary-kubeconfig", "", "Path to primary kubeconfig file")
	replicaKubeconfigDir = flag.String("replica-kubeconfig-dir", "", "Directory containing replica kubeconfig files")
	workerCount          = flag.Int("worker-count", 3, "Number of worker goroutines")
	syncNamespacesStr    = flag.String("sync-namespaces", "", "Comma-separated list of namespaces to sync. If empty, sync all namespaces.")
)

// Global variables.
var (
	workQueue         workqueue.TypedRateLimitingInterface[any]
	progressMap       sync.Map // key: jobID, value: status string
	db                *sql.DB
	primaryClient     *kubernetes.Clientset
	replicaClusters   []*kubernetes.Clientset
	allowedNamespaces map[string]bool

	// jobCache holds replication tasks keyed by job ID.
	jobCache sync.Map // key: jobID, value: ReplicationTask

	// failedReplications stores, per job ID, a mapping of replica index -> error message.
	failedReplications sync.Map // key: jobID, value: map[int]string
)

// Prometheus Metrics
var (
	jobsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_jobs_total",
		Help: "Total number of sync jobs run, labeled by outcome",
	}, []string{"status"})

	jobLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sync_job_latency_seconds",
		Help:    "Latency of sync jobs in seconds",
		Buckets: prometheus.DefBuckets,
	})
)

// ReplicationTask holds information about a change event to replicate.
type ReplicationTask struct {
	ResourceType string
	Namespace    string
	Name         string
	Operation    string // "add", "update", or "delete"
	Object       interface{}
	TaskID       string
	CreatedAt    time.Time
	ChangeKey    string // used to prevent re-replication
}

// User model and login types
type User struct {
	Username  string
	Password  string // stored as bcrypt hash
	CreatedAt time.Time
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type CreateUserRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Main function
func main() {
	flag.Parse()

	if *primaryKubeconfig == "" {
		fmt.Println("primary-kubeconfig flag is required")
		os.Exit(1)
	}

	// Build allowed namespaces map if provided.
	if *syncNamespacesStr != "" {
		allowedNamespaces = make(map[string]bool)
		for _, ns := range strings.Split(*syncNamespacesStr, ",") {
			allowedNamespaces[strings.TrimSpace(ns)] = true
		}
	}

	// Initialize SQLite (includes tables for jobs and users).
	if err := initDB("sync_jobs.db"); err != nil {
		fmt.Printf("Failed to initialize SQLite DB: %v\n", err)
		os.Exit(1)
	}

	// Check for admin user; if not found, create one.
	ensureAdminUser()

	// Initialize work queue.
	workQueue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[any]())

	// Build primary client.
	var err error
	primaryClient, err = getKubeClient(*primaryKubeconfig)
	if err != nil {
		fmt.Printf("Error creating primary client: %v\n", err)
		os.Exit(1)
	}

	// Load replica clusters from the provided directory.
	replicaClusters, err = loadReplicaClustersConfig(*replicaKubeconfigDir)
	if err != nil {
		fmt.Printf("Error loading replica clusters: %v\n", err)
		os.Exit(1)
	}

	// Register Prometheus metrics.
	prometheus.MustRegister(jobsTotal, jobLatency)

	// Set up HTTP endpoints.
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/progress", progressHandler) // for internal use
	http.HandleFunc("/api/login", loginHandler)
	http.HandleFunc("/api/dashboard", dashboardHandler)
	http.HandleFunc("/api/status", statusHandlerAPI)
	http.HandleFunc("/api/users", createUserHandler) // POST to add a user
	http.HandleFunc("/retry", retryHandler)          // already implemented earlier

	// Start replication informers.
	factory := informers.NewSharedInformerFactory(primaryClient, time.Minute)
	setupDeploymentInformer(factory)
	setupStatefulSetInformer(factory)
	setupSecretInformer(factory)
	setupConfigMapInformer(factory)
	stopCh := make(chan struct{})
	defer close(stopCh)
	factory.Start(stopCh)
	factory.WaitForCacheSync(stopCh)

	// Start worker goroutines.
	for range *workerCount {
		go worker()
	}

	// Start HTTP server.
	go func() {
		fmt.Println("HTTP endpoints listening on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	// Graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println("Shutting down...")
	workQueue.ShutDown()
}

//
// Change Key & Duplication Prevention
//

// getChangeKey generates a key for the change so that already replicated changes can be skipped.
func getChangeKey(resourceType, namespace, name, operation string, obj interface{}) string {
	if operation == "delete" {
		return fmt.Sprintf("%s:%s:%s:delete", resourceType, namespace, name)
	}
	var resourceVersion string
	switch resourceType {
	case "deployment":
		if dep, ok := obj.(*appsv1.Deployment); ok {
			resourceVersion = dep.ResourceVersion
		}
	case "statefulset":
		if sts, ok := obj.(*appsv1.StatefulSet); ok {
			resourceVersion = sts.ResourceVersion
		}
	case "secret":
		if secret, ok := obj.(*corev1.Secret); ok {
			resourceVersion = secret.ResourceVersion
		}
	case "configmap":
		if cm, ok := obj.(*corev1.ConfigMap); ok {
			resourceVersion = cm.ResourceVersion
		}
	}
	return fmt.Sprintf("%s:%s:%s:%s", resourceType, namespace, name, resourceVersion)
}

// alreadyReplicated checks in the SQLite DB if a change with the given key was already successfully replicated.
func alreadyReplicated(changeKey string) (bool, error) {
	row := db.QueryRow("SELECT COUNT(*) FROM jobs WHERE change_key = ? AND status = 'Completed'", changeKey)
	var count int
	if err := row.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

//
// SQLite Functions
//

func initDB(dbPath string) error {
	var err error
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	// Create jobs table.
	createJobsTableSQL := `CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		change_key TEXT,
		resource_type TEXT,
		namespace TEXT,
		name TEXT,
		operation TEXT,
		status TEXT,
		created_at DATETIME,
		updated_at DATETIME,
		started_at DATETIME,
		finished_at DATETIME,
		error_message TEXT
	);`
	if _, err := db.Exec(createJobsTableSQL); err != nil {
		return err
	}
	// Create users table.
	createUsersTableSQL := `CREATE TABLE IF NOT EXISTS users (
		username TEXT PRIMARY KEY,
		password TEXT,
		created_at DATETIME
	);`
	_, err = db.Exec(createUsersTableSQL)
	return err
}

func insertJob(task ReplicationTask) error {
	now := time.Now()
	_, err := db.Exec(`INSERT INTO jobs (id, change_key, resource_type, namespace, name, operation, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.TaskID, task.ChangeKey, task.ResourceType, task.Namespace, task.Name, task.Operation, "enqueued", task.CreatedAt, now)
	return err
}

func updateJob(taskID, status string, startedAt, finishedAt *time.Time, errorMsg string) error {
	now := time.Now()
	_, err := db.Exec(`UPDATE jobs SET status = ?, updated_at = ?, started_at = COALESCE(?, started_at), finished_at = ?, error_message = ?
		WHERE id = ?`,
		status, now, startedAt, finishedAt, errorMsg, taskID)
	return err
}

func ensureAdminUser() {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM users WHERE username = 'admin'").Scan(&count)
	if err != nil {
		fmt.Printf("Error checking for admin user: %v\n", err)
		return
	}
	if count == 0 {
		// Generate a random password.
		pass := uuid.New().String()[0:8]
		hashed, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.DefaultCost)
		if err != nil {
			fmt.Printf("Error generating password hash: %v\n", err)
			return
		}
		_, err = db.Exec("INSERT INTO users (username, password, created_at) VALUES (?, ?, ?)", "admin", string(hashed), time.Now())
		if err != nil {
			fmt.Printf("Error inserting admin user: %v\n", err)
			return
		}
		fmt.Printf("Admin user created. Username: admin, Password: %s\n", pass)
	}
}

//
// API Endpoints
//

// loginHandler handles POST /api/login.
func loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	// Look up user.
	var storedHash string
	err := db.QueryRow("SELECT password FROM users WHERE username = ?", req.Username).Scan(&storedHash)
	if err != nil {
		http.Error(w, `{"success": false, "message": "User not found"}`, http.StatusUnauthorized)
		return
	}
	// Compare passwords.
	if err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(req.Password)); err != nil {
		http.Error(w, `{"success": false, "message": "Invalid credentials"}`, http.StatusUnauthorized)
		return
	}
	// On successful login, return success (in a real system, you would issue a session or JWT token).
	resp := LoginResponse{Success: true}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// dashboardHandler handles GET /api/dashboard.
func dashboardHandler(w http.ResponseWriter, r *http.Request) {
	// Query aggregate data from jobs table.
	var total, ongoing, failed, pending int
	if err := db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&total); err != nil {
		http.Error(w, "Error querying total jobs", http.StatusInternalServerError)
		return
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status = 'In Progress'").Scan(&ongoing); err != nil {
		http.Error(w, "Error querying ongoing jobs", http.StatusInternalServerError)
		return
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status = 'Failed'").Scan(&failed); err != nil {
		http.Error(w, "Error querying failed jobs", http.StatusInternalServerError)
		return
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status = 'enqueued'").Scan(&pending); err != nil {
		http.Error(w, "Error querying pending jobs", http.StatusInternalServerError)
		return
	}
	var failureRate float64
	if total > 0 {
		failureRate = float64(failed) / float64(total) * 100
	}
	data := map[string]interface{}{
		"totalJobs":      total,
		"ongoingJobs":    ongoing,
		"failedJobs":     failed,
		"failureRate":    failureRate,
		"pendingUpdates": pending,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// statusHandlerAPI handles GET /api/status.
func statusHandlerAPI(w http.ResponseWriter, r *http.Request) {
	type ClusterStatus struct {
		Status        string `json:"status"`
		ServerVersion string `json:"serverVersion,omitempty"`
		Error         string `json:"error,omitempty"`
	}
	resp := struct {
		Primary  ClusterStatus   `json:"primary"`
		Replicas []ClusterStatus `json:"replicas"`
	}{}
	// Check primary.
	if ver, err := primaryClient.Discovery().ServerVersion(); err != nil {
		resp.Primary = ClusterStatus{Status: "failed", Error: err.Error()}
	} else {
		resp.Primary = ClusterStatus{Status: "ok", ServerVersion: ver.GitVersion}
	}
	// Check replicas.
	for _, replica := range replicaClusters {
		var cs ClusterStatus
		if ver, err := replica.Discovery().ServerVersion(); err != nil {
			cs = ClusterStatus{Status: "failed", Error: err.Error()}
		} else {
			cs = ClusterStatus{Status: "ok", ServerVersion: ver.GitVersion}
		}
		resp.Replicas = append(resp.Replicas, cs)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// createUserHandler handles POST /api/users to add a new user.
func createUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	// Check if user already exists.
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", req.Username).Scan(&count)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if count > 0 {
		http.Error(w, `{"success": false, "message": "User already exists"}`, http.StatusBadRequest)
		return
	}
	hashed, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "Error hashing password", http.StatusInternalServerError)
		return
	}
	_, err = db.Exec("INSERT INTO users (username, password, created_at) VALUES (?, ?, ?)", req.Username, string(hashed), time.Now())
	if err != nil {
		http.Error(w, "Error inserting user", http.StatusInternalServerError)
		return
	}
	resp := map[string]interface{}{"success": true}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Retry endpoint (already implemented earlier) remains unchanged.
// It expects query parameters: jobid and replica (index).
func retryHandler(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("jobid")
	replicaIdxStr := r.URL.Query().Get("replica")
	if jobID == "" || replicaIdxStr == "" {
		http.Error(w, "Missing jobid or replica parameter", http.StatusBadRequest)
		return
	}
	replicaIdx, err := strconv.Atoi(replicaIdxStr)
	if err != nil || replicaIdx < 0 || replicaIdx >= len(replicaClusters) {
		http.Error(w, "Invalid replica index", http.StatusBadRequest)
		return
	}
	val, ok := jobCache.Load(jobID)
	if !ok {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}
	task := val.(ReplicationTask)
	// Attempt replication for the specified replica.
	err = replicateToReplica(task, replicaClusters[replicaIdx])
	if err != nil {
		updateFailure(jobID, replicaIdx, err.Error())
		http.Error(w, fmt.Sprintf("Retry failed: %v", err), http.StatusInternalServerError)
		return
	}
	removeFailure(jobID, replicaIdx)
	if noFailures(jobID) {
		updateJob(jobID, "Completed", nil, ptrTime(time.Now()), "")
		updateTaskProgress(jobID, "Completed")
	}
	w.Write([]byte("Retry succeeded"))
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

//
// Helpers for Failure Tracking
//

func updateFailure(jobID string, replicaIdx int, msg string) {
	var failures map[int]string
	val, _ := failedReplications.LoadOrStore(jobID, make(map[int]string))
	failures = val.(map[int]string)
	failures[replicaIdx] = msg
	failedReplications.Store(jobID, failures)
}

func removeFailure(jobID string, replicaIdx int) {
	val, ok := failedReplications.Load(jobID)
	if !ok {
		return
	}
	failures := val.(map[int]string)
	delete(failures, replicaIdx)
	if len(failures) == 0 {
		failedReplications.Delete(jobID)
	} else {
		failedReplications.Store(jobID, failures)
	}
}

func noFailures(jobID string) bool {
	_, ok := failedReplications.Load(jobID)
	return !ok
}

//
// Kubernetes Client Functions
//

func getKubeClient(kubeconfigPath string) (*kubernetes.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func loadReplicaClustersConfig(dir string) ([]*kubernetes.Clientset, error) {
	var clusters []*kubernetes.Clientset
	if dir == "" {
		fmt.Println("No replica kubeconfig directory provided.")
		return clusters, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filePath := filepath.Join(dir, entry.Name())
		config, err := clientcmd.BuildConfigFromFlags("", filePath)
		if err != nil {
			fmt.Printf("Skipping file %s: %v\n", filePath, err)
			continue
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			fmt.Printf("Skipping file %s: %v\n", filePath, err)
			continue
		}
		clusters = append(clusters, clientset)
	}
	return clusters, nil
}

// Task Enqueue & Processing (Replication Logic)
func enqueueReplicationTask(resourceType, namespace, name, operation string, obj interface{}) {
	if !shouldSync(namespace) {
		return
	}

	changeKey := getChangeKey(resourceType, namespace, name, operation, obj)
	already, err := alreadyReplicated(changeKey)
	if err != nil {
		fmt.Printf("Error checking duplication: %v\n", err)
	}
	if already {
		// Skip already replicated change.
		return
	}

	task := ReplicationTask{
		ResourceType: resourceType,
		Namespace:    namespace,
		Name:         name,
		Operation:    operation,
		Object:       obj,
		TaskID:       generateTaskID(),
		CreatedAt:    time.Now(),
		ChangeKey:    changeKey,
	}
	jobCache.Store(task.TaskID, task)
	fmt.Printf("Enqueuing task %s: %s %s/%s\n", task.TaskID, operation, namespace, name)
	if err := insertJob(task); err != nil {
		fmt.Printf("Error inserting job in SQLite: %v\n", err)
	}
	workQueue.Add(task)
}

func worker() {
	for {
		taskRaw, shutdown := workQueue.Get()
		if shutdown {
			break
		}
		task, ok := taskRaw.(ReplicationTask)
		if !ok {
			workQueue.Done(taskRaw)
			continue
		}
		go processReplicationTask(task)
		workQueue.Done(taskRaw)
	}
}

func processReplicationTask(task ReplicationTask) {
	startTime := time.Now()
	updateJob(task.TaskID, "In Progress", &startTime, nil, "")
	updateTaskProgress(task.TaskID, "In Progress")

	var wg sync.WaitGroup
	var mu sync.Mutex
	jobFailed := false
	var errorMsg string
	// local map to track failures for this task.
	localFailures := make(map[int]string)

	for idx, replica := range replicaClusters {
		wg.Add(1)
		go func(idx int, replica *kubernetes.Clientset) {
			defer wg.Done()
			err := replicateToReplica(task, replica)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				jobFailed = true
				msg := err.Error()
				errorMsg += fmt.Sprintf("Replica[%d]: %s; ", idx, msg)
				localFailures[idx] = msg
			} else {
				removeFailure(task.TaskID, idx)
			}
		}(idx, replica)
	}
	wg.Wait()

	if len(localFailures) > 0 {
		failedReplications.Store(task.TaskID, localFailures)
	}

	finishTime := time.Now()
	duration := finishTime.Sub(startTime).Seconds()
	jobLatency.Observe(duration)

	if jobFailed {
		fmt.Printf("Task %s finished with failures: %s\n", task.TaskID, errorMsg)
		updateJob(task.TaskID, "Failed", nil, &finishTime, errorMsg)
		updateTaskProgress(task.TaskID, "Failed")
		jobsTotal.WithLabelValues("failed").Inc()
	} else {
		fmt.Printf("Task %s succeeded\n", task.TaskID)
		updateJob(task.TaskID, "Completed", nil, &finishTime, "")
		updateTaskProgress(task.TaskID, "Completed")
		jobsTotal.WithLabelValues("completed").Inc()
	}
}

func replicateToReplica(task ReplicationTask, replica *kubernetes.Clientset) error {
	switch task.ResourceType {
	case "deployment":
		deployment, ok := task.Object.(*appsv1.Deployment)
		if !ok {
			return fmt.Errorf("object type assertion to Deployment failed")
		}
		return replicateDeployment(task, deployment, replica)
	case "statefulset":
		sts, ok := task.Object.(*appsv1.StatefulSet)
		if !ok {
			return fmt.Errorf("object type assertion to StatefulSet failed")
		}
		return replicateStatefulSet(task, sts, replica)
	case "secret":
		secret, ok := task.Object.(*corev1.Secret)
		if !ok {
			return fmt.Errorf("object type assertion to Secret failed")
		}
		return replicateSecret(task, secret, replica)
	case "configmap":
		cm, ok := task.Object.(*corev1.ConfigMap)
		if !ok {
			return fmt.Errorf("object type assertion to ConfigMap failed")
		}
		return replicateConfigMap(task, cm, replica)
	default:
		return fmt.Errorf("unsupported resource type: %s", task.ResourceType)
	}
}

func replicateDeployment(task ReplicationTask, dep *appsv1.Deployment, replica *kubernetes.Clientset) error {
	if task.Operation == "delete" {
		return replica.AppsV1().Deployments(task.Namespace).Delete(context.TODO(), task.Name, metav1.DeleteOptions{})
	}
	existing, err := replica.AppsV1().Deployments(task.Namespace).Get(context.TODO(), task.Name, metav1.GetOptions{})
	if err != nil {
		dep.ResourceVersion = ""
		_, err = replica.AppsV1().Deployments(task.Namespace).Create(context.TODO(), dep, metav1.CreateOptions{})
		return err
	}
	dep.ResourceVersion = existing.ResourceVersion
	_, err = replica.AppsV1().Deployments(task.Namespace).Update(context.TODO(), dep, metav1.UpdateOptions{})
	return err
}

func replicateStatefulSet(task ReplicationTask, sts *appsv1.StatefulSet, replica *kubernetes.Clientset) error {
	if task.Operation == "delete" {
		return replica.AppsV1().StatefulSets(task.Namespace).Delete(context.TODO(), task.Name, metav1.DeleteOptions{})
	}
	existing, err := replica.AppsV1().StatefulSets(task.Namespace).Get(context.TODO(), task.Name, metav1.GetOptions{})
	if err != nil {
		sts.ResourceVersion = ""
		_, err = replica.AppsV1().StatefulSets(task.Namespace).Create(context.TODO(), sts, metav1.CreateOptions{})
		return err
	}
	sts.ResourceVersion = existing.ResourceVersion
	_, err = replica.AppsV1().StatefulSets(task.Namespace).Update(context.TODO(), sts, metav1.UpdateOptions{})
	return err
}

func replicateSecret(task ReplicationTask, secret *corev1.Secret, replica *kubernetes.Clientset) error {
	if task.Operation == "delete" {
		return replica.CoreV1().Secrets(task.Namespace).Delete(context.TODO(), task.Name, metav1.DeleteOptions{})
	}
	existing, err := replica.CoreV1().Secrets(task.Namespace).Get(context.TODO(), task.Name, metav1.GetOptions{})
	if err != nil {
		secret.ResourceVersion = ""
		_, err = replica.CoreV1().Secrets(task.Namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
		return err
	}
	secret.ResourceVersion = existing.ResourceVersion
	_, err = replica.CoreV1().Secrets(task.Namespace).Update(context.TODO(), secret, metav1.UpdateOptions{})
	return err
}

func replicateConfigMap(task ReplicationTask, cm *corev1.ConfigMap, replica *kubernetes.Clientset) error {
	if task.Operation == "delete" {
		return replica.CoreV1().ConfigMaps(task.Namespace).Delete(context.TODO(), task.Name, metav1.DeleteOptions{})
	}
	existing, err := replica.CoreV1().ConfigMaps(task.Namespace).Get(context.TODO(), task.Name, metav1.GetOptions{})
	if err != nil {
		cm.ResourceVersion = ""
		_, err = replica.CoreV1().ConfigMaps(task.Namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
		return err
	}
	cm.ResourceVersion = existing.ResourceVersion
	_, err = replica.CoreV1().ConfigMaps(task.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
	return err
}

func generateTaskID() string {
	return uuid.New().String()
}

func updateTaskProgress(taskID, status string) {
	progressMap.Store(taskID, status)
}

//
// HTTP Handlers for Frontend
//

// progressHandler shows the replication tasks and their statuses.
func progressHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Replication Tasks Progress:\n")
	progressMap.Range(func(key, value interface{}) bool {
		fmt.Fprintf(w, "Task %s: %s\n", key, value)
		return true
	})
}

//
// Informer Setup Functions
//

func setupDeploymentInformer(factory informers.SharedInformerFactory) {
	informer := factory.Apps().V1().Deployments().Informer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dep := obj.(*appsv1.Deployment)
			enqueueReplicationTask("deployment", dep.Namespace, dep.Name, "add", dep)
		},
		UpdateFunc: func(old, new interface{}) {
			dep := new.(*appsv1.Deployment)
			enqueueReplicationTask("deployment", dep.Namespace, dep.Name, "update", dep)
		},
		DeleteFunc: func(obj interface{}) {
			var dep *appsv1.Deployment
			switch t := obj.(type) {
			case *appsv1.Deployment:
				dep = t
			case cache.DeletedFinalStateUnknown:
				dep = t.Obj.(*appsv1.Deployment)
			}
			if dep != nil {
				enqueueReplicationTask("deployment", dep.Namespace, dep.Name, "delete", dep)
			}
		},
	})
}

func setupStatefulSetInformer(factory informers.SharedInformerFactory) {
	informer := factory.Apps().V1().StatefulSets().Informer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			sts := obj.(*appsv1.StatefulSet)
			enqueueReplicationTask("statefulset", sts.Namespace, sts.Name, "add", sts)
		},
		UpdateFunc: func(old, new interface{}) {
			sts := new.(*appsv1.StatefulSet)
			enqueueReplicationTask("statefulset", sts.Namespace, sts.Name, "update", sts)
		},
		DeleteFunc: func(obj interface{}) {
			var sts *appsv1.StatefulSet
			switch t := obj.(type) {
			case *appsv1.StatefulSet:
				sts = t
			case cache.DeletedFinalStateUnknown:
				sts = t.Obj.(*appsv1.StatefulSet)
			}
			if sts != nil {
				enqueueReplicationTask("statefulset", sts.Namespace, sts.Name, "delete", sts)
			}
		},
	})
}

func setupSecretInformer(factory informers.SharedInformerFactory) {
	informer := factory.Core().V1().Secrets().Informer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			secret := obj.(*corev1.Secret)
			enqueueReplicationTask("secret", secret.Namespace, secret.Name, "add", secret)
		},
		UpdateFunc: func(old, new interface{}) {
			secret := new.(*corev1.Secret)
			enqueueReplicationTask("secret", secret.Namespace, secret.Name, "update", secret)
		},
		DeleteFunc: func(obj interface{}) {
			var secret *corev1.Secret
			switch t := obj.(type) {
			case *corev1.Secret:
				secret = t
			case cache.DeletedFinalStateUnknown:
				secret = t.Obj.(*corev1.Secret)
			}
			if secret != nil {
				enqueueReplicationTask("secret", secret.Namespace, secret.Name, "delete", secret)
			}
		},
	})
}

func setupConfigMapInformer(factory informers.SharedInformerFactory) {
	informer := factory.Core().V1().ConfigMaps().Informer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			cm := obj.(*corev1.ConfigMap)
			enqueueReplicationTask("configmap", cm.Namespace, cm.Name, "add", cm)
		},
		UpdateFunc: func(old, new interface{}) {
			cm := new.(*corev1.ConfigMap)
			enqueueReplicationTask("configmap", cm.Namespace, cm.Name, "update", cm)
		},
		DeleteFunc: func(obj interface{}) {
			var cm *corev1.ConfigMap
			switch t := obj.(type) {
			case *corev1.ConfigMap:
				cm = t
			case cache.DeletedFinalStateUnknown:
				cm = t.Obj.(*corev1.ConfigMap)
			}
			if cm != nil {
				enqueueReplicationTask("configmap", cm.Namespace, cm.Name, "delete", cm)
			}
		},
	})
}

// shouldSync returns true if the given namespace is allowed for syncing.
func shouldSync(namespace string) bool {
	if allowedNamespaces == nil {
		return true
	}
	return allowedNamespaces[namespace]
}

// Log panics.
// func init() {
// 	runtime.ErrorHandlers = []func(error){
// 		func(err error) {
// 			fmt.Printf("Runtime error: %v\n", err)
// 		},
// 	}
// }
