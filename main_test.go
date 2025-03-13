// main_test.go
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// TestGenerateTaskID verifies that generateTaskID produces non-empty, unique IDs.
func TestGenerateTaskID(t *testing.T) {
	id1 := generateTaskID()
	id2 := generateTaskID()
	if id1 == "" || id2 == "" {
		t.Error("TaskID should not be empty")
	}
	if id1 == id2 {
		t.Error("TaskIDs should be unique")
	}
}

// TestGetChangeKey tests the computed change key for both deletion and update events.
func TestGetChangeKey(t *testing.T) {
	// For a deletion, no resource version is needed.
	ck := getChangeKey("deployment", "default", "test-dep", "delete", nil)
	expected := "deployment:default:test-dep:delete"
	if ck != expected {
		t.Errorf("Expected %q, got %q", expected, ck)
	}
	// For an add/update, we use the ResourceVersion.
	dep := &appsv1.Deployment{}
	dep.ResourceVersion = "123"
	ck = getChangeKey("deployment", "default", "test-dep", "add", dep)
	expected = "deployment:default:test-dep:123"
	if ck != expected {
		t.Errorf("Expected %q, got %q", expected, ck)
	}
}

// TestAlreadyReplicated tests that a job marked as Completed in the DB is recognized.
func TestAlreadyReplicated(t *testing.T) {
	// Use an in-memory SQLite DB.
	var err error
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := initDB(":memory:"); err != nil {
		t.Fatal(err)
	}
	task := ReplicationTask{
		TaskID:    "job1",
		ChangeKey: "test-change-key",
		CreatedAt: time.Now(),
	}
	if err := insertJob(task); err != nil {
		t.Fatal(err)
	}
	// Mark the job as Completed.
	if err := updateJob("job1", "Completed", nil, ptrTime(time.Now()), ""); err != nil {
		t.Fatal(err)
	}
	exists, err := alreadyReplicated("test-change-key")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("Expected alreadyReplicated to return true")
	}
}

// TestEnsureAdminUser checks that if no admin exists, one is created.
func TestEnsureAdminUser(t *testing.T) {
	var err error
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := initDB(":memory:"); err != nil {
		t.Fatal(err)
	}
	ensureAdminUser()
	var username string
	err = db.QueryRow("SELECT username FROM users WHERE username = 'admin'").Scan(&username)
	if err != nil {
		t.Fatal("Admin user not created")
	}
	if username != "admin" {
		t.Error("Expected admin username to be 'admin'")
	}
}

// TestLoginHandler tests the /api/login endpoint with valid and invalid credentials.
func TestLoginHandler(t *testing.T) {
	var err error
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := initDB(":memory:"); err != nil {
		t.Fatal(err)
	}
	// Create a test user "admin" with password "password".
	hashed, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO users (username, password, created_at) VALUES (?, ?, ?)", "admin", string(hashed), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// Test valid login.
	loginReq := LoginRequest{Username: "admin", Password: "password"}
	body, _ := json.Marshal(loginReq)
	req := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	loginHandler(w, req)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
	var loginResp LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		t.Fatal(err)
	}
	if !loginResp.Success {
		t.Error("Expected login success, got failure")
	}

	// Test invalid login.
	loginReq = LoginRequest{Username: "admin", Password: "wrong"}
	body, _ = json.Marshal(loginReq)
	req = httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	loginHandler(w, req)
	resp = w.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected status 401 for wrong password, got %d", resp.StatusCode)
	}
}

// TestDashboardHandler verifies that the /api/dashboard endpoint returns aggregated job statistics.
func TestDashboardHandler(t *testing.T) {
	var err error
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := initDB(":memory:"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// Insert sample jobs.
	_, err = db.Exec("INSERT INTO jobs (id, change_key, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
		"job1", "ck1", "Completed", now, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO jobs (id, change_key, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
		"job2", "ck2", "In Progress", now, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO jobs (id, change_key, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
		"job3", "ck3", "Failed", now, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO jobs (id, change_key, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
		"job4", "ck4", "enqueued", now, now)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/dashboard", nil)
	w := httptest.NewRecorder()
	dashboardHandler(w, req)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatal(err)
	}
	if data["totalJobs"] == nil {
		t.Error("totalJobs missing in response")
	}
}

// TestStatusHandlerAPI uses fake Kubernetes clients to verify /api/status endpoint.
func TestStatusHandlerAPI(t *testing.T) {
	// Use fake client sets.
	primaryClient = fake.NewClientset()
	replicaClusters = []*kubernetes.Clientset{fake.NewClientset()}
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()
	statusHandlerAPI(w, req)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatal(err)
	}
	if data["primary"] == nil {
		t.Error("primary key missing")
	}
	if data["replicas"] == nil {
		t.Error("replicas key missing")
	}
}

// TestCreateUserHandler verifies that a new user can be created via /api/users.
func TestCreateUserHandler(t *testing.T) {
	var err error
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := initDB(":memory:"); err != nil {
		t.Fatal(err)
	}
	reqBody := `{"username": "testuser", "password": "testpass"}`
	req := httptest.NewRequest(http.MethodPost, "/api/users", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	createUserHandler(w, req)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatal(err)
	}
	success, ok := data["success"].(bool)
	if !ok || !success {
		t.Error("User creation not successful")
	}
}

// TestRetryHandler simulates a retry attempt for a failed replication.
func TestRetryHandler(t *testing.T) {
	// Insert a dummy job in jobCache.
	task := ReplicationTask{
		TaskID:       "retry-job",
		ResourceType: "deployment",
		Namespace:    "default",
		Name:         "test-dep",
		Operation:    "add",
		Object: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "test-dep",
				Namespace:       "default",
				ResourceVersion: "1",
			},
		},
		CreatedAt: time.Now(),
		ChangeKey: "deployment:default:test-dep:1",
	}
	jobCache.Store("retry-job", task)
	// Override replicateToReplica to always return an error.
	origFunc := replicateToReplica
	replicateToReplica = func(task ReplicationTask, replica *kubernetes.Clientset) error {
		return fmt.Errorf("simulated error")
	}
	defer func() { replicateToReplica = origFunc }()
	// Set replicaClusters with one fake client.
	replicaClusters = []*kubernetes.Clientset{fake.NewSimpleClientset()}
	req := httptest.NewRequest(http.MethodGet, "/retry?jobid=retry-job&replica=0", nil)
	w := httptest.NewRecorder()
	retryHandler(w, req)
	resp := w.Result()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Expected status 500 due to simulated error, got %d", resp.StatusCode)
	}
}
