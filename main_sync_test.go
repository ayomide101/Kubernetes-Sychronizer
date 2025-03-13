// main_sync_test.go
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// helper: reset globals before each test.
func resetGlobals() {
	db = nil
	primaryClient = nil
	replicaClusters = nil
	workQueue = nil
	progressMap = sync.Map{}
	jobCache = sync.Map{}
	failedReplications = sync.Map{}
	allowedNamespaces = nil
}

// TestSyncDeployments verifies that syncDeployments correctly replicates deployments.
func TestSyncDeployments(t *testing.T) {
	resetGlobals()

	// Create a fake deployment in the primary cluster.
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-deployment",
			Namespace:       "default",
			ResourceVersion: "1",
		},
	}
	primaryClient = fake.NewSimpleClientset(dep)

	// Create a fake replica client (initially empty).
	replica := fake.NewSimpleClientset()
	replicaClusters = []*kubernetes.Clientset{replica}

	// Call the one-time sync for deployments.
	if err := syncDeployments(); err != nil {
		t.Fatalf("syncDeployments returned error: %v", err)
	}

	// Verify that the replica now has the deployment.
	createdDep, err := replica.AppsV1().Deployments("default").Get(context.TODO(), "test-deployment", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Replica did not create deployment: %v", err)
	}
	if createdDep.Name != "test-deployment" {
		t.Errorf("Expected deployment name 'test-deployment', got %s", createdDep.Name)
	}
}

// TestSyncStatefulSets verifies that syncStatefulSets replicates statefulsets.
func TestSyncStatefulSets(t *testing.T) {
	resetGlobals()

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-sts",
			Namespace:       "default",
			ResourceVersion: "1",
		},
	}
	primaryClient = fake.NewSimpleClientset(sts)
	replica := fake.NewSimpleClientset()
	replicaClusters = []*kubernetes.Clientset{replica}

	if err := syncStatefulSets(); err != nil {
		t.Fatalf("syncStatefulSets returned error: %v", err)
	}

	createdSts, err := replica.AppsV1().StatefulSets("default").Get(context.TODO(), "test-sts", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Replica did not create statefulset: %v", err)
	}
	if createdSts.Name != "test-sts" {
		t.Errorf("Expected statefulset name 'test-sts', got %s", createdSts.Name)
	}
}

// TestSyncConfigMaps verifies that syncConfigMaps replicates configmaps.
func TestSyncConfigMaps(t *testing.T) {
	resetGlobals()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cm",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		Data: map[string]string{"key": "value"},
	}
	primaryClient = fake.NewSimpleClientset(cm)
	replica := fake.NewSimpleClientset()
	replicaClusters = []*kubernetes.Clientset{replica}

	if err := syncConfigMaps(); err != nil {
		t.Fatalf("syncConfigMaps returned error: %v", err)
	}

	createdCm, err := replica.CoreV1().ConfigMaps("default").Get(context.TODO(), "test-cm", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Replica did not create configmap: %v", err)
	}
	if createdCm.Name != "test-cm" {
		t.Errorf("Expected configmap name 'test-cm', got %s", createdCm.Name)
	}
	if createdCm.Data["key"] != "value" {
		t.Errorf("Expected configmap data key 'value', got %s", createdCm.Data["key"])
	}
}

// TestSyncSecrets verifies that syncSecrets replicates secrets.
func TestSyncSecrets(t *testing.T) {
	resetGlobals()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-secret",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		StringData: map[string]string{"password": "12345"},
	}
	primaryClient = fake.NewSimpleClientset(secret)
	replica := fake.NewSimpleClientset()
	replicaClusters = []*kubernetes.Clientset{replica}

	if err := syncSecrets(); err != nil {
		t.Fatalf("syncSecrets returned error: %v", err)
	}

	createdSecret, err := replica.CoreV1().Secrets("default").Get(context.TODO(), "test-secret", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Replica did not create secret: %v", err)
	}
	if createdSecret.Name != "test-secret" {
		t.Errorf("Expected secret name 'test-secret', got %s", createdSecret.Name)
	}
}

// TestOneTimeSync verifies that oneTimeSync replicates all resource types.
func TestOneTimeSync(t *testing.T) {
	resetGlobals()

	// Create one object per resource type in the primary client.
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "sync-dep",
			Namespace:       "default",
			ResourceVersion: "1",
		},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "sync-sts",
			Namespace:       "default",
			ResourceVersion: "1",
		},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "sync-cm",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		Data: map[string]string{"foo": "bar"},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "sync-secret",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		StringData: map[string]string{"key": "value"},
	}

	primaryClient = fake.NewSimpleClientset(dep, sts, cm, secret)
	replica := fake.NewSimpleClientset()
	replicaClusters = []*kubernetes.Clientset{replica}

	if err := oneTimeSync(); err != nil {
		t.Fatalf("oneTimeSync returned error: %v", err)
	}

	// Verify that each resource exists in the replica.
	createdDep, err := replica.AppsV1().Deployments("default").Get(context.TODO(), "sync-dep", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Replica did not create deployment: %v", err)
	}
	if createdDep.Name != "sync-dep" {
		t.Errorf("Expected deployment name 'sync-dep', got %s", createdDep.Name)
	}

	createdSts, err := replica.AppsV1().StatefulSets("default").Get(context.TODO(), "sync-sts", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Replica did not create statefulset: %v", err)
	}
	if createdSts.Name != "sync-sts" {
		t.Errorf("Expected statefulset name 'sync-sts', got %s", createdSts.Name)
	}

	createdCm, err := replica.CoreV1().ConfigMaps("default").Get(context.TODO(), "sync-cm", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Replica did not create configmap: %v", err)
	}
	if createdCm.Data["foo"] != "bar" {
		t.Errorf("Expected configmap data 'bar', got %s", createdCm.Data["foo"])
	}

	createdSecret, err := replica.CoreV1().Secrets("default").Get(context.TODO(), "sync-secret", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Replica did not create secret: %v", err)
	}
}

// TestOneTimeSync_DuplicatePrevention simulates running oneTimeSync twice.
// It verifies that the replica does not end up with duplicate objects (only one update occurs).
func TestOneTimeSync_DuplicatePrevention(t *testing.T) {
	resetGlobals()

	// Create a resource in primary.
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "dup-dep",
			Namespace:       "default",
			ResourceVersion: "1",
		},
	}
	primaryClient = fake.NewSimpleClientset(dep)
	replica := fake.NewSimpleClientset()
	replicaClusters = []*kubernetes.Clientset{replica}

	// Run oneTimeSync twice.
	if err := oneTimeSync(); err != nil {
		t.Fatalf("oneTimeSync first run error: %v", err)
	}
	// Simulate a change in primary that does not change resourceVersion (i.e. duplicate change).
	if err := oneTimeSync(); err != nil {
		t.Fatalf("oneTimeSync second run error: %v", err)
	}

	// List deployments in the replica.
	list, err := replica.AppsV1().Deployments("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Error listing deployments: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("Expected 1 deployment in replica, got %d", len(list.Items))
	}
}

// TestOneTimeSync_ErrorHandling tests that if a replication call fails, the error is reported.
// We simulate failure by injecting a reactor into the fake replica client.
func TestOneTimeSync_ErrorHandling(t *testing.T) {
	resetGlobals()

	// Create a fake deployment in primary.
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "fail-dep",
			Namespace:       "default",
			ResourceVersion: "1",
		},
	}
	primaryClient = fake.NewSimpleClientset(dep)

	// Create a fake replica client and inject a reactor to simulate error on Create.
	replica := fake.NewSimpleClientset()
	replica.Fake.PrependReactor("create", "deployments", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, fmt.Errorf("simulated creation failure")
	})
	replicaClusters = []*kubernetes.Clientset{replica}

	// Capture stdout to check error messages.
	var buf bytes.Buffer
	stdout := os.Stdout
	os.Stdout = &buf
	defer func() { os.Stdout = stdout }()

	err := syncDeployments()
	if err != nil {
		t.Fatalf("syncDeployments returned error: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "Failed") || !strings.Contains(output, "simulated creation failure") {
		t.Errorf("Expected error output with 'simulated creation failure', got: %s", output)
	}
}
