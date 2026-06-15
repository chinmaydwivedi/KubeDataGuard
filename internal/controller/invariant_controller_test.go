package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestBuildSyntheticInvariantStatusDefaultsHealthy(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)
	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)

	status, err := BuildSyntheticInvariantStatus(invariant, now)
	if err != nil {
		t.Fatalf("BuildSyntheticInvariantStatus returned error: %v", err)
	}

	if status["healthy"] != true {
		t.Fatalf("healthy = %v, want true", status["healthy"])
	}
	if status["phase"] != PhaseHealthy {
		t.Fatalf("phase = %v, want %s", status["phase"], PhaseHealthy)
	}
	if status["guarantee"] != "existence+fieldEquality" {
		t.Fatalf("guarantee = %v", status["guarantee"])
	}
	if status["driftCount"] != int64(0) {
		t.Fatalf("driftCount = %v, want 0", status["driftCount"])
	}
	if status["observedGeneration"] != int64(3) {
		t.Fatalf("observedGeneration = %v, want 3", status["observedGeneration"])
	}
}

func TestBuildSyntheticInvariantStatusCanSimulateDrift(t *testing.T) {
	invariant := invariantObject(
		"paid-orders-aggregate",
		"aggregate",
		map[string]string{
			AnnotationSyntheticDriftCount: "4",
			AnnotationStreamTopic:         "orders.events",
		},
	)
	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)

	status, err := BuildSyntheticInvariantStatus(invariant, now)
	if err != nil {
		t.Fatalf("BuildSyntheticInvariantStatus returned error: %v", err)
	}

	if status["healthy"] != false {
		t.Fatalf("healthy = %v, want false", status["healthy"])
	}
	if status["phase"] != PhaseDriftDetected {
		t.Fatalf("phase = %v, want %s", status["phase"], PhaseDriftDetected)
	}
	if status["guarantee"] != "aggregate" {
		t.Fatalf("guarantee = %v", status["guarantee"])
	}
	if status["driftCount"] != int64(4) {
		t.Fatalf("driftCount = %v, want 4", status["driftCount"])
	}

	window := status["observationWindow"].(map[string]any)
	if window["streamTopic"] != "orders.events" {
		t.Fatalf("streamTopic = %v", window["streamTopic"])
	}
	if window["maxLagSeconds"] != int64(60) {
		t.Fatalf("maxLagSeconds = %v, want 60", window["maxLagSeconds"])
	}
}

func TestBuildSyntheticInvariantStatusRejectsInvalidDriftAnnotation(t *testing.T) {
	invariant := invariantObject(
		"paid-orders-indexed",
		"existence",
		map[string]string{AnnotationSyntheticDriftCount: "not-a-number"},
	)
	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)

	if _, err := BuildSyntheticInvariantStatus(invariant, now); err == nil {
		t.Fatal("BuildSyntheticInvariantStatus returned nil error for invalid drift annotation")
	}
}

func TestBuildCheckerJobUsesInvariantConfiguration(t *testing.T) {
	invariant := invariantObject(
		"paid-orders-indexed",
		"existence",
		map[string]string{
			AnnotationCheckerImage:          "example/dataguard:dev",
			AnnotationCheckerServiceAccount: "custom-checker",
			AnnotationPostgresDSN:           "postgresql://example",
			AnnotationKafkaBootstrapServers: "redpanda:9092",
			AnnotationOpenSearchURL:         "http://opensearch:9200",
			AnnotationOrdersIndex:           "orders-v2",
			AnnotationReportStore:           "s3",
			AnnotationReportBucket:          "reports-bucket",
			AnnotationReportPrefix:          "checks/prod",
			AnnotationReportS3EndpointURL:   "http://minio:9000",
			AnnotationAWSRegion:             "us-west-2",
		},
	)

	run := CheckRun{ID: "g3"}
	job, err := BuildCheckerJob(invariant, run)
	if err != nil {
		t.Fatalf("BuildCheckerJob returned error: %v", err)
	}

	if job.Namespace != "default" {
		t.Fatalf("job namespace = %q, want default", job.Namespace)
	}
	if job.Spec.Template.Spec.ServiceAccountName != "custom-checker" {
		t.Fatalf("service account = %q", job.Spec.Template.Spec.ServiceAccountName)
	}

	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "example/dataguard:dev" {
		t.Fatalf("image = %q", container.Image)
	}
	if !contains(container.Args, "check-job") {
		t.Fatalf("job args do not contain check-job: %#v", container.Args)
	}
	if !contains(container.Args, "--observed-generation") {
		t.Fatalf("job args do not contain observed generation: %#v", container.Args)
	}
	if !contains(container.Args, "--check-id") || !contains(container.Args, "g3") {
		t.Fatalf("job args do not contain check id: %#v", container.Args)
	}

	env := map[string]string{}
	for _, item := range container.Env {
		env[item.Name] = item.Value
	}
	if env["POSTGRES_DSN"] != "postgresql://example" {
		t.Fatalf("POSTGRES_DSN = %q", env["POSTGRES_DSN"])
	}
	if env["ORDERS_INDEX"] != "orders-v2" {
		t.Fatalf("ORDERS_INDEX = %q", env["ORDERS_INDEX"])
	}
	if env["REPORT_STORE"] != "s3" {
		t.Fatalf("REPORT_STORE = %q", env["REPORT_STORE"])
	}
	if env["REPORT_BUCKET"] != "reports-bucket" {
		t.Fatalf("REPORT_BUCKET = %q", env["REPORT_BUCKET"])
	}
	if env["REPORT_PREFIX"] != "checks/prod" {
		t.Fatalf("REPORT_PREFIX = %q", env["REPORT_PREFIX"])
	}
	if env["REPORT_S3_ENDPOINT_URL"] != "http://minio:9000" {
		t.Fatalf("REPORT_S3_ENDPOINT_URL = %q", env["REPORT_S3_ENDPOINT_URL"])
	}
	if env["AWS_REGION"] != "us-west-2" {
		t.Fatalf("AWS_REGION = %q", env["AWS_REGION"])
	}
}

func TestBuildCheckerJobSetsLifecycleLimits(t *testing.T) {
	invariant := invariantObject(
		"paid-orders-indexed",
		"existence",
		map[string]string{
			AnnotationJobActiveDeadline:   "45",
			AnnotationJobTTLAfterFinished: "120",
		},
	)

	job, err := BuildCheckerJob(invariant, CheckRun{ID: "g3"})
	if err != nil {
		t.Fatalf("BuildCheckerJob returned error: %v", err)
	}

	if *job.Spec.BackoffLimit != int32(0) {
		t.Fatalf("BackoffLimit = %d, want 0", *job.Spec.BackoffLimit)
	}
	if *job.Spec.ActiveDeadlineSeconds != int64(45) {
		t.Fatalf("ActiveDeadlineSeconds = %d, want 45", *job.Spec.ActiveDeadlineSeconds)
	}
	if *job.Spec.TTLSecondsAfterFinished != int32(120) {
		t.Fatalf("TTLSecondsAfterFinished = %d, want 120", *job.Spec.TTLSecondsAfterFinished)
	}
}

func TestBuildRepairJobUsesSpecLifecycleLimits(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)
	spec := invariant.Object["spec"].(map[string]any)
	spec["jobActiveDeadlineSeconds"] = int64(90)
	spec["jobTTLSecondsAfterFinished"] = int64(30)

	job, err := BuildRepairJob(invariant, "dataguard-report-paid-orders-indexed-g3", CheckRun{ID: "g3"})
	if err != nil {
		t.Fatalf("BuildRepairJob returned error: %v", err)
	}

	if *job.Spec.BackoffLimit != int32(0) {
		t.Fatalf("BackoffLimit = %d, want 0", *job.Spec.BackoffLimit)
	}
	if *job.Spec.ActiveDeadlineSeconds != int64(90) {
		t.Fatalf("ActiveDeadlineSeconds = %d, want 90", *job.Spec.ActiveDeadlineSeconds)
	}
	if *job.Spec.TTLSecondsAfterFinished != int32(30) {
		t.Fatalf("TTLSecondsAfterFinished = %d, want 30", *job.Spec.TTLSecondsAfterFinished)
	}
}

func TestResolveCheckerEnvUsesDataSourceAndDerivedViewSecretRefs(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)
	dataSource := dataSourceObject(
		"orders-postgres",
		"postgres",
		"orders-postgres-secret",
		map[string]any{"connectionSecretKey": "postgresDsn"},
	)
	derivedView := derivedViewObject(
		"orders-search-index",
		"orders-postgres",
		map[string]any{
			"target": map[string]any{
				"type":                "opensearch",
				"connectionSecret":    "orders-opensearch-secret",
				"connectionSecretKey": "endpoint",
				"index":               "orders-v2",
			},
			"pipeline": map[string]any{
				"type":                "kafka",
				"connectionSecret":    "orders-kafka-secret",
				"bootstrapServersKey": "brokers",
				"topic":               "orders.events.v2",
			},
		},
	)
	scheme := runtime.NewScheme()
	reconciler := &InvariantReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(invariant, dataSource, derivedView).
			Build(),
	}

	env, err := reconciler.resolveCheckerEnv(context.Background(), invariant)
	if err != nil {
		t.Fatalf("resolveCheckerEnv returned error: %v", err)
	}
	job, err := BuildCheckerJobWithEnv(invariant, CheckRun{ID: "g3"}, env)
	if err != nil {
		t.Fatalf("BuildCheckerJobWithEnv returned error: %v", err)
	}

	envMap := envByName(job.Spec.Template.Spec.Containers[0].Env)
	assertSecretEnv(t, envMap["POSTGRES_DSN"], "orders-postgres-secret", "postgresDsn")
	assertSecretEnv(t, envMap["OPENSEARCH_URL"], "orders-opensearch-secret", "endpoint")
	assertSecretEnv(t, envMap["KAFKA_BOOTSTRAP_SERVERS"], "orders-kafka-secret", "brokers")
	if envMap["ORDERS_INDEX"].Value != "orders-v2" {
		t.Fatalf("ORDERS_INDEX = %q, want orders-v2", envMap["ORDERS_INDEX"].Value)
	}
	if envMap["ORDER_EVENTS_TOPIC"].Value != "orders.events.v2" {
		t.Fatalf("ORDER_EVENTS_TOPIC = %q, want orders.events.v2", envMap["ORDER_EVENTS_TOPIC"].Value)
	}
}

func TestResolveCheckerEnvUsesRedisDerivedViewSecretRefs(t *testing.T) {
	invariant := invariantObject("paid-orders-redis-cache-freshness", "redis-freshness", nil)
	setInvariantDerivedViewRef(invariant, "orders-redis-cache")
	dataSource := dataSourceObject("orders-postgres", "postgres", "orders-postgres-secret", nil)
	derivedView := derivedViewObject(
		"orders-redis-cache",
		"orders-postgres",
		map[string]any{
			"target": map[string]any{
				"type":                "redis",
				"connectionSecret":    "orders-redis-secret",
				"connectionSecretKey": "url",
				"keyPrefix":           "order-cache",
			},
		},
	)
	reconciler := &InvariantReconciler{
		Client: fake.NewClientBuilder().
			WithObjects(invariant, dataSource, derivedView).
			Build(),
	}

	env, err := reconciler.resolveCheckerEnv(context.Background(), invariant)
	if err != nil {
		t.Fatalf("resolveCheckerEnv returned error: %v", err)
	}

	envMap := envByName(env)
	assertSecretEnv(t, envMap["REDIS_URL"], "orders-redis-secret", "url")
	if envMap["ORDER_CACHE_PREFIX"].Value != "order-cache" {
		t.Fatalf("ORDER_CACHE_PREFIX = %q, want order-cache", envMap["ORDER_CACHE_PREFIX"].Value)
	}
}

func TestResolveCheckerEnvUsesClickHouseDerivedViewSecretRefs(t *testing.T) {
	invariant := invariantObject("paid-orders-clickhouse-aggregate", "clickhouse-aggregate", nil)
	setInvariantDerivedViewRef(invariant, "orders-clickhouse-analytics")
	dataSource := dataSourceObject("orders-postgres", "postgres", "orders-postgres-secret", nil)
	derivedView := derivedViewObject(
		"orders-clickhouse-analytics",
		"orders-postgres",
		map[string]any{
			"target": map[string]any{
				"type":             "clickhouse",
				"connectionSecret": "orders-clickhouse-secret",
				"database":         "analytics",
				"table":            "paid_orders",
			},
		},
	)
	reconciler := &InvariantReconciler{
		Client: fake.NewClientBuilder().
			WithObjects(invariant, dataSource, derivedView).
			Build(),
	}

	env, err := reconciler.resolveCheckerEnv(context.Background(), invariant)
	if err != nil {
		t.Fatalf("resolveCheckerEnv returned error: %v", err)
	}

	envMap := envByName(env)
	assertSecretEnv(t, envMap["CLICKHOUSE_URL"], "orders-clickhouse-secret", "url")
	assertSecretEnv(t, envMap["CLICKHOUSE_USER"], "orders-clickhouse-secret", "user")
	assertSecretEnv(t, envMap["CLICKHOUSE_PASSWORD"], "orders-clickhouse-secret", "password")
	if envMap["CLICKHOUSE_DATABASE"].Value != "analytics" {
		t.Fatalf("CLICKHOUSE_DATABASE = %q, want analytics", envMap["CLICKHOUSE_DATABASE"].Value)
	}
	if envMap["CLICKHOUSE_TABLE"].Value != "paid_orders" {
		t.Fatalf("CLICKHOUSE_TABLE = %q, want paid_orders", envMap["CLICKHOUSE_TABLE"].Value)
	}
}

func TestInvariantsForDerivedViewEnqueuesOnlyReferencingInvariants(t *testing.T) {
	indexed := invariantObject("paid-orders-indexed", "existence", nil)
	freshness := invariantObject("paid-orders-freshness", "freshness", nil)
	warehouse := invariantObject("warehouse-indexed", "existence", nil)
	setInvariantDerivedViewRef(warehouse, "warehouse-search-index")
	derivedView := derivedViewObject("orders-search-index", "orders-postgres", nil)

	reconciler := &InvariantReconciler{
		Client: fake.NewClientBuilder().
			WithObjects(indexed, freshness, warehouse, derivedView).
			Build(),
	}

	requests := reconciler.invariantsForDerivedView(context.Background(), derivedView)

	assertRequests(t, requests, []types.NamespacedName{
		{Namespace: "default", Name: "paid-orders-freshness"},
		{Namespace: "default", Name: "paid-orders-indexed"},
	})
}

func TestInvariantsForDataSourceEnqueuesThroughDependentDerivedViews(t *testing.T) {
	dataSource := dataSourceObject("orders-postgres", "postgres", "orders-postgres-secret", nil)
	searchView := derivedViewObject("orders-search-index", "orders-postgres", nil)
	analyticsView := derivedViewObject("orders-analytics-rollup", "orders-postgres", nil)
	warehouseView := derivedViewObject("warehouse-search-index", "warehouse-postgres", nil)

	indexed := invariantObject("paid-orders-indexed", "existence", nil)
	aggregate := invariantObject("paid-orders-aggregate", "aggregate", nil)
	setInvariantDerivedViewRef(aggregate, "orders-analytics-rollup")
	warehouse := invariantObject("warehouse-indexed", "existence", nil)
	setInvariantDerivedViewRef(warehouse, "warehouse-search-index")

	reconciler := &InvariantReconciler{
		Client: fake.NewClientBuilder().
			WithObjects(dataSource, searchView, analyticsView, warehouseView, indexed, aggregate, warehouse).
			Build(),
	}

	requests := reconciler.invariantsForDataSource(context.Background(), dataSource)

	assertRequests(t, requests, []types.NamespacedName{
		{Namespace: "default", Name: "paid-orders-aggregate"},
		{Namespace: "default", Name: "paid-orders-indexed"},
	})
}

func TestInvariantsForSecretEnqueuesThroughDataSourceAndDerivedViewRefs(t *testing.T) {
	dataSource := dataSourceObject("orders-postgres", "postgres", "orders-postgres-secret", nil)
	searchView := derivedViewObject(
		"orders-search-index",
		"orders-postgres",
		map[string]any{
			"target": map[string]any{
				"type":             "opensearch",
				"connectionSecret": "orders-opensearch-secret",
			},
			"pipeline": map[string]any{
				"type":             "kafka",
				"connectionSecret": "orders-kafka-secret",
			},
		},
	)
	indexed := invariantObject("paid-orders-indexed", "existence", nil)
	freshness := invariantObject("paid-orders-freshness", "freshness", nil)
	warehouse := invariantObject("warehouse-indexed", "existence", nil)
	setInvariantDerivedViewRef(warehouse, "warehouse-search-index")

	reconciler := &InvariantReconciler{
		Client: fake.NewClientBuilder().
			WithObjects(dataSource, searchView, indexed, freshness, warehouse).
			Build(),
	}

	for _, secretName := range []string{
		"orders-postgres-secret",
		"orders-opensearch-secret",
		"orders-kafka-secret",
	} {
		secret := &corev1.Secret{}
		secret.Name = secretName
		secret.Namespace = "default"

		requests := reconciler.invariantsForSecret(context.Background(), secret)
		assertRequests(t, requests, []types.NamespacedName{
			{Namespace: "default", Name: "paid-orders-freshness"},
			{Namespace: "default", Name: "paid-orders-indexed"},
		})
	}
}

func TestBuildRepairJobUsesReportHandoffConfiguration(t *testing.T) {
	invariant := invariantObject(
		"paid-orders-indexed",
		"existence",
		map[string]string{
			AnnotationCheckerImage:         "example/dataguard:checker",
			AnnotationRepairImage:          "example/dataguard:repair",
			AnnotationRepairServiceAccount: "repair-runner",
		},
	)

	run := CheckRun{ID: "g3"}
	job, err := BuildRepairJob(invariant, "dataguard-report-paid-orders-indexed-g3", run)
	if err != nil {
		t.Fatalf("BuildRepairJob returned error: %v", err)
	}

	if job.Spec.Template.Spec.ServiceAccountName != "repair-runner" {
		t.Fatalf("service account = %q", job.Spec.Template.Spec.ServiceAccountName)
	}

	container := job.Spec.Template.Spec.Containers[0]
	if container.Name != "repair" {
		t.Fatalf("container name = %q, want repair", container.Name)
	}
	if container.Image != "example/dataguard:repair" {
		t.Fatalf("image = %q", container.Image)
	}
	for _, want := range []string{
		"repair-job",
		"--report-config-map",
		"dataguard-report-paid-orders-indexed-g3",
		"--config-map",
		"--verify-invariant",
		"existence",
		"--check-id",
		"g3",
	} {
		if !contains(container.Args, want) {
			t.Fatalf("repair job args do not contain %q: %#v", want, container.Args)
		}
	}
}

func TestBuildCheckerJobPassesFreshnessInvariantToWorker(t *testing.T) {
	invariant := invariantObject("paid-orders-freshness", "freshness", nil)

	job, err := BuildCheckerJob(invariant, CheckRun{ID: "g3"})
	if err != nil {
		t.Fatalf("BuildCheckerJob returned error: %v", err)
	}

	args := job.Spec.Template.Spec.Containers[0].Args
	if !contains(args, "--invariant") || !contains(args, "freshness") {
		t.Fatalf("checker job args do not select freshness: %#v", args)
	}
}

func TestBuildCheckerJobPassesChecksumInvariantConfigurationToWorker(t *testing.T) {
	invariant := invariantObject("paid-orders-checksum", "checksum", nil)
	spec := invariant.Object["spec"].(map[string]any)
	spec["checksumPrefixLength"] = int64(3)

	job, err := BuildCheckerJob(invariant, CheckRun{ID: "g3"})
	if err != nil {
		t.Fatalf("BuildCheckerJob returned error: %v", err)
	}

	args := job.Spec.Template.Spec.Containers[0].Args
	for _, want := range []string{
		"--invariant",
		"checksum",
		"--checksum-prefix-length",
		"3",
	} {
		if !contains(args, want) {
			t.Fatalf("checksum checker args do not contain %q: %#v", want, args)
		}
	}
}

func TestBuildCheckerJobPassesQueryInvariantConfigurationToWorker(t *testing.T) {
	invariant := invariantObject("paid-orders-query-check", "query", nil)
	spec := invariant.Object["spec"].(map[string]any)
	spec["sourceQuery"] = "select id, status from orders where status = 'paid'"
	spec["targetQuery"] = `{"query":{"term":{"status":"paid"}}}`
	spec["keyField"] = "id"
	spec["sourceScanPageSize"] = int64(250)
	spec["sourceCheckpointId"] = "paid-orders-query"
	spec["sourceMaxPages"] = int64(4)
	spec["compareFields"] = []any{"status", "version"}

	job, err := BuildCheckerJob(invariant, CheckRun{ID: "g3"})
	if err != nil {
		t.Fatalf("BuildCheckerJob returned error: %v", err)
	}

	args := job.Spec.Template.Spec.Containers[0].Args
	for _, want := range []string{
		"--invariant",
		"query",
		"--source-query",
		"select id, status from orders where status = 'paid'",
		"--target-query",
		`{"query":{"term":{"status":"paid"}}}`,
		"--key-field",
		"id",
		"--source-scan-page-size",
		"250",
		"--source-checkpoint-id",
		"paid-orders-query",
		"--source-max-pages",
		"4",
		"--compare-fields",
		"status,version",
	} {
		if !contains(args, want) {
			t.Fatalf("query checker args do not contain %q: %#v", want, args)
		}
	}
}

func TestRepairPolicyAllowsAutoReindexRequiresExplicitUnsafeOptIn(t *testing.T) {
	policy := repairPolicyObject("paid-orders-indexed", false, "reindex-records")

	allowed, err := repairPolicyAllowsAutoReindex(policy)
	if err != nil {
		t.Fatalf("repairPolicyAllowsAutoReindex returned error: %v", err)
	}
	if allowed {
		t.Fatal("repairPolicyAllowsAutoReindex = true without unsafe annotation, want false")
	}

	annotated := repairPolicyObject("paid-orders-indexed", false, "reindex-records")
	annotated.SetAnnotations(map[string]string{AnnotationAllowUnsafeReindex: "true"})
	allowed, err = repairPolicyAllowsAutoReindex(annotated)
	if err != nil {
		t.Fatalf("repairPolicyAllowsAutoReindex returned error: %v", err)
	}
	if !allowed {
		t.Fatal("repairPolicyAllowsAutoReindex = false with unsafe annotation, want true")
	}

	approvalRequired := repairPolicyObject("paid-orders-indexed", true, "reindex-records")
	approvalRequired.SetAnnotations(map[string]string{AnnotationAllowUnsafeReindex: "true"})
	allowed, err = repairPolicyAllowsAutoReindex(approvalRequired)
	if err != nil {
		t.Fatalf("repairPolicyAllowsAutoReindex returned error: %v", err)
	}
	if allowed {
		t.Fatal("repairPolicyAllowsAutoReindex = true for approval-required policy, want false")
	}

	safeDispatch := repairPolicyObject("paid-orders-indexed", false, "replay-kafka")
	repairMode, allowedDispatch, err := repairPolicyAutoRepairMode(safeDispatch)
	if err != nil {
		t.Fatalf("repairPolicyAutoRepairMode returned error: %v", err)
	}
	if !allowedDispatch {
		t.Fatal("repairPolicyAutoRepairMode = false for safe replay-kafka action, want true")
	}
	if repairMode != "replay-kafka" {
		t.Fatalf("repair mode = %q, want replay-kafka", repairMode)
	}

	allowed, err = repairPolicyAllowsAutoReindex(safeDispatch)
	if err != nil {
		t.Fatalf("repairPolicyAllowsAutoReindex returned error: %v", err)
	}
	if allowed {
		t.Fatal("repairPolicyAllowsAutoReindex = true for safe dispatch action, want false")
	}
}

func TestRepairPolicyEnvProjectsActionConfiguration(t *testing.T) {
	policy := repairPolicyObject("paid-orders-indexed", false, "call-webhook")
	actions := policy.Object["spec"].(map[string]any)["actions"].([]any)
	action := actions[0].(map[string]any)
	action["endpoint"] = "http://orders.default.svc/reconcile"
	action["batchSize"] = int64(250)

	env := envByName(repairPolicyEnv(policy, "call-webhook"))
	if env["REPAIR_WEBHOOK_URL"].Value != "http://orders.default.svc/reconcile" {
		t.Fatalf("REPAIR_WEBHOOK_URL = %q", env["REPAIR_WEBHOOK_URL"].Value)
	}
	if env["REPAIR_WEBHOOK_BATCH_SIZE"].Value != "250" {
		t.Fatalf("REPAIR_WEBHOOK_BATCH_SIZE = %q", env["REPAIR_WEBHOOK_BATCH_SIZE"].Value)
	}

	kafkaPolicy := repairPolicyObject("paid-orders-indexed", false, "replay-kafka")
	kafkaAction := kafkaPolicy.Object["spec"].(map[string]any)["actions"].([]any)[0].(map[string]any)
	kafkaAction["topic"] = "orders.reconcile"
	env = envByName(repairPolicyEnv(kafkaPolicy, "replay-kafka"))
	if env["REPAIR_KAFKA_TOPIC"].Value != "orders.reconcile" {
		t.Fatalf("REPAIR_KAFKA_TOPIC = %q", env["REPAIR_KAFKA_TOPIC"].Value)
	}
}

func TestLifecycleFailureStatusesDistinguishCheckAndRepairFailure(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)
	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	run := CheckRun{ID: "g3"}

	checkFailed := BuildJobFailedStatus(invariant, now, "check-job", run, "source unavailable")
	if checkFailed["phase"] != PhaseCheckFailed {
		t.Fatalf("check failed phase = %v, want %s", checkFailed["phase"], PhaseCheckFailed)
	}
	if checkFailed["checkStatus"] != CheckFailed {
		t.Fatalf("check status = %v, want %s", checkFailed["checkStatus"], CheckFailed)
	}
	if checkFailed["reason"] != "source unavailable" {
		t.Fatalf("check reason = %v", checkFailed["reason"])
	}
	if checkFailed["checkID"] != "g3" {
		t.Fatalf("checkID = %v, want g3", checkFailed["checkID"])
	}

	repairing := BuildRepairRunningStatus(invariant, now, "repair-job", run, 8)
	if repairing["phase"] != PhaseRepairing {
		t.Fatalf("repairing phase = %v, want %s", repairing["phase"], PhaseRepairing)
	}
	if repairing["driftCount"] != int64(8) {
		t.Fatalf("repairing driftCount = %v, want 8", repairing["driftCount"])
	}

	repairFailed := BuildRepairFailedStatus(invariant, now, "repair-job", run, 8, "verification still found drift")
	if repairFailed["phase"] != PhaseRepairFailed {
		t.Fatalf("repair failed phase = %v, want %s", repairFailed["phase"], PhaseRepairFailed)
	}
	if repairFailed["checkStatus"] != CheckFailed {
		t.Fatalf("repair checkStatus = %v, want %s", repairFailed["checkStatus"], CheckFailed)
	}
	if repairFailed["driftCount"] != int64(8) {
		t.Fatalf("repair failed driftCount = %v, want 8", repairFailed["driftCount"])
	}
	if repairFailed["reason"] != "verification still found drift" {
		t.Fatalf("repair reason = %v", repairFailed["reason"])
	}
}

func TestLifecycleFailureStatusesClassifyDeadlineExceededAsTimeout(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)
	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	run := CheckRun{ID: "g3"}
	job := &batchv1.Job{
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{
					Type:               batchv1.JobFailed,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.NewTime(now),
					Reason:             "DeadlineExceeded",
					Message:            "Job was active longer than activeDeadlineSeconds",
				},
			},
		},
	}

	if !jobFailed(job) {
		t.Fatal("jobFailed = false, want true")
	}
	if !jobTimedOut(job) {
		t.Fatal("jobTimedOut = false, want true")
	}

	checkFailed := checkerFailureStatusForJob(
		invariant,
		now,
		"check-job",
		run,
		job,
		"checker job failed before publishing a valid status report",
	)
	if checkFailed["phase"] != PhaseCheckFailed {
		t.Fatalf("check phase = %v, want %s", checkFailed["phase"], PhaseCheckFailed)
	}
	if checkFailed["checkStatus"] != CheckTimedOut {
		t.Fatalf("checkStatus = %v, want %s", checkFailed["checkStatus"], CheckTimedOut)
	}
	if !strings.Contains(checkFailed["reason"].(string), "DeadlineExceeded") {
		t.Fatalf("reason = %v, want DeadlineExceeded detail", checkFailed["reason"])
	}

	repairFailed := repairFailureStatusForJob(
		invariant,
		now,
		"repair-job",
		run,
		4,
		job,
		"repair job failed before publishing a valid status report",
	)
	if repairFailed["phase"] != PhaseRepairFailed {
		t.Fatalf("repair phase = %v, want %s", repairFailed["phase"], PhaseRepairFailed)
	}
	if repairFailed["checkStatus"] != CheckTimedOut {
		t.Fatalf("repair checkStatus = %v, want %s", repairFailed["checkStatus"], CheckTimedOut)
	}
	if repairFailed["driftCount"] != int64(4) {
		t.Fatalf("repair driftCount = %v, want 4", repairFailed["driftCount"])
	}
}

func TestJobNamesAreStableForSameCheckID(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)

	first := checkerJobName(invariant, "g3-t42")
	second := checkerJobName(invariant, "g3-t42")
	repairFirst := repairJobName(invariant, "g3-t42")
	repairSecond := repairJobName(invariant, "g3-t42")

	if first != second {
		t.Fatalf("checker job name changed: %q != %q", first, second)
	}
	if repairFirst != repairSecond {
		t.Fatalf("repair job name changed: %q != %q", repairFirst, repairSecond)
	}
	if first == repairFirst {
		t.Fatalf("checker and repair jobs share a name: %q", first)
	}
}

func TestCurrentCheckRunUsesIntervalSlotWhenScheduled(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)
	spec := invariant.Object["spec"].(map[string]any)
	spec["checkIntervalSeconds"] = int64(60)
	now := time.Unix(125, 0).UTC()

	run, err := currentCheckRun(invariant, now)
	if err != nil {
		t.Fatalf("currentCheckRun returned error: %v", err)
	}

	if run.ID != "g3-t2" {
		t.Fatalf("check ID = %q, want g3-t2", run.ID)
	}
	if !run.Scheduled {
		t.Fatal("Scheduled = false, want true")
	}
	if run.NextRequeue != 55*time.Second {
		t.Fatalf("NextRequeue = %v, want 55s", run.NextRequeue)
	}
}

func TestDecodeStatusPayloadNormalizesNumbersAndOmitsNulls(t *testing.T) {
	status, err := decodeStatusPayload(`{
		"healthy": false,
		"phase": "DriftDetected",
		"driftCount": 3,
		"observedGeneration": 7,
		"observationWindow": {
			"streamOffsetStart": null,
			"streamOffsetEnd": 42
		}
	}`)
	if err != nil {
		t.Fatalf("decodeStatusPayload returned error: %v", err)
	}

	if status["driftCount"] != int64(3) {
		t.Fatalf("driftCount = %#v, want int64(3)", status["driftCount"])
	}
	if status["observedGeneration"] != int64(7) {
		t.Fatalf("observedGeneration = %#v, want int64(7)", status["observedGeneration"])
	}

	window := status["observationWindow"].(map[string]any)
	if _, ok := window["streamOffsetStart"]; ok {
		t.Fatalf("streamOffsetStart should have been omitted when null")
	}
	if window["streamOffsetEnd"] != int64(42) {
		t.Fatalf("streamOffsetEnd = %#v, want int64(42)", window["streamOffsetEnd"])
	}
}

func TestReportStatusAlreadyAppliedComparesControllerStatusIdentity(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)
	status := map[string]any{
		"healthy":             false,
		"phase":               PhaseDriftDetected,
		"checkStatus":         CheckComplete,
		"guarantee":           "existence+fieldEquality",
		"driftCount":          int64(8),
		"counterexampleCount": int64(8),
		"checkedRecords":      int64(41),
		"reportRef":           "configmap://default/dataguard-report-paid-orders-indexed-g3/report.json",
		"observedGeneration":  int64(3),
		"checkID":             "g3",
	}
	if err := unstructured.SetNestedMap(invariant.Object, status, "status"); err != nil {
		t.Fatalf("set status: %v", err)
	}

	if !reportStatusAlreadyApplied(invariant, status) {
		t.Fatal("reportStatusAlreadyApplied = false, want true")
	}

	telemetryOnly := map[string]any{}
	for key, value := range status {
		telemetryOnly[key] = value
	}
	telemetryOnly["checkedRecords"] = int64(42)
	telemetryOnly["reportRef"] = "file:///tmp/report-next.json"
	telemetryOnly["checkID"] = "g3-t99"
	if !reportStatusAlreadyApplied(invariant, telemetryOnly) {
		t.Fatal("reportStatusAlreadyApplied = false for telemetry-only changes, want true")
	}

	changed := map[string]any{}
	for key, value := range status {
		changed[key] = value
	}
	changed["driftCount"] = int64(9)
	if reportStatusAlreadyApplied(invariant, changed) {
		t.Fatal("reportStatusAlreadyApplied = true after driftCount changed, want false")
	}
}

func TestShouldPatchCheckerRunningStatusDoesNotChurnSettledHealth(t *testing.T) {
	invariant := invariantObject("paid-orders-indexed", "existence", nil)
	if !shouldPatchCheckerRunningStatus(invariant) {
		t.Fatal("shouldPatchCheckerRunningStatus = false without current status, want true")
	}

	if err := unstructured.SetNestedMap(invariant.Object, map[string]any{"phase": PhaseHealthy}, "status"); err != nil {
		t.Fatalf("set status: %v", err)
	}
	if shouldPatchCheckerRunningStatus(invariant) {
		t.Fatal("shouldPatchCheckerRunningStatus = true for Healthy, want false")
	}

	if err := unstructured.SetNestedMap(invariant.Object, map[string]any{"phase": PhaseCheckFailed}, "status"); err != nil {
		t.Fatalf("set status: %v", err)
	}
	if !shouldPatchCheckerRunningStatus(invariant) {
		t.Fatal("shouldPatchCheckerRunningStatus = false for CheckFailed, want true")
	}
}

func invariantObject(name string, invariantType string, annotations map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "dataguard.io/v1alpha1",
			"kind":       "Invariant",
			"metadata": map[string]any{
				"name":       name,
				"namespace":  "default",
				"generation": int64(3),
			},
			"spec": map[string]any{
				"derivedViewRef": "orders-search-index",
				"type":           invariantType,
				"maxLagSeconds":  int64(60),
			},
		},
	}
	obj.SetGroupVersionKind(InvariantGVK)
	obj.SetAnnotations(annotations)
	return obj
}

func dataSourceObject(name string, sourceType string, connectionSecret string, extraSpec map[string]any) *unstructured.Unstructured {
	spec := map[string]any{
		"type":             sourceType,
		"connectionSecret": connectionSecret,
	}
	for key, value := range extraSpec {
		spec[key] = value
	}
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "dataguard.io/v1alpha1",
			"kind":       "DataSource",
			"metadata": map[string]any{
				"name":      name,
				"namespace": "default",
			},
			"spec": spec,
		},
	}
	obj.SetGroupVersionKind(DataSourceGVK)
	return obj
}

func derivedViewObject(name string, sourceRef string, specOverrides map[string]any) *unstructured.Unstructured {
	spec := map[string]any{
		"sourceRef": sourceRef,
	}
	for key, value := range specOverrides {
		spec[key] = value
	}
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "dataguard.io/v1alpha1",
			"kind":       "DerivedView",
			"metadata": map[string]any{
				"name":      name,
				"namespace": "default",
			},
			"spec": spec,
		},
	}
	obj.SetGroupVersionKind(DerivedViewGVK)
	return obj
}

func repairPolicyObject(invariantRef string, approvalRequired bool, actionType string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "dataguard.io/v1alpha1",
			"kind":       "RepairPolicy",
			"metadata": map[string]any{
				"name":      "reindex-missing-paid-orders",
				"namespace": "default",
			},
			"spec": map[string]any{
				"invariantRef":     invariantRef,
				"approvalRequired": approvalRequired,
				"actions": []any{
					map[string]any{"type": actionType},
				},
			},
		},
	}
	obj.SetGroupVersionKind(RepairPolicyGVK)
	return obj
}

func setInvariantDerivedViewRef(obj *unstructured.Unstructured, derivedViewRef string) {
	spec := obj.Object["spec"].(map[string]any)
	spec["derivedViewRef"] = derivedViewRef
}

func assertRequests(t *testing.T, got []reconcile.Request, want []types.NamespacedName) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d requests (%#v), want %d (%#v)", len(got), got, len(want), want)
	}
	requests := map[types.NamespacedName]struct{}{}
	for _, request := range got {
		requests[request.NamespacedName] = struct{}{}
	}
	for _, name := range want {
		if _, ok := requests[name]; !ok {
			t.Fatalf("missing request for %s; got %#v", name.String(), got)
		}
	}
}

func envByName(env []corev1.EnvVar) map[string]corev1.EnvVar {
	values := map[string]corev1.EnvVar{}
	for _, item := range env {
		values[item.Name] = item
	}
	return values
}

func assertSecretEnv(t *testing.T, env corev1.EnvVar, secretName string, secretKey string) {
	t.Helper()
	if env.Value != "" {
		t.Fatalf("%s has literal value %q, want secret ref", env.Name, env.Value)
	}
	if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("%s is missing secret key ref", env.Name)
	}
	if env.ValueFrom.SecretKeyRef.Name != secretName {
		t.Fatalf("%s secret name = %q, want %q", env.Name, env.ValueFrom.SecretKeyRef.Name, secretName)
	}
	if env.ValueFrom.SecretKeyRef.Key != secretKey {
		t.Fatalf("%s secret key = %q, want %q", env.Name, env.ValueFrom.SecretKeyRef.Key, secretKey)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
