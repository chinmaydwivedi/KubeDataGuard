package controller

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

func TestBuildCheckerJobPassesQueryInvariantConfigurationToWorker(t *testing.T) {
	invariant := invariantObject("paid-orders-query-check", "query", nil)
	spec := invariant.Object["spec"].(map[string]any)
	spec["sourceQuery"] = "select id, status from orders where status = 'paid'"
	spec["targetQuery"] = `{"query":{"term":{"status":"paid"}}}`
	spec["keyField"] = "id"
	spec["sourceScanPageSize"] = int64(250)
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

	unsupported := repairPolicyObject("paid-orders-indexed", false, "replay-kafka")
	allowed, err = repairPolicyAllowsAutoReindex(unsupported)
	if err != nil {
		t.Fatalf("repairPolicyAllowsAutoReindex returned error: %v", err)
	}
	if allowed {
		t.Fatal("repairPolicyAllowsAutoReindex = true for unsupported action, want false")
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

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
