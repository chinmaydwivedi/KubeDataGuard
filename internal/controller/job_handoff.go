package controller

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	AnnotationCheckerImage          = "dataguard.io/checker-image"
	AnnotationCheckerServiceAccount = "dataguard.io/checker-service-account"
	AnnotationRepairImage           = "dataguard.io/repair-image"
	AnnotationRepairServiceAccount  = "dataguard.io/repair-service-account"
	AnnotationPostgresDSN           = "dataguard.io/postgres-dsn"
	AnnotationKafkaBootstrapServers = "dataguard.io/kafka-bootstrap-servers"
	AnnotationOpenSearchURL         = "dataguard.io/opensearch-url"
	AnnotationOrdersIndex           = "dataguard.io/orders-index"
	AnnotationOrderEventsTopic      = "dataguard.io/order-events-topic"
	AnnotationAllowUnsafeReindex    = "dataguard.io/allow-unsafe-direct-reindex"

	CheckPartial = "partial"
	CheckFailed  = "failed"

	PhaseUnknown     = "Unknown"
	PhaseCheckFailed = "CheckFailed"

	DefaultCheckerImage          = "kubedataguard-dataguard:latest"
	DefaultCheckerServiceAccount = "dataguard-checker"
	DefaultPostgresDSN           = "postgresql://dataguard:dataguard@host.docker.internal:5432/dataguard"
	DefaultKafkaBootstrapServers = "host.docker.internal:19092"
	DefaultOpenSearchURL         = "http://host.docker.internal:9200"
	DefaultOrdersIndex           = "orders"
	DefaultReportDir             = "/tmp/dataguard-reports"
)

type CheckRun struct {
	ID              string
	IntervalSeconds int64
	NextRequeue     time.Duration
	Scheduled       bool
}

func (r *InvariantReconciler) reconcileJobBackedInvariant(
	ctx context.Context,
	req ctrl.Request,
	invariant *unstructured.Unstructured,
) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	run, err := currentCheckRun(invariant, r.clock().Now())
	if err != nil {
		return ctrl.Result{}, err
	}
	configMapName := reportConfigMapName(invariant, run.ID)
	repairMapName := repairConfigMapName(invariant, run.ID)

	repairStatus, repairFound, err := r.statusFromReportConfigMap(ctx, invariant, repairMapName, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if repairFound {
		if reportStatusAlreadyApplied(invariant, repairStatus) {
			return requeueForRun(run), nil
		}
		if err := r.patchInvariantStatus(ctx, invariant, repairStatus); err != nil {
			return ctrl.Result{}, err
		}
		log.Info(
			"updated invariant status from repair job report",
			"invariant", req.NamespacedName.String(),
			"phase", repairStatus["phase"],
			"driftCount", repairStatus["driftCount"],
			"reportRef", repairStatus["reportRef"],
		)
		return requeueForRun(run), nil
	}

	status, found, err := r.statusFromReportConfigMap(ctx, invariant, configMapName, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if found {
		if phase, _ := stringField(status, "phase"); phase == PhaseDriftDetected {
			result, handled, err := r.reconcileRepairForDrift(ctx, req, invariant, status, configMapName, run)
			if err != nil || handled {
				return result, err
			}
		}
		if reportStatusAlreadyApplied(invariant, status) {
			return requeueForRun(run), nil
		}
		if err := r.patchInvariantStatus(ctx, invariant, status); err != nil {
			return ctrl.Result{}, err
		}
		log.Info(
			"updated invariant status from checker job report",
			"invariant", req.NamespacedName.String(),
			"phase", status["phase"],
			"driftCount", status["driftCount"],
			"reportRef", status["reportRef"],
		)
		return requeueForRun(run), nil
	}

	serviceAccountName := checkerServiceAccountName(invariant)
	if err := r.ensureCheckerRBAC(ctx, invariant.GetNamespace(), serviceAccountName); err != nil {
		return ctrl.Result{}, err
	}

	jobName := checkerJobName(invariant, run.ID)
	job := &batchv1.Job{}
	err = r.Get(ctx, types.NamespacedName{Namespace: invariant.GetNamespace(), Name: jobName}, job)
	if apierrors.IsNotFound(err) {
		job, err := BuildCheckerJob(invariant, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		runningStatus := BuildJobRunningStatus(invariant, r.clock().Now(), jobName, run)
		if shouldPatchCheckerRunningStatus(invariant) {
			if err := r.patchInvariantStatus(ctx, invariant, runningStatus); err != nil {
				return ctrl.Result{}, err
			}
		}
		log.Info(
			"created checker job for invariant",
			"invariant", req.NamespacedName.String(),
			"job", jobName,
			"reportConfigMap", configMapName,
		)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if jobFailed(job) {
		status := BuildJobFailedStatus(invariant, r.clock().Now(), jobName, run, "checker job failed before publishing a valid status report")
		if err := r.patchInvariantStatus(ctx, invariant, status); err != nil {
			return ctrl.Result{}, err
		}
		return requeueForRun(run), nil
	}

	if jobComplete(job) {
		status := BuildJobFailedStatus(invariant, r.clock().Now(), jobName, run, "checker job completed without publishing a valid status report")
		if err := r.patchInvariantStatus(ctx, invariant, status); err != nil {
			return ctrl.Result{}, err
		}
		return requeueForRun(run), nil
	}

	if shouldPatchCheckerRunningStatus(invariant) {
		if err := r.patchInvariantStatus(ctx, invariant, BuildJobRunningStatus(invariant, r.clock().Now(), jobName, run)); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *InvariantReconciler) statusFromReportConfigMap(
	ctx context.Context,
	invariant *unstructured.Unstructured,
	name string,
	run CheckRun,
) (map[string]any, bool, error) {
	configMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: invariant.GetNamespace(), Name: name}, configMap)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	raw := configMap.Data["status.json"]
	if raw == "" {
		return nil, false, fmt.Errorf("report ConfigMap %s/%s is missing status.json", configMap.Namespace, configMap.Name)
	}
	status, err := decodeStatusPayload(raw)
	if err != nil {
		return nil, false, fmt.Errorf("decode status.json from %s/%s: %w", configMap.Namespace, configMap.Name, err)
	}
	observedGeneration, ok := int64Field(status, "observedGeneration")
	if !ok || observedGeneration != invariant.GetGeneration() {
		return nil, false, nil
	}
	if run.ID != "" {
		observedCheckID, ok := stringField(status, "checkID")
		if !ok || observedCheckID != run.ID {
			return nil, false, nil
		}
	}
	enrichScheduledStatus(status, r.clock().Now(), run)
	return status, true, nil
}

func (r *InvariantReconciler) reconcileRepairForDrift(
	ctx context.Context,
	req ctrl.Request,
	invariant *unstructured.Unstructured,
	driftStatus map[string]any,
	checkReportConfigMapName string,
	run CheckRun,
) (ctrl.Result, bool, error) {
	log := ctrl.LoggerFrom(ctx)
	policy, found, err := r.findAutoRepairPolicy(ctx, invariant)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if !found {
		return ctrl.Result{}, false, nil
	}

	serviceAccountName := repairServiceAccountName(invariant)
	if err := r.ensureCheckerRBAC(ctx, invariant.GetNamespace(), serviceAccountName); err != nil {
		return ctrl.Result{}, true, err
	}

	jobName := repairJobName(invariant, run.ID)
	job := &batchv1.Job{}
	err = r.Get(ctx, types.NamespacedName{Namespace: invariant.GetNamespace(), Name: jobName}, job)
	driftCount, _ := int64Field(driftStatus, "driftCount")
	if apierrors.IsNotFound(err) {
		job, err := BuildRepairJob(invariant, checkReportConfigMapName, run)
		if err != nil {
			return ctrl.Result{}, true, err
		}
		if err := r.Create(ctx, job); err != nil {
			return ctrl.Result{}, true, err
		}
		status := BuildRepairRunningStatus(invariant, r.clock().Now(), jobName, run, driftCount)
		if err := r.patchInvariantStatus(ctx, invariant, status); err != nil {
			return ctrl.Result{}, true, err
		}
		log.Info(
			"created repair job for invariant",
			"invariant", req.NamespacedName.String(),
			"repairPolicy", policy.GetName(),
			"job", jobName,
			"sourceReportConfigMap", checkReportConfigMapName,
			"repairConfigMap", repairConfigMapName(invariant, run.ID),
		)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
	}
	if err != nil {
		return ctrl.Result{}, true, err
	}

	if jobFailed(job) {
		status := BuildRepairFailedStatus(invariant, r.clock().Now(), jobName, run, driftCount, "repair job failed before publishing a valid status report")
		if err := r.patchInvariantStatus(ctx, invariant, status); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{}, true, nil
	}

	if jobComplete(job) {
		status := BuildRepairFailedStatus(invariant, r.clock().Now(), jobName, run, driftCount, "repair job completed without publishing a valid status report")
		if err := r.patchInvariantStatus(ctx, invariant, status); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{}, true, nil
	}

	status := BuildRepairRunningStatus(invariant, r.clock().Now(), jobName, run, driftCount)
	if err := r.patchInvariantStatus(ctx, invariant, status); err != nil {
		return ctrl.Result{}, true, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
}

func (r *InvariantReconciler) findAutoRepairPolicy(
	ctx context.Context,
	invariant *unstructured.Unstructured,
) (*unstructured.Unstructured, bool, error) {
	policies := &unstructured.UnstructuredList{}
	policies.SetGroupVersionKind(RepairPolicyGVK.GroupVersion().WithKind("RepairPolicyList"))
	if err := r.List(ctx, policies, client.InNamespace(invariant.GetNamespace())); err != nil {
		return nil, false, err
	}

	for index := range policies.Items {
		policy := &policies.Items[index]
		invariantRef, _, err := unstructured.NestedString(policy.Object, "spec", "invariantRef")
		if err != nil {
			return nil, false, fmt.Errorf("read RepairPolicy %s spec.invariantRef: %w", policy.GetName(), err)
		}
		if invariantRef != invariant.GetName() {
			continue
		}
		allowed, err := repairPolicyAllowsAutoReindex(policy)
		if err != nil {
			return nil, false, err
		}
		if allowed {
			return policy, true, nil
		}
	}
	return nil, false, nil
}

func repairPolicyAllowsAutoReindex(policy *unstructured.Unstructured) (bool, error) {
	approvalRequired, found, err := unstructured.NestedBool(policy.Object, "spec", "approvalRequired")
	if err != nil {
		return false, fmt.Errorf("read RepairPolicy %s spec.approvalRequired: %w", policy.GetName(), err)
	}
	if found && approvalRequired {
		return false, nil
	}
	if policy.GetAnnotations()[AnnotationAllowUnsafeReindex] != "true" {
		return false, nil
	}

	actions, found, err := unstructured.NestedSlice(policy.Object, "spec", "actions")
	if err != nil {
		return false, fmt.Errorf("read RepairPolicy %s spec.actions: %w", policy.GetName(), err)
	}
	if !found {
		return false, nil
	}
	for _, raw := range actions {
		action, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		actionType, _ := action["type"].(string)
		if actionType == "reindex-records" {
			return true, nil
		}
	}
	return false, nil
}

func BuildCheckerJob(invariant *unstructured.Unstructured, run CheckRun) (*batchv1.Job, error) {
	maxLagSeconds, err := maxLagSeconds(invariant)
	if err != nil {
		return nil, err
	}

	jobName := checkerJobName(invariant, run.ID)
	configMapName := reportConfigMapName(invariant, run.ID)
	serviceAccountName := checkerServiceAccountName(invariant)
	backoffLimit := int32(0)
	ttlSeconds := int32(600)
	args := []string{
		"check-job",
		"--invariant", checkerInvariantArg(invariant),
		"--max-lag-seconds", strconv.FormatInt(maxLagSeconds, 10),
		"--namespace", invariant.GetNamespace(),
		"--config-map", configMapName,
		"--invariant-name", invariant.GetName(),
		"--observed-generation", strconv.FormatInt(invariant.GetGeneration(), 10),
		"--check-id", run.ID,
	}
	args = appendQueryArgs(args, invariant)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            jobName,
			Namespace:       invariant.GetNamespace(),
			Labels:          checkerLabels(invariant),
			OwnerReferences: invariantOwnerReferences(invariant),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: checkerLabels(invariant),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccountName,
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "checker",
							Image:           annotationOrDefault(invariant, AnnotationCheckerImage, DefaultCheckerImage),
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            args,
							Env:             checkerEnv(invariant),
						},
					},
				},
			},
		},
	}, nil
}

func BuildRepairJob(invariant *unstructured.Unstructured, sourceReportConfigMapName string, run CheckRun) (*batchv1.Job, error) {
	maxLagSeconds, err := maxLagSeconds(invariant)
	if err != nil {
		return nil, err
	}

	jobName := repairJobName(invariant, run.ID)
	configMapName := repairConfigMapName(invariant, run.ID)
	serviceAccountName := repairServiceAccountName(invariant)
	backoffLimit := int32(0)
	ttlSeconds := int32(600)
	args := []string{
		"repair-job",
		"--namespace", invariant.GetNamespace(),
		"--report-config-map", sourceReportConfigMapName,
		"--config-map", configMapName,
		"--invariant-name", invariant.GetName(),
		"--observed-generation", strconv.FormatInt(invariant.GetGeneration(), 10),
		"--check-id", run.ID,
		"--repair-mode", "direct-reindex",
		"--max-lag-seconds", strconv.FormatInt(maxLagSeconds, 10),
		"--verify-invariant", checkerInvariantArg(invariant),
	}
	args = appendQueryArgs(args, invariant)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            jobName,
			Namespace:       invariant.GetNamespace(),
			Labels:          repairLabels(invariant),
			OwnerReferences: invariantOwnerReferences(invariant),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: repairLabels(invariant),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccountName,
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "repair",
							Image:           repairImage(invariant),
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            args,
							Env:             checkerEnv(invariant),
						},
					},
				},
			},
		},
	}, nil
}

func BuildJobRunningStatus(invariant *unstructured.Unstructured, now time.Time, jobName string, run CheckRun) map[string]any {
	return jobLifecycleStatus(invariant, now, run, map[string]any{
		"healthy":             false,
		"phase":               PhaseUnknown,
		"checkStatus":         CheckPartial,
		"driftCount":          int64(0),
		"counterexampleCount": int64(0),
		"reportRef":           fmt.Sprintf("job://%s/%s", invariant.GetNamespace(), jobName),
	})
}

func BuildJobFailedStatus(invariant *unstructured.Unstructured, now time.Time, jobName string, run CheckRun, reason string) map[string]any {
	return jobLifecycleStatus(invariant, now, run, map[string]any{
		"healthy":             false,
		"phase":               PhaseCheckFailed,
		"checkStatus":         CheckFailed,
		"driftCount":          int64(0),
		"counterexampleCount": int64(1),
		"reportRef":           fmt.Sprintf("job://%s/%s", invariant.GetNamespace(), jobName),
		"reason":              reason,
	})
}

func BuildRepairRunningStatus(invariant *unstructured.Unstructured, now time.Time, jobName string, run CheckRun, driftCount int64) map[string]any {
	return jobLifecycleStatus(invariant, now, run, map[string]any{
		"healthy":             false,
		"phase":               PhaseRepairing,
		"checkStatus":         CheckPartial,
		"driftCount":          driftCount,
		"counterexampleCount": driftCount,
		"reportRef":           fmt.Sprintf("job://%s/%s", invariant.GetNamespace(), jobName),
	})
}

func BuildRepairFailedStatus(invariant *unstructured.Unstructured, now time.Time, jobName string, run CheckRun, driftCount int64, reason string) map[string]any {
	return jobLifecycleStatus(invariant, now, run, map[string]any{
		"healthy":             false,
		"phase":               PhaseRepairFailed,
		"checkStatus":         CheckFailed,
		"driftCount":          driftCount,
		"counterexampleCount": driftCount,
		"reportRef":           fmt.Sprintf("job://%s/%s", invariant.GetNamespace(), jobName),
		"reason":              reason,
	})
}

func jobLifecycleStatus(invariant *unstructured.Unstructured, now time.Time, run CheckRun, base map[string]any) map[string]any {
	maxLag, err := maxLagSeconds(invariant)
	if err != nil {
		maxLag = 60
	}
	checkedAt := now.UTC()
	base["guarantee"] = guaranteeForInvariantType(invariantType(invariant))
	base["checkedRecords"] = int64(0)
	base["lastCheckedAt"] = checkedAt.Format(time.RFC3339)
	base["observationWindow"] = map[string]any{
		"checkedAt":             checkedAt.Format(time.RFC3339),
		"targetReadAt":          checkedAt.Format(time.RFC3339),
		"maxLagSeconds":         maxLag,
		"eligibleRecordsBefore": checkedAt.Add(-time.Duration(maxLag) * time.Second).Format(time.RFC3339),
		"streamTopic":           streamTopic(invariant),
		"completeness":          base["checkStatus"],
	}
	base["observedGeneration"] = invariant.GetGeneration()
	base["checkID"] = run.ID
	if run.Scheduled {
		base["checkIntervalSeconds"] = run.IntervalSeconds
		base["nextCheckAfter"] = checkedAt.Add(run.NextRequeue).Format(time.RFC3339)
	}
	return base
}

func (r *InvariantReconciler) ensureCheckerRBAC(ctx context.Context, namespace string, serviceAccountName string) error {
	serviceAccount := &corev1.ServiceAccount{}
	key := types.NamespacedName{Namespace: namespace, Name: serviceAccountName}
	if err := r.Get(ctx, key, serviceAccount); apierrors.IsNotFound(err) {
		serviceAccount = &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: serviceAccountName, Namespace: namespace},
		}
		if err := r.Create(ctx, serviceAccount); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	role := &rbacv1.Role{}
	if err := r.Get(ctx, key, role); apierrors.IsNotFound(err) {
		role = &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: serviceAccountName, Namespace: namespace},
			Rules: []rbacv1.PolicyRule{
				{
					APIGroups: []string{""},
					Resources: []string{"configmaps"},
					Verbs:     []string{"get", "create", "patch", "update"},
				},
			},
		}
		if err := r.Create(ctx, role); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	roleBinding := &rbacv1.RoleBinding{}
	if err := r.Get(ctx, key, roleBinding); apierrors.IsNotFound(err) {
		roleBinding = &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: serviceAccountName, Namespace: namespace},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      serviceAccountName,
					Namespace: namespace,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "Role",
				Name:     serviceAccountName,
			},
		}
		if err := r.Create(ctx, roleBinding); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return nil
}

func decodeStatusPayload(raw string) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()

	var status map[string]any
	if err := decoder.Decode(&status); err != nil {
		return nil, err
	}
	normalized, ok := normalizeJSONValue(status).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("status payload is not an object")
	}
	return normalized, nil
}

func normalizeJSONValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(typed))
		for key, value := range typed {
			if value == nil {
				continue
			}
			normalized[key] = normalizeJSONValue(value)
		}
		return normalized
	case []any:
		values := make([]any, 0, len(typed))
		for _, value := range typed {
			if value == nil {
				continue
			}
			values = append(values, normalizeJSONValue(value))
		}
		return values
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer
		}
		if float, err := typed.Float64(); err == nil {
			return float
		}
		return typed.String()
	default:
		return value
	}
}

func int64Field(values map[string]any, key string) (int64, bool) {
	switch value := values[key].(type) {
	case int64:
		return value, true
	case int:
		return int64(value), true
	case float64:
		return int64(value), true
	default:
		return 0, false
	}
}

func stringField(values map[string]any, key string) (string, bool) {
	value, ok := values[key].(string)
	return value, ok
}

func boolField(values map[string]any, key string) (bool, bool) {
	value, ok := values[key].(bool)
	return value, ok
}

func reportStatusAlreadyApplied(invariant *unstructured.Unstructured, desired map[string]any) bool {
	current, found, err := unstructured.NestedMap(invariant.Object, "status")
	if err != nil || !found {
		return false
	}

	for _, key := range []string{
		"driftCount",
		"counterexampleCount",
		"observedGeneration",
		"checkIntervalSeconds",
	} {
		currentValue, currentOK := int64Field(current, key)
		desiredValue, desiredOK := int64Field(desired, key)
		if currentOK != desiredOK || currentValue != desiredValue {
			return false
		}
	}

	for _, key := range []string{"phase", "checkStatus", "guarantee", "reason", "repairAction"} {
		currentValue, currentOK := stringField(current, key)
		desiredValue, desiredOK := stringField(desired, key)
		if currentOK != desiredOK || currentValue != desiredValue {
			return false
		}
	}

	currentHealthy, currentOK := boolField(current, "healthy")
	desiredHealthy, desiredOK := boolField(desired, "healthy")
	return currentOK == desiredOK && currentHealthy == desiredHealthy
}

func shouldPatchCheckerRunningStatus(invariant *unstructured.Unstructured) bool {
	current, found, err := unstructured.NestedMap(invariant.Object, "status")
	if err != nil || !found {
		return true
	}
	phase, ok := stringField(current, "phase")
	if !ok {
		return true
	}
	return phase != PhaseHealthy && phase != PhaseDriftDetected
}

func checkerEnv(invariant *unstructured.Unstructured) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "POSTGRES_DSN", Value: annotationOrDefault(invariant, AnnotationPostgresDSN, DefaultPostgresDSN)},
		{Name: "KAFKA_BOOTSTRAP_SERVERS", Value: annotationOrDefault(invariant, AnnotationKafkaBootstrapServers, DefaultKafkaBootstrapServers)},
		{Name: "ORDER_EVENTS_TOPIC", Value: annotationOrDefault(invariant, AnnotationOrderEventsTopic, streamTopic(invariant))},
		{Name: "OPENSEARCH_URL", Value: annotationOrDefault(invariant, AnnotationOpenSearchURL, DefaultOpenSearchURL)},
		{Name: "ORDERS_INDEX", Value: annotationOrDefault(invariant, AnnotationOrdersIndex, DefaultOrdersIndex)},
		{Name: "REPORT_DIR", Value: DefaultReportDir},
	}
}

func checkerInvariantArg(invariant *unstructured.Unstructured) string {
	switch invariantType(invariant) {
	case "query":
		return "query"
	case "aggregate":
		return "aggregate"
	case "freshness":
		return "freshness"
	default:
		return "existence"
	}
}

func appendQueryArgs(args []string, invariant *unstructured.Unstructured) []string {
	if checkerInvariantArg(invariant) != "query" {
		return args
	}
	args = append(args, "--key-field", keyField(invariant))
	if fields := compareFields(invariant); fields != "" {
		args = append(args, "--compare-fields", fields)
	}
	if query := sourceQuery(invariant); query != "" {
		args = append(args, "--source-query", query)
	}
	if query := targetQuery(invariant); query != "" {
		args = append(args, "--target-query", query)
	}
	return args
}

func checkerServiceAccountName(invariant *unstructured.Unstructured) string {
	return annotationOrDefault(invariant, AnnotationCheckerServiceAccount, DefaultCheckerServiceAccount)
}

func repairServiceAccountName(invariant *unstructured.Unstructured) string {
	return annotationOrDefault(invariant, AnnotationRepairServiceAccount, checkerServiceAccountName(invariant))
}

func repairImage(invariant *unstructured.Unstructured) string {
	return annotationOrDefault(
		invariant,
		AnnotationRepairImage,
		annotationOrDefault(invariant, AnnotationCheckerImage, DefaultCheckerImage),
	)
}

func annotationOrDefault(invariant *unstructured.Unstructured, key string, fallback string) string {
	value := invariant.GetAnnotations()[key]
	if value == "" {
		return fallback
	}
	return value
}

func maxLagSeconds(invariant *unstructured.Unstructured) (int64, error) {
	maxLagSeconds, found, err := unstructured.NestedInt64(invariant.Object, "spec", "maxLagSeconds")
	if err != nil {
		return 0, fmt.Errorf("read spec.maxLagSeconds: %w", err)
	}
	if !found {
		return 60, nil
	}
	return maxLagSeconds, nil
}

func invariantType(invariant *unstructured.Unstructured) string {
	invariantType, _, _ := unstructured.NestedString(invariant.Object, "spec", "type")
	return invariantType
}

func sourceQuery(invariant *unstructured.Unstructured) string {
	value, _, _ := unstructured.NestedString(invariant.Object, "spec", "sourceQuery")
	return value
}

func targetQuery(invariant *unstructured.Unstructured) string {
	value, _, _ := unstructured.NestedString(invariant.Object, "spec", "targetQuery")
	return value
}

func keyField(invariant *unstructured.Unstructured) string {
	value, found, _ := unstructured.NestedString(invariant.Object, "spec", "keyField")
	if !found || value == "" {
		return "id"
	}
	return value
}

func compareFields(invariant *unstructured.Unstructured) string {
	values, found, _ := unstructured.NestedStringSlice(invariant.Object, "spec", "compareFields")
	if !found || len(values) == 0 {
		return ""
	}
	return strings.Join(values, ",")
}

func currentCheckRun(invariant *unstructured.Unstructured, now time.Time) (CheckRun, error) {
	intervalSeconds, found, err := unstructured.NestedInt64(invariant.Object, "spec", "checkIntervalSeconds")
	if err != nil {
		return CheckRun{}, fmt.Errorf("read spec.checkIntervalSeconds: %w", err)
	}
	if !found || intervalSeconds <= 0 {
		return CheckRun{
			ID: fmt.Sprintf("g%d", invariant.GetGeneration()),
		}, nil
	}

	slot := now.UTC().Unix() / intervalSeconds
	nextUnix := (slot + 1) * intervalSeconds
	next := time.Duration(nextUnix-now.UTC().Unix()) * time.Second
	if next <= 0 {
		next = time.Duration(intervalSeconds) * time.Second
	}
	return CheckRun{
		ID:              fmt.Sprintf("g%d-t%d", invariant.GetGeneration(), slot),
		IntervalSeconds: intervalSeconds,
		NextRequeue:     next,
		Scheduled:       true,
	}, nil
}

func requeueForRun(run CheckRun) ctrl.Result {
	if !run.Scheduled {
		return ctrl.Result{}
	}
	return ctrl.Result{RequeueAfter: run.NextRequeue}
}

func enrichScheduledStatus(status map[string]any, now time.Time, run CheckRun) {
	if run.ID != "" {
		status["checkID"] = run.ID
	}
	if !run.Scheduled {
		return
	}
	status["checkIntervalSeconds"] = run.IntervalSeconds
	status["nextCheckAfter"] = now.UTC().Add(run.NextRequeue).Format(time.RFC3339)
}

func checkerLabels(invariant *unstructured.Unstructured) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": "kubedataguard",
		"dataguard.io/invariant": invariant.GetName(),
		"dataguard.io/component": "checker",
	}
}

func repairLabels(invariant *unstructured.Unstructured) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": "kubedataguard",
		"dataguard.io/invariant": invariant.GetName(),
		"dataguard.io/component": "repair",
	}
}

func invariantOwnerReferences(invariant *unstructured.Unstructured) []metav1.OwnerReference {
	controller := true
	return []metav1.OwnerReference{
		{
			APIVersion: "dataguard.io/v1alpha1",
			Kind:       "Invariant",
			Name:       invariant.GetName(),
			UID:        invariant.GetUID(),
			Controller: &controller,
		},
	}
}

func checkerJobName(invariant *unstructured.Unstructured, checkID string) string {
	return dnsLabel("dataguard-check", fmt.Sprintf("%s-%s", invariant.GetName(), checkID))
}

func reportConfigMapName(invariant *unstructured.Unstructured, checkID string) string {
	return dnsLabel("dataguard-report", fmt.Sprintf("%s-%s", invariant.GetName(), checkID))
}

func repairJobName(invariant *unstructured.Unstructured, checkID string) string {
	return dnsLabel("dataguard-repair", fmt.Sprintf("%s-%s", invariant.GetName(), checkID))
}

func repairConfigMapName(invariant *unstructured.Unstructured, checkID string) string {
	return dnsLabel("dataguard-repair-report", fmt.Sprintf("%s-%s", invariant.GetName(), checkID))
}

func dnsLabel(prefix string, value string) string {
	raw := strings.ToLower(prefix + "-" + value)
	var builder strings.Builder
	for _, char := range raw {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' {
			builder.WriteRune(char)
		} else {
			builder.WriteRune('-')
		}
	}
	clean := strings.Trim(builder.String(), "-")
	if clean == "" {
		clean = "dataguard"
	}
	if len(clean) <= 63 {
		return clean
	}
	sum := sha1.Sum([]byte(clean))
	suffix := hex.EncodeToString(sum[:])[:8]
	keep := 63 - len(suffix) - 1
	return strings.Trim(clean[:keep], "-") + "-" + suffix
}

func jobComplete(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailed(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
