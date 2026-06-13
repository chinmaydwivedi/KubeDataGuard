package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	AnnotationSyntheticDriftCount = "dataguard.io/synthetic-drift-count"
	AnnotationStreamTopic         = "dataguard.io/stream-topic"
	AnnotationCheckerMode         = "dataguard.io/checker-mode"
	CheckerModeJob                = "job"
	CheckComplete                 = "complete"
	PhaseHealthy                  = "Healthy"
	PhaseDriftDetected            = "DriftDetected"
	PhaseRepairing                = "Repairing"
	PhaseRepairFailed             = "RepairFailed"
)

var InvariantGVK = schema.GroupVersionKind{
	Group:   "dataguard.io",
	Version: "v1alpha1",
	Kind:    "Invariant",
}

var RepairPolicyGVK = schema.GroupVersionKind{
	Group:   "dataguard.io",
	Version: "v1alpha1",
	Kind:    "RepairPolicy",
}

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

type InvariantReconciler struct {
	client.Client
	Now Clock
}

func (r *InvariantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	invariant := &unstructured.Unstructured{}
	invariant.SetGroupVersionKind(InvariantGVK)
	if err := r.Get(ctx, req.NamespacedName, invariant); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if checkerMode(invariant) == CheckerModeJob {
		return r.reconcileJobBackedInvariant(ctx, req, invariant)
	}

	status, err := BuildSyntheticInvariantStatus(invariant, r.clock().Now())
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.patchInvariantStatus(ctx, invariant, status); err != nil {
		return ctrl.Result{}, err
	}

	log.Info(
		"updated synthetic invariant status",
		"invariant", req.NamespacedName.String(),
		"phase", status["phase"],
		"driftCount", status["driftCount"],
	)
	return ctrl.Result{}, nil
}

func (r *InvariantReconciler) patchInvariantStatus(
	ctx context.Context,
	invariant *unstructured.Unstructured,
	status map[string]any,
) error {
	patch := client.MergeFrom(invariant.DeepCopy())
	if err := unstructured.SetNestedMap(invariant.Object, status, "status"); err != nil {
		return fmt.Errorf("set status: %w", err)
	}
	if err := r.Status().Patch(ctx, invariant, patch); err != nil {
		return err
	}
	return nil
}

func (r *InvariantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	invariant := &unstructured.Unstructured{}
	invariant.SetGroupVersionKind(InvariantGVK)
	return ctrl.NewControllerManagedBy(mgr).
		For(invariant).
		Owns(&batchv1.Job{}).
		Complete(r)
}

func (r *InvariantReconciler) clock() Clock {
	if r.Now == nil {
		return RealClock{}
	}
	return r.Now
}

func BuildSyntheticInvariantStatus(invariant *unstructured.Unstructured, now time.Time) (map[string]any, error) {
	driftCount, err := syntheticDriftCount(invariant)
	if err != nil {
		return nil, err
	}

	maxLagSeconds, found, err := unstructured.NestedInt64(invariant.Object, "spec", "maxLagSeconds")
	if err != nil {
		return nil, fmt.Errorf("read spec.maxLagSeconds: %w", err)
	}
	if !found {
		maxLagSeconds = 60
	}

	invariantType, _, err := unstructured.NestedString(invariant.Object, "spec", "type")
	if err != nil {
		return nil, fmt.Errorf("read spec.type: %w", err)
	}
	guarantee := guaranteeForInvariantType(invariantType)
	phase := PhaseHealthy
	if driftCount > 0 {
		phase = PhaseDriftDetected
	}

	checkedAt := now.UTC()
	observationWindow := map[string]any{
		"checkedAt":             checkedAt.Format(time.RFC3339),
		"targetReadAt":          checkedAt.Format(time.RFC3339),
		"maxLagSeconds":         maxLagSeconds,
		"eligibleRecordsBefore": checkedAt.Add(-time.Duration(maxLagSeconds) * time.Second).Format(time.RFC3339),
		"sourceWatermark":       checkedAt.Format(time.RFC3339),
		"streamTopic":           streamTopic(invariant),
		"streamOffsetStart":     int64(1),
		"streamOffsetEnd":       int64(0),
		"completeness":          CheckComplete,
	}

	return map[string]any{
		"healthy":             driftCount == 0,
		"phase":               phase,
		"guarantee":           guarantee,
		"checkStatus":         CheckComplete,
		"driftCount":          driftCount,
		"counterexampleCount": driftCount,
		"checkedRecords":      int64(0),
		"lastCheckedAt":       checkedAt.Format(time.RFC3339),
		"observationWindow":   observationWindow,
		"reportRef":           fmt.Sprintf("synthetic://%s/%s", invariant.GetNamespace(), invariant.GetName()),
		"observedGeneration":  invariant.GetGeneration(),
	}, nil
}

func guaranteeForInvariantType(invariantType string) string {
	switch invariantType {
	case "existence":
		return "existence+fieldEquality"
	case "aggregate":
		return "aggregate"
	case "freshness":
		return "boundedFreshness"
	case "fieldEquality":
		return "fieldEquality"
	case "count":
		return "aggregate.count"
	case "checksum":
		return "aggregate.checksum"
	default:
		return "unknown"
	}
}

func syntheticDriftCount(invariant *unstructured.Unstructured) (int64, error) {
	raw := invariant.GetAnnotations()[AnnotationSyntheticDriftCount]
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", AnnotationSyntheticDriftCount, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must be non-negative", AnnotationSyntheticDriftCount)
	}
	return value, nil
}

func streamTopic(invariant *unstructured.Unstructured) string {
	if topic := invariant.GetAnnotations()[AnnotationStreamTopic]; topic != "" {
		return topic
	}
	return "orders.events"
}

func checkerMode(invariant *unstructured.Unstructured) string {
	return invariant.GetAnnotations()[AnnotationCheckerMode]
}
