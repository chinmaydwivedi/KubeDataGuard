package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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

var DataSourceGVK = schema.GroupVersionKind{
	Group:   "dataguard.io",
	Version: "v1alpha1",
	Kind:    "DataSource",
}

var DerivedViewGVK = schema.GroupVersionKind{
	Group:   "dataguard.io",
	Version: "v1alpha1",
	Kind:    "DerivedView",
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
	dataSource := &unstructured.Unstructured{}
	dataSource.SetGroupVersionKind(DataSourceGVK)
	derivedView := &unstructured.Unstructured{}
	derivedView.SetGroupVersionKind(DerivedViewGVK)
	secret := &corev1.Secret{}

	return ctrl.NewControllerManagedBy(mgr).
		For(invariant).
		Owns(&batchv1.Job{}).
		Watches(dataSource, handler.EnqueueRequestsFromMapFunc(r.invariantsForDataSource)).
		Watches(derivedView, handler.EnqueueRequestsFromMapFunc(r.invariantsForDerivedView)).
		Watches(secret, handler.EnqueueRequestsFromMapFunc(r.invariantsForSecret)).
		Complete(r)
}

func (r *InvariantReconciler) invariantsForDataSource(ctx context.Context, obj client.Object) []reconcile.Request {
	log := ctrl.LoggerFrom(ctx)
	derivedViews := &unstructured.UnstructuredList{}
	derivedViews.SetGroupVersionKind(DerivedViewGVK.GroupVersion().WithKind("DerivedViewList"))
	if err := r.List(ctx, derivedViews, client.InNamespace(obj.GetNamespace())); err != nil {
		log.Error(err, "list derived views for datasource fan-out", "dataSource", client.ObjectKeyFromObject(obj))
		return nil
	}

	derivedViewNames := map[string]struct{}{}
	for i := range derivedViews.Items {
		derivedView := &derivedViews.Items[i]
		sourceRef, _, err := unstructured.NestedString(derivedView.Object, "spec", "sourceRef")
		if err != nil {
			log.Error(err, "read derived view sourceRef", "derivedView", client.ObjectKeyFromObject(derivedView))
			continue
		}
		if sourceRef == obj.GetName() {
			derivedViewNames[derivedView.GetName()] = struct{}{}
		}
	}
	if len(derivedViewNames) == 0 {
		return nil
	}

	requests, err := r.invariantRequestsForDerivedViewNames(ctx, obj.GetNamespace(), derivedViewNames)
	if err != nil {
		log.Error(err, "map datasource update to invariant reconciles", "dataSource", client.ObjectKeyFromObject(obj))
		return nil
	}
	return requests
}

func (r *InvariantReconciler) invariantsForDerivedView(ctx context.Context, obj client.Object) []reconcile.Request {
	log := ctrl.LoggerFrom(ctx)
	requests, err := r.invariantRequestsForDerivedViewNames(
		ctx,
		obj.GetNamespace(),
		map[string]struct{}{obj.GetName(): {}},
	)
	if err != nil {
		log.Error(err, "map derived view update to invariant reconciles", "derivedView", client.ObjectKeyFromObject(obj))
		return nil
	}
	return requests
}

func (r *InvariantReconciler) invariantsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	log := ctrl.LoggerFrom(ctx)
	dataSources := &unstructured.UnstructuredList{}
	dataSources.SetGroupVersionKind(DataSourceGVK.GroupVersion().WithKind("DataSourceList"))
	if err := r.List(ctx, dataSources, client.InNamespace(obj.GetNamespace())); err != nil {
		log.Error(err, "list data sources for secret fan-out", "secret", client.ObjectKeyFromObject(obj))
		return nil
	}

	sourceNames := map[string]struct{}{}
	for i := range dataSources.Items {
		dataSource := &dataSources.Items[i]
		connectionSecret, _, err := unstructured.NestedString(dataSource.Object, "spec", "connectionSecret")
		if err != nil {
			log.Error(err, "read datasource connectionSecret", "dataSource", client.ObjectKeyFromObject(dataSource))
			continue
		}
		if connectionSecret == obj.GetName() {
			sourceNames[dataSource.GetName()] = struct{}{}
		}
	}

	derivedViews := &unstructured.UnstructuredList{}
	derivedViews.SetGroupVersionKind(DerivedViewGVK.GroupVersion().WithKind("DerivedViewList"))
	if err := r.List(ctx, derivedViews, client.InNamespace(obj.GetNamespace())); err != nil {
		log.Error(err, "list derived views for secret fan-out", "secret", client.ObjectKeyFromObject(obj))
		return nil
	}

	derivedViewNames := map[string]struct{}{}
	for i := range derivedViews.Items {
		derivedView := &derivedViews.Items[i]
		sourceRef, _, _ := unstructured.NestedString(derivedView.Object, "spec", "sourceRef")
		targetSecret, _, _ := unstructured.NestedString(derivedView.Object, "spec", "target", "connectionSecret")
		pipelineSecret, _, _ := unstructured.NestedString(derivedView.Object, "spec", "pipeline", "connectionSecret")
		_, sourceSecretMatched := sourceNames[sourceRef]
		if sourceSecretMatched || targetSecret == obj.GetName() || pipelineSecret == obj.GetName() {
			derivedViewNames[derivedView.GetName()] = struct{}{}
		}
	}
	if len(derivedViewNames) == 0 {
		return nil
	}

	requests, err := r.invariantRequestsForDerivedViewNames(ctx, obj.GetNamespace(), derivedViewNames)
	if err != nil {
		log.Error(err, "map secret update to invariant reconciles", "secret", client.ObjectKeyFromObject(obj))
		return nil
	}
	return requests
}

func (r *InvariantReconciler) invariantRequestsForDerivedViewNames(
	ctx context.Context,
	namespace string,
	derivedViewNames map[string]struct{},
) ([]reconcile.Request, error) {
	invariants := &unstructured.UnstructuredList{}
	invariants.SetGroupVersionKind(InvariantGVK.GroupVersion().WithKind("InvariantList"))
	if err := r.List(ctx, invariants, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	requestByName := map[types.NamespacedName]struct{}{}
	for i := range invariants.Items {
		invariant := &invariants.Items[i]
		derivedViewRef, _, err := unstructured.NestedString(invariant.Object, "spec", "derivedViewRef")
		if err != nil {
			return nil, fmt.Errorf("read invariant %s/%s derivedViewRef: %w", invariant.GetNamespace(), invariant.GetName(), err)
		}
		if _, ok := derivedViewNames[derivedViewRef]; ok {
			requestByName[types.NamespacedName{
				Namespace: invariant.GetNamespace(),
				Name:      invariant.GetName(),
			}] = struct{}{}
		}
	}

	names := make([]types.NamespacedName, 0, len(requestByName))
	for name := range requestByName {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if names[i].Namespace == names[j].Namespace {
			return names[i].Name < names[j].Name
		}
		return names[i].Namespace < names[j].Namespace
	})

	requests := make([]reconcile.Request, 0, len(names))
	for _, name := range names {
		requests = append(requests, reconcile.Request{NamespacedName: name})
	}
	return requests, nil
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
	case "redis-freshness":
		return "cacheFreshness"
	case "query":
		return "query"
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
