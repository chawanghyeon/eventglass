package alerts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/google/uuid"
)

type CountResultReader interface {
	ReadAlertCount(context.Context, control.QueryStatus) (int64, error)
}

type Evaluator struct {
	Alerts            *control.AlertOperations
	Queries           *control.QueryOperations
	Objects           query.CatalogObjectReader
	Results           CountResultReader
	Owner             string
	PublicURL         string
	StorageGeneration int64
	Working           *resource.Budget
	runMu             sync.Mutex
	cursorTenant      int64
	cursorAlert       string
}

func (e *Evaluator) EvaluateOnce(ctx context.Context) error {
	if e == nil || e.Alerts == nil || e.Queries == nil || e.Objects == nil || e.Results == nil || e.Working == nil || e.Owner == "" || e.StorageGeneration <= 0 {
		return errors.New("alert evaluator dependencies are required")
	}
	e.runMu.Lock()
	defer e.runMu.Unlock()
	rules, err := e.Alerts.ListEnabledAlertPage(ctx, 1000, e.cursorTenant, e.cursorAlert)
	if err != nil {
		return err
	}
	var result error
	for _, rule := range rules {
		var evaluateErr error
		if rule.Kind == string(KindIssue) {
			evaluateErr = e.evaluateIssues(ctx, rule)
		} else {
			evaluateErr = e.evaluateThreshold(ctx, rule)
		}
		result = errors.Join(result, evaluateErr)
		if ctx.Err() != nil {
			return errors.Join(result, ctx.Err())
		}
	}
	if len(rules) == 1000 {
		e.cursorTenant, e.cursorAlert = rules[len(rules)-1].TenantID, rules[len(rules)-1].AlertID
	} else {
		e.cursorTenant, e.cursorAlert = 0, ""
	}
	return result
}

func (e *Evaluator) evaluateThreshold(ctx context.Context, rule control.AlertRule) error {
	for range 10 {
		reserved, err := e.Alerts.ReserveThresholdWindow(ctx, rule.TenantID, rule.AlertID, uuid.NewString())
		if err != nil {
			return err
		}
		if reserved == nil {
			break
		}
	}
	open, err := e.Alerts.ListOpenThresholdEvaluations(ctx, rule.TenantID, rule.AlertID, 10)
	if err != nil {
		return err
	}
	for _, evaluation := range open {
		if evaluation.State == "waiting" {
			promoted, promoteErr := e.Alerts.PromoteThresholdEvaluation(ctx, rule.TenantID, evaluation.EvaluationID)
			if promoteErr != nil {
				return promoteErr
			}
			evaluation = *promoted
			if evaluation.State != "queued" {
				break
			}
		}
		if evaluation.State != "queued" && evaluation.State != "running" {
			continue
		}
		validated, err := ValidateRule(KindThreshold, rule.RuleBytes)
		if err != nil {
			return err
		}
		if evaluation.SnapshotID == nil && evaluation.State == "queued" {
			snapshot, createErr := e.createAlertSnapshot(ctx, rule, evaluation, validated)
			if createErr != nil {
				return createErr
			}
			evaluation.SnapshotID = &snapshot.SnapshotID
		}
		if evaluation.SnapshotID == nil {
			return errors.New("running alert evaluation has no snapshot")
		}
		status, err := e.Queries.FindAlertQuery(ctx, rule.TenantID, rule.AlertID, *evaluation.SnapshotID)
		if err != nil {
			return err
		}
		if status == nil {
			created, createErr := e.submitAlertQuery(ctx, rule, *evaluation.SnapshotID, validated)
			if createErr != nil {
				return createErr
			}
			status = &created
		}
		claim, err := e.Alerts.ClaimThresholdEvaluation(ctx, rule.TenantID, evaluation.EvaluationID, e.Owner, time.Minute)
		if err != nil {
			if errors.Is(err, control.ErrAlertLeaseLost) {
				continue
			}
			return err
		}
		status, err = e.awaitAlertQuery(ctx, rule, claim, *status)
		if err != nil {
			_ = e.Alerts.FailThresholdEvaluation(context.WithoutCancel(ctx), rule.TenantID, claim.EvaluationID, claim.Owner, claim.Fence, "query_failed")
			return err
		}
		observed, err := e.Results.ReadAlertCount(ctx, *status)
		if err != nil {
			_ = e.Alerts.FailThresholdEvaluation(context.WithoutCancel(ctx), rule.TenantID, claim.EvaluationID, claim.Owner, claim.Fence, "result_invalid")
			return err
		}
		deliveryID := uuid.NewString()
		body, err := json.Marshal(WebhookBody{Version: 1, DeliveryID: deliveryID, AlertID: rule.AlertID, AlertRevision: strconv.FormatInt(rule.Revision, 10), ProjectID: strconv.FormatInt(rule.ProjectID, 10), Type: KindThreshold, OccurredAt: time.UnixMicro(evaluation.WindowEndUS).UTC(), Threshold: &ThresholdBody{WindowStartUS: strconv.FormatInt(evaluation.WindowStartUS, 10), WindowEndUS: strconv.FormatInt(evaluation.WindowEndUS, 10), Observed: strconv.FormatInt(observed, 10), Operator: validated.Threshold.Operator, Threshold: validated.Threshold.Threshold}, Link: e.alertLink(rule)})
		if err != nil {
			return err
		}
		if _, err := e.Alerts.CompleteThresholdEvaluation(ctx, control.CompleteThresholdCommand{TenantID: rule.TenantID, EvaluationID: claim.EvaluationID, Owner: claim.Owner, Fence: claim.Fence, ObservedCount: observed, DeliveryID: deliveryID, BodyBytes: body}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Evaluator) createAlertSnapshot(ctx context.Context, rule control.AlertRule, evaluation control.ThresholdEvaluation, validated Rule) (model.QuerySnapshot, error) {
	canonical, err := query.CanonicalFilter(validated.Filter)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	spec := model.DatasetSpec{TenantID: rule.TenantID, ProjectIDs: []int64{rule.ProjectID}, Kinds: append([]model.Kind(nil), validated.Threshold.Kinds...), TimeBasis: model.QueryTimeReceived, StartUS: evaluation.WindowStartUS, EndUS: evaluation.WindowEndUS, Filter: canonical}
	digest, encoded, err := query.DatasetHash(spec)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	return e.Queries.CreateAlertSnapshot(ctx, control.CreateAlertSnapshotCommand{TenantID: rule.TenantID, AlertID: rule.AlertID, EvaluationID: evaluation.EvaluationID, SnapshotID: uuid.NewString(), DatasetSHA256: digest, DatasetBytes: encoded})
}

func (e *Evaluator) submitAlertQuery(ctx context.Context, rule control.AlertRule, snapshotID string, validated Rule) (control.QueryStatus, error) {
	// Reloading through the alert catalog validates the server rule principal;
	// no operator session or browser token participates.
	canonical, err := query.CanonicalFilter(validated.Filter)
	if err != nil {
		return control.QueryStatus{}, err
	}
	spec := model.DatasetSpec{TenantID: rule.TenantID, ProjectIDs: []int64{rule.ProjectID}, Kinds: validated.Threshold.Kinds, TimeBasis: model.QueryTimeReceived, Filter: canonical}
	var snapshot model.QuerySnapshot
	// Dataset bounds and cuts are authoritative in the registered snapshot.
	open, err := e.Alerts.ListOpenThresholdEvaluations(ctx, rule.TenantID, rule.AlertID, 10)
	if err != nil {
		return control.QueryStatus{}, err
	}
	found := false
	for _, evaluation := range open {
		if evaluation.SnapshotID != nil && *evaluation.SnapshotID == snapshotID {
			spec.StartUS, spec.EndUS = evaluation.WindowStartUS, evaluation.WindowEndUS
			for lane := range evaluation.Cut {
				snapshot.Lanes[lane] = model.SnapshotLane{LaneID: lane, CutSeq: evaluation.Cut[lane]}
			}
			found = true
			break
		}
	}
	if !found {
		return control.QueryStatus{}, errors.New("alert snapshot evaluation is unavailable")
	}
	datasetHash, datasetBytes, err := query.DatasetHash(spec)
	if err != nil {
		return control.QueryStatus{}, err
	}
	snapshot.SnapshotID, snapshot.TenantID, snapshot.DatasetSHA256, snapshot.DatasetBytes = snapshotID, rule.TenantID, datasetHash, datasetBytes
	var cuts [model.LaneCount]int64
	for lane := range snapshot.Lanes {
		cuts[lane] = snapshot.Lanes[lane].CutSeq
	}
	compiled, err := query.BuildPlan(spec, model.SnapshotScope{LaneCuts: cuts}, validated.Filter)
	if err != nil {
		return control.QueryStatus{}, err
	}
	operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{Plan: compiled, Metrics: []query.AggregateMetric{{Name: "count", Op: "count"}}, Top: 1, Order: query.AggregateOrder{Metric: "count", Direction: "desc"}})
	if err != nil {
		return control.QueryStatus{}, err
	}
	job, err := (query.Submission{Control: e.Queries, Objects: e.Objects, Working: e.Working, Generation: e.StorageGeneration}).SubmitAlert(ctx, rule.AlertID, snapshot, operation)
	if err != nil {
		return control.QueryStatus{}, err
	}
	return e.Queries.GetAlertQueryStatus(ctx, rule.TenantID, rule.AlertID, job.Authority.QueryID)
}

func (e *Evaluator) awaitAlertQuery(ctx context.Context, rule control.AlertRule, claim control.ThresholdEvaluation, status control.QueryStatus) (*control.QueryStatus, error) {
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		current, err := e.Queries.GetAlertQueryStatus(ctx, rule.TenantID, rule.AlertID, status.QueryID)
		if err != nil {
			return nil, err
		}
		switch current.State {
		case "succeeded":
			return &current, nil
		case "failed", "canceled":
			return nil, control.ErrQueryTerminal
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-poll.C:
		case <-heartbeat.C:
			if err := e.Alerts.HeartbeatThresholdEvaluation(ctx, claim.TenantID, claim.EvaluationID, claim.Owner, claim.Fence, time.Minute); err != nil {
				return nil, err
			}
		}
	}
}

func (e *Evaluator) evaluateIssues(ctx context.Context, rule control.AlertRule) error {
	pending, err := e.Alerts.ListPendingIssueTransitions(ctx, rule.TenantID, rule.AlertID, 100)
	if err != nil {
		return err
	}
	for _, transition := range pending {
		deliveryID := uuid.NewString()
		body, err := json.Marshal(WebhookBody{Version: 1, DeliveryID: deliveryID, AlertID: rule.AlertID, AlertRevision: strconv.FormatInt(rule.Revision, 10), ProjectID: strconv.FormatInt(rule.ProjectID, 10), Type: KindIssue, OccurredAt: time.UnixMicro(transition.ReceivedUS).UTC(), Issue: &IssueBody{IssueID: transition.IssueID, Transition: transition.Type, Revision: strconv.FormatInt(transition.IssueRevision, 10), Title: transition.Title}, Link: e.issueLink(rule, transition.IssueID)})
		if err != nil {
			return err
		}
		if _, err := e.Alerts.CommitIssueTransition(ctx, control.CommitIssueTransitionCommand{TenantID: rule.TenantID, AlertID: rule.AlertID, TransitionID: transition.TransitionID, DeliveryID: deliveryID, ExpectedRevision: rule.Revision, BodyBytes: body}); err != nil && !errors.Is(err, control.ErrRevisionConflict) {
			return err
		}
	}
	return nil
}
func (e *Evaluator) alertLink(rule control.AlertRule) string {
	return strings.TrimRight(e.PublicURL, "/") + fmt.Sprintf("/projects/%d/alerts/%s", rule.ProjectID, rule.AlertID)
}
func (e *Evaluator) issueLink(rule control.AlertRule, issueID string) string {
	return strings.TrimRight(e.PublicURL, "/") + fmt.Sprintf("/projects/%d/issues/%s", rule.ProjectID, issueID)
}
