package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

type AssessmentPointerChange struct {
	PreviousID  string
	PreviousSeq uint64
	CurrentID   string
	CurrentSeq  uint64
	Changed     bool
}

func NextAssessmentSeq(plan RolloutPlan) uint64 {
	return plan.LatestAssessmentSeq + 1
}

func ApplyAssessmentPointer(
	plan *RolloutPlan,
	assessment RolloutAssessment,
	replay bool,
) AssessmentPointerChange {
	change := AssessmentPointerChange{
		PreviousID:  plan.LatestAssessmentID,
		PreviousSeq: plan.LatestAssessmentSeq,
		CurrentID:   plan.LatestAssessmentID,
		CurrentSeq:  plan.LatestAssessmentSeq,
	}
	if assessment.PlanID != plan.ID {
		return change
	}
	if !replay &&
		plan.LatestAssessmentID == assessment.ID &&
		plan.LatestAssessmentSeq == assessment.AssessmentSeq {
		return change
	}
	plan.LatestAssessmentID = assessment.ID
	plan.LatestAssessmentSeq = assessment.AssessmentSeq
	plan.Version++
	change.CurrentID = assessment.ID
	change.CurrentSeq = assessment.AssessmentSeq
	change.Changed = true
	return change
}

func CurrentAssessment(
	plan RolloutPlan,
	assessments map[string]RolloutAssessment,
) (RolloutAssessment, bool) {
	if plan.LatestAssessmentID == "" {
		return RolloutAssessment{}, false
	}
	assessment, ok := assessments[plan.LatestAssessmentID]
	if !ok || assessment.PlanID != plan.ID {
		return RolloutAssessment{}, false
	}
	return assessment, true
}

func ReapplyCurrentAssessment(
	plan *RolloutPlan,
	assessments map[string]RolloutAssessment,
) AssessmentPointerChange {
	assessment, ok := CurrentAssessment(*plan, assessments)
	if !ok {
		return AssessmentPointerChange{
			PreviousID:  plan.LatestAssessmentID,
			PreviousSeq: plan.LatestAssessmentSeq,
			CurrentID:   plan.LatestAssessmentID,
			CurrentSeq:  plan.LatestAssessmentSeq,
		}
	}
	return ApplyAssessmentPointer(plan, assessment, true)
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	probes []Probe,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	if len(probes) == 0 {
		reasons["no_probes"] = struct{}{}
	}
	probeCount := len(probes)
	successCount := 0
	maximumLatency := 0
	nodes := make(map[string]struct{})
	for _, probe := range probes {
		nodes[probe.NodeID] = struct{}{}
		if probe.LatencyMS > maximumLatency {
			maximumLatency = probe.LatencyMS
		}
		if probe.Success {
			successCount++
			if probe.ObservedSerial != revision.Serial {
				reasons["serial_mismatch"] = struct{}{}
			}
		}
	}
	if len(nodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
	}
	successRatio := 0.0
	if probeCount > 0 {
		successRatio = round(float64(successCount)/float64(probeCount), 6)
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
	}
	if maximumLatency > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if len(reasonList) > 0 {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:               id,
		PlanID:           plan.ID,
		RevisionID:       revision.ID,
		Verdict:          verdict,
		Reasons:          reasonList,
		ProbeCount:       probeCount,
		SuccessCount:     successCount,
		SuccessRatio:     successRatio,
		MaximumLatencyMS: maximumLatency,
		CreatedAt:        now.UTC(),
	}
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
