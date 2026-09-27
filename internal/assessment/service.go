package assessment

import (
	"context"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Evaluate(
	ctx context.Context,
	planID string,
	input domain.AssessmentInput,
) (domain.RolloutAssessment, error) {
	if err := domain.ValidateAssessmentInput(input); err != nil {
		return domain.RolloutAssessment{}, err
	}
	now := s.clock.Now().UTC()
	requestIndex := planID + "\x00" + input.RequestKey
	var result domain.RolloutAssessment
	err := s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State == domain.PlanDraft {
			return domain.InvalidState("plan must be active or completed for assessment", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		if existingID, exists := state.AssessmentRequestKeys[requestIndex]; exists {
			existing, found := state.Assessments[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "assessment index is inconsistent", map[string]any{
					"assessment_id": existingID,
				})
			}
			// 重放只返回首次事实：计划的最新判定、版本和操作历史
			// 都必须保持在重放发生之前的状态，不能让更早的判定倒退指针。
			result = existing
			return nil
		}
		if plan.LatestRevisionID == "" {
			return domain.InvalidState("plan has no zone revision", map[string]any{
				"plan_id": planID,
			})
		}
		revision, ok := state.Revisions[plan.LatestRevisionID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing revision", map[string]any{
				"revision_id": plan.LatestRevisionID,
			})
		}
		probes := make([]domain.Probe, 0)
		for _, probe := range state.Probes {
			if probe.PlanID == planID {
				probes = append(probes, probe)
			}
		}
		if len(probes) == 0 {
			return domain.NewError(domain.CodeNoProbes, "plan has no probes to assess", map[string]any{
				"plan_id": planID,
			})
		}
		result = domain.BuildAssessment(s.ids.New("asm"), plan, revision, probes, now)
		result.RequestKey = input.RequestKey
		result.AssessmentSeq = domain.NextAssessmentSeq(plan)
		state.Assessments[result.ID] = result
		state.AssessmentRequestKeys[requestIndex] = result.ID
		domain.RecordAssessment(&plan, result)
		state.Plans[planID] = plan
		store.AddAudit(state, s.ids, "assessment", result.ID, "completed", now, map[string]any{
			"plan_id":        planID,
			"verdict":        result.Verdict,
			"assessment_seq": result.AssessmentSeq,
		})
		return nil
	})
	return result, err
}

func (s *Service) Latest(ctx context.Context, planID string) (domain.RolloutAssessment, error) {
	var result domain.RolloutAssessment
	err := s.store.Read(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.LatestAssessmentID == "" {
			return domain.NotFound("assessment", planID)
		}
		found, ok := state.Assessments[plan.LatestAssessmentID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing assessment", map[string]any{
				"assessment_id": plan.LatestAssessmentID,
			})
		}
		result = found
		return nil
	})
	return result, err
}
