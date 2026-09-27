package workflowprobe

import (
	"fmt"
	"net/http"
	"sync"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runConsistencyAssessment() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-assess-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-assess-02", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "assess.example", 3001, 2, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "assess-lease-0001", "west", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("assess-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 10+index); err != nil {
			return err
		}
	}

	var consistent domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &consistent); err != nil {
		return err
	}
	if consistent.Verdict != domain.VerdictConsistent || consistent.ProbeCount != 2 {
		return ensure(false, "consistent assessment did not include both probes")
	}
	var replay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != consistent.ID {
		return ensure(false, "assessment replay returned another result")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != consistent.ID {
		return ensure(false, "latest assessment does not match")
	}

	reviewPlan, reviewRevision, err := createActivePlan(harness, "review.example", 4001, 1, 1, 20)
	if err != nil {
		return err
	}
	reviewGroup, err := allocate(harness, reviewPlan.ID, "review-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(
		harness,
		reviewGroup.LeaseIDs[0],
		"review-probe-0001",
		true,
		reviewRevision.Serial+1,
		30,
	); err != nil {
		return err
	}
	var review domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &review); err != nil {
		return err
	}
	if review.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "mismatched serial did not require review")
	}
	return verifyAssessmentReplay(harness)
}

// verifyAssessmentReplay 复现值班场景：较新的判定先完成，随后重放更早一次
// 判定请求。重放只能返回首次事实，计划的最新判定、版本和审计历史都不能
// 倒退或重复推进；重启后最新结论依旧保留。
func verifyAssessmentReplay(harness *harness) error {
	if _, err := enrollNode(harness, "resolver-replay-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-replay-02", "east"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "replay.example", 5001, 2, 1, 40)
	if err != nil {
		return err
	}

	// 激活后正常读取不应改变计划版本。
	var before domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &before); err != nil {
		return err
	}
	if before.Version != plan.Version {
		return ensure(false, "reading the plan before assessment moved the version")
	}

	group, err := allocate(harness, plan.ID, "replay-lease-0001", "east", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("replay-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 10+index); err != nil {
			return err
		}
	}

	assess := func(requestKey string) (domain.RolloutAssessment, error) {
		var result domain.RolloutAssessment
		err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
			RequestKey: requestKey,
		}, &result)
		return result, err
	}

	first, err := assess("replay-assess-0001")
	if err != nil {
		return err
	}
	second, err := assess("replay-assess-0002")
	if err != nil {
		return err
	}
	if second.ID == first.ID || second.AssessmentSeq <= first.AssessmentSeq {
		return ensure(false, "the second assessment did not become a newer result")
	}

	// 全新判定才推进最新指针和版本：初始版本 -> 两次全新判定各 +1。
	var afterSecond domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &afterSecond); err != nil {
		return err
	}
	if afterSecond.LatestAssessmentID != second.ID || afterSecond.LatestAssessmentSeq != second.AssessmentSeq {
		return ensure(false, "the newer assessment did not become the plan latest result")
	}
	if afterSecond.Version != plan.Version+2 {
		return ensure(false, "fresh assessments did not advance the plan version exactly once each")
	}

	// 网络重试把更早的请求键重放回来：只返回首次事实，指针/版本不变。
	replayed, err := assess("replay-assess-0001")
	if err != nil {
		return err
	}
	if replayed.ID != first.ID {
		return ensure(false, "assessment replay returned another result")
	}
	var afterReplay domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &afterReplay); err != nil {
		return err
	}
	if afterReplay.LatestAssessmentID != second.ID ||
		afterReplay.LatestAssessmentSeq != second.AssessmentSeq ||
		afterReplay.Version != afterSecond.Version {
		return ensure(false, "replaying an older assessment regressed the plan latest result or version")
	}

	// 读取最新结果仍是较新判定，且重复读取不推进版本。
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != second.ID {
		return ensure(false, "latest assessment regressed after replay")
	}
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &afterReplay); err != nil {
		return err
	}
	if afterReplay.Version != afterSecond.Version {
		return ensure(false, "reading the plan after completion moved the version")
	}

	// 重启后倒退修复结果必须仍然保留。
	if err := harness.restart(); err != nil {
		return err
	}
	var restarted domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &restarted); err != nil {
		return err
	}
	if restarted.LatestAssessmentID != second.ID ||
		restarted.LatestAssessmentSeq != second.AssessmentSeq ||
		restarted.Version != afterSecond.Version {
		return ensure(false, "replay regression survived the service restart")
	}
	var restartedLatest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &restartedLatest); err != nil {
		return err
	}
	if restartedLatest.ID != second.ID {
		return ensure(false, "latest assessment regressed after restart")
	}

	// 重启后再次重放旧请求，指针依旧不动。
	if _, err := assess("replay-assess-0001"); err != nil {
		return err
	}
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &restarted); err != nil {
		return err
	}
	if restarted.LatestAssessmentID != second.ID || restarted.Version != afterSecond.Version {
		return ensure(false, "older replay moved the pointer after restart")
	}

	return verifyConcurrentAssessments(harness)
}

// verifyConcurrentAssessments 确认并发提交的全新判定都会保留，并按提交
// 顺序得到连续的判定序号，最新指针指向最后一次全新判定。
func verifyConcurrentAssessments(harness *harness) error {
	if _, err := enrollNode(harness, "resolver-concur-01", "north"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-concur-02", "north"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "concur.example", 6001, 2, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "concur-lease-0001", "north", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("concur-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 12+index); err != nil {
			return err
		}
	}

	keys := []string{"concur-assess-0001", "concur-assess-0002", "concur-assess-0003"}
	results := make([]domain.RolloutAssessment, len(keys))
	errs := make([]error, len(keys))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for index, key := range keys {
		wg.Add(1)
		go func(i int, requestKey string) {
			defer wg.Done()
			<-start
			results[i], errs[i] = func() (domain.RolloutAssessment, error) {
				var result domain.RolloutAssessment
				err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
					RequestKey: requestKey,
				}, &result)
				return result, err
			}()
		}(index, key)
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	seqByID := make(map[string]uint64, len(results))
	var maxSeq uint64
	var latestID string
	for _, result := range results {
		seqByID[result.ID] = result.AssessmentSeq
		if result.AssessmentSeq > maxSeq {
			maxSeq = result.AssessmentSeq
			latestID = result.ID
		}
	}
	if len(seqByID) != len(keys) {
		return ensure(false, "concurrent assessments collapsed into duplicate results")
	}
	var viewed domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &viewed); err != nil {
		return err
	}
	if viewed.LatestAssessmentID != latestID || viewed.LatestAssessmentSeq != maxSeq {
		return ensure(false, "concurrent assessments did not leave the newest result as latest")
	}
	if viewed.Version != plan.Version+uint64(len(keys)) {
		return ensure(false, "concurrent fresh assessments did not each advance the version exactly once")
	}

	// 每个请求键重放都返回各自首次事实，不改变并发建立的最新指针。
	for index, key := range keys {
		var replay domain.RolloutAssessment
		if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
			RequestKey: key,
		}, &replay); err != nil {
			return err
		}
		if replay.ID != results[index].ID || replay.AssessmentSeq != results[index].AssessmentSeq {
			return ensure(false, "concurrent request replay returned a different first fact")
		}
	}
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &viewed); err != nil {
		return err
	}
	if viewed.LatestAssessmentID != latestID || viewed.LatestAssessmentSeq != maxSeq {
		return ensure(false, "replaying concurrent assessments moved the latest pointer")
	}
	return nil
}
