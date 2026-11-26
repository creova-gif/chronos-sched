package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/creova-gif/chronos-sched/internal/store"
)

const headerIdempotencyKey = "Idempotency-Key"

func workflowHashYAML(wfYAML string) string {
	sum := sha256.Sum256([]byte(wfYAML))
	return hex.EncodeToString(sum[:])
}

func (c *Coordinator) lookupIdempotentRun(ctx context.Context, key, wfYAML string) (SubmitWorkflowResponse, bool, bool, error) {
	if key == "" {
		return SubmitWorkflowResponse{}, false, false, nil
	}
	is, ok := c.store.(store.IdempotencyStore)
	if !ok {
		return SubmitWorkflowResponse{}, false, false, nil
	}
	rec, found, err := is.GetIdempotencyKey(ctx, TenantFromContext(ctx), key)
	if err != nil || !found {
		return SubmitWorkflowResponse{}, false, false, err
	}
	wfHash := workflowHashYAML(wfYAML)
	if rec.WorkflowHash != "" && wfHash != rec.WorkflowHash {
		return SubmitWorkflowResponse{}, true, true, nil
	}
	return SubmitWorkflowResponse{RunID: rec.RunID}, true, false, nil
}

func (c *Coordinator) recordIdempotentRun(ctx context.Context, key, runID, wfHash string) {
	if key == "" {
		return
	}
	if is, ok := c.store.(store.IdempotencyStore); ok {
		_ = is.PutIdempotencyKey(ctx, store.IdempotencyRecord{
			TenantID:     TenantFromContext(ctx),
			Key:          key,
			RunID:        runID,
			WorkflowHash: wfHash,
			CreatedAtNs:  c.nowFn().UnixNano(),
		})
	}
}
