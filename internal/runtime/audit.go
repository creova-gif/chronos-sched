package runtime

import (
	"context"
	"io"
	"log/slog"
)

func (c *Coordinator) logger() *slog.Logger {
	if c.cfg.Logger != nil {
		return c.cfg.Logger
	}
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func (c *Coordinator) auditLog(ctx context.Context, action, outcome, tenantID, runID, actor, detail string) {
	logSecurityAudit(ctx, c.logger(), action, outcome, tenantID, runID, actor, detail)
}

// logSecurityAudit emits a structured security audit record via logger.
// If logger is nil, it is a no-op. Denied outcomes are logged at Warn level;
// all other outcomes are logged at Info level.
func logSecurityAudit(ctx context.Context, logger *slog.Logger, action, outcome, tenantID, runID, actor, detail string) {
	if logger == nil {
		return
	}
	level := slog.LevelInfo
	if outcome == "denied" {
		level = slog.LevelWarn
	}
	logger.LogAttrs(
		ctx,
		level,
		"security audit",
		slog.Bool("audit", true),
		slog.String("action", action),
		slog.String("outcome", outcome),
		slog.String("tenant_id", tenantID),
		slog.String("run_id", runID),
		slog.String("actor", actor),
		slog.String("detail", detail),
	)
}
