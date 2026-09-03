package controller

import (
	"context"
	"errors"
	"fmt"

	"pentagentx/pkg/database"
	"pentagentx/pkg/graph/subscriptions"
	"pentagentx/pkg/observability/langfuse"
	"pentagentx/pkg/orchestrator"
	"pentagentx/pkg/providers"
	"pentagentx/pkg/tools"

	"github.com/sirupsen/logrus"
)

var ErrNothingToLoad = errors.New("nothing to load")

type FlowContext struct {
	DB database.Querier

	UserID  int64
	FlowID  int64
	TraceID string

	Executor     tools.FlowToolsExecutor
	Provider     providers.FlowProvider
	Orchestrator orchestrator.TaskClient
	Publisher    subscriptions.FlowPublisher

	TermLog    FlowTermLogWorker
	MsgLog     FlowMsgLogWorker
	Screenshot FlowScreenshotWorker

	// Multi-agent migration: raw DB access for multi-agent extension queries
	RawDB database.DBTX
}

type TaskContext struct {
	TaskID    int64
	TaskTitle string
	TaskInput string

	FlowContext
}

type SubtaskContext struct {
	MsgChainID         int64
	SubtaskID          int64
	SubtaskTitle       string
	SubtaskDescription string

	TaskContext
}

func wrapErrorEndSpan(ctx context.Context, span langfuse.Span, msg string, err error) error {
	logrus.WithContext(ctx).WithError(err).Error(msg)
	err = fmt.Errorf("%s: %w", msg, err)
	span.End(
		langfuse.WithSpanStatus(err.Error()),
		langfuse.WithSpanLevel(langfuse.ObservationLevelError),
	)
	return err
}
