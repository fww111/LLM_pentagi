package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"pentagentx/pkg/csum"
	"pentagentx/pkg/database"
	"pentagentx/pkg/graphiti"
	obs "pentagentx/pkg/observability"
	"pentagentx/pkg/observability/langfuse"
	"pentagentx/pkg/orchestrator"
	"pentagentx/pkg/providers/embeddings"
	"pentagentx/pkg/providers/pconfig"
	"pentagentx/pkg/providers/provider"
	"pentagentx/pkg/templates"
	"pentagentx/pkg/tools"

	"github.com/vxcontrol/langchaingo/llms/reasoning"
	"github.com/vxcontrol/langchaingo/llms/streaming"
)

const ToolPlaceholder = "Execute operations via function invocation - textual responses are not acceptable for task completion."

const TasksNumberLimit = 15

const (
	msgGeneratorSizeLimit = 150 * 1024 // 150 KB
	msgRefinerSizeLimit   = 100 * 1024 // 100 KB
	msgReporterSizeLimit  = 100 * 1024 // 100 KB
	msgSummarizerLimit    = 16 * 1024  // 16 KB
)

const textTruncateMessage = "\n\n[...truncated]"

type PerformResult int

const (
	PerformResultError PerformResult = iota
	PerformResultWaiting
	PerformResultDone
)

type StreamMessageChunkType streaming.ChunkType

const (
	StreamMessageChunkTypeThinking StreamMessageChunkType = "thinking"
	StreamMessageChunkTypeContent  StreamMessageChunkType = "content"
	StreamMessageChunkTypeResult   StreamMessageChunkType = "result"
	StreamMessageChunkTypeFlush    StreamMessageChunkType = "flush"
	StreamMessageChunkTypeUpdate   StreamMessageChunkType = "update"
)

type StreamMessageChunk struct {
	Type         StreamMessageChunkType
	MsgType      database.MsglogType
	Content      string
	Thinking     *reasoning.ContentReasoning
	Result       string
	ResultFormat database.MsglogResultFormat
	StreamID     int64
}

type StreamMessageHandler func(ctx context.Context, chunk *StreamMessageChunk) error

type FlowProvider interface {
	ID() int64
	DB() database.Querier
	Type() provider.ProviderType
	Name() provider.ProviderName
	Model(opt pconfig.ProviderOptionsType) string
	Image() string
	Title() string
	Language() string
	ToolCallIDTemplate() string
	Embedder() embeddings.Embedder
	Executor() tools.FlowToolsExecutor
	Prompter() templates.Prompter

	SetTitle(title string)
	SetAgentLogProvider(agentLog tools.AgentLogProvider)
	SetMsgLogProvider(msgLog tools.MsgLogProvider)
	SetProvider(ctx context.Context, newProvider provider.Provider) error

	GetTaskTitle(ctx context.Context, input string) (string, error)
	DecidePrimaryAgentStep(ctx context.Context, taskID, subtaskID, msgChainID int64) (*orchestrator.PrimaryAgentDecision, error)
	ExecuteDelegatedAgent(ctx context.Context, taskID, subtaskID int64, agentType string, payload json.RawMessage) (*orchestrator.AgentExecutionResult, error)
	WritePrimaryAgentToolResult(ctx context.Context, msgChainID int64, agentType, toolCallID, result string) error

	// Multi-agent migration: supervisor step for multi-agent orchestration
	DecideSupervisorStep(ctx context.Context, taskID int64, nodeRole string, msgChainID int64) (*orchestrator.SupervisorDecision, error)

	FlowProviderHandlers
}

type FlowProviderHandlers interface {
	GetAskAdviceHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetCoderHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetInstallerHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetIntegratorHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetMemoristHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetPentesterHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetSubtaskSearcherHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetTesterHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetTaskSearcherHandler(ctx context.Context, taskID int64) (tools.ExecutorHandler, error)
	GetSummarizeResultHandler(taskID, subtaskID *int64) tools.SummarizeHandler
	GetReviewerHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
	GetReporterHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error)
}

type tasksInfo struct {
	Task     database.Task
	Tasks    []database.Task
	Subtasks []database.Subtask
}

type subtasksInfo struct {
	Subtask   *database.Subtask
	Planned   []database.Subtask
	Completed []database.Subtask
}

type flowProvider struct {
	db database.Querier
	mx *sync.RWMutex

	embedder       embeddings.Embedder
	graphitiClient *graphiti.Client

	flowID        int64
	publicIP      string
	dockerNetwork string

	callCounter *atomic.Int64

	image    string
	title    string
	language string
	askUser  bool
	planning bool

	tcIDTemplate string

	prompter templates.Prompter
	executor tools.FlowToolsExecutor
	agentLog tools.AgentLogProvider
	msgLog   tools.MsgLogProvider
	streamCb StreamMessageHandler

	summarizer csum.Summarizer

	maxGACallsLimit int
	maxLACallsLimit int
	buildMonitor    executionMonitorBuilder

	provider.Provider
}

func (fp *flowProvider) SetAgentLogProvider(agentLog tools.AgentLogProvider) {
	fp.mx.Lock()
	defer fp.mx.Unlock()

	fp.agentLog = agentLog
}

func (fp *flowProvider) SetMsgLogProvider(msgLog tools.MsgLogProvider) {
	fp.mx.Lock()
	defer fp.mx.Unlock()

	fp.msgLog = msgLog
}

func (fp *flowProvider) SetProvider(ctx context.Context, newProvider provider.Provider) error {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.SetProvider")
	defer span.End()

	fp.mx.Lock()
	defer fp.mx.Unlock()

	// Update provider-specific fields
	fp.Provider = newProvider

	var err error
	fp.tcIDTemplate, err = newProvider.GetToolCallIDTemplate(ctx, fp.prompter)
	if err != nil {
		return fmt.Errorf("failed to get tool call ID template: %w", err)
	}

	return nil
}

func (fp *flowProvider) ID() int64 {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.flowID
}

func (fp *flowProvider) DB() database.Querier {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.db
}

func (fp *flowProvider) Image() string {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.image
}

func (fp *flowProvider) Title() string {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.title
}

func (fp *flowProvider) SetTitle(title string) {
	fp.mx.Lock()
	defer fp.mx.Unlock()

	fp.title = title
}

func (fp *flowProvider) Language() string {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.language
}

func (fp *flowProvider) ToolCallIDTemplate() string {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.tcIDTemplate
}

func (fp *flowProvider) Embedder() embeddings.Embedder {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.embedder
}

func (fp *flowProvider) Executor() tools.FlowToolsExecutor {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.executor
}

func (fp *flowProvider) Prompter() templates.Prompter {
	fp.mx.RLock()
	defer fp.mx.RUnlock()

	return fp.prompter
}

func (fp *flowProvider) GetTaskTitle(ctx context.Context, input string) (string, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.GetTaskTitle")
	defer span.End()

	ctx, observation := obs.Observer.NewObservation(ctx)
	getterEvaluator := observation.Evaluator(
		langfuse.WithEvaluatorName("get task title"),
		langfuse.WithEvaluatorInput(input),
		langfuse.WithEvaluatorMetadata(langfuse.Metadata{
			"lang": fp.language,
		}),
	)
	ctx, _ = getterEvaluator.Observation(ctx)

	titleTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeTaskDescriptor, map[string]any{
		"Input":       input,
		"Lang":        fp.language,
		"CurrentTime": getCurrentTime(),
		"N":           150,
	})
	if err != nil {
		return "", wrapErrorEndEvaluatorSpan(ctx, getterEvaluator, "failed to get flow title template", err)
	}

	title, err := fp.Call(ctx, pconfig.OptionsTypeSimple, titleTmpl)
	if err != nil {
		return "", wrapErrorEndEvaluatorSpan(ctx, getterEvaluator, "failed to get flow title", err)
	}

	getterEvaluator.End(
		langfuse.WithEvaluatorStatus("success"),
		langfuse.WithEvaluatorOutput(title),
	)

	return title, nil
}

func (fp *flowProvider) putMsgLog(
	ctx context.Context,
	msgType database.MsglogType,
	taskID, subtaskID *int64,
	streamID int64,
	thinking, msg string,
) (int64, error) {
	fp.mx.RLock()
	msgLog := fp.msgLog
	fp.mx.RUnlock()

	if msgLog == nil {
		return 0, nil
	}

	return msgLog.PutMsg(ctx, msgType, taskID, subtaskID, streamID, thinking, msg)
}

func (fp *flowProvider) updateMsgLogResult(
	ctx context.Context,
	msgID, streamID int64,
	result string,
	resultFormat database.MsglogResultFormat,
) error {
	fp.mx.RLock()
	msgLog := fp.msgLog
	fp.mx.RUnlock()

	if msgLog == nil || msgID <= 0 {
		return nil
	}

	return msgLog.UpdateMsgResult(ctx, msgID, streamID, result, resultFormat)
}

func (fp *flowProvider) putAgentLog(
	ctx context.Context,
	initiator, executor database.MsgchainType,
	task, result string,
	taskID, subtaskID *int64,
) (int64, error) {
	fp.mx.RLock()
	agentLog := fp.agentLog
	fp.mx.RUnlock()

	if agentLog == nil {
		return 0, nil
	}

	return agentLog.PutLog(ctx, initiator, executor, task, result, taskID, subtaskID)
}
