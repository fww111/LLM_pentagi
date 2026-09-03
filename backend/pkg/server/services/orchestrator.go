package services

import (
	"encoding/json"
	"net/http"
	"strconv"

	"pentagentx/pkg/controller"
	"pentagentx/pkg/server/logger"

	"github.com/gin-gonic/gin"
)

type OrchestratorService struct {
	fc controller.FlowController
}

type orchestratorFlowTaskRequest struct {
	FlowID           int64 `json:"flow_id" binding:"required,min=1"`
	MsgChainID       int64 `json:"msg_chain_id,omitempty"`
	HasExistingPlan  bool  `json:"has_existing_plan,omitempty"`
}

type orchestratorExecuteAgentRequest struct {
	FlowID    int64  `json:"flow_id" binding:"required,min=1"`
	AgentType string `json:"agent_type" binding:"required"`
	Payload   any    `json:"payload"`
}

type orchestratorWriteResultRequest struct {
	FlowID     int64  `json:"flow_id" binding:"required,min=1"`
	AgentType  string `json:"agent_type" binding:"required"`
	ToolCallID string `json:"tool_call_id"`
	Result     string `json:"result"`
}

type orchestratorFailTaskRequest struct {
	FlowID int64  `json:"flow_id" binding:"required,min=1"`
	Result string `json:"result"`
}

func NewOrchestratorService(fc controller.FlowController) *OrchestratorService {
	return &OrchestratorService{fc: fc}
}

func (s *OrchestratorService) GenerateSubtasks(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}

	if err := task.GenerateSubtasks(c); err != nil {
		logger.FromContext(c).WithError(err).Error("error generating subtasks through internal orchestrator API")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *OrchestratorService) SelectNextSubtask(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}

	subtask, err := task.SelectNextSubtask(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error selecting next subtask through internal orchestrator API")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if subtask == nil {
		c.JSON(http.StatusOK, gin.H{"subtask": nil})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"subtask": gin.H{
			"id":          subtask.GetSubtaskID(),
			"title":       subtask.GetTitle(),
			"description": subtask.GetDescription(),
		},
	})
}

func (s *OrchestratorService) PreparePrimaryAgentContext(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	subtask, ok := s.getSubtask(c, req.FlowID)
	if !ok {
		return
	}

	msgChainID, err := subtask.EnsurePrepared(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error preparing primary agent context through internal orchestrator API")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg_chain_id": msgChainID})
}

func (s *OrchestratorService) PrimaryAgentStep(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	subtask, ok := s.getSubtask(c, req.FlowID)
	if !ok {
		return
	}

	decision, err := subtask.StepPrimaryAgent(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error executing primary agent step through internal orchestrator API")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"decision": decision})
}

func (s *OrchestratorService) ExecuteAgent(c *gin.Context) {
	var req orchestratorExecuteAgentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	subtask, ok := s.getSubtask(c, req.FlowID)
	if !ok {
		return
	}

	payload, err := marshalRawMessage(req.Payload)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	execution, err := subtask.ExecuteAgent(c, req.AgentType, payload)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error executing delegated agent through internal orchestrator API")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"execution": execution})
}

func (s *OrchestratorService) WritePrimaryAgentResult(c *gin.Context) {
	var req orchestratorWriteResultRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	subtask, ok := s.getSubtask(c, req.FlowID)
	if !ok {
		return
	}

	if err := subtask.WritePrimaryAgentToolResult(c, req.AgentType, req.ToolCallID, req.Result); err != nil {
		logger.FromContext(c).WithError(err).Error("error writing delegated result back to primary agent chain")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *OrchestratorService) RefineSubtasks(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}

	if err := task.RefineSubtasks(c); err != nil {
		logger.FromContext(c).WithError(err).Error("error refining subtasks through internal orchestrator API")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *OrchestratorService) ReportTaskResult(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}

	if err := task.ReportTaskResult(c); err != nil {
		logger.FromContext(c).WithError(err).Error("error reporting task result through internal orchestrator API")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	status, err := task.GetStatus(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error reading task status after reporting result")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result, err := task.GetResult(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error reading task result after reporting result")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"task": gin.H{
			"status": status,
			"result": result,
		},
	})
}

func (s *OrchestratorService) FailTask(c *gin.Context) {
	var req orchestratorFailTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}

	if err := task.Fail(c, req.Result); err != nil {
		logger.FromContext(c).WithError(err).Error("error failing task through internal orchestrator API")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	status, err := task.GetStatus(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error reading task status after fail")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result, err := task.GetResult(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error reading task result after fail")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"task": gin.H{
			"status": status,
			"result": result,
		},
	})
}

func (s *OrchestratorService) getTask(c *gin.Context, flowID int64) (controller.TaskWorker, bool) {
	flow, err := s.fc.GetFlow(c, flowID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return nil, false
	}

	taskID, err := strconv.ParseInt(c.Param("taskID"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return nil, false
	}

	task, err := flow.GetTask(c, taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return nil, false
	}

	return task, true
}

func (s *OrchestratorService) getSubtask(c *gin.Context, flowID int64) (controller.SubtaskWorker, bool) {
	task, ok := s.getTask(c, flowID)
	if !ok {
		return nil, false
	}

	subtaskID, err := strconv.ParseInt(c.Param("subtaskID"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return nil, false
	}

	subtask, err := task.GetSubtask(c, subtaskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return nil, false
	}

	return subtask, true
}

func marshalRawMessage(value any) (json.RawMessage, error) {
	if value == nil {
		return json.RawMessage("{}"), nil
	}

	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(raw), nil
}

// ========================================
// Multi-agent migration: new service methods
// ========================================

type orchestratorAgentExecuteRequest struct {
	FlowID    int64  `json:"flow_id" binding:"required,min=1"`
	AgentRole string `json:"agent_role" binding:"required"`
	TodoID    string `json:"todo_id"`
	Payload   any    `json:"payload"`
}

type orchestratorStoreArtifactRequest struct {
	FlowID       int64  `json:"flow_id" binding:"required,min=1"`
	ArtifactID   string `json:"artifact_id" binding:"required"`
	Name         string `json:"name" binding:"required"`
	ArtifactType string `json:"artifact_type" binding:"required"`
	Content      string `json:"content"`
}

type orchestratorAuthRequest struct {
	FlowID        int64  `json:"flow_id" binding:"required,min=1"`
	TodoID        string `json:"todo_id"`
	Action        string `json:"action" binding:"required"`
	RiskLevel     string `json:"risk_level" binding:"required"`
	Justification string `json:"justification" binding:"required"`
}

type orchestratorResolveAuthRequest struct {
	FlowID   int64  `json:"flow_id" binding:"required,min=1"`
	Status   string `json:"status" binding:"required"`
	Response string `json:"response"`
}

type orchestratorStoreFindingRequest struct {
	FlowID      int64  `json:"flow_id" binding:"required,min=1"`
	TodoID      string `json:"todo_id"`
	FindingType string `json:"finding_type"`
	Severity    string `json:"severity"`
	Title       string `json:"title" binding:"required"`
	Description string `json:"description"`
	RawOutput   string `json:"raw_output"`
}

type orchestratorRejectTaskRequest struct {
	FlowID int64  `json:"flow_id" binding:"required,min=1"`
	Result string `json:"result"`
}

type orchestratorUpdateSharedStateRequest struct {
	FlowID       int64                  `json:"flow_id" binding:"required,min=1"`
	ActiveNode   string                 `json:"active_node"`
	ActiveTodoID string                 `json:"active_todo_id"`
	StatusCode   *int                   `json:"task_status_code"`
	Updates      map[string]interface{} `json:"updates"`
}

func (s *OrchestratorService) DesignerStep(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	decision, err := task.DesignerStep(c, req.MsgChainID)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error in designer step")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"decision": decision})
}

func (s *OrchestratorService) PlannerStep(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	decision, err := task.PlannerStep(c, req.MsgChainID)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error in planner step")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"decision": decision})
}

func (s *OrchestratorService) SupervisorStep(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	decision, err := task.SupervisorStep(c, req.MsgChainID)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error in supervisor step")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"decision": decision})
}

func (s *OrchestratorService) GenerateTodoPlan(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	todos, err := task.GenerateTodoPlan(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error generating todo plan")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"decision": gin.H{"result": gin.H{"todos": todos}}})
}

func (s *OrchestratorService) RefineTodoPlan(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	todos, err := task.RefineTodoPlan(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error refining todo plan")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"decision": gin.H{"result": gin.H{"todos": todos}}})
}

func (s *OrchestratorService) AgentExecute(c *gin.Context) {
	var req orchestratorAgentExecuteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	payload, err := marshalRawMessage(req.Payload)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result, err := task.AgentExecute(c, req.AgentRole, req.TodoID, payload)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error executing agent")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (s *OrchestratorService) StoreArtifact(c *gin.Context) {
	var req orchestratorStoreArtifactRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	if err := task.StoreArtifact(c, req.ArtifactID, req.Name, req.ArtifactType, req.Content); err != nil {
		logger.FromContext(c).WithError(err).Error("error storing artifact")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *OrchestratorService) StoreAuthRequest(c *gin.Context) {
	var req orchestratorAuthRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	if err := task.StoreAuthRequest(c, req.TodoID, req.Action, req.RiskLevel, req.Justification); err != nil {
		logger.FromContext(c).WithError(err).Error("error storing auth request")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *OrchestratorService) ResolveAuthRequest(c *gin.Context) {
	var req orchestratorResolveAuthRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	if err := task.ResolveAuthRequest(c, c.Param("authID"), req.Status, req.Response); err != nil {
		logger.FromContext(c).WithError(err).Error("error resolving auth request")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *OrchestratorService) StoreFinding(c *gin.Context) {
	var req orchestratorStoreFindingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	if err := task.StoreFinding(c, req.TodoID, req.FindingType, req.Severity, req.Title, req.Description, req.RawOutput); err != nil {
		logger.FromContext(c).WithError(err).Error("error storing finding")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *OrchestratorService) RejectTask(c *gin.Context) {
	var req orchestratorRejectTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	if err := task.RejectTask(c, req.Result); err != nil {
		logger.FromContext(c).WithError(err).Error("error rejecting task")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	status, err := task.GetStatus(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error reading task status after reject")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result, err := task.GetResult(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error reading task result after reject")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"task": gin.H{
			"status": status,
			"result": result,
		},
	})
}

func (s *OrchestratorService) CompleteTask(c *gin.Context) {
	var req orchestratorFlowTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	if err := task.CompleteTask(c); err != nil {
		logger.FromContext(c).WithError(err).Error("error completing task")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	status, err := task.GetStatus(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error reading task status after complete")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result, err := task.GetResult(c)
	if err != nil {
		logger.FromContext(c).WithError(err).Error("error reading task result after complete")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"task": gin.H{
			"status": status,
			"result": result,
		},
	})
}

func (s *OrchestratorService) UpdateSharedState(c *gin.Context) {
	var req orchestratorUpdateSharedStateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	if err := task.UpdateSharedState(c, req.ActiveNode, req.ActiveTodoID, req.StatusCode, req.Updates); err != nil {
		logger.FromContext(c).WithError(err).Error("error updating shared state")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
