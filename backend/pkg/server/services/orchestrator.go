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

type orchestratorAuthRequest struct {
	FlowID        int64  `json:"flow_id" binding:"required,min=1"`
	TodoID        string `json:"todo_id"`
	Action        string `json:"action" binding:"required"`
	RiskLevel     string `json:"risk_level" binding:"required"`
	Justification string `json:"justification" binding:"required"`
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

// InputRequired marks a running task as waiting after the LangGraph graph
// raised an ask_user interrupt. The graph thread keeps its checkpoint, so a
// later putUserInput resumes execution from the interrupt.
func (s *OrchestratorService) InputRequired(c *gin.Context) {
	var req orchestratorInputRequiredRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, ok := s.getTask(c, req.FlowID)
	if !ok {
		return
	}
	if err := task.InputRequired(c, req.Message); err != nil {
		logger.FromContext(c).WithError(err).Error("error marking task input-required")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

type orchestratorInputRequiredRequest struct {
	FlowID  int64  `json:"flow_id" binding:"required,min=1"`
	Message string `json:"message"`
}
