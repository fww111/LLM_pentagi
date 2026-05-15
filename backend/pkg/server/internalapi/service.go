package internalapi

import (
	"net/http"
	"strings"

	"pentagi/pkg/config"

	"github.com/gin-gonic/gin"
)

const internalKeyHeader = "X-Internal-Key"

type Service struct {
	cfg *config.Config
}

type providerResolveRequest struct {
	FlowID    int64  `json:"flow_id,omitempty"`
	TaskID    int64  `json:"task_id,omitempty"`
	ContextID string `json:"context_id,omitempty"`
}

type providerResolveResponse struct {
	ProviderName string         `json:"provider_name"`
	ProviderType string         `json:"provider_type"`
	Model        string         `json:"model"`
	ServerURL    string         `json:"server_url,omitempty"`
	ResolvedAuth map[string]any `json:"resolved_auth,omitempty"`
	Configured   bool           `json:"configured"`
}

func NewService(cfg *config.Config) *Service {
	return &Service{cfg: cfg}
}

func (s *Service) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":     "ok",
		"configured": strings.TrimSpace(s.cfg.InternalAPIKey) != "",
	})
}

func (s *Service) AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := strings.TrimSpace(s.cfg.InternalAPIKey)
		if key == "" {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "internal API is not configured",
			})
			return
		}

		if c.GetHeader(internalKeyHeader) != key {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid internal API key",
			})
			return
		}

		c.Next()
	}
}

func (s *Service) ExecuteTool(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error": "tool execution proxy is not implemented in stage 0",
	})
}

func (s *Service) WriteRuntimeEvent(c *gin.Context) {
	c.JSON(http.StatusAccepted, gin.H{
		"accepted": true,
	})
}

func (s *Service) RegisterArtifact(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error": "artifact registration is not implemented in stage 0",
	})
}

func (s *Service) WriteWorkspace(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error": "workspace write proxy is not implemented in stage 0",
	})
}

func (s *Service) ResolveProviderConfig(c *gin.Context) {
	var req providerResolveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid provider resolve request"})
		return
	}

	resp := s.defaultProviderConfig()
	c.JSON(http.StatusOK, resp)
}

func (s *Service) GetProviderRuntimeProfile(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"task_id":        c.Param("taskID"),
		"provider_type":  "",
		"model":          "",
		"supports_tools": false,
		"configured":     false,
	})
}

func (s *Service) CallWithTools(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error": "LLM provider proxy is not implemented in stage 0",
	})
}

func (s *Service) defaultProviderConfig() providerResolveResponse {
	if strings.TrimSpace(s.cfg.OpenAIKey) != "" {
		return providerResolveResponse{
			ProviderName: "openai",
			ProviderType: "openai",
			Model: firstNonEmpty(
				strings.TrimSpace(s.cfg.LLMServerModel),
				"gpt-4.1",
			),
			ServerURL: s.cfg.OpenAIServerURL,
			ResolvedAuth: map[string]any{
				"api_key": s.cfg.OpenAIKey,
			},
			Configured: true,
		}
	}
	if strings.TrimSpace(s.cfg.AnthropicAPIKey) != "" {
		return providerResolveResponse{
			ProviderName: "anthropic",
			ProviderType: "anthropic",
			ServerURL:    s.cfg.AnthropicServerURL,
			ResolvedAuth: map[string]any{
				"api_key": s.cfg.AnthropicAPIKey,
			},
			Configured: true,
		}
	}
	if strings.TrimSpace(s.cfg.GeminiAPIKey) != "" {
		return providerResolveResponse{
			ProviderName: "gemini",
			ProviderType: "gemini",
			ServerURL:    s.cfg.GeminiServerURL,
			ResolvedAuth: map[string]any{
				"api_key": s.cfg.GeminiAPIKey,
			},
			Configured: true,
		}
	}
	if strings.TrimSpace(s.cfg.LLMServerURL) != "" {
		return providerResolveResponse{
			ProviderName: firstNonEmpty(s.cfg.LLMServerProvider, "custom"),
			ProviderType: firstNonEmpty(s.cfg.LLMServerProvider, "custom"),
			Model:        s.cfg.LLMServerModel,
			ServerURL:    s.cfg.LLMServerURL,
			ResolvedAuth: map[string]any{
				"api_key": s.cfg.LLMServerKey,
			},
			Configured: true,
		}
	}

	return providerResolveResponse{
		ProviderName: "",
		ProviderType: "",
		Configured:   false,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
