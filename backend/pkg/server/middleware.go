package router

import (
	"net/http"
	"pentagi/pkg/server/models"
	"pentagi/pkg/server/response"
	"sync"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

func localUserRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.IsAborted() {
			return
		}

		session := sessions.Default(c)
		tid, ok := session.Get("tid").(string)

		if !ok || tid != models.UserTypeLocal.String() {
			response.Error(c, response.ErrLocalUserRequired, nil)
			return
		}

		c.Next()
	}
}

func noCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate") // HTTP 1.1
		c.Header("Pragma", "no-cache")                                   // HTTP 1.0
		c.Header("Expires", "0")                                         // prevents caching at the proxy server
		c.Next()
	}
}

func maxBodySizeMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if maxBytes > 0 && c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		}
		c.Next()
	}
}

func rateLimitMiddleware(r rate.Limit, burst int) gin.HandlerFunc {
	type clientLimiter struct {
		limiter *rate.Limiter
	}

	clients := make(map[string]*clientLimiter)
	var mu sync.Mutex

	return func(c *gin.Context) {
		key := c.ClientIP()
		if key == "" {
			key = "unknown"
		}

		mu.Lock()
		entry, ok := clients[key]
		if !ok {
			entry = &clientLimiter{limiter: rate.NewLimiter(r, burst)}
			clients[key] = entry
		}
		mu.Unlock()

		if !entry.limiter.Allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded",
			})
			return
		}

		c.Next()
	}
}
