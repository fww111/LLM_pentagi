package graph

import (
	"pentagentx/pkg/config"
	"pentagentx/pkg/controller"
	"pentagentx/pkg/database"
	"pentagentx/pkg/graph/subscriptions"
	"pentagentx/pkg/providers"
	"pentagentx/pkg/server/auth"
	"pentagentx/pkg/templates"

	"github.com/sirupsen/logrus"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

type Resolver struct {
	DB              database.Querier
	Config          *config.Config
	Logger          *logrus.Entry
	TokenCache      *auth.TokenCache
	DefaultPrompter templates.Prompter
	ProvidersCtrl   providers.ProviderController
	Controller      controller.FlowController
	Subscriptions   subscriptions.SubscriptionsController
}
