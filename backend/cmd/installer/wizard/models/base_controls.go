package models

import (
	"pentagentx/cmd/installer/loader"
	"pentagentx/cmd/installer/wizard/styles"
	"pentagentx/cmd/installer/wizard/window"

	"github.com/charmbracelet/bubbles/textinput"
)

func NewBooleanInput(styles styles.Styles, window window.Window, envVar loader.EnvVar) textinput.Model {
	input := textinput.New()
	input.Prompt = ""
	input.PlaceholderStyle = styles.FormPlaceholder
	input.ShowSuggestions = true
	input.SetSuggestions([]string{"true", "false"})

	if envVar.Default != "" {
		input.Placeholder = envVar.Default
	}

	if envVar.IsPresent() || envVar.IsChanged {
		input.SetValue(envVar.Value)
	}

	return input
}

func NewTextInput(styles styles.Styles, window window.Window, envVar loader.EnvVar) textinput.Model {
	input := textinput.New()
	input.Prompt = ""
	input.PlaceholderStyle = styles.FormPlaceholder

	if envVar.Default != "" {
		input.Placeholder = envVar.Default
	}

	if envVar.IsPresent() || envVar.IsChanged {
		input.SetValue(envVar.Value)
	}

	return input
}
