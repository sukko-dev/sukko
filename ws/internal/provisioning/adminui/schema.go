package adminui

import "encoding/json"

// channelRulesJSONSchema is the JSON Schema for the channel rules editor.
// Built programmatically from the known ChannelRules struct fields.
// Served at GET /admin/schemas/channel-rules.json for the CodeMirror JSON schema validator.
var channelRulesJSONSchema = mustMarshal(map[string]any{
	"$schema":              "https://json-schema.org/draft-07/schema#",
	"title":                "Channel Rules",
	"description":          "Per-tenant subscribe/publish channel rules",
	"type":                 "object",
	"additionalProperties": false,
	"properties": map[string]any{
		"public": map[string]any{
			"type":        "array",
			"description": "Patterns for channels any subscriber can subscribe to (no group required)",
			"items": map[string]any{
				"type":      "string",
				"minLength": 1,
				"maxLength": 200,
				"pattern":   `^[a-zA-Z0-9_\-.*{}/]+$`,
			},
		},
		"group_mappings": map[string]any{
			"type":        "object",
			"description": "Map of channel pattern → required JWT group claim",
			"additionalProperties": map[string]any{
				"type": "string",
			},
		},
		"publish_public": map[string]any{
			"type":        "array",
			"description": "Patterns for channels any publisher can publish to",
			"items": map[string]any{
				"type":      "string",
				"minLength": 1,
				"maxLength": 200,
			},
		},
		"publish_group_mappings": map[string]any{
			"type":        "object",
			"description": "Map of publish channel pattern → required JWT group claim",
			"additionalProperties": map[string]any{
				"type": "string",
			},
		},
	},
})

// ChannelRulesSchema returns the JSON Schema bytes for the channel rules editor.
func ChannelRulesSchema() []byte {
	return channelRulesJSONSchema
}

func mustMarshal(v any) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic("adminui: schema marshal failed: " + err.Error())
	}
	return b
}
