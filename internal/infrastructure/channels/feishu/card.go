package feishu

import "encoding/json"

func buildMarkdownCard(content string) (string, error) {
	return marshalCard(map[string]any{
		"schema": "2.0",
		"body": map[string]any{
			"elements": []any{
				map[string]any{"tag": "markdown", "content": content},
			},
		},
	})
}

func marshalCard(card map[string]any) (string, error) {
	data, err := json.Marshal(card)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
