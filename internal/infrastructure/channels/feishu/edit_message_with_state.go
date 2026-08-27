package feishu

import (
	"context"
	"fmt"
	"strings"
)

func (c *Channel) EditMessageWithState(ctx context.Context, targetID, messageID, content, state string, metadata map[string]string) error {
	content = strings.TrimSpace(content)
	switch state {
	case "progress":
		if preview := strings.TrimSpace(metadata["progress.preview"]); preview != "" && preview != content {
			content = fmt.Sprintf("%s\n\n%s", content, preview)
		}
	case "failed":
		content = "**处理失败**\n\n" + content
	}
	return c.EditMessage(ctx, targetID, messageID, content)
}
