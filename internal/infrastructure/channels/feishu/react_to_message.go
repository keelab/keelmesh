package feishu

import (
	"context"
	"errors"
	"fmt"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func (c *Channel) ReactToMessage(ctx context.Context, _ string, messageID, reaction string) (func(), func(), error) {
	if !c.Running() {
		return nil, nil, errors.New("feishu: channel is not running")
	}
	if reaction == "" {
		reaction = "THUMBSUP"
	}
	emoji := larkim.NewEmojiBuilder().EmojiType(reaction).Build()
	response, err := c.client.Im.V1.MessageReaction.Create(ctx, larkim.NewCreateMessageReactionReqBuilder().MessageId(messageID).Body(larkim.NewCreateMessageReactionReqBodyBuilder().ReactionType(emoji).Build()).Build())
	if err != nil {
		return nil, nil, fmt.Errorf("feishu: create reaction: %w", err)
	}
	if !response.Success() || response.Data == nil || response.Data.ReactionId == nil {
		return nil, nil, fmt.Errorf("feishu: create reaction failed: code=%d message=%s", response.Code, response.Msg)
	}
	reactionID := *response.Data.ReactionId
	remove := func() {
		_, _ = c.client.Im.V1.MessageReaction.Delete(context.Background(), larkim.NewDeleteMessageReactionReqBuilder().MessageId(messageID).ReactionId(reactionID).Build())
	}
	return func() {}, remove, nil
}
