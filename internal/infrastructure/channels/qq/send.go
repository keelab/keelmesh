package qq

import (
	"context"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/tencent-connect/botgo/dto"
)

func (c *Channel) Send(ctx context.Context, msg domain.Outbound) (domain.ReceiptEntity, error) {
	if strings.HasPrefix(msg.TargetID, "group:") {
		_, err := c.api.PostGroupMessage(ctx, strings.TrimPrefix(msg.TargetID, "group:"), &dto.MessageToCreate{Content: msg.Content})
		if err != nil {
			return domain.ReceiptEntity{}, err
		}
	} else {
		_, err := c.api.PostC2CMessage(ctx, strings.TrimPrefix(msg.TargetID, "user:"), &dto.MessageToCreate{Content: msg.Content})
		if err != nil {
			return domain.ReceiptEntity{}, err
		}
	}
	return domain.ReceiptEntity{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}
