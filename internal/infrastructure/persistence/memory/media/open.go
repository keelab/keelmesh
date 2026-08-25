package media

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/keelab/keelmesh/internal/domain"
)

func (s *Repository) Open(ctx context.Context, ref string) (domain.MediaEntity, error) {
	if err := ctx.Err(); err != nil {
		return domain.MediaEntity{}, err
	}
	ref = strings.TrimSpace(ref)
	if !strings.HasPrefix(ref, "media://") {
		return domain.MediaEntity{}, fmt.Errorf("channelcore: media reference must use media:// scheme")
	}
	id := strings.TrimPrefix(ref, "media://")
	if id == "" || filepath.Base(id) != id || strings.Contains(id, string(filepath.Separator)) {
		return domain.MediaEntity{}, fmt.Errorf("channelcore: invalid media reference %q", ref)
	}
	file, err := os.Open(filepath.Join(s.root, id))
	if err != nil {
		return domain.MediaEntity{}, fmt.Errorf("channelcore: open media %q: %w", ref, err)
	}
	return domain.MediaEntity{
		Reader: file,
	}, nil
}
