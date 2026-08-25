package media

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (s *Repository) Release(ctx context.Context, ref string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ref = strings.TrimSpace(ref)
	if !strings.HasPrefix(ref, "media://") {
		return fmt.Errorf("channelcore: media reference must use media:// scheme")
	}
	id := strings.TrimPrefix(ref, "media://")
	if id == "" || filepath.Base(id) != id || strings.Contains(id, string(filepath.Separator)) {
		return fmt.Errorf("channelcore: invalid media reference %q", ref)
	}
	if err := os.Remove(filepath.Join(s.root, id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
