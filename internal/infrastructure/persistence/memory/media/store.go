package media

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/keelab/keelmesh/internal/domain"
)

func (s *Repository) Store(ctx context.Context, filename, contentType string, source io.Reader) (domain.MediaPartEntity, error) {
	if err := ctx.Err(); err != nil {
		return domain.MediaPartEntity{}, err
	}
	if source == nil {
		return domain.MediaPartEntity{}, fmt.Errorf("channelcore: media source is nil")
	}
	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return domain.MediaPartEntity{}, fmt.Errorf("channelcore: generate media id: %w", err)
	}
	id := hex.EncodeToString(idBytes[:])
	if err := os.MkdirAll(s.root, 0o750); err != nil {
		return domain.MediaPartEntity{}, fmt.Errorf("channelcore: create media root: %w", err)
	}
	path := filepath.Join(s.root, id)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return domain.MediaPartEntity{}, fmt.Errorf("channelcore: create media object: %w", err)
	}
	if _, err = io.Copy(file, source); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return domain.MediaPartEntity{}, fmt.Errorf("channelcore: store media: %w", err)
	}
	if err = file.Close(); err != nil {
		_ = os.Remove(path)
		return domain.MediaPartEntity{}, fmt.Errorf("channelcore: close media object: %w", err)
	}
	return domain.MediaPartEntity{Ref: "media://" + id, Filename: filepath.Base(filename), ContentType: contentType}, nil
}
