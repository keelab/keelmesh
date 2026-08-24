package channelcore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// MediaStore is the channelcore-owned boundary for attachment lifetime.
// Channels receive managed references, never arbitrary filesystem paths.
type MediaStore interface {
	Store(context.Context, string, string, io.Reader) (MediaPart, error)
	Open(context.Context, string) (MediaResource, error)
	Release(context.Context, string) error
}

type MediaResource struct {
	Reader      io.ReadCloser
	Filename    string
	ContentType string
}

type FileMediaStore struct{ root string }

func NewFileMediaStore(root string) (*FileMediaStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("channelcore: media root is required")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("channelcore: resolve media root: %w", err)
	}
	return &FileMediaStore{root: filepath.Clean(root)}, nil
}

func (s *FileMediaStore) Store(ctx context.Context, filename, contentType string, source io.Reader) (MediaPart, error) {
	if err := ctx.Err(); err != nil {
		return MediaPart{}, err
	}
	if source == nil {
		return MediaPart{}, fmt.Errorf("channelcore: media source is nil")
	}
	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return MediaPart{}, fmt.Errorf("channelcore: generate media id: %w", err)
	}
	id := hex.EncodeToString(idBytes[:])
	if err := os.MkdirAll(s.root, 0o750); err != nil {
		return MediaPart{}, fmt.Errorf("channelcore: create media root: %w", err)
	}
	path := filepath.Join(s.root, id)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return MediaPart{}, fmt.Errorf("channelcore: create media object: %w", err)
	}
	if _, err = io.Copy(file, source); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return MediaPart{}, fmt.Errorf("channelcore: store media: %w", err)
	}
	if err = file.Close(); err != nil {
		_ = os.Remove(path)
		return MediaPart{}, fmt.Errorf("channelcore: close media object: %w", err)
	}
	return MediaPart{Ref: "media://" + id, Filename: filepath.Base(filename), ContentType: contentType}, nil
}

func (s *FileMediaStore) Open(ctx context.Context, ref string) (MediaResource, error) {
	if err := ctx.Err(); err != nil {
		return MediaResource{}, err
	}
	ref = strings.TrimSpace(ref)
	if !strings.HasPrefix(ref, "media://") {
		return MediaResource{}, fmt.Errorf("channelcore: media reference must use media:// scheme")
	}
	id := strings.TrimPrefix(ref, "media://")
	if id == "" || filepath.Base(id) != id || strings.Contains(id, string(filepath.Separator)) {
		return MediaResource{}, fmt.Errorf("channelcore: invalid media reference %q", ref)
	}
	file, err := os.Open(filepath.Join(s.root, id))
	if err != nil {
		return MediaResource{}, fmt.Errorf("channelcore: open media %q: %w", ref, err)
	}
	return MediaResource{Reader: file}, nil
}

func (s *FileMediaStore) Release(ctx context.Context, ref string) error {
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
