package media

import (
	"fmt"
	"path/filepath"
	"strings"
)

type Repository struct {
	root string
}

func NewRepository(root string) (*Repository, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("channelcore: media root is required")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("channelcore: resolve media root: %w", err)
	}
	return &Repository{
		root: filepath.Clean(root),
	}, nil
}
