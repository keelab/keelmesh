package channel

import (
	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) List() []domain.DefinitionEntity {
	channels := r.registry.All()
	definitions := make([]domain.DefinitionEntity, 0, len(channels))
	for _, channel := range channels {
		definitions = append(definitions, channel.Definition())
	}
	return definitions
}
