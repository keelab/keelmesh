package channel

import (
	"sort"

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

// Catalog returns configured channels together with the built-in channel kinds
// that are currently disabled or not configured.
func (r *Repository) Catalog() []domain.DefinitionEntity {
	knownKinds := []string{"dingtalk", "feishu", "qq", "telegram", "webhook", "wecom", "wecom_aibot", "wecom_app"}
	definitions := r.List()
	known := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		known[definition.Kind] = struct{}{}
	}
	for _, kind := range knownKinds {
		if _, ok := known[kind]; ok {
			continue
		}
		definitions = append(definitions, domain.DefinitionEntity{ID: kind, Kind: kind})
	}
	sort.Slice(definitions, func(i, j int) bool {
		if definitions[i].Kind != definitions[j].Kind {
			return definitions[i].Kind < definitions[j].Kind
		}
		return definitions[i].ID < definitions[j].ID
	})
	return definitions
}
