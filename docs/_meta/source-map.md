# 文档证据映射

| 文档 | 依据 | 验证 |
| --- | --- | --- |
| `README.md` | `go.mod`, `Makefile`, `cmd/`, `api/proto/` | `make check` |
| `docs/getting-started/development.md` | `go.mod`, `Makefile` | `go mod download`, `make check` |
| `docs/concepts/architecture.md` | `cmd/`, `internal/`, `api/proto/`, `migrations/` | `make check` |
| `docs/reference/configuration.md` | `Makefile`, `scripts/` | 人工审阅 |
