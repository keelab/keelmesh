# 配置与开发命令参考

## 常用命令

| 命令 | 作用 |
| --- | --- |
| `make check` | 格式检查、`go vet`、单元测试 |
| `make lint` | 运行 golangci-lint |
| `make test-race` | 运行竞态检测 |
| `make build` | 构建 HTTP 和 gRPC 示例目标 |
| `make generate` | 运行 API 生成流程 |
| `make migrate-up` | 应用全部迁移 |
| `make migrate-down` | 回滚一条迁移 |

## 环境变量

数据库迁移脚本要求 `DATABASE_URL`。不要把连接串、令牌或密码写入仓库；本地敏感文件已被 `.gitignore` 排除。

## 生成 API

`./scripts/generate.sh` 会运行 `buf dep update`、`buf lint`、`buf generate`，并复制生成的 OpenAPI 文件。修改接口时应审阅生成文件差异并在 PR 中说明兼容性影响。
