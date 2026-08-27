# 贡献指南

感谢参与 Keelmesh。提交代码前，请先阅读 [行为准则](CODE_OF_CONDUCT.md) 和 [安全政策](SECURITY.md)。

## 开发环境

1. 安装与 `go.mod` 兼容的 Go 版本。
2. 克隆仓库并创建 `feat/`、`fix/` 或 `chore/` 前缀的分支。
3. 使用 `go mod download` 准备依赖。
4. 修改 Protocol Buffers 时运行 `./scripts/generate.sh`，并提交生成代码。

## 提交前检查

```bash
make check
```

需要额外检查时运行 `make lint`、`make test-race` 或 `make build`。不要提交密钥、`.env`、`.secrets/`、编辑器配置或本地构建产物。

## 提交与 Pull Request

- 提交信息使用 Conventional Commits，例如 `feat: add gate task events`、`fix: handle nil channel`。
- 一个 PR 聚焦一个主题，说明动机、主要改动、测试结果、迁移影响和已知限制。
- API 或 schema 变更必须同时更新生成代码、文档和兼容性说明。
- CI 必须通过；维护者可能要求补充测试、文档或变更记录。
- PR 合并采用 squash merge，提交者无需自行合并。

## 数据库迁移

迁移文件必须成对提供 up/down 脚本，使用递增目录和序号命名，并说明回滚风险。不要在没有备份和审批的情况下对共享数据库运行迁移。
