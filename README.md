# Keelmesh

[![CI](https://github.com/keelab/keelmesh/actions/workflows/ci.yml/badge.svg)](https://github.com/keelab/keelmesh/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/keelab/keelmesh.svg)](https://pkg.go.dev/github.com/keelab/keelmesh)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Keelmesh 是一个基于 Go 的消息通道与 Agent 运行时服务集合，提供 Channel、Agent、Gate 和 Loop 等进程边界，以及 HTTP/gRPC 和 Protocol Buffers 接口。

## 项目状态

项目仍在积极演进中。公开 API、数据库迁移和配置格式可能发生不兼容变化；请在升级前阅读 [CHANGELOG.md](CHANGELOG.md) 和迁移说明。

## 能力概览

- Channel：统一消息、媒体、流式消息、入站事件和通知能力。
- Agent：提供可替换的 Agent 后端与任务事件处理。
- Gate：提供任务治理、审计、投递 outbox 和持久化接口。
- Loop：编排运行过程，并通过服务边界连接 Agent、Gate 与 Channel。
- 传输与生成代码：使用 Protocol Buffers、gRPC 和 HTTP 生成类型安全的接口。

## 快速开始

### 环境要求

- Go 1.26.7 或与 `go.mod` 中 `go` 指令兼容的更高版本。
- `buf`（仅在修改 `.proto` 文件或重新生成 API 时需要）。
- `golangci-lint`（仅在本地运行完整 lint 时需要）。

### 验证源码

```bash
git clone https://github.com/keelab/keelmesh.git
cd keelmesh
make check
```

`make check` 会依次执行格式检查、`go vet ./...` 和 `go test ./...`。

### 构建服务

```bash
make build
```

当前构建目标以仓库中的 Makefile 为准。服务启动所需的配置和外部依赖请参阅 [文档索引](docs/index.md)。

## 文档

- [文档索引](docs/index.md)
- [架构概览](docs/concepts/architecture.md)
- [开发指南](CONTRIBUTING.md)
- [安全政策](SECURITY.md)
- [变更记录](CHANGELOG.md)

## 参与贡献

请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。所有贡献都需要通过 CI，并遵守 [行为准则](CODE_OF_CONDUCT.md)。

## 许可证

本项目以 Apache License 2.0 授权，详见 [LICENSE](LICENSE)。
