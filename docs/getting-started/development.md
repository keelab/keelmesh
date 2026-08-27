# 开发环境与首次验证

**级别：** 入门  ·  **时间：** 5 分钟

## 前置条件

- Go 版本与 `go.mod` 的 `go 1.26.7` 兼容。
- Git。

## 验证

```bash
git clone https://github.com/keelab/keelmesh.git
cd keelmesh
go mod download
make check
```

成功时会看到 `go vet ./...` 和 `go test ./...` 完成且没有失败包。该检查不需要数据库、消息平台凭据或生产配置。

## 构建

```bash
make build
```

构建目标和输出路径由 [Makefile](../../Makefile) 定义。修改 `.proto` 文件后，先安装 `buf`，再运行 `./scripts/generate.sh`。

## 下一步

阅读[架构概览](../concepts/architecture.md)，然后按 [贡献指南](../../CONTRIBUTING.md) 创建分支和 Pull Request。
