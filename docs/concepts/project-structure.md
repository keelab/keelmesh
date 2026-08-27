# 项目结构

| 路径 | 用途 |
| --- | --- |
| `cmd/` | 各进程的可执行入口 |
| `api/proto/` | Protocol Buffers 源定义 |
| `gen/` | 提交到仓库的生成代码 |
| `internal/application/` | 用例和领域服务 |
| `internal/infrastructure/` | 通道、客户端、数据库等适配器 |
| `internal/transport/` | HTTP、gRPC、WebSocket 和中间件 |
| `configs/` | 本地/开发配置示例 |
| `migrations/` | 数据库 up/down 迁移 |
| `scripts/` | 检查、生成、lint 和迁移脚本 |
