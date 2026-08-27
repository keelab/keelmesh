# 架构概览

## 主要组件

仓库包含七个命令入口：`agentcore`、`apicore`、`channelcore`、`contextcore`、`fabriccore`、`gatecore` 和 `loopcore`。应用层、基础设施层和传输层分别位于 `internal/application`、`internal/infrastructure` 和 `internal/transport`。

## 服务边界

Protocol Buffers 定义位于 `api/proto/*/v1`，生成的 Go 与 gRPC 代码位于 `gen/*/v1`。Channel 负责对接 DingTalk、飞书、QQ、Telegram、Webhook 和企业微信等通道；Agent、Gate、Loop 通过客户端和服务接口与其协作。

## 数据与迁移

数据库迁移位于 `migrations/20260826`，包括 Gate 任务、Agent 事件、审计、投递 outbox 和 Loop 运行记录。迁移通过 `scripts/migrate.sh` 执行，并要求显式提供 `DATABASE_URL`。

## 验证边界

`make check` 覆盖格式、静态检查和 Go 单元测试；它不会连接外部数据库或第三方消息平台。生产部署拓扑和具体配置应以部署环境的密钥、网络和运行手册为准。
