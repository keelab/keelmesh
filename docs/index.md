# Keelmesh 文档

Keelmesh 是一个由 Channel、Agent、Gate 和 Loop 组成的 Go 服务集合。本索引按“先运行、再理解、最后运维”的顺序组织内容。

## 推荐路径

1. [开发环境与验证](getting-started/development.md)
2. [架构概览](concepts/architecture.md)
3. [项目结构](concepts/project-structure.md)
4. [配置与迁移](reference/configuration.md)
5. [贡献指南](../CONTRIBUTING.md)

## 主题

- [入门](getting-started/development.md)：安装工具、运行质量门和构建服务。
- [概念](concepts/architecture.md)：理解进程边界、接口生成和主要执行流。
- [参考](reference/configuration.md)：查看 Make 目标、脚本和配置入口。

## 文档约定

命令均从仓库根目录执行；需要外部服务、凭据或生产权限的操作会明确标注。若代码与文档不一致，以当前代码和 CI 结果为准，并欢迎提交修正 PR。
