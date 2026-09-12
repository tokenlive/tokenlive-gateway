# Gateway 版本上报

版本上报用于专业版 Admin 的「关于」和更新提醒，不是节点管理或自动升级系统。上报的是本进程构建版本与 build kind，不是镜像标签；本地构建默认 `dev`，只有受控正式构建才标记 `release`。无法识别的版本仍可显示，但不参与稳定版升级比较。

## 通道和配置

引擎成功构建后立即异步上报一次，此后每 30 秒上报；单次发送最多 5 秒。上报不在用户请求路径中，不阻断 Gateway 启动后的业务请求。配置为 `embedded` 的 all-in-one 不启动专业版节点上报。

| 条件 | 固定使用的通道 |
| --- | --- |
| 已有配置的 Redis 客户端 | 直接写 Redis，优先级最高 |
| 无 Redis，配置了 `ADMIN_SERVER_URL` 和非空 `GATEWAY_SYNC_TOKEN` | POST 到 Admin 的 `/api/v1/gateway/version` |
| 两者均不可用 | 不启动上报，正常提供网关服务 |

Redis 发送失败不会自动切到 HTTP。双方需要相同 Redis DB（若走 Redis）和 `GATEWAY_VERSION_NAMESPACE`；namespace 默认 `default`、区分大小写、允许 1–64 位字母/数字/`_`/`-`。同库的独立部署使用不同 namespace，避免互相观测。

HTTP 通道复用部署同步 token，不使用用户 JWT。Admin 必须显式配置同值的 `GATEWAY_SYNC_TOKEN`；空 token、错误 token 或其他 namespace 会被拒绝。报告上限 4 KiB。HTTP sender 不跟随重定向，避免同步 token 被带至其他目标；请配置正确的 Admin 基础 URL。

## 协议与存活窗口

```json
{
  "schema_version": 1,
  "namespace": "default",
  "instance_id": "11111111-1111-4111-8111-111111111111",
  "version": "v1.2.3",
  "build_kind": "release"
}
```

示例 UUID 仅用于说明；真实 UUID 在进程启动时生成，在该进程内稳定。重启后的旧 UUID 记录自行过期。Redis 键为 `tokenlive:gateway-versions:<namespace>:<UUID>`，每次发送覆盖写并设置 3 分钟 TTL，无永久索引。Admin 以自身接收时间或 Redis TTL 判定有效性，不信任客户端时钟。

Admin 只对外显示 `{version, build_kind, count}` 聚合，不公开 UUID。3 分钟未上报的节点消失，其旧版本提示也随读取重新计算，且不重新请求外部发行来源。关闭 Admin 的 `UPDATE_CHECK_ENABLED` 只停止公网检查，内部上报照常。

Redis sender 使用自己拥有的、绑定发送 context 的短连接，不修改或关闭应用共享 Redis 客户端。自定义网络调用方可使用 `NewRedisSenderWithDialer`；dialer 必须遵守 context 并返回独占且已完成 TLS（如适用）的连接。

## 兼容和故障表现

- 新 Admin + 旧 Gateway：没有新协议报告时显示 `unknown`，不猜版本。
- 旧 Admin + 新 Gateway：缺少 HTTP 接口时发送失败，最多每分钟记录一次不含 token/URL/响应正文的警告，下一周期重试；业务继续。
- 无 Redis 的多个 Admin：内存注册表仅看到发送到该实例的节点；负载均衡不保证各实例分布一致。需要跨实例一致观测时使用共享 Redis DB/namespace。
- 专业版候选分别来自 Admin/Gateway 仓库的 latest 稳定 Release，不借用 standalone Homebrew 来源；当前开发构建不能被识别为正式版。
- 首次启用仍需人工升级，控制台不会安装、重启或执行远程命令。

## 隔离验证和清理

`go test -race ./pkg/versionreport` 验证协议、超时和发送生命周期。跨组件测试在 standalone 的 `internal/assemble/version_integration_test.go`：真实 Sender → Admin 公共 facade/HTTP receiver → 匿名聚合与更新结果，包含错误 token、双通道去重、关闭外部检查及 TTL 清理。

测试仅使用内存、临时 SQLite、独立 miniredis 和受控 Release 响应；节点有 TTL 和按键删除的测试清理。无需连接开发/生产 Redis，无需真实用户、发布操作或部署。联合源码使用临时 go.work，不把本机路径写入模块配置；发布仍需选择实际存在的兼容模块 tag。Admin/standalone 的配套文档记录了干净机器依赖与镜像验收门槛。
