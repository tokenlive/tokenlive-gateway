# Provider RequestTypes 运行时校验

Provider 的 RequestTypes（支持的 request_type 列表）在运行时路由阶段校验，而非启动时。

启动时校验的问题：Provider 的 RequestTypes 可能随运行时状态变化（如热重载配置、动态注册），启动时校验无法覆盖这些场景。

运行时校验的流程：路由时从 model_providers 获取候选 provider 列表 → 过滤掉 RequestTypes 不包含该 model 的 request_type 的 provider → 从剩余 provider 中按 priority/weight 选择。无效绑定不报错，仅跳过。
