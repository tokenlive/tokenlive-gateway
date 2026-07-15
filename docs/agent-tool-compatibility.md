# Agent 工具兼容指南

> TokenLive 网关通过协议转换能力，使主流 AI Agent 开发工具能够配置第三方大模型使用。本文概述支持的 Agent 工具、兼容性处理细节及能力边界。

## 概述

TokenLive 网关在 Provider 适配层（ProviderInvoker）实现了跨协议的自动翻译。当 Agent 工具以特定协议格式（如 Anthropic Messages、OpenAI Responses）发起请求时，网关会自动将其翻译为上游 Provider 实际支持的协议格式（如 OpenAI Chat Completions），并将响应（含流式 SSE）逆向翻译回 Agent 工具期望的格式。整个过程对 Agent 工具完全透明。

这一能力使得 Claude Code、Codex 等 Agent 工具无需关心后端模型来源的协议差异，只需将 API 地址指向 TokenLive 网关并配置模型名称，即可使用任意已接入的第三方大模型。

## 已适配的 Agent 工具

### Claude Code

Claude Code 使用 Anthropic Messages 协议（`/v1/messages` 端点）与后端通信。TokenLive 网关对该协议的兼容处理包括：

- **协议翻译**：将 Anthropic Messages 请求体翻译为 OpenAI Chat Completions 格式发送给上游，响应体逆向翻译回 Anthropic 格式。涵盖 system prompt 合并、max_tokens 映射、stop_sequences 转换、tool_choice 语义翻译等核心字段。
- **流式 SSE 帧级转换**：将上游 OpenAI SSE 事件实时解码并重组为 Anthropic 协议事件（message_start、content_block_delta、message_stop 等），保持低时延流式体验。
- **连通性探测识别**：Claude Code 在启动时会发送探测请求（特征：max_tokens=1、单条 content 为 "." 的消息）。网关自动识别此类请求并返回符合 Anthropic 协议的模拟响应，避免不必要的上游调用。
- **Anthropic SDK 兼容处理**：system prompt 中以 `x-anthropic-` 开头的元数据行自动剔除；thinking 字段的 adaptive→auto 类型映射；ID 归一化（`chatcmpl-` → `msg_` 前缀、`call_` → `toolu_` 前缀）；响应头 `anthropic-version` 回写等。
- **第三方端点参数净化**：针对第三方 OpenAI 兼容上游不支持的字段（top_k、metadata、output_config、thinking 等）自动剔除，防止 400 报错。

### Codex

Codex 使用 OpenAI Responses 协议（`/v1/responses` 端点）与后端通信。TokenLive 网关对该协议的兼容处理包括：

- **协议降级翻译**：将 Responses API 请求体翻译为 Chat Completions 格式发送给上游。涵盖 instructions→system message 合并、input→messages 映射、max_output_tokens→max_completion_tokens 转换等。
- **流式 SSE 帧级转换**：将上游 Chat Completions SSE 流实时转换为 Responses 协议格式事件输出。
- **非标准 tools 格式纠正**：Codex 发送的 tools 字段可能使用 namespace 嵌套或平铺的自定义格式。网关自动将其打平并归一化为标准的 `{"type":"function","function":{...}}` 嵌套结构，同时过滤无 name 的无效工具定义。
- **XML 工具调用流式拦截**：针对 Codex 在流式输出中以 XML 标签包裹工具调用的行为，网关内置了 XML 解析与剥离逻辑，确保工具调用格式的一致性。

## 能力边界

**当前支持：**

- Anthropic Messages ↔ OpenAI Chat Completions 双向翻译（请求+响应，含流式）
- OpenAI Responses → OpenAI Chat Completions 降级翻译（请求+响应，含流式）
- Endpoint 级别的协议覆盖（同一 Provider 下不同端点可暴露不同协议）
- 针对第三方 OpenAI 兼容端点的参数净化与兼容性适配

**不在当前范围：**

- 反向翻译（将 OpenAI Chat 请求翻译为 Anthropic Messages 格式发送给上游）尚未实现，架构上已预留扩展点。
- 非 OpenAI / Anthropic 协议族（如 Google Gemini 原生协议）的翻译尚未实现。

## 架构参考

- 协议翻译的核心架构决策记录在 [ADR-0015](./adr/0015-protocol-translation-at-provider-invoker.md)。
- Endpoint 级协议覆盖机制记录在 [ADR-0012](./adr/0012-protocol-override-at-endpoint.md)。
- 协议翻译实施计划记录在 [docs/plans/2026-06-06-protocol-translation.md](./plans/2026-06-06-protocol-translation.md)。
