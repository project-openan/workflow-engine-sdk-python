# A2A-T Workflow Engine SDK（Python）

面向宿主智能体的嵌入式工作流执行引擎。引擎负责工作流 DAG、A2A 消息封装、任务与会话关联、远端任务等待、Negotiation-T 交互循环和生命周期；宿主负责业务输入解释、A2A-T 内容生成与语义校验，以及路由和协商决策。

Python 与 Java 版本遵循同一业务契约。Python 当前要求 Python 3.12+、`a2a-sdk>=1.1.2,<2` 和 `a2a-t-sdk>=1.0.9,<2`。

## 安装

```bash
python -m pip install workflow-exec-engine
```

执行引擎直接使用最新版 A2A-T core 元数据类型识别协议，但不会调用 LLM，也不会替宿主生成或校验 Task-T、Negotiation-T、Authorization-T、Notification-T 的业务内容；这些内容操作仍由宿主调用 `a2a-t-sdk` 完成。

## 最小集成

```python
from a2a.types import Part
from a2a_t.client import A2ATClient
from a2a_t.core.standard_templates import PRIVATE_LINE_COMPLAINT_URI
from workflow_engine import (
    A2ATransport, A2atMessages, ControlPoint, ExtensionSender,
    MessageContent, NegotiationReply, RouteDecision, TaskResult,
    WorkflowEngineClient, WorkflowExecutor,
)


class BusinessCallbacks(ControlPoint):
    async def on_task(self, request):
        # a2a-t-sdk 负责内容生成；执行引擎只接收生成结果并发送。
        generated = a2at.generate_task_prompt_from_text(
            request.input.text,
            PRIVATE_LINE_COMPLAINT_URI,
        )
        return A2atMessages.from_generated(
            generated,
            [Part(text=request.instruction)],
        )

    async def on_self_task(self, request):
        outputs = aggregate(request.workflow_input.upstream_results)
        return TaskResult.succeeded(outputs)

    async def on_route(self, request):
        # 每条非空条件边独立调用；允许多条边同时返回 true。
        return RouteDecision.allow() if matches(request.condition) else RouteDecision.deny()

    async def on_negotiation(self, request):
        generated = generate_terminal_negotiation_reply_with_a2at(request)
        return NegotiationReply.send(
            A2atMessages.from_generated(generated, [Part(text="补充后的业务内容")])
        )


a2at = A2ATClient(env_path=a2at_env_path)
transport = A2ATransport(agent_cards=agent_cards)
client = WorkflowEngineClient(transport)
result = await WorkflowExecutor(workflow, BusinessCallbacks(), client).run()
await transport.close()
```

`on_task` 不接收客户端对象，也不自行发送消息。这样业务代码只负责最终内容，引擎统一保证 A2A 信封、上下文 ID、远端任务 ID、协议头和后续协商仍属于同一次任务交互。

## 路由规则

- 空条件边：无需调用 `on_route`，默认放行。
- 非空条件边：每条边独立调用一次 `on_route(RouteRequest)`。
- 一个节点可激活 0 到 N 条后继边；全部拒绝表示该分支正常结束。
- 同一节点的全部条件判断完成后才调度后继节点；任一判断异常时不调度任何后继节点。

## 上游结果

引擎不再把前置结果拼成 `Runtime Context` Markdown。回调通过 `request.workflow_input` 获取：

- `runtime_intent`：本次执行的运行时意图；
- `upstream_results`：按步骤分组的上游任务结果；
- 每个任务结果包含有序 `outputs`、来源 Agent/skill、逻辑任务 ID、状态和安全错误信息。

工作流的 `context_from` 控制聚合范围：省略时使用直接前驱，`["*"]` 使用全部祖先，空列表表示不传上游结果，指定名称时仅传对应祖先步骤。

## 独立协议操作

Authorization-T 和 Notification-T 不属于工作流因果链，应使用独立 `A2ATransport` 和 `ExtensionSender`：

```python
sender = ExtensionSender(independent_transport)
authorization = await sender.send_authorization(agent_name, authorization_content)

subscription = sender.open_notification(agent_name, notification_content, on_notification)
ack = await subscription.acknowledgement
# 收到业务结果后由集成方显式关闭：
subscription.close()
await subscription.completion
```

授权或订阅失败不会自动阻断后续工作流。`send_authorization` 会等待授权任务到达终态，集成方通过 `is_success` 判断结果；订阅确认通过 `is_failure` 排除失败、拒绝或取消。订阅确认与长连接结束是两个不同 Future；`heartbeat` 和 `is_healthy()` 可用于本地存活性判断。

## 远端任务管理

```python
await client.get_task(agent_name, task_id)
page = await client.list_tasks(agent_name, list_tasks_request)
await client.cancel_task(agent_name, task_id)
await client.subscribe_to_task(agent_name, task_id, on_event)
```

这些是标准 A2A 任务接口；取消任务不等同于 Negotiation-T Abort。

同一个 `WorkflowEngineClient` 同一时刻只能绑定一个工作流执行；并发执行应创建独立 client。协商在本地停止、超时、取消或协议校验失败时，引擎会对已知远端任务做有界的尽力取消，取消失败只记录日志，不覆盖原始错误。

## 认证与 TLS

`A2ATransport` 支持 AgentCard 声明的凭据配置、自定义 `AuthProvider`、自定义 CA、mTLS 客户端证书、CRL、协议偏好和发送超时。默认验证服务端证书；指定的证书文件缺失或无效时启动失败，不会静默降级为不校验。

仅受控开发环境可显式配置 `ssl_verify=False`。调用方传入的 `httpx.AsyncClient` 仍由调用方关闭；执行引擎只关闭自己创建的客户端资源。

## 文档与验证

- [设计说明](DESIGN.md)
- [集成开发指南](DEVELOPER_GUIDE.md)
- [英文 README](README_en.md)

```bash
python -m pip install -e ".[dev]"
python -m pytest -q
python -m build
```
