> 代码位置：F:\workspace\ai\deer-flow
> 功能描述：deer-flow 项目全生命周期深入分析。
> 重点关注问题：未单独填写；按整个项目分析，覆盖运行协作、核心实现、状态与资源生命周期、失败路径和适用边界。

# 1. DeerFlow 智能体实现深度分析：阅读指南与边界

DeerFlow 将模型驱动的多轮工具执行组织成可保存状态、委派任务、请求人工输入并持续输出事件的 agent harness。主要使用者是服务操作者、通过 Web/IM 提交任务的用户及直接嵌入 Harness 的 Python 调用方。生成工具调用只是执行意图；授权、分发、回填、持久终态确认和资源清理是不同阶段，不能合并成“模型已完成任务”。

分析基于本地提交 **90ea0a3748e6315a2b530b9a8ce080fca4377487** 的工作树；其上一提交为 **5f6d7c06e0896f4c92aebe7bfb01f5a5d21952ae**，两者之间只增加既有分析材料，业务源码未变。本次仅静态阅读源码、配置、依赖锁和已有测试，没有执行项目模块、服务、测试、容器、迁移或外部业务能力。测试引用代表预期契约，不代表本次通过。实际部署配置、模型、实例数和线上表现未知，不展示真实凭据或隐私数据。

全文围绕六种寿命展开：进程、thread、run、模型与工具调用、子 Agent、后台任务。第 2 至 3 章建立工程与执行全貌，第 4 章解释九项协作机制，第 5 章逐函数讲解核心实现，第 6 章连接跨模块数据流，第 7 章区分已确认不一致、条件性风险和未知。

代码事实、条件推断与未知分别标明。注释或文档与实现冲突时明确指出。源码链接相对本文所在 `docs` 目录；`#L…` 标记记录本次快照的行号，支持源码行锚点的仓库浏览器可直接定位。本地 Markdown 阅读器若不处理行锚点，应按链接文件与标记行号阅读。分析不把局部覆盖宣称为全仓库完整覆盖：核心文件逐函数展开，关联文件按调用链展开；各 IM 协议、全部扩展业务及整个前端不在逐函数范围内。

Checkpoint 是图状态，不是外部副作用事务日志；run lease 决定运行写入权，sandbox ownership lease 决定容器销毁权；SSE 结束、业务完成、记忆更新完成和容器销毁各有独立条件。“恢复”既可能继续中断图，也可能仅将失联 run 收敛为 error，必须区分。

# 2. 总体架构与请求入口

## 2.1 工程形态与依赖版本

后端使用 Python，Harness 独立分发为 deerflow-harness，App 是 FastAPI Gateway 和 IM 集成；前端为 Next.js。App 可以导入 deerflow，Harness 不反向导入 app，仓库边界测试检查这一约束。LangChain 提供 create_agent 和中间件，LangGraph 提供图、channel、checkpoint、中断。声明要求 Python >=3.12，源码自身版本为 2.3.0-dev，不能据此确认线上版本。

源码依据：[Gateway 依赖声明](../backend/pyproject.toml#L1-L45) · [Harness 依赖声明](../backend/packages/harness/pyproject.toml#L1-L75) · [前端依赖声明](../frontend/package.json#L1-L45) · [已有断言 test_harness_boundary.py](../backend/tests/test_harness_boundary.py#L1-L47)

锁定版本为 langchain 1.3.14、langgraph 1.2.14、langgraph-checkpoint 4.3.0、FastAPI 0.136.1、sse-starlette 3.3.4。声明范围和锁定结果不同，例如 LangGraph 的声明是 >=1.2.9,<1.3；本机实际依赖环境未执行确认。

源码依据：[解析依赖版本](../backend/uv.lock#L2052-L2054) · [解析依赖版本](../backend/uv.lock#L2214-L2216) · [解析依赖版本](../backend/uv.lock#L2277-L2279) · [解析依赖版本](../backend/uv.lock#L1322-L1324) · [解析依赖版本](../backend/uv.lock#L4705-L4707)

## 2.2 请求入口与执行环境

下图区分 HTTP 边界和进程内执行。

~~~text
浏览器 / HTTP SDK → nginx :2026
                    ├─ /                  → Frontend :3000
                    ├─ /api/langgraph/*    → 重写 /api/* → Gateway :8001
                    └─ 其他 /api/*         → Gateway :8001
                                                → 认证/归属/准入
                                                → 内嵌 LangGraph runtime
                                                → 模型、工具、沙箱、checkpoint
Python 调用方 → DeerFlowClient → 同进程图执行（无 HTTP/SSE）
可选 Studio → langgraph.json → deerflow.agents:make_lead_agent
~~~

常规服务的 runtime 嵌入 Gateway。nginx 明确将 /api/langgraph/* 重写后交给 Gateway；langgraph.json 保留另一入口，声明 lead_agent、认证、HTTP App 和 checkpointer 工厂。模型和容器可以跨进程，普通 tool 也可以只做进程内计算；工具名称本身不提供隔离保证。Provisioner 仅在对应 backend 选择下参与。

源码依据：[nginx.conf 实现](../docker/nginx/nginx.conf#L92-L125) · [nginx.conf 实现](../docker/nginx/nginx.conf#L346-L365) · [langgraph.json 实现](../backend/langgraph.json#L1-L21) · [deps.py 实现](../backend/app/gateway/deps.py#L772-L1127) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L349-L384)

## 2.3 输入、身份与 worker

stream_run/create_run → start_run → normalize_input/build_run_config → RunManager 准入 → 注册 run_agent。返回 RunRecord 表示接受运行，实际模型执行在 worker 中发生。服务端先剥离客户伪造的内部上下文和状态元数据，再注入认证身份；持久历史不能授予当前权限。调度任务和 MCP 通知也经宿主入口建立受控 context，而非让模型自行构造可信能力。

源码依据：[thread_runs.py 实现](../backend/app/gateway/routers/thread_runs.py#L1091-L1138) · [thread_runs.py 实现](../backend/app/gateway/routers/thread_runs.py#L1073-L1088) · [services.py 实现](../backend/app/gateway/services.py#L2295-L3064) · [services.py 实现](../backend/app/gateway/services.py#L769-L848) · [services.py 实现](../backend/app/gateway/services.py#L1479-L1608) · [services.py 实现](../backend/app/gateway/services.py#L1261-L1340) · [services.py 实现](../backend/app/gateway/services.py#L3065-L3168)

## 2.4 启动形态与配置生效顺序

根 Makefile 的 `config` 调用配置生成器，`install` 安装依赖，`dev` 先检查环境再调用 `serve.sh --dev`；生产启动使用同一编排脚本的 prod 模式。这里描述声明的启动方式，本次没有执行这些命令。Compose 开发配置另声明 Redis、Gateway、Frontend、nginx 和 profile 控制的 provisioner；存在配置文件不证明实际部署采用它。

源码依据：[配置和安装目标](../Makefile#L87-L119) · [开发与生产编排](../Makefile#L155-L174) · [Compose 服务声明](../docker/docker-compose-dev.yaml#L42-L94)

`AppConfig.resolve_config_path` 按显式参数、`DEER_FLOW_CONFIG_PATH`、调用者项目根目录、旧 backend/仓库位置选择路径。显式路径不存在就失败，不回退另一份配置。YAML 读取后先检查配置版本，再解析环境变量、补数据库默认值、合并 extensions 并执行模型校验。环境变量替换只处理整个字符串以 `$` 开头的情形，读取其后完整名称；它不是任意模板插值，缺失变量会抛错。本文不展示实际凭据。

源码依据：[路径选择](../backend/packages/harness/deerflow/config/app_config.py#L452-L480) · [YAML 装配](../backend/packages/harness/deerflow/config/app_config.py#L499-L547) · [环境变量解析](../backend/packages/harness/deerflow/config/app_config.py#L648-L670)

`extensions_config.json` 提供功能配置底稿，YAML 显式声明的 `extensions` 字段覆盖对应值，未声明项保留 JSON 值。它与可导入 Python 代码的 `plugins:` 不是同一信任面。AppConfig 缓存跟踪路径和内容签名，显式 reload 路径可被固定；配置重读不等于所有运行对象原地重建。checkpoint mode/cadence 的进程冻结，以及扩展在应用创建阶段注册，分别限制热更新边界。

源码依据：[extensions 覆盖顺序](../backend/packages/harness/deerflow/config/app_config.py#L519-L534) · [缓存和路径固定](../backend/packages/harness/deerflow/config/app_config.py#L765-L883) · [模式冻结](../backend/packages/harness/deerflow/runtime/checkpoint_mode.py#L1-L147) · [扩展装配时机](../backend/app/gateway/app.py#L1290-L1332)

前端声明 Next.js `^16.4.1`、React `^19.0.0` 与 LangGraph SDK `^1.5.3`，pnpm 锁解析为 Next.js 16.4.1、React 19.2.4、SDK 1.6.0。这些是仓库声明和解析结果，不确认本机 node_modules 或服务镜像版本。

源码依据：[前端声明](../frontend/package.json#L34-L115) · [SDK 解析结果](../frontend/pnpm-lock.yaml#L40-L46) · [Next.js 与 React 解析结果](../frontend/pnpm-lock.yaml#L181-L210)

## 2.5 模型、工具与扩展的实际绑定

主装配器按模型名选择配置，`create_chat_model` 根据 `use` 解析实际模型类。工厂分离元数据和供应商构造参数，合并 Agent 非空 sampling overrides，归一化 thinking/effort 与 endpoint，最后建立模型对象。对象创建成功与远端请求被接受是两个阶段；不同适配器的认证、超时和协议不能只凭统一类型外推。

源码依据：[模型解析和参数合并](../backend/packages/harness/deerflow/models/factory.py#L542-L640) · [reasoning 与供应商转换](../backend/packages/harness/deerflow/models/factory.py#L641-L714)

工具经过目录加载、Agent 组限制、授权过滤和延迟发现后才进入图。catalog 中存在工具不表示首轮已全部绑定。MCP 还涉及连接、路由、OAuth 与宿主配置；本文追踪项目侧装配与结果处理，不将外部 MCP server 的内部实现视为已审计。

源码依据：[目录、授权和延迟装配](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L1352-L1401) · [共享工具绑定](../backend/packages/harness/deerflow/tools/tools.py#L1-L115)

`load_extensions` 按 `plugins:` 顺序解析 `module:install`。禁用项不导入；启用项检查入口可调用和 extension API 兼容性，再记录 registry mark，带来源归属执行 install。失败默认记诊断后跳过，`required=true` 则使启动失败。安装回退按注册位置进行，避免同一入口多份配置时删掉先前成功实例。

源码依据：[扩展声明](../backend/packages/harness/deerflow/extensions/loader.py#L31-L80) · [兼容性和按位置回退](../backend/packages/harness/deerflow/extensions/loader.py#L198-L266)

**基于代码的推断：**注册回退不能撤销扩展导入或 install 自行产生的文件、网络或进程副作用。扩展以 Gateway 权限执行；`_frozen_config` 实际只复制顶层 dict，嵌套容器仍共享，不能把它理解成不可变配置沙箱。注册契约覆盖 middleware、router、task lifecycle、knowledge、模型观察者和 browser plugin 等，扩展业务不必全部硬编码到主循环。

源码依据：[执行与局部回退](../backend/packages/harness/deerflow/extensions/loader.py#L244-L266) · [浅复制边界](../backend/packages/harness/deerflow/extensions/loader.py#L294-L303) · [注册能力](../backend/packages/harness/deerflow/extensions/registry.py#L111-L185)

**本章其他来源**

- `backend/pyproject.toml`：配置来源、默认值或声明。
- `backend/packages/harness/pyproject.toml`：配置来源、默认值或声明。
- `frontend/package.json`：配置来源、默认值或声明。
- `backend/tests/test_harness_boundary.py`：已有断言与预期契约，未执行。
- `backend/uv.lock`：依赖解析版本。
- `docker/nginx/nginx.conf`：服务编排与入口代理。
- `backend/langgraph.json`：关联实现或工程声明。
- `backend/app/gateway/deps.py`：HTTP 边界与宿主资源。
- `Makefile`：关联实现或工程声明。
- `docker/docker-compose-dev.yaml`：服务编排与入口代理。
- `backend/packages/harness/deerflow/config/app_config.py`：配置来源、默认值或声明。
- `backend/packages/harness/deerflow/runtime/checkpoint_mode.py`：运行控制与持久状态协作。
- `frontend/pnpm-lock.yaml`：依赖解析版本。
- `backend/packages/harness/deerflow/models/factory.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/tools/tools.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/extensions/loader.py`：扩展加载与注册契约。
- `backend/packages/harness/deerflow/extensions/registry.py`：扩展加载与注册契约。

# 3. 全生命周期：创建、运行、等待、恢复与销毁

## 3.1 进程创建与启动失败

app.lifespan 持有宿主服务；deps.langgraph_runtime 以 AsyncExitStack 持有运行依赖。顺序为冻结 checkpoint 模式/快照频率 → bridge → engine → checkpointer → Store → repositories/runtime → RunManager → 孤儿 run 收敛 → heartbeat。engine 先建立让创建数据库先于 checkpointer 建表；回调先登记再初始化，覆盖部分启动失败，释放则逆序执行。

源码依据：[app.py 实现](../backend/app/gateway/app.py#L625-L635) · [deps.py 实现](../backend/app/gateway/deps.py#L772-L1127)

必须先 drain 活动 run 再 close checkpoint pool，否则仍在写 checkpoint 的图任务会访问已关闭连接。宿主还分别关闭记忆、批任务、通道、MCP 和沙箱。有的清理受时间预算约束，有的已开始操作必须排空后再抛出取消，不能将 close 一概理解为立即完成。

源码依据：[deps.py 实现](../backend/app/gateway/deps.py#L484-L522) · [app.py 实现](../backend/app/gateway/app.py#L293-L325) · [app.py 实现](../backend/app/gateway/app.py#L587-L623)

## 3.2 对象和状态边界

Thread 是持久会话，run 是一次执行。create_thread 建立元数据和用户归属，checkpoint 保存图状态；同线程多轮有不同 run_id 和 owner_worker_id。tool_call_id 只保证响应内关联，provider 可以重复使用，因此委派还需 dispatch_message_id。文件和 sandbox 身份按 user/thread 组织。

源码依据：[threads.py 实现](../backend/app/gateway/routers/threads.py#L1008-L1019) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L276-L284) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L226-L234) · [identity.py 实现](../backend/packages/harness/deerflow/sandbox/identity.py#L26-L40)

下图表示状态分支，不是每次请求必经的直线。

~~~text
输入 → 准入 ├─ 同幂等键匹配 → 返回已有 run
            ├─ 冲突/越权/非法 → 拒绝
            └─ pending → running
                          ├─ 正常图结束 → success → 等待终态提交/清理
                          ├─ 人工中断 → waiting_for_input → 合法恢复/拒绝
                          ├─ 超时 → timeout
                          ├─ 异常 → error
                          └─ interrupt/rollback → interrupted 或失败收敛
失联 owner / 过期 lease → reconcile → error（不自动重跑工具）
~~~

RunStatus 没有单独的 cancelled 枚举，取消按具体控制路径映射；waiting_for_input 也不是业务完成。finalizing 和持久终态 ACK 是状态之外的条件，内存已改变不代表屏障已释放。

源码依据：[schemas.py 实现](../backend/packages/harness/deerflow/runtime/runs/schemas.py#L21-L29) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1420-L1469) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L709-L733) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L692-L695)

## 3.3 准入、重复请求与追加输入

_admit_thread_operation 仅接受 reject/interrupt/rollback。它建立 pending 记录，在 thread/run 局部锁与持久存储原子操作下决定是否接收。重复 idempotency_key 必须属于相同 user/thread；支持相应请求快照和 execution fence 才能承担对应幂等契约，旧第三方 RunStore 缺少能力时会拒绝。后续请求还需等待旧 run finalization，不能只看 running 标志。

源码依据：[manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2413-L2704) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2306-L2358) · [base.py 实现](../backend/packages/harness/deerflow/runtime/runs/store/base.py#L521-L573) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1862-L1893)

Queue/Steer 是 run 内输入控制，不是 multitask_strategy=enqueue。create_run_input → submit_run_input → RunInputs/RunInputControl → RunInputMiddleware 在安全执行边界消费，并记录 applied_run_inputs。取消输入、重试输入有独立控制面；这不证明外部工具可被任意即时替换。RunInputMiddleware 在 after_agent 反序阶段优先决定是否继续，随后才进入其他清理。

源码依据：[thread_runs.py 实现](../backend/app/gateway/routers/thread_runs.py#L110-L113) · [run_inputs.py 实现](../backend/app/gateway/run_inputs.py#L39-L65) · [input_control.py 实现](../backend/packages/harness/deerflow/runtime/runs/input_control.py#L81-L89) · [run_input_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/run_input_middleware.py#L80-L88) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L485-L822)

## 3.4 人工等待与恢复

Clarification 工具 wrapper 处理 ask_clarification；非交互运行不能等待无人响应的确认。工具审批另有 owner、run_id、request_id、interrupt-ID。answer_approval 检查归属及活动状态；validate_resume_request 对照 checkpoint 的待处理中断，不能仅因客户提交 resume 就执行。批准是允许继续动作，拒绝、过期和无效回复不是同一种结果。

源码依据：[clarification_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/clarification_middleware.py#L557-L580) · [tool_approvals.py 实现](../backend/app/gateway/tool_approvals.py#L55-L101) · [hitl_approval.py 实现](../backend/app/gateway/hitl_approval.py#L50-L64) · [hitl_approval.py 实现](../backend/app/gateway/hitl_approval.py#L249-L335)

恢复重新装配图和服务端 context，再解释 checkpoint 的 pending writes/interrupt。全部拒绝的旧审批在 HITL 已关闭时有受服务端控制的 escape，只用于消费拒绝而非授权旧动作。具体从哪个节点续跑取决于 checkpoint；保存过历史不能证明外部动作恰好执行一次。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3226-L3272) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3291-L3403) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1005-L2499)

## 3.5 取消、回滚、断连和崩溃

cancel_run 的 interrupt 停止并保留 checkpoint，rollback 还恢复运行前图状态。RunManager 区分本地和远端 owner，建立持久取消条件后通知 worker；_finish_cancellation 尝试取消该执行的沙箱进程，再处理回滚和终态。收到取消请求不表示底层线程、子任务、远端动作或回滚已经完成。

源码依据：[thread_runs.py 实现](../backend/app/gateway/routers/thread_runs.py#L1367-L1425) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2089-L2290) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1124-L1184) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3404-L3515)

图状态回滚不撤销工具已发出的任意网络动作。取消 await 不能自动终止已经运行的线程；代码使用 drain 和 execution lease 约束收尾，仍不能推导出全局补偿事务。SSE 断连仅在 creator、on_disconnect=cancel、仍活动等条件下取消，观察者 join 和 replay gap 不应终止原生产者。

源码依据：[file_io.py 实现](../backend/packages/harness/deerflow/utils/file_io.py#L82-L110) · [lease.py 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L819-L829) · [services.py 实现](../backend/app/gateway/services.py#L3261-L3383)

run lease 丢失后旧 worker 被 fence，不能写自己的终态覆盖已接管状态。启动/周期 reconcile 将符合条件的 orphan run 标 error 并恢复终态可见性，不自动重放原 run。容器 ownership 是另一套销毁权机制。

源码依据：[manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3146-L3179) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2798-L2909) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3495-L3523)

## 3.6 结束后的后台与资源生命周期

worker finalization 确认完成记录和持久终态授权才发布 END；嵌套 finally 关闭审批/journal，结束 stream producer lease，释放 sandbox execution lease，断开 graph/runtime 强引用，并安排 bridge 60 秒延后清理及默认五分钟 run registry 清理。业务返回、持久完成和内存回收有不同时间。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2021-L2481) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L236-L287) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3003-L3039)

Memory after_agent 提交后台工作，返回不保证 facts 已落盘；sandbox release 可只是进 Warm Pool。子任务 completed/failed 与父任务验收也不同。优雅关闭依赖各组件收尾策略；硬退出没有 Python finally 必然执行的保证。

## 3.7 分支与销毁

_branch_thread_with_reservation 在 thread-operation reservation 下复制分支状态与元数据。fork-restored sandbox 值由 SandboxMiddleware 识别并跳过父容器释放，避免子线程归还父资源。

源码依据：[threads.py 实现](../backend/app/gateway/routers/threads.py#L1133-L1351) · [middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L462-L493)

删除在 delete reservation 内统一 owner，先清审批并抑制批任务续跑，再删合法 ID 对应文件、checkpoint，以及历史和元数据。checkpoint 删除失败返回 500 并保留 owner 信息；run/events/feedback/thread_meta 存在 best-effort 分支，所以不是跨存储原子清空。非法 legacy thread ID 跳过本地路径拼接，继续安全的存储清理。

源码依据：[threads.py 实现](../backend/app/gateway/routers/threads.py#L787-L809) · [threads.py 实现](../backend/app/gateway/routers/threads.py#L828-L975)

## 3.8 目标续跑与结束判定

一次 run 可以包含用户可见回合和后续隐藏目标回合。run_agent 首先 _stream_once，再检查 input_control 与 pending pause；只有没有暂停、没有 abort、没有模型错误回退时，才调用 _prepare_goal_continuation_input。该函数要求存在 checkpointer 和 active goal，且 scheduled expected_goal 仍是同一实例；没有 durable assistant 回合结束凭据、最后仍在向用户提问或递归预算失败时，不继续消耗目标回合。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1812-L1867) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2832-L3104)

目标判断不是直接信任模型一句“完成”。实现读取 checkpoint/messages，记录可见对话与 AI 证据签名，调用 evaluator；格式错误最多重试一次，重试前先确认目标和对话没变。判断后再次比较 goal instance、checkpoint ID 与 conversation signature，用户新消息或清目标的操作应使旧判断停下。satisfied 返回 _GoalCompletionCandidate，尚不是无条件清空 goal；未满足时根据 blocker、max_continuations、no_progress_count 和 hard stop 决定是否追加 continuation message。消息先脱敏再记次数，脱敏失败不发原始目标、不花该次 continuation 预算。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2961-L3102) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2746-L2759)

_persist_goal_evaluation 在 goal_thread_lock 内重读当前目标，以新鲜 continuation_count 计算下一次数，并传 expected_checkpoint_id 给 write_thread_goal；后者写新 ID、父 checkpoint、channel version 和 metadata。预期 ID 检查是写前冲突检测，不能据此称数据库级原子 CAS；goal_thread_lock 的契约也仅是当前 event loop，跨实例还依赖 Gateway 的运行准入/写入 reservation。持久次数后又读消息签名，若竞态出现，仅记录 stand-down，避免把同一次尝试再加一次。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2760-L2819) · [goal.py 实现](../backend/packages/harness/deerflow/runtime/goal.py#L88-L92) · [goal.py 实现](../backend/packages/harness/deerflow/runtime/goal.py#L851-L925)

## 3.9 预算、工具错误与产物交付

graph 正常返回和 run.success 之间还有宿主判断。ToolErrorHandlingMiddleware 将普通 Exception 转成关联 ToolMessage，但 GraphBubbleUp 原样传播，保证 interrupt 不被“工具报错”吃掉；asyncio 取消也不能被当普通业务错误强行吞掉。模型错误回退消息由 worker 识别并使 run.error，不能因为有一条可显示的 AI 文本就宣告成功。

源码依据：[tool_error_handling_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L340-L357) · [tool_error_handling_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L359-L377) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1871-L1886)

loop/token/safety/subagent/model-length 的 hard guard 会移除调用并写 stop_reason；图可能正常结束，worker 将 cap 原因带进 RunRecord，且隐藏目标回合不会清掉用户回合已发生的 cap。主图 GraphRecursionError 是 error + recursion_capped，会列出已创建但未呈现的产物并暂停目标；子图另有“可用部分文本转 completed+turn_capped”的分支，因此不能把父子状态枚举直接等价。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1888-L1907) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1936-L1996) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1758-L2246)

正常结束前，worker 比较运行前 workspace snapshot 和产出路径，再将交付工具中呈现的路径与产物匹配。_delivery_content_with_outputs 以至少一个产出被匹配决定 satisfied；若有产出却未呈现，_delivery_error 返回错误并令 run.error。它不检查每个产物内容的业务正确性；初始 snapshot 截断且没有可确认产出时会标 verification_skipped，而非假定已全面核验。finalization 还需把 delivery receipt/完成证据持久化，这些是源码现有行为，本次没有执行产物交付。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L392-L421) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L422-L428) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L310-L358)


## 3.10 定时任务：发生次数、启动租约与运行终态

### 3.10.1 对象与调度次序

定时执行有三个不同对象：任务定义、一次 occurrence 的 `ScheduledTaskRunRow`、真正执行图的 run。任务的 enabled/paused/running 不替代 occurrence 的 queued/launching/running；run 的 success/error 又是另一层。新线程模式下，每次 occurrence 可以使用不同 thread，单靠 run 的同线程约束不足以阻止同任务重复发生。

`ScheduledTaskRunRow` 保存 task_id、occurrence_seq、thread_id、run_id、scheduled_for、trigger、launch_accounted 和启动 lease。存储对 `(task_id, occurrence_seq)` 建唯一索引，并对 queued/launching/running 的同 task 建部分唯一索引。这实际约束“每任务最多一个活跃 occurrence”，作用域与 RunStore 不同。

源码依据：[occurrence 数据与约束](../backend/packages/harness/deerflow/persistence/scheduled_task_runs/model.py#L11-L57)

`ScheduledTaskService.run_once(now)` 先恢复/协调过期状态，处理等待超期，排已有队列，再 claim 到期任务逐个 dispatch。单项 dispatch 异常记录后继续同批其他任务；claim 查询本身失败不能解释成本轮正常无任务。

源码依据：[轮询顺序与批内故障隔离](../backend/app/scheduler/service.py#L446-L494)

### 3.10.2 从持久入队到图启动

1. `dispatch_task` 先处理任务结束规则，再按 context_mode 选复用线程或新 UUID，并校验线程 ID。旧数据中非法 ID 走失败记账，避免反复阻断同批任务。
2. 有 active occurrence 就返回已有结果，否则创建 queued 行。scheduled 触发核对任务 lease；manual 触发核对 owner、状态和 updated_at 快照。入队与释放父任务准入 lease 在仓储事务内协调。
3. 入队时固定 scheduled_for；队列重试仍使用同一发生时间，避免重试 now 改变 provenance，导致相同幂等 key 的请求内容发生漂移。

源码依据：[任务快照、线程选择和入队](../backend/app/scheduler/service.py#L581-L691)

4. `_attempt_queued_run` 为非 HTTP poller 建独立 trace scope，再进入 `_launch_queued_occurrence`。仓储 `claim_queued_run` 在数据库锁下共同检查全局与每用户容量：PostgreSQL 使用事务 advisory lock，SQLite 使用 BEGIN IMMEDIATE。未取得容量保持 queued。
5. claim 成功才改 launching、写 lease 和增加 attempt_count；同线程更老的 active occurrence 还会阻止后来项越过。这个预算覆盖定时执行，不能推广成所有 Agent 的全局资源上限。

源码依据：[trace 与 launch claim](../backend/app/scheduler/service.py#L700-L743) · [预算、顺序和状态更新](../backend/packages/harness/deerflow/persistence/scheduled_task_runs/sql.py#L560-L671)

6. 服务组合 stop condition、standing_notes、goal 和上次 occurrence 引用，调用注入的 `_launch_run`。Gateway 实际启动器注入 `non_interactive=True`、可信 owner 与 occurrence authority，用 `scheduled-task:{occurrence_id}` 作幂等键，经共享 `start_run` 准入。
7. run 建立后才记录 occurrence 的 run_id、running 和父任务计数/next_run_at。一次任务保持 running，直到真实运行终态被观察；manual 试跑不消费未来一次计划，循环任务的手动试跑也保持原 cadence。

源码依据：[提示、启动与记账](../backend/app/scheduler/service.py#L745-L805) · [状态与时间游标](../backend/app/scheduler/service.py#L514-L566) · [内部启动与幂等身份](../backend/app/gateway/services.py#L3065-L3168)

### 3.10.3 启动后的部分失败

忙线程发生在实际启动前时，只有特定运行冲突被识别为 overlap 并重新 queued；其他 409，例如幂等输入不符，不被当忙线程反复重试。前置失败按 lease owner 条件结束 launching；若 claim 已被接管，原执行者不覆盖恢复者负责的状态。

run 已启动但后续数据库记账失败时，代码保留 active occurrence 槽，尽力补存 run_id 与父任务关联，仍返回 `outcome=launched` 并携带记账错误。它表示执行已存在、投影不完整；不能解释成“没有执行”，也不能称为全部持久操作成功。后续终态协调和 claim 恢复负责继续收敛，图执行与这组记账没有共同事务。

源码依据：[忙线程分类](../backend/app/scheduler/service.py#L496-L512) · [启动后失败与活跃槽保护](../backend/app/scheduler/service.py#L806-L915)

service 的 finalization observer 还会记录 chat event 与 notification outbox。通知因此有独立投递阶段，occurrence completed 不证明所有外部 IM 端均收到消息。本文未逐个展开 provider 的通知协议。

源码依据：[终态观察与通知入队](../backend/app/scheduler/service.py#L280-L420)

## 3.11 IM：连接归属、消息去重与结果消费

### 3.11.1 外部消息如何取得可信身份

`ChannelManager._handle_message` 用 admission scope 包围分发，在 finally 清理 inbound 临时文件。`_dispatch_message` 先归一 effective owner；普通消息在创建线程/run 前检查绑定，命令走自身检查。bound identity 开启时，manager 根据 provider 身份重新读连接仓储，核对 connection_id 和 owner_user_id，不能只信 InboundMessage 可变字段。特定 webhook policy 可退出逐发送者绑定，其可信性仍需由对应 webhook 入口提供。

源码依据：[分发和临时文件清理](../backend/app/channels/manager.py#L2767-L2822) · [绑定重读与拒绝](../backend/app/channels/manager.py#L2824-L2894)

去重键优先取 provider message ID，组合 channel、workspace、chat 和 message。Discord 使用自身 snowflake/channel 语义；其他 provider 缺少足以区分 workspace 的信息时返回无 key，避免错误合并不同会话。相同键派生 run 幂等 key 和稳定 human message ID；没有足够标识时不承诺同样的重投递保护。

源码依据：[去重字段](../backend/app/channels/manager.py#L2618-L2660) · [run key 和消息 ID](../backend/app/channels/manager.py#L2662-L2674)

`_handle_chat_on_thread` 在附件下载可能重写消息前计算这些标识，并固定文件归属桶。ingest 后把线程上传元数据放进 human message；若等待期间发生 /new，handoff 前可再次检查会话绑定，避免投到已切换线程。工具实际得到的是线程上传/沙箱路径。

源码依据：[附件准备与绑定再检查](../backend/app/channels/manager.py#L3382-L3446)

### 3.11.2 同一运行内核的三种消费契约

| 路径 | 返回时点 | 结果使用方 |
| --- | --- | --- |
| 普通 IM wait | 等待结果或取得已有 run 状态 | manager 提取回复与产物并发布 outbound |
| 支持流式的 IM | 连续消费中间输出 | streaming handler 与 provider |
| fire_and_forget | runs.create 返回 pending/已有记录 | 运行工具或特定通道自行发送结果 |

源码依据：[流式分支](../backend/app/channels/manager.py#L3447-L3462) · [后台 create 与 wait](../backend/app/channels/manager.py#L3464-L3555) · [流式消费入口](../backend/app/channels/manager.py#L3619-L3656)

policy 还决定 interaction_mode、澄清、递归限制、凭据提供者、同线程串行与 busy follow-up 缓冲，由 channel_name 注册表实际选择。缺省通道保持交互默认。fire_and_forget 省去长时间 wait 连接，create 被接受仍不证明最终外部发送成功。

源码依据：[策略契约和注册表](../backend/app/channels/run_policy.py#L27-L130) · [策略应用位置](../backend/app/channels/manager.py#L2315-L2361)

### 3.11.3 重投递和结果未知

异常后 manager 根据是否可能已请求/确认 run 决定保留或释放去重键，避免对已执行消息立即重做。忙线程和旧模型 pin 有独立分支；fire_and_forget 可按 policy 缓冲 follow-up 并在原 run 结束后排出，缓冲有独立进程寿命，不能称为所有通道的持久可靠队列。

重投递复用已有 run 时，wait 可能只有状态没有消息。`_admitted_turn` 以稳定 human ID 从 durable thread state 定位对应回合，避免把后来回复错配给旧消息；读取失败返回 None。最终 outbound 发布与 provider 送达仍分开，下游是否重试取决于具体适配器。

源码依据：[去重保留条件](../backend/app/channels/manager.py#L2730-L2757) · [异常后释放](../backend/app/channels/manager.py#L2799-L2822) · [已有 run 回合定位](../backend/app/channels/manager.py#L3599-L3617) · [回复和产物出站](../backend/app/channels/manager.py#L3556-L3597)

## 3.12 浏览器：发送、重连、停止与显示状态

### 3.12.1 发送和流式数据

前端 `useThreadStream` 使用 SDK 的 `useStream`，配置 reconnectOnMount、history limit 1 和同 macrotask 更新合并。发送先完成附件上传并更新乐观消息；上传失败清理状态并抛错。到实际提交前才触发 onSent，随后 `thread.submit` 携带 human ID、context、recursion_limit 与 streamResumable。

提交不设置 streamSubgraphs，子任务进度由 root custom 事件进入 UI，避免把子图 messages/values 混成主对话。history、live、optimistic message 和 run 列表分别持有信息，依赖 thread/run/message ID 关联；页面 loading 不替代服务端运行状态。

源码依据：[SDK 流创建](../frontend/src/core/threads/hooks.ts#L2364-L2385) · [上传失败与提交](../frontend/src/core/threads/hooks.ts#L3186-L3258)

### 3.12.2 同标签页和新标签页的恢复

SDK 原生重连使用本标签页 sessionStorage 中的 run 指针；重开浏览器或新标签页没有指针时，应用从服务端运行列表解析 knownActiveRunId，再 joinStream 加入已有运行。恢复状态保存 thread、run、attempts、inFlight、settled 和 timer。

1. thread/run 改变时清旧 timer、对应指针，重置恢复状态。
2. 没有 run、已完成、正在重连、等待 timer、已 settled、尝试耗尽或 SDK 正加载时不重复加入。
3. sessionStorage 已匹配该 run 时先让原生重连负责，避免双 SSE consumer。
4. 原生 join 出错后释放失效指针，应用接管。重试延迟为 1 秒和 2 秒，加首次共三次，不是无限重试。
5. finish 记录 completed run 并 settle；切换线程/卸载清 timer 和对应指针，阻止迟到回调串会话。

源码依据：[重连预算](../frontend/src/core/threads/hooks.ts#L459-L461) · [失败后的接管](../frontend/src/core/threads/hooks.ts#L2290-L2345) · [活跃 run 加入条件](../frontend/src/core/threads/hooks.ts#L2754-L2839)

后端 `sse_consumer` 按 Last-Event-ID 订阅。保留事件缺失时发 gap，携带 recovery=reload_durable_state；这不代表 run 失败，也不能补回无限 token 历史。心跳可识别已被恢复者收敛的终态或 producer 失效，只有满足相应条件才结束；普通 bridge 异常不会被当成主动断连。

源码依据：[游标、gap 与心跳](../backend/app/gateway/services.py#L3291-L3372)

### 3.12.3 停止与审批界面

observer/join 流关闭 apply_on_disconnect，防止只读观察者通过断开取消别人运行。创建者流仅在确认断连、没有 gap/END、本地拥有且仍 pending/running 时，根据 cancel 模式请求取消。浏览器显式停止在 SDK 没有 loading 但已知活跃 run 时调 runs.cancel，否则用 thread.stop；服务端资源释放仍受 worker 收尾边界约束。

源码依据：[创建者与观察者契约](../backend/app/gateway/services.py#L3261-L3288) · [断连取消条件](../backend/app/gateway/services.py#L3373-L3381) · [页面停止分支](../frontend/src/core/threads/hooks.ts#L2842-L2868)

onError 清理乐观消息、重放遮罩和审批提交等待，并失效 history 等缓存；它按当前仍 pending 的审批 ID 决定是否重新开放回答，不复活所有历史卡片。onFinish 记录 completed、settle rejoin 并失效停止后的缓存。界面超时只说明本地未等到足够反馈，不能据此重做已开始执行的工具；审批接受与动作执行仍由后端身份和请求绑定控制。

源码依据：[错误与完成回调](../frontend/src/core/threads/hooks.ts#L2544-L2610) · [超时审批仍待答检查](../frontend/src/core/threads/hooks.ts#L2241-L2257)

**本章其他来源**

- `backend/packages/harness/deerflow/sandbox/identity.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/runtime/runs/schemas.py`：运行控制与持久状态协作。
- `backend/packages/harness/deerflow/runtime/runs/store/base.py`：运行控制与持久状态协作。
- `backend/packages/harness/deerflow/runtime/runs/input_control.py`：运行控制与持久状态协作。
- `backend/packages/harness/deerflow/agents/middlewares/run_input_middleware.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/agents/middlewares/clarification_middleware.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/utils/file_io.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/runtime/goal.py`：运行控制与持久状态协作。
- `backend/packages/harness/deerflow/persistence/scheduled_task_runs/model.py`：定时发生、调度和存储约束。
- `backend/app/scheduler/service.py`：定时发生、调度和存储约束。
- `backend/packages/harness/deerflow/persistence/scheduled_task_runs/sql.py`：定时发生、调度和存储约束。
- `backend/app/channels/manager.py`：IM 身份、路由与结果交付。
- `backend/app/channels/run_policy.py`：IM 身份、路由与结果交付。
- `frontend/src/core/threads/hooks.ts`：浏览器请求与状态恢复。

# 4. 模块深度拆解

## 4.1 九项运行协作机制

### 4.1.1 Lead Agent 装配链

#### 4.1.1.1 调用链追踪

发布入口是 agents.__init__.make_lead_agent(config)，惰性导入 lead_agent.agent.make_lead_agent；后者返回 assemble_lead_agent(config).graph。assemble 选择 AppConfig，冻结进程 checkpoint 模式和 cadence，注入内部模式键，再调用 _assemble_lead_agent。_make_lead_agent 是绕过该冻结步骤的内部 graph-only 包装，也调用 _assemble_lead_agent；**它不是发布入口必经的一层**。create_deerflow_agent 是独立的纯参数 SDK 工厂，直接调用同一 LangChain primitive，而不是被 _make_lead_agent 调用。

源码依据：[__init__.py 实现](../backend/packages/harness/deerflow/agents/__init__.py#L20-L33) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L868-L872) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L873-L913) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L914-L923) · [factory.py 实现](../backend/packages/harness/deerflow/agents/factory.py#L67-L204)

实际装配过程为 _get_runtime_config 合并 configurable/context → resolve_config_user_id → 加载 custom Agent/skill 权限 → _resolve_model_name/_authorize_model_name → create_chat_model → get_available_tools → 交互策略过滤/附加 memory 与项目工具 → apply_tool_authorization → assemble_deferred_tools → build_middlewares → narrow_declared_tools/HITL → apply_prompt_template → normalize_middleware_state_schemas → verify_declared_tool_view → create_agent(model, tools, middleware, system_prompt, state_schema, context_schema=dict) → _complete_assembly。工具配置通过反射解析实际实现，MCP 取已初始化目录，host grant 控制调度工具；模型解析和 schema 选择不是简单字符串透传。

源码依据：[agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L990-L1483) · [tools.py 实现](../backend/packages/harness/deerflow/tools/tools.py#L163-L380) · [tools.py 实现](../backend/packages/harness/deerflow/tools/tools.py#L116-L145)

~~~text
装配
 ├─ 无模型配置 → ValueError，不能生成图
 ├─ 请求模型未知 → 回退首个模型 → 继续授权判断
 ├─ 模型被拒 → 搜索可用且 model:use 允许的替代项
 │               └─ 无替代项 → 按 fail_closed 抛错或保留原选择
 ├─ is_bootstrap → 窄技能集合 + setup_agent；不拥有普通 thread skill 投影
 └─ 常规 Agent
     ├─ 命名 Agent 且非 GitHub webhook → 可附加 update_agent
     ├─ allowed_subagents=[] → 即使请求开启也禁用 task
     └─ 完成工具/中间件/Prompt 装配 → create_agent
~~~

图形态取决于实际 middleware、schema 和工具集，而非只有 model：is_plan_mode 加 Todo；subagent_enabled 加 task 与限流；memory_enabled 决定读写链；vision、summarization、token budget、PII、审批、deferred MCP、run input control 和扩展均可改变节点或 wrapper。model 还影响 vision/推理能力和绑定参数。

源码依据：[agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L485-L822)

#### 4.1.1.2 状态流转

装配本身不执行 ThreadState 更新；它决定 state_schema、channel 和后续读写者。cfg 是 configurable 的副本，再由 context 覆盖；thinking/reasoning 采用“键是否存在”区分显式 false 与缺省，而模型名采用请求 → Agent 配置 → 全局首项。装配会回写有效 subagent_enabled、metadata 和 tracing callbacks 到调用配置，故输入 config 不是完全不可变。

源码依据：[agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L219-L227) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L167-L182) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L990-L1483)

LeadAgentAssembly 的字段为 graph、descriptor、effective_model；resolved_extensions 是装配时解析的局部对象，不是该数据类字段。无 observer 且无 capture 请求时 descriptor=None，不能假定每次装配都有完整描述。

源码依据：[agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L96-L104) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L924-L989)

#### 4.1.1.3 并发与隔离

工厂不是运行准入锁，Gateway 用 RunManager 防止同线程冲突；独立 SDK 调用者须自行建立相应边界。模式和快照频率进程冻结，客户配置不能任意切换共享存储解释方式。工具可能来自进程级单例/缓存，_ensure_sync_invocable_tool 用 threading.Lock 双检只安装一次同步包装，描述修改则克隆工具，避免一个 Agent 的提示污染其他图。subagent_runtime 显式传入时共享容量边界，省略时保留应用兼容的进程级后备。

源码依据：[manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2413-L2704) · [tools.py 实现](../backend/packages/harness/deerflow/tools/tools.py#L72-L87) · [tools.py 实现](../backend/packages/harness/deerflow/tools/tools.py#L107-L115) · [factory.py 实现](../backend/packages/harness/deerflow/agents/factory.py#L67-L204)

#### 4.1.1.4 生命周期

每次装配产生图、中间件对象及可能的 descriptor/model client 引用，不在此启动整个 Gateway 的资源系统。worker 保存图到本次执行，finalization 主动断开 runtime/config/journal 强引用，并安排回收；调用方持有的 SDK graph 则由调用方管理。create_deerflow_agent 不加载 YAML，也不安装全局依赖；带 durable batch 的显式 SubagentRuntime 必须先 start 或进入 async context。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L236-L287) · [factory.py 实现](../backend/packages/harness/deerflow/agents/factory.py#L67-L204)

#### 4.1.1.5 异常与边界

独立工厂的 middleware= 是完整接管，不能与 features/extra_middleware 混用，也会绕过自动链的策略安装。该入口拒绝“delta + 非空 checkpointer”，因为没有应用侧模式标记和 fail-closed 兼容门禁；无 saver 的 ephemeral delta 才允许。另一个误读点是 _resolve_model_name 注释称无模型返回 None，函数实际抛 ValueError；应以函数体为准。

源码依据：[factory.py 实现](../backend/packages/harness/deerflow/agents/factory.py#L67-L204) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L228-L242)

### 4.1.2 中间件钩子契约与洋葱模型

#### 4.1.2.1 调用链追踪

Lead build_middlewares 首先调用 build_lead_runtime_middlewares → _build_runtime_middlewares 形成共享基础，再追加 Lead 专属项，最后 compose_with_extensions 合并扩展并检查 ordering。子 Agent 的 _create_agent 则调用 build_subagent_runtime_middlewares。不要用旧流程文档中固定编号解释当前链。

源码依据：[agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L485-L822) · [tool_error_handling_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L378-L573) · [tool_error_handling_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L603-L893) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1131-L1326) · [stack.py 实现](../backend/packages/harness/deerflow/extensions/stack.py#L134-L171)

| 钩子 | 触发位置与当前用途 |
| --- | --- |
| before_agent | 每次图执行开始的前置节点；线程数据、sandbox retain、技能激活等 |
| before_model | 每轮模型前；摘要压缩、预热启动、上下文和工具状态准备 |
| wrap_model_call | 包住一次模型调用；请求投影、告警注入、内容兼容、模型错误重试 |
| after_model | 响应写入后、决定工具路由前；截断委派、循环/预算/安全终止判断 |
| after_agent | 本次图执行结束的后置节点；续输入决定、记忆入队和资源归还 |
| wrap_tool_call | 实际工具 handler 周围；授权、审批、审计、进度、错误转换和沙箱更新 |

正常一轮执行顺序如下，跳转、中断和短路会改变实际经过的节点。

~~~text
START
 → before_agent：0 → N
 → [每轮 before_model：0 → N]
 → wrap_model_call 进入：外层0 → 内层N → MODEL
 ← wrap_model_call 返回：内层N → 外层0
 → after_model：N → 0
    ├─ 有允许的 tool_calls
    │   → 每个工具 wrap_tool_call：外 → 内 → handler → 内 → 外
    │   → ToolMessage/Command → reducer 提交 → 下一轮 before_model
    ├─ 人工中断 → checkpoint/等待 → 后续恢复
    └─ 无继续动作 → after_agent：N → 0 → END
~~~

共享基础顺序为 InputSanitization → KnowledgeScope → ToolOutputBudget → ToolResultSanitization → 可选 PII → ThreadData → Lead 的 Uploads → Sandbox → DanglingToolCall → LLMErrorHandling → 可选 receipt/artifact/authorization/guardrail → SandboxAudit → 可选 ReadBeforeWrite/HarnessGuard/ToolProgress → ToolErrorHandling → 可选 ArtifactCapture。Lead 再加 DynamicContext、SkillActivation、DurableContext、可选摘要/Todo/token usage、Title、条件 Memory/Vision/MCP、SystemMessageCoalescing、条件委派/loop/token guards、ModelContentCompatibility、定制项、TerminalResponse、ModelLength、条件 Safety、Clarification，最后可能加入 run-input 控制。

源码依据：[tool_error_handling_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L378-L573) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L485-L822)

实际观察：数量是条件装配结果，子 Agent 同样有 DanglingToolCall、SkillActivation、模型长度限制、DurableContext、日期和系统消息合并，并可启用 loop/token/摘要等；预期“主14、子4”已过时。给出可复核的静态计数例：关闭所有可选链、关闭 Agent 记忆、无 vision/deferred/扩展/输入控制时，Lead 内置列表为20，子链为16；这不是部署默认值或已运行观测。旧文档仍写14/4，作为冲突证据保留。

源码依据：[tool_error_handling_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L603-L893) · [middleware-execution-flow.md 实现](../backend/docs/middleware-execution-flow.md#L1-L32)

#### 4.1.2.2 状态流转

before/after 返回 dict 或 Command 的更新才经 channel/reducer 保存；wrap_model_call 的 request.override 只是当前请求视图。例如循环 warning 与系统消息合并不等同持久历史，sandbox wrapper 返回 Command 才能把 lazy ID 提交给图。ToolErrorHandling 给可见结果加结构化元数据，ToolProgress/receipt 消费这些元数据，不能从自然语言猜状态。

源码依据：[middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L554-L581) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L940-L957) · [tool_error_handling_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L340-L357)

#### 4.1.2.3 并发与隔离

“洋葱”描述单次调用的 wrapper 嵌套，不是全局互斥。不同工具可能并行，因此其共享字段必须有 reducer，进程共享字典必须自行同步。assert_ordering 在扩展合并后校验全部同类实例的索引，违反即 RuntimeError；扩展不能插入到校验后的缝隙中绕过约束。范围为一个最终链，而不是跨 Gateway 的分布式排序。

源码依据：[ordering.py 实现](../backend/packages/harness/deerflow/extensions/ordering.py#L36-L59) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L79-L99)

#### 4.1.2.4 生命周期

before_agent/after_agent 是正常图生命周期钩子，不是任意异常必经的 try/finally。Gateway 的外层 execution lease/finalizer 负责中断、取消和异常收尾；自定义直接图调用方不能仅因装了 SandboxMiddleware 就假设拥有全部宿主清理保证。组件对象可以服务多次调用，瞬时 warning 和工具进度须按 entry/run 约束。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2021-L2481) · [lease.py 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L845-L865)

#### 4.1.2.5 异常与边界

数据必须早于消费者、资源获取早于使用、清理晚于使用。具体可执行证据是 ToolProgress 必须在 ToolErrorHandling 外层：先由内层将异常转成带 deerflow_tool_meta 的结果，再由外层统计；颠倒会把工具失败误计成成功或丢失状态。receipt 还必须包住会短路的授权/写门禁，否则拒绝没有 receipt。

源码依据：[ordering.py 实现](../backend/packages/harness/deerflow/extensions/ordering.py#L61-L121)

“Clarification 必须最后”应限定语境：它是普通业务链末端，但当前 builder 会在它后面加入 RunInputMiddleware，扩展还按锚点合并；不能继续声称它永远是最终列表最后一个对象。ThreadData 早于 Sandbox、模型前压缩早于其上下文消费者的目的仍成立，但具体受钩子类型与 wrapper 展开影响。

源码依据：[agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L485-L822)

### 4.1.3 ThreadState 与 Reducer 系统

#### 4.1.3.1 调用链追踪

create_agent 获取 get_thread_state_schema(mode) 选择 ThreadState 或动态 delta schema；工具和 middleware 产生部分更新，经字段 channel 合并，再由 checkpointer 保存。Gateway 用 CheckpointStateAccessor 读取、history 或修改；Gateway 列表走 ThreadMetaStore.search，而不是扫描所有 checkpoint channel_values。

源码依据：[thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L564-L569) · [agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L990-L1483) · [checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L113-L121) · [threads.py 实现](../backend/app/gateway/routers/threads.py#L1354-L1396)

~~~text
状态写入
 ├─ messages → AgentState 的消息合并；delta 模式改用 DeltaChannel
 ├─ 显式 Overwrite → 替换 channel 值；task_notes 仍再次规范化
 ├─ 带 reducer 字段 → 字段自己的规则
 └─ 其他字段 → 普通覆盖语义，不自动深合并嵌套 dict
读取
 ├─ 线程列表/归属/展示 → ThreadMetaStore
 └─ 图执行/状态/history → CheckpointStateAccessor → saver
~~~

#### 4.1.3.2 状态流转

ThreadState 继承 AgentState 的 messages 等上游字段，当前自行新增19项；下表完整列出新增字段。 [消息 delta schema 适配](../backend/packages/harness/deerflow/agents/thread_state.py#L422-L441)普通字段的“覆盖”不表示整个 ThreadState 被全量替换。

| 字段 | 数据与合并语义 |
| --- | --- |
| sandbox | SandboxStateField / merge_sandbox：None 保留，相同 sandbox_id 幂等，不同 ID 抛错 |
| thread_data | workspace/uploads/outputs 宿主路径；普通可选字段 |
| title | 会话标题，普通可选字段 |
| artifacts | merge_artifacts：追加去重、保持首次顺序；空列表不会清空已有 |
| todos | merge_todos：最后非 None 值整体胜出，[] 显式清空 |
| goal | merge_goal：新 None 保留，否则替换 |
| goal_outcome | 目标结束解释，普通可选字段 |
| applied_run_inputs | run 输入应用记录，普通可选字段 |
| uploaded_files | 上传文件元信息，普通可选字段 |
| viewed_images | merge_viewed_images：按 key 更新并刷新顺序；{} 清空、None 保留 |
| promoted | merge_promoted：同 catalog/turn 合并 names；catalog 或明确 turn 改变则替换 |
| delegations | merge_delegations：按 run/call/dispatch 关联；终态不降为非终态；最多保留50项 |
| skill_context | merge_skill_context：按 path 去重刷新最近性，丢旧正文键，仅保留8个引用 |
| tool_artifacts | merge_tool_artifacts：按 handle 更新、保留首次顺序，支持 trim_to，绝对上限1000 |
| tool_artifact_processed | merge_artifacts：已处理标识追加去重 |
| task_notes | TaskNotesChannel + merge_task_notes：按 note key 合并，None 删除该 key，最多8项，值规范化为 model_report |
| task_history | 历史归档引用，普通可选字段 |
| summary_text | 压缩后的摘要文本，普通可选字段 |
| background_tasks | MCP 后台任务投影列表，普通可选字段 |


源码依据：[thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L422-L430) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L165-L192) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L258-L298) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L322-L359) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L383-L421) · [state.py 实现](../backend/packages/harness/deerflow/agents/task_continuity/state.py#L81-L91) · [state.py 实现](../backend/packages/harness/deerflow/agents/task_continuity/state.py#L99-L104)

todos 的历史故障有直接回归材料：流式阶段看到 write_todos 的列表，后续节点返回 todos=None，以无 reducer 的覆盖通道抹掉已有值，导致结束时消失。merge_todos 不按 task ID 合并，也不追加；完整新列表是显式替换，None 才是“不动它”。这解释了 UI 可见与最终状态不同，而非虚构客户端缓存故障。

源码依据：[已有断言 test_thread_state_reducers.py](../backend/tests/test_thread_state_reducers.py#L1-L7) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L139-L151)

#### 4.1.3.3 并发与隔离

reducer 是并行图写的合并规则，不是线程锁，也不普遍可交换：todos 多个非 None 更新仍依赖写入顺序；不同 sandbox ID 被 fail-closed 拒绝，避免任意最后写者掩盖隔离错误。delegation_key 使用 (run_id,id,dispatch_message_id)，兼容旧记录时只允许受控匹配，防止重复 provider ID 低估预算。ledger 的50项上限和单 run 总预算上限配合，不能外推为无限历史准确计数。

源码依据：[thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L79-L99) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L235-L257) · [subagent_limit_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L91-L103)

#### 4.1.3.4 生命周期

channel_values 随 checkpoint 保存执行状态；ThreadMetaStore 保存轻量的列表、归属、状态与展示信息，适合排序过滤，不承载完整消息图。delta schema 对累积消息保存增量并按 cadence 做 snapshot，读取需物化；merge_message_writes 保持公开 add_messages 的 ID 替换、删除和 REMOVE_ALL_MESSAGES 语义。viewed_images 仅存元数据/引用，不长期复制 base64。

源码依据：[base.py 实现](../backend/packages/harness/deerflow/persistence/thread_meta/base.py#L59-L67) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L474-L530) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L531-L538) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L62-L70)

#### 4.1.3.5 异常与边界

以下10行是冲突拒绝的实际分支，来源

源码依据：[thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L88-L97)

~~~python
    if new is None:
        return existing
    if existing is None:
        return new

    existing_id = existing.get("sandbox_id")
    new_id = new.get("sandbox_id")
    if existing_id == new_id:
        return existing
    raise ValueError(f"Conflicting sandbox state updates: {existing_id!r} != {new_id!r}")
~~~

sandbox 的相同 ID 比较不是完整 dict 合并：ID 相同直接保留 existing，不能用新同 ID 记录偷偷更新其他元信息；不同 ID 的受控生命周期替换须显式 Overwrite，而不是放松 reducer。{}、[] 和 None 在不同字段的意义不同，例如 artifacts=[] 不清空、viewed_images={} 清空、todos=[] 清空。task_notes 即使首次写入或 Overwrite 也经自定义 channel 规范化，避免依赖 reducer 仅在二次合并才触发。

源码依据：[thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L79-L99) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L103-L112) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L113-L138) · [state.py 实现](../backend/packages/harness/deerflow/agents/task_continuity/state.py#L99-L104)


### 4.1.4 沙箱生命周期与 Provider 抽象

#### 4.1.4.1 调用链追踪

SandboxMiddleware.before_agent/abefore_agent 先确定 user_id、thread_id、技能授权及当前恢复值；非 lazy 路径调用 provider.acquire/acquire_async，lazy 路径可在 abefore_model 启动预热。首个沙箱工具通过 wrap_tool_call/awrap_tool_call 与工具侧初始化路径会合。SandboxProvider 的 acquire(thread_id, *, user_id) 返回 ID，acquire_async 默认把同步实现移到线程；get/get_scoped 返回已跟踪客户端，release 表达调用者不再使用。抽象层还承载技能同步、网络策略事件及宿主关闭所有权，不能把 release 等同于 destroy。

源码依据：[middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L247-L331) · [middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L367-L444) · [sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L31-L38) · [sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L39-L48) · [sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L89-L104)

LocalSandboxProvider.acquire 在构造线程路径映射后进入缓存锁，再次检查同一 (user_id,thread_id) 是否已有实例；命中复用，未命中创建 LocalSandbox 并按容量淘汰缓存。_setup_path_mappings 建公共映射，_build_thread_path_mappings 加入用户/线程数据和技能投影，LocalSandbox 的路径转换将虚拟路径解析到主机路径。这是路径命名与可见性约束，不是容器或操作系统隔离。bash_tool 入口默认拒绝主机 bash，必须显式启用 allow_host_bash；允许后进程在宿主上执行。LocalSandbox.execute_command 本身不重复该能力 gate，文件 API 的只读映射也不构成 shell 写保护。

源码依据：[security.py 实现](../backend/packages/harness/deerflow/sandbox/security.py#L35-L46) · [tools.py 实现](../backend/packages/harness/deerflow/sandbox/tools.py#L2979-L2989) · [local_sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L458-L540) · [local_sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L334-L432) · [local_sandbox.py 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L844-L965)

AIO 的调用和所有权分支如下，图仅描述实现的可能路径：

~~~text
acquire/acquire_async(user,thread)
 → AcquireSerializer 同一作用域串行化
 → 检查 shutdown、身份碰撞、隔离/隔离区策略
 ├─ active 命中且可复用 → 刷新使用/所有权 → 返回 ID
 ├─ warm 命中且健康 → warm 移出、active 注册 → 返回 ID
 └─ discovery/create
      → 跨进程文件锁（异步路径不阻塞事件循环等待锁）
      ├─ 已存在、策略兼容 → 注册 discovered
      ├─ 不兼容 → 获得销毁许可后替换，或拒绝/延迟
      └─ 不存在 → backend.create → 注册 created → 返回 ID
release → 活跃引用归零 → 关闭当前客户端 → warm 保留容器
        → 超时/LRU/显式 destroy/shutdown → 所有权检查 → backend.destroy
~~~

_create_backend 按配置选择本地容器 backend 或远端 provisioner；具体容器/HTTP 细节位于 SandboxBackend 实现。provider 负责身份、池和生命周期，backend 负责发现、创建、销毁及健康状态。AcquireSerializer 和文件锁保护创建，ownership lease 则裁定跨 Gateway 的销毁权，三者解决不同问题。

源码依据：[aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L349-L384) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2319-L2354) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2355-L2398) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2399-L2435) · [backend.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/backend.py#L173-L181)

#### 4.1.4.2 状态流转

本模块读取 sandbox、thread_data、messages 及运行 context；可信 user/thread、lease owner、技能策略来自 runtime，不由历史 sandbox ID 授权。before_agent 的状态更新是部分映射；恢复值不符合当前作用域时，通过受控 Overwrite 切换 sandbox，普通 reducer 仍禁止两个非空不同 ID 合并。fork-restored 值可以来自父线程，因此 after_agent 特判，不能把父容器作为子线程资源归还。

源码依据：[middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L247-L331) · [middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L462-L493) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L79-L99)

lazy 初始化有两种可见性：工具内部先改当前 runtime.state，随后 wrapper 比较调用前后的 sandbox ID，把变化补入 Command.update，让 LangGraph channel/checkpointer 能保存；仅修改 Python 字典并不等于持久状态提交。已有 Command 的 update 若属于不能安全合并的形态，不擅自改写。abefore_model 的预热 task 也不是 checkpoint 内容，恢复后需重新建立。

源码依据：[middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L591-L606) · [middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L608-L637) · [middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L453-L460)

#### 4.1.4.3 并发与隔离

单 Agent 多工具并发和父子 Agent 共用容器时，sandbox/lease.py 通过 owner 和引用计数管理执行占用，避免一个 after_agent 释放仍在使用的容器。Local provider 用 threading.Lock 保护缓存，同 scope 双重检查；缓存键含 user_id/thread_id。子 Agent 是独立图上下文，但允许显式继承 sandbox 和 thread_data，因此文件副作用并未自动隔离，工具自己的并发规则仍然重要。

源码依据：[lease.py 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L1-L110) · [local_sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L458-L540) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1548-L1716)

跨 Gateway 的 SandboxOwnershipStore 提供 take、claim、renew、release、owner。MemoryOwnershipStore 只覆盖进程；RedisOwnershipStore 用 Lua 原子检查 token 和 TTL。take 可接管普通 own: 租约，却不能覆盖 del: 销毁标记；claim 只允许无主或自己，并在销毁时转 del:，防止已进入销毁的实例被当作普通可复用资源。renew 返回 RENEWED/LAPSED/LOST：已失效可重新建立租约，明确输给其他 owner 则丢弃本地跟踪而不停止对方容器。

源码依据：[base.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L89-L109) · [base.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L111-L151) · [memory.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L83-L92) · [redis.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L1-L124)

续约同时覆盖 active 与 warm，并不依赖 idle_timeout。独立续约线程运行同步 store；本地 teardown reservation 与 acquire epoch 检查拦住迟到的 store 响应，避免“检查时属于我，返回时已重新获取”的竞态。该租约是销毁权协议，不代替工具授权，也不是所有文件写的事务锁。

源码依据：[aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1441-L1464) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L482-L554) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L555-L577)

#### 4.1.4.4 生命周期

Middleware 的 before_agent/after_agent 对称的是使用权而非容器生死：获取或 retain，结束时 release。异步预热的清理可交给 worker 外层 fence 持有，直到整个执行收尾才释放；异常不能只依赖 after_agent 必然执行。lazy_init=True 表示按需建立、以后可凭作用域重新取得，跨轮复用依赖 provider 和容器是否仍存活，并不保证永远是同一 ID。

源码依据：[middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L495-L542) · [lease.py 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L640-L829)

Local release 不清缓存，reset/shutdown 才清除实例记录；它不会负责销毁主机文件。AIO release 进入 Warm Pool；过期扫描、LRU 淘汰和 shutdown 才可能 stop 容器。replicas 是池的软容量目标：优先淘汰最旧 warm，如果全是 active 可以超目标创建，不能作为严格并发配额。Warm Pool 用单调时钟计算空闲时长；被隔离的失败销毁还记录 generation，后续处理必须对应同一代资源。

源码依据：[local_sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L578-L588) · [local_sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L603-L608) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2807-L2875) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2623-L2683) · [warm_pool_lifecycle.py 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L1-L132)

#### 4.1.4.5 异常与边界

容易误读的边界有三处。第一，get 是通常面向已跟踪对象的热路径，但 Local get("local") 有兼容性的懒创建分支，不能宣称所有 get 永无 I/O。第二，Local release 注释关于“middleware 不调用”的旧描述不能覆盖实际 after_agent；真正调用链以上述实现为准。第三，ownership 不确定、策略不兼容、身份冲突或销毁中均可拒绝获取，属于避免错误复用/错误销毁的 fail-closed 路径。

源码依据：[local_sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L555-L577) · [local_sandbox_provider.py 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L578-L588) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L106-L110) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L131-L140)

release 的回收失败可能只记日志，不把已完成 run 改成失败；destroy 失败或“未能证明不存在”不能等价于删除成功，隔离记录承担后续约束。条件性风险是宿主硬退出、远端 provisioner 结果未知或 ownership 服务不可用时，清理可能延后；代码没有跨容器引擎与 Redis 的事务。这里是静态时序推断，不能据此断言已发生容器泄漏。

源码依据：[aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2787-L2806) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2495-L2549) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2575-L2581)

### 4.1.5 记忆异步流水线

#### 4.1.5.1 调用链追踪

MemoryMiddleware.after_agent/aafter_agent 调用 _resolve_add_args：检查配置 enabled、thread_id 和 messages，捕获当前 user_id/agent_name、trace 与 locale，复制并按策略脱敏，再调用 manager.add/aadd。同步路径可能完成本地入队，异步路径仍 await 入队准入；立即返回的准确含义是“不等待后续提取模型和落盘”，不是整个 after_agent 没有任何等待。过滤 user/final AI 的实际规则位于默认 DeerMem 后端，不是 Middleware 本体。

源码依据：[memory_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L196-L230) · [memory_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L236-L254) · [memory_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L256-L271) · [manager.py 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1-L180)

DeerMem.model_post_init 创建配置、storage、LLM/updater 和队列；from_config 注入宿主路径与能力。add → filter_messages_for_memory → MemoryUpdateQueue.add_nowait/add → _enqueue_locked → Timer → _process_queue → MemoryUpdater.update_memory → _do_update_memory_sync_impl → LLM.invoke → _finalize_update → _apply_updates → FileMemoryStorage.apply_changes/_commit_changes_locked。队列线程用同步模型调用，避免在多轮 asyncio.run 之间重用绑定旧 event loop 的 HTTP 连接池。

源码依据：[deer_mem.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L116-L158) · [deer_mem.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L211-L248) · [message_processing.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L157-L220) · [queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L271-L366) · [updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1825-L2050)

队列的决策树保留了防抖、容量和特殊采样的区别：

~~~text
add(key=(thread,user,agent), 累积消息, signals)
 ├─ emergency snapshot → 独立快照、立即可处理、不推进普通 watermark
 ├─ key 已存在 → 以新累积历史替换，signals 取并集
 ├─ 新普通项且容量满、无重要 signal → 拒绝准入
 └─ 接受 → due=min(本次入队+debounce,首次入队+max_wait)
Timer 到期 → 锁内摘取 due 批次 → 锁外逐项提取/写入 → finally 重排 Timer
提取 → JSON/结构归一化 → 置信度及 scope/authority/durability 门控
     → 深拷贝当前记忆后应用变更 → revision 检查并提交
     ├─ revision 冲突 → reload，全量重新应用同一提取结果（最多三次）
     └─ 成功普通批次 → 推进 watermark
~~~

默认 debounce 为 30 秒、max_wait 为 300 秒，队列默认深度 1000； [记忆队列默认值](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/config.py#L142-L159)触发时间不代表该时间内完成，已有批次、模型延迟和逐项 0.5 秒间隔会增加等待。紧急快照和带 signal 的准入规则意味着 max_depth 不是所有场景下的绝对内存上界。

源码依据：[queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L1-L108) · [queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L170-L239) · [updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1533-L1634)

#### 4.1.5.2 状态流转

输入来自 ThreadState.messages，但本模块不把 facts 写回 ThreadState；记忆是图状态外的数据源，下次装配/提示构造按当前身份读取。过滤保留用户内容和最终 AI 回复，排除 System、ToolMessage、AI 的工具调用中间步骤及无效/raw 工具表示，避免把临时工具输出、内部步骤和未完成计划提炼为长期事实。带结构标记的真实澄清回复可保留；仅上传文件的回合可以抑制其后 AI 提取，具体规则以 message_processing 为准，而不是简单 isinstance 两次判断。

源码依据：[message_processing.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L157-L220) · [memory_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L196-L230)

默认 fact_confidence_threshold=0.7，等于阈值可以继续，但仍须通过 scope/durability/authority 等门控，不能认为“通过置信度即保存”。

源码依据：[config.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/config.py#L191-L196)

下面10行显示阈值与 scope 检查的实际顺序，来源

源码依据：[updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L2368-L2377)

~~~python
        for fact_index, fact in enumerate(new_facts):
            confidence = fact.get("confidence", 0.5)
            if confidence >= config.fact_confidence_threshold:
                passed_threshold += 1
            rejection_reason = _fact_scope_gate_reason(fact)
            if rejection_reason is not None:
                reject_by_scope_gate("facts", rejection_reason)
                continue
            if confidence < config.fact_confidence_threshold:
                continue
~~~

updater 用 (thread,user,agent) watermark 确定未消费片段，同时有进程缓存和持久 sidecar；成功普通批次才前移，失败不前移，emergency 不推进。_finalize_update 先深拷贝再修改，避免提交失败污染共享缓存；存储支持 apply_changes 时计算 upsert/delete 和版本，带 expected_manifest_revision 提交；冲突 reload 后重新应用完整提取，不把基于旧快照的删除/容量决策拆成无条件点写。

源码依据：[updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1797-L1824) · [updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1725-L1743) · [updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1533-L1634)

当前 canonical 记忆布局按用户组织。宿主注入根目录后，用户摘要/manifest 位于 {root}/users/{safe_uid}/memory.json，事实位于 users/{uid}/agents/{lower_agent}/facts/{hashprefix}/{id}.md；默认 agent 使用 __default__ 桶。旧 agent memory.json 只出现在迁移兼容路径。standalone 还支持显式 storage_path、DEERMEM_DATA_DIR 或用户目录默认值，不能把宿主路径规则外推到独立包。

源码依据：[paths.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L1-L162) · [storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1143-L1145)

#### 4.1.5.3 并发与隔离

queue 用 threading.Lock 维护待处理表、processing 和 Timer，锁内出队、锁外调用 LLM，单消费者顺序提取；同键替换累积历史避免每次对同一整段历史重复排队。不同子 Agent 可共享同一 manager/队列，但键含 user 和 agent；facts 按用户/agent 隔离，用户摘要在该用户范围内共享，这不是完全每 agent 独立的所有记忆。

源码依据：[queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L52-L60) · [queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L271-L366) · [paths.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L1-L162)

多 Gateway 不共享 Python 队列，但 FileMemoryStorage 有 scope RLock、进程文件锁、manifest/fact revision 检查；锁覆盖读—计算—写对应的文件操作，revision 还保护此前锁外 LLM 提取基于的快照。检索索引通知有自己的兼容性与失效处理，不等于文件提交和远端索引原子提交。保护要求实例访问同一实际文件系统及有效文件锁；部署存储语义未确认。

源码依据：[storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L494-L547) · [storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L985-L988) · [storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1878-L1977) · [storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1008-L1086)

#### 4.1.5.4 生命周期

后台资源包括 daemon Timer、队列项、LLM/检索客户端、缓存、sidecar 和事实文件。_process_queue 在 finally 清 processing 并重新调度；clear/cancel_by_agent 可删除待处理项，却不能据此证明已开始提取立即停止。flush/flush_sync 处理待消费工作，flush_sync 还考虑当前 worker；宿主关闭尽力调用清理。进程硬退出可能丢失尚在内存中的队列，持久 checkpoint 不能替队列提供可靠消息投递。

源码依据：[queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L390-L459) · [queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L470-L514) · [deer_mem.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L591-L595)

_atomic_write 使用临时文件、flush/fsync、replace 和父目录同步；涉及 manifest 与多个 Markdown fact 的提交另有 journal/backup 与 _recover_if_needed。不能把“单文件原子替换”说成“多个文件的单次原子事务”。revision 与恢复日志维持库内一致性，成功返回前后的文件操作和索引通知仍有各自故障窗口。

源码依据：[storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L476-L492) · [storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1322-L1474) · [storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1274-L1321)

#### 4.1.5.5 异常与边界

源码边界：DeerMem.add 只把 QueueFull 记录并跳过，下一轮可能重新馈入尚未推进水位的历史；MemoryMiddleware 本身没有通用 catch，manager 初始化、脱敏或其他后端异常仍可能沿 after_agent 传播。MemoryManager.aadd 默认直接调用同步 add，async 接口并不自动把任意插件后端的阻塞工作移到线程。

源码依据：[deer_mem.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L211-L248) · [manager.py 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L574-L584)

另一个已确认的注释/实现矛盾在同步 MemoryUpdater.update_memory：当当前线程已有活动 event loop，它提交 _SYNC_MEMORY_UPDATER_EXECUTOR 后立即 future.result()，调用线程仍同步等待；注释所称“不阻塞 caller loop”不能成立。触发前提是异步代码直接调用这一同步接口，普通 Timer 消费路径不经过该分支；真正 aupdate_memory 使用 await asyncio.to_thread。此结论来自等待语句，不依赖延迟实测。

源码依据：[updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L2123-L2146) · [updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1635-L1666)

模型 JSON 无效、置信度不合规、scope/authority 不满足、容量淘汰、版本竞争和文件写失败是不同分支。门控会丢弃候选事实；解析/存储失败不应被读成事实已保存。用户记忆提交后，关联项目记忆阶段还可能失败；实现会避免简单重放已消费用户写入，因而“整个 batch 全成功或全失败”也不成立。

源码依据：[updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L448-L468) · [updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L2157-L2717) · [updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1825-L2050)

“主流程不阻塞”和“记忆绝不丢失”不能互相推出：after_agent 只保证提交尝试与错误边界，队列不持久化，关闭 flush 也是尽力而为。已 drain 表示队列消费完，不保证每项 LLM/写入都成功。提取模型输出是待校验输入，置信度是过滤参数而非事实真伪证明；手工事实、删除与合并另有来源约束，不能将模型任意文本直接当文件路径。

源码依据：[queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L271-L366) · [updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L268-L447) · [storage.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L375-L388)


### 4.1.6 子智能体委派限流

#### 4.1.6.1 调用链追踪

模型生成 task 调用后，SubagentLimitMiddleware.after_model/aafter_model 调用 _truncate_task_calls，读取最后一条 AIMessage 与当前 run 的 delegations，先于 ToolNode 执行删去超额调用。保留下来的 task_tool 才会解析目标 agent、创建 SubagentExecutor 并执行；_aexecute 再向 SubagentExecutionCapacity.slot 申请实际运行槽位，获准后进入 _aexecute_admitted，后者组装子图并消费 agent.astream。软提示、每次响应截断、全 run 委派预算和进程执行容量是四个层次。

源码依据：[subagent_limit_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L133-L183) · [task_tool.py 实现](../backend/packages/harness/deerflow/tools/builtins/task_tool.py#L819-L1472) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1717-L1757) · [capacity.py 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L143-L150)

配额计算的实际三行如下，来源

源码依据：[subagent_limit_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L154-L156)

~~~python
        prior_delegation_count = _count_prior_delegations(state.get("delegations"), run_id=run_id)
        remaining_total = max(0, self.max_total - prior_delegation_count)
        allowed_task_calls = min(self.max_concurrent, remaining_total)
~~~

max_concurrent 限制“本条模型响应内可以保留多少个 task”，不直接统计所有线程正在运行的任务；max_total 限制当前 run 已登记的委派总数。代码计算 allowed=min(max_concurrent,max(0,max_total-prior))，只删除多余 task，其他工具原顺序保留。默认构造为 3/6，分别 clamp 到 [1,64]/[1,50]，调用方还会按执行容量缩小。Orchestration 提示向模型说明何时委派、预算与汇总职责；模型不遵守提示时，中间件仍执行程序限制。

源码依据：[subagent_limit_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L104-L125) · [subagents_config.py 实现](../backend/packages/harness/deerflow/config/subagents_config.py#L1-L120) · [prompt.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/prompt.py#L451-L662)

~~~text
after_model
 ├─ 无 messages / 最后非 AI / 无 task → 不更新
 └─ task 数 > allowed
      → 移除超额 task、清理消息的所有工具调用表示
      ├─ remaining_total > 0 → 仅截断
      └─ remaining_total == 0 → 加提示、stop_reason=subagent_limit_capped
剩余 task → executor
 ├─ capacity 不足且禁止排队 → FAILED(admission_failure)
 └─ 等待/取得 slot → RUNNING → 子图循环
       ├─ 正常/可用的预算内部分结果 → COMPLETED（可能带 stop_reason）
       ├─ 工具直接返回错误/模型失败 → FAILED
       └─ 取消/超时 → 对应子任务终态并收尾
~~~


源码依据：[executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1758-L2246) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2317-L2501)

#### 4.1.6.2 状态流转

中间件读取 messages、delegations，只返回部分 messages 更新。clone_ai_message_with_tool_calls 保留消息 ID，使 reducer 替换原 AIMessage，同时清除 raw/additional_kwargs/content 中已删调用，不能只改 tool_calls 属性而留下另一份可被后续解析的副本。已移除调用从未执行，不生成伪 ToolMessage；保留调用才需要一一关联的工具反馈。DanglingToolCallMiddleware 处理历史或中断留下的未配对调用，不能用它替代本次消息的一致性改写。

源码依据：[tool_call_metadata.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_call_metadata.py#L87-L130) · [dangling_tool_call_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/dangling_tool_call_middleware.py#L1-L160)

prior 以 run_id 过滤并对 (tool_call_id,dispatch_message_id) 去重；provider 在后续 AI 响应复用同一 call ID 时仍算不同委派。durable ledger 保证同一 run 多次规划不会重置预算。子任务状态保存在 SubagentResult，再通过 task 工具的结果和 delegation 更新回到父图；子图只显式继承 sandbox/thread_data/uploaded_files 等，不把整个父 messages 直接当自己历史，任务快照也有明确消息形式。

源码依据：[subagent_limit_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L91-L103) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L258-L298) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1548-L1716)

#### 4.1.6.3 并发与隔离

SubagentLimitMiddleware 本身不持有可变全局计数，依赖图当前 ledger，因而不是 Redis 配额。相同 thread 的 run 并发准入由 RunManager 和持久 execution fence 负责；直接 SDK 绕开该层时不能从中间件推导多实例全局限额。SubagentExecutionCapacity 使用 asyncio.Lock 保护当前 event loop 的 running/waiter 状态，以 Future 排队；controller 活跃时拒绝跨 loop 迁移，空闲才允许重建，并对取消后的移除与释放单独处理；slot 的 finally 覆盖正常与异常。该容量是宿主进程/显式 runtime 内资源，不是全体 Gateway 共享的分布式 semaphore。

源码依据：[capacity.py 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L62-L86) · [capacity.py 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L97-L108) · [capacity.py 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L118-L141) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2413-L2704)

SubagentResult 用 _state_lock 保证终态不会被迟到成功覆盖，另用事件表达 cancel 与 execution_done。同步/后台任务可在隔离 event loop 线程执行；隔离的是调度上下文，不是内存、进程、权限或文件。子图创建不复用父 checkpointer，运行 context 显式传 user_role、授权属性、approval runtime、sandbox lease owner 和命令 scope；共用 sandbox 仍可能读写同一文件。

源码依据：[executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L267-L310) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L857-L894) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1131-L1326) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1758-L2246)

#### 4.1.6.4 生命周期

任务建立 result、future、输入门、token collector、子图和独立 sandbox 使用权；execute_async 注册后台结果，supervisor 维护实际执行结束，cleanup 不能仅按“结果已终态”就删除仍运行的对象。_aexecute finally 关闭 input_gate、置 execution_done_event；_aexecute_admitted 的 finally 关闭 agent stream、排空 sandbox 预热清理、释放执行 lease。SubagentRuntime.start/stop 管理宿主工作和队列；模块级隔离 loop 有独立关闭逻辑。

源码依据：[executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1717-L1757) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1758-L2246) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2591-L2629) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L682-L733) · [runtime.py 实现](../backend/packages/harness/deerflow/subagents/runtime.py#L131-L168)

#### 4.1.6.5 异常与边界

没有 runtime.run_id 时不是重置为零：实现记录警告，保守统计线程全部 delegations，可能较早达到总预算。预算耗尽仅阻止新委派，不能撤销已启动任务。GraphRecursionError 可把最后可用 AI 文本作为部分完成，并带 turn_capped；所以 completed 不必然等于满足父任务验收。直接返回工具 status=error 则转 FAILED，不能把“图正常结束”当作“业务成功”。

源码依据：[subagent_limit_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L133-L183) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1758-L2246)

子 Agent 取消区分请求、stream 关闭、沙箱进程取消和实际执行结束；超时等待者不能证明底层立即停止。任务 terminal 状态和 execution_done_event 分开正是为了避免过早释放/删除。条件性风险是共享文件副作用、宿主退出和多实例独立容量产生的总资源放大；已有代码不能支持“全系统严格最多三个子任务”的结论。

源码依据：[executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2317-L2501) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2630-L2648)

### 4.1.7 循环检测

#### 4.1.7.1 调用链追踪

装配器按配置创建 LoopDetectionMiddleware；after_model/aafter_model → _apply → _track_and_check 对本轮 AI 工具调用做两层检测。低阈值返回 warning 并排入 pending；下一次 wrap_model_call/awrap_model_call → _drain_pending_warnings → _inject_warnings，在当前模型请求中追加 HumanMessage。高阈值移除所有待执行工具调用、追加停止说明并标 stop_reason，阻止本批工具进入执行。工具调用元数据的一致清理由 clone_ai_message_with_tool_calls 负责。

源码依据：[agent.py 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L485-L822) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L818-L869) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L579-L765) · [tool_call_metadata.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_call_metadata.py#L87-L130)

_hash_tool_calls 将每个调用的规范化 name/args 稳定序列化，排序后把这一轮调用多重集合生成 MD5 前 12 位。排序消除并行调用排列差异，重复元素仍保留；MD5 在这里是非安全去重标记，不提供防碰撞证明。read_file 的行号/范围有归一化；已知工具的 UI description 不影响重复判定，write_file 的真实内容则参与；未知工具保留其参数，避免过度归一化吞掉业务差异。

源码依据：[loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L191-L224) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L162-L190) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L225-L254)

~~~text
after_model 最后一条 AI 有 tool_calls?
 ├─ 否 → 无处理
 └─ 是 → 同 (thread,run) 的 hash 滑动窗口 + 工具类型频率窗口
       ├─ 任一 hard 阈值达到 → 删除本轮全部 calls、loop_capped
       ├─ 新 warning 阈值达到 → 本批仍执行，下一轮请求注入 warning
       └─ 未达到 → 照常执行
wrap_model_call
 → 取出 pending → 请求内注入 → 调模型
   └─ 异常 → 恢复已取出的 warning，再向上抛
after_agent → 清本 run pending 与 fallback identity
~~~

默认 hash window=20、warn=3、hard=5；工具类型默认 warn=30/hard=50，允许按工具覆盖，频率窗口会扩到足以达到最大阈值。hash 候选 warning 不会提前 return 跳过频率硬限制，后者可覆盖前者。

源码依据：[loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L328-L405) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L579-L765)

#### 4.1.7.2 状态流转

图状态读取 messages，hard stop 返回同 ID AIMessage 的部分替换；warning 只改变下一次 ModelRequest.messages，不追加到 ThreadState 历史。这避免在 AI tool_calls 和 ToolMessage 之间插入未配对 HumanMessage。pending_warnings 按 (thread_id,run_id) 隔离，history、warned hash、工具 deque/Counter 都是中间件实例内状态，不是 reducer/checkpoint 字段。恢复图不等于恢复此前所有检测计数。

源码依据：[loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L940-L957) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L567-L578) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L818-L869)

before_agent/abefore_agent 当前是 no-op，清理由 after_agent 和有界淘汰负责。after_agent 清当前 pending/fallback 身份，但不会清空所有历史；reset 才整体重置。run_id 键存在即权威，包括显式 None；只有缺键才依次尝试 runtime.execution_info.run_id，再以 runtime.control（没有则 runtime）作 anchor 映射随机 token，强引用避免对象地址重用。thread_id 缺失则使用 "default"。因此复用同一实例且反复显式传 None 的嵌入路径可能共用 (thread,None) 历史；标准 Gateway 注入的真实 thread/run 标识才提供明确范围。

源码依据：[loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L883-L888) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L437-L478) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L903-L907) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L984-L1017)

#### 4.1.7.3 并发与隔离

thread_id 来自 runtime.context，run_id 进一步区分同线程运行；共享同一 middleware 的线程访问以 threading.Lock 保护 history、pending、LRU 和频率计数。锁只覆盖短内存操作，不跨模型网络调用。每个子图可有自己的 middleware 实例，审计 recorder 由 context 注入并携带 agent 标识；不因此共享同一父子总循环预算。多 Gateway 各自维护内存窗口，不是 Redis 计数器；运行 lease 防止旧 worker 写终态，不会把循环历史自动复制到接管实例。

源码依据：[loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L430-L436) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L513-L516) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L579-L765) · [executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1758-L2246)

#### 4.1.7.4 生命周期

资源是有上限的内存历史、Counter/deque、pending warnings 和后备 run identity。_evict_if_needed 以 LRU 淘汰历史，_prune_pending_warning_state_locked 限制 warning 状态；after_agent 释放当前 pending，reset 清所有。频率计算对每个调用更新 Counter 为 O(1)，窗口驱逐摊销 O(1)；hash 排序对本批 k 次调用为 O(k log k)，序列化成本随参数大小增长。这里是代码复杂度，未声称任何实测性能。

源码依据：[loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L521-L537) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L554-L566) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L225-L254)

#### 4.1.7.5 异常与边界

低阈值不会阻止当前工具执行，高阈值则直接终止本轮 calls；两者结合，不是只写一条日志或永远靠模型自觉。hard stop 不抛专用异常，也不保证再调模型生成“自然结束语”，它写停止说明并由图的后续条件结束；stop_reason 使 worker/子任务结果能表示受限完成。模型异常后恢复 pending，避免 warning 因“已取出但请求失败”永久丢失。

源码依据：[loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L818-L869) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L959-L970) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L972-L983)

边界是工具“频率高”不证明任务无进展：合法遍历大量不同文件也会达到类型阈值；按工具覆盖和滑动衰减是现有可调机制。工具参数表示差异也可能影响 hash，规范化只覆盖明确工具规则。进程重启/新图实例丢失计数是内存设计的适用边界，不能宣称 checkpoint 提供累计循环防护。

源码依据：[loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L124-L149) · [loop_detection_middleware.py 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L579-L765)


### 4.1.8 流式输出双路径

#### 4.1.8.1 调用链追踪

Gateway 入口 stream_run/create_run 调用 start_run 完成准入并启动后台 worker；run_agent._stream_once 消费 agent.astream，多 mode 的每项先经过 _unpack_stream_item 解析 (namespace,mode,chunk)，_compose_sse_event 把子图命名空间编码为 mode|ns1|ns2，serialize 转换可传输数据，再调用 StreamBridge.publish(run_id,event,data)。HTTP 一侧 sse_consumer 订阅 bridge，format_sse 组装 event/id/data 行。producer 与 HTTP 消费者分离，创建返回、图执行和网络发送不是同一时刻。

源码依据：[thread_runs.py 实现](../backend/app/gateway/routers/thread_runs.py#L1091-L1138) · [services.py 实现](../backend/app/gateway/services.py#L2295-L3064) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1691-L1816) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3945-L3977) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3978-L3992) · [services.py 实现](../backend/app/gateway/services.py#L3261-L3383)

实际观察：MemoryStreamBridge 当前是 events:list + asyncio.Condition 的有界日志，预期“asyncio Queue”及 client 注释中的 queue 描述已经过时。publish 深拷贝数据、追加事件、裁剪旧项、notify_all；每个 subscribe 有独立绝对 offset，不是多个消费者争抢一个 Queue 元素。RedisStreamBridge 则用 Redis Streams 的 XADD/XREAD，在多 Gateway 间共享事件和 producer lease。

源码依据：[memory.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L119-L131) · [memory.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L138-L200) · [redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L741-L762) · [redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L834-L1011)

DeerFlowClient.stream → _stream_turn → _ensure_agent → 同进程 agent.stream，直接 yield Python StreamEvent；chat 消费这个迭代器拼答案。当前公开路径是同步生成器，没有证据支持把它描述为已有 client.astream 异步 API。它复用图装配与状态机制，但不走 Gateway RunManager/SSE。

源码依据：[client.py 实现](../backend/packages/harness/deerflow/client.py#L859-L912) · [client.py 实现](../backend/packages/harness/deerflow/client.py#L913-L1313) · [client.py 实现](../backend/packages/harness/deerflow/client.py#L1314-L1341)

~~~text
Gateway：
后台 agent.astream → 序列化 → bridge 有界保留
                                  ├─ creator SSE
                                  ├─ join SSE（独立游标）
                                  └─ 重连 Last-Event-ID → replay 或 gap
SDK：
调用方 next() → agent.stream → Python event → 调用方处理 → 再 next()
~~~

#### 4.1.8.2 状态流转

流桥不写 ThreadState；values/updates/messages 是图状态或增量的观察结果，不能把“token 已看见”当作已提交最终 channel。worker 还会在发送前区分 root/subgraph，防止子图 values 被当作根线程状态。_MessageSeqStamper 把已持久消息的 feed seq 附到输出副本，不回写 checkpoint；查不到的 seq 会随 journal generation 前进再查询，命中后缓存。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1076-L1084) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3978-L3992)

Last-Event-ID 只控制事件游标，不回滚或重启图。memory ID 是时间戳-序号，确认在保留范围内的 ID 从其后回放；落后保留水位返回 StreamGap；恰好等于最后淘汰事件可从当前最早项续接；其他未知 ID 有保守/兼容分支。Redis 对合法未来游标、已观察 stream 消失和保留区间丢失显式给 gap。sse_consumer 发送 recovery=reload_durable_state 的 gap 后返回，**它本身不替客户端加载状态**。

源码依据：[memory.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L82-L112) · [redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L834-L1011) · [services.py 实现](../backend/app/gateway/services.py#L3261-L3383)

#### 4.1.8.3 并发与隔离

Memory bridge 按 run_id 隔离，Condition 保护日志和结束标记，只支持同进程；每个订阅者拿到拷贝避免互相改 payload。队列参数名仍叫 queue_maxsize，默认 256，含义是保留事件数：生产者不 await 消费者腾位置，慢消费者可能 gap。因此网络背压不会逐 token 阻塞图，却可能丢失可回放区间。

源码依据：[memory.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L37-L44) · [memory.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L138-L200)

Redis 用 run 对应 stream 和 token 化 producer lease；Lua 在校验 active/tracking token、确认 END 状态后原子 XADD/裁剪/设 TTL，防止失去所有权的旧 producer 继续发事件或在 END 后追加。lease 默认 30 秒、10 秒续约，丢失会取消 owner task；lease、cleanup 的 token 比较避免旧清理删掉新 owner 的流。它不是消费确认队列：订阅者各自读游标，没有业务 ack 或无限保留。

源码依据：[redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L59-L157) · [redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L349-L379) · [redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L286-L295) · [redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L1012-L1039)

同步 client 的直接迭代让调用方处理速度自然限制继续请求事件的速度；底层模型/外部工具是否已在后台产生输出由依赖实现决定，不能推导所有下游严格按 next 执行。Client 不提供多实例恢复流和多订阅者 fan-out；同一个实例的并发使用也不能从同步 API 推导为安全。

源码依据：[client.py 实现](../backend/packages/harness/deerflow/client.py#L913-L1313)

#### 4.1.8.4 生命周期

Memory stream 由首次发布/订阅建立，publish_end 设置 ended；cleanup 标 retired、清 payload、移除 registry 并唤醒旧生成器，避免它们持有已删除的一代日志永远等下去。worker finalizer 延迟 60 秒清 bridge，RunManager 默认再按五分钟 registry 清理。Redis close 停续约和关闭连接，流还受保留 TTL 控制；cleanup 必须尊重活动 producer，不能看到 terminal record 就无条件 DEL。

源码依据：[memory.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L201-L218) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L236-L287) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3003-L3039) · [redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L1040-L1069)

client generator 的 finally 关闭图流并执行 sandbox lease 清理；消费者提前结束也需要生成器关闭发生后才触发该清理。没有 HTTP 心跳、event ID 和重连缓存，减少了网络协议层资源。两条路径存在的直接理由在 client 注释中：同步嵌入场景若强行复用 async worker，需要额外 loop/thread，并不需要 SSE 编解码与重连协议。

源码依据：[client.py 实现](../backend/packages/harness/deerflow/client.py#L106-L118) · [client.py 实现](../backend/packages/harness/deerflow/client.py#L950-L980)

#### 4.1.8.5 异常与边界

sse_consumer 区分 HEARTBEAT、END、gap、断连与异常：bridge 普通错误不能伪装成用户断连；订阅无终态结束会抛 RuntimeError。creator 断连仅在 apply_on_disconnect、非 store_only、仍 pending/running、无 gap/END、配置 cancel 时才取消；join 观察者断连不创建取消请求，保护只读用户不会借断连取消别人的执行。

源码依据：[services.py 实现](../backend/app/gateway/services.py#L3261-L3383)

Redis 临时读取错误有最多三次连续重试、指数退避且不超过 heartbeat 间隔；ResponseError 对非法游标不静默归零重放。END 只表明流终止，最终业务状态仍须看持久 RunRecord。回放过期不是任务重跑条件；持续丢失网络可能只剩 checkpoint/feed 等 durable state 可读，而不会恢复所有 token delta。

源码依据：[redis.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L834-L1011) · [base.py 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L1-L68)

### 4.1.9 断点续跑与 Checkpointer

#### 4.1.9.1 调用链追踪

Gateway lifespan/deps.langgraph_runtime 进入 make_checkpointer(app_config)；后者是 asynccontextmanager，_select_inner_checkpointer 先选 legacy checkpointer，再选统一 database 的非 memory backend，最后 InMemorySaver。SQLite 路径先解析并建立父目录，AsyncSqliteSaver.from_conn_string → setup → yield；Postgres 验证 URL/可选依赖，构造 pool、确保 schema、setup 后 yield。显式非法类型、缺少依赖或连接串抛错，只有“没有配置”的兜底是 memory，数据库故障不会静默降为不持久。

源码依据：[deps.py 实现](../backend/app/gateway/deps.py#L772-L1127) · [async_provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L220-L258) · [async_provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L192-L218) · [async_provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L113-L153)

~~~text
make_checkpointer
 ├─ legacy checkpointer != None → type: memory / sqlite / postgres / error
 ├─ 否则 database.backend != memory → sqlite / postgres / error
 └─ 否则 → InMemorySaver
 raw saver → effective checkpoint mode
              ├─ full → 直接 yield
              └─ delta → context 内 cache → CachedHistorySaver → yield
服务状态读取/修改 → CheckpointStateAccessor → 模式校验/图物化 → saver
Gateway 线程列表 → ThreadMetaStore（不扫描 checkpoint 内容）
~~~

同步 SDK 通过 provider.get_checkpointer/checkpointer_context 使用同样 backend 解析，但 singleton 与 context 的资源所有权不同，不能把 async 工厂“无全局状态”的性质外推到同步 getter。Studio 的 langgraph.json 指向异步工厂，宿主何时退出 context 决定资源何时关闭。

源码依据：[provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L58-L81) · [provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L196-L242) · [provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L268-L287) · [langgraph.json 实现](../backend/langgraph.json#L1-L21)

#### 4.1.9.2 状态流转

full 模式可在 checkpoint.channel_values 中保存 messages、todos、sandbox 等图 channel；delta 模式的原始值可能是 sentinel，必须通过图与 CachedHistorySaver 物化，不能直接把原始 channel_values 当完整 ThreadState。CheckpointStateAccessor.bind 绑定 graph/saver/store；_prepare_config 复制 configurable/metadata 并注入模式；get/aget/history/ahistory 读取后校验，update/aupdate 写前校验。get_tuple 是明确的 raw 接口，不能借它跳过物化后假定拿到完整状态。

源码依据：[checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L120-L133) · [checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L134-L151) · [checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L158-L177) · [checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L235-L245)

恢复需要 messages、计划、委派 ledger、pending tool/interrupt 等保持关联；runtime 的认证、当前授权、运行能力、预热 task、连接和进程锁则重新建立。sandbox 恢复的是 ID/引用，不是活容器：Middleware 按 user/thread、fork 和 provider 现状验证，再复用或受控重新获取。不同 ID 普通 merge 会抛错，合法重绑定必须用 Overwrite。图状态不是对资源存在性的保证。

源码依据：[middleware.py 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L367-L444) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L79-L99) · [worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L648-L706)

Gateway 的 ThreadMetaStore 保存列表所需的标题、创建/更新时间、owner、状态等轻量记录；thread checkpoint 承担执行和恢复。列表不逐个反序列化整段历史，更新状态时元数据与 checkpoint 仍是两个存储操作，没有跨库原子事务承诺。访问图状态的扩展渠道也通过有效 graph schema，避免忽略 middleware 新增字段。

源码依据：[base.py 实现](../backend/packages/harness/deerflow/persistence/thread_meta/base.py#L1-L150) · [threads.py 实现](../backend/app/gateway/routers/threads.py#L1354-L1396) · [checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L37-L69)

SDK 是明确例外：DeerFlowClient.list_threads 通过 list_thread_spans 查询 saver 的首尾 checkpoint 标识，再加载每个返回线程的首尾 checkpoint；它不走 Gateway ThreadMetaStore。不能将 Gateway 的列表分工当作所有 Python 客户端的通用实现。

源码依据：[client.py 实现](../backend/packages/harness/deerflow/client.py#L762-L807)

#### 4.1.9.3 并发与隔离

saver 的分区依赖 thread_id/checkpoint_ns/checkpoint_id；user 归属由 Gateway/Auth 与元数据层强制，不应声称 saver 自动提供用户权限。RunManager 的持久 admission/execution lease 防止多实例对一个运行同时推进，CheckpointStateAccessor 的模式 gate 防止 full/delta 混用；reducer 只处理图 channel 合并，不能代替上述执行权协议。

源码依据：[manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2413-L2704) · [checkpoint_mode.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_mode.py#L1-L147) · [checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L134-L151)

InMemorySaver 仅当前进程持有，重启丢失，多 Gateway 各自不同；SQLite 提供本地文件数据库，Postgres pool 支持共享数据库连接，数据库本身并不让任意并发工具副作用自动幂等。delta cache 随 make_checkpointer context 存活，命名空间隔离和版本检查不能被简化为“缓存就是持久权威”。

源码依据：[async_provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L220-L258) · [cached_saver.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/cached_saver.py#L1-L135)

#### 4.1.9.4 生命周期

SQLite 连接和 Postgres pool 的进入/退出都被 drained_async_context 包装，取消时排空 exit，防止连接尚未归还就先关闭池。Postgres 使用 autocommit=True、prepare_threshold=0、dict_row、TCP keepalive 和连接检查；search_path 合并进 DSN，保留既有 libpq options，不覆盖配置的 statement_timeout。schema 在 saver 建表前创建。宿主 shutdown 先 drain worker 再退 saver context，部分启动失败按 AsyncExitStack 逆序释放。

源码依据：[async_provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L52-L79) · [async_provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L80-L91) · [deps.py 实现](../backend/app/gateway/deps.py#L772-L1127)

同步 get_checkpointer 持有 singleton context，reset_checkpointer 负责关闭并清缓存；checkpointer_context 是明确的局部所有者。InMemorySaver 兜底让未配置的嵌入场景仍能在当前进程连续多轮；这是基于接口使用方式的设计解释，不意味着生产部署已持久化。

源码依据：[provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L196-L242) · [provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L243-L266) · [provider.py 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L268-L287)

#### 4.1.9.5 异常与边界

模式不兼容 fail-closed，防止把 delta sentinel 当普通状态继续写。aget 在未指定历史 checkpoint_id 的 delta 最新读取中，会检查解析出的目标是否被 retention 删除，必要时从新 head 重试一次；显式选择历史版本不会静默移动到别的 checkpoint。build_state_mutation_graph 用真实有效 schema 建只含 finish 的图，保证整体替换后无 pending agent 节点；缺少扩展 channel 的基础 schema 会丢写未知字段，这正是源码要求传有效 schema 的原因。

源码依据：[checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L158-L177) · [checkpoint_state.py 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L37-L69)

断点续跑必须同时解释 pending writes、工具反馈和中断，不能只加载 messages。恢复后某节点可能重做，外部工具已产生的网络/文件副作用不受 saver 事务撤销；run 失联的 reconcile 只是标 error，并非自动继续图。memory 兜底不会修复数据库配置错误，恢复 sandbox ID 也不会创造容器。以上边界决定该实现提供图执行状态恢复，而不能据此声称所有外部动作恰好一次。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1005-L2499) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2798-L2909)

**本章其他来源**

- `backend/packages/harness/deerflow/extensions/stack.py`：扩展加载与注册契约。
- `backend/docs/middleware-execution-flow.md`：仓库说明材料。
- `backend/packages/harness/deerflow/extensions/ordering.py`：扩展加载与注册契约。
- `backend/packages/harness/deerflow/agents/task_continuity/state.py`：关联实现或工程声明。
- `backend/tests/test_thread_state_reducers.py`：已有断言与预期契约，未执行。
- `backend/packages/harness/deerflow/persistence/thread_meta/base.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/sandbox/security.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/sandbox/tools.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/community/aio_sandbox/backend.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/config.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/tools/builtins/task_tool.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/config/subagents_config.py`：配置来源、默认值或声明。
- `backend/packages/harness/deerflow/agents/lead_agent/prompt.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/agents/middlewares/tool_call_metadata.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/agents/middlewares/dangling_tool_call_middleware.py`：关联实现或工程声明。
- `backend/packages/harness/deerflow/runtime/checkpointer/cached_saver.py`：运行控制与持久状态协作。

# 5. 核心文件逐函数讲解

本章逐一覆盖 45 个核心文件中自行定义的 1466 个 `def`/`async def` 函数、方法及属性（含局部回调）；模块级关键初始化和退出 lambda 在文件说明中另行解释。每个核心文件首次出现时列出完整仓库相对路径，标题以职责命名。关联文件只按实际调用链展开，不将其全部函数算入覆盖承诺。

复杂函数使用独立编号小节；连续的简单访问、转发和局部辅助函数合并说明。参数摘要保留名称与默认值，类型以源码定义为准；参数摘要不是完整签名。第 4 章给出机制级前因后果，本章说明具体函数承担的步骤、分支和副作用。局部回调在父函数建立后由宿主触发，静态存在调用不表示每条路径必经；抽象契约和实际实现分别说明。

## 5.1 发布入口与惰性导出

源码位置：`backend/packages/harness/deerflow/agents/__init__.py`。

发布入口仅做惰性转发与按需属性导出，避免导入 deerflow.agents 就立即加载完整模型和中间件依赖。这里不创建运行资源；实际装配见 4.1.1。

### 5.1.1 辅助函数与局部契约

- **`make_lead_agent`**（`config`）：只在被调用时导入应用工厂并转发 config，返回实际编译图；包导入阶段不装配 Agent。

  源码依据：[make_lead_agent 实现](../backend/packages/harness/deerflow/agents/__init__.py#L20-L33)


- **`__getattr__`**（`name`）：仅为白名单导出名惰性导入真实符号，未知属性抛 AttributeError；动态导出不是任意模块反射入口。

  源码依据：[__getattr__ 实现](../backend/packages/harness/deerflow/agents/__init__.py#L34-L51)


## 5.2 参数化 Agent 工厂

源码位置：`backend/packages/harness/deerflow/agents/factory.py`。

纯参数 SDK 工厂把模型、工具、RuntimeFeatures 和调用者管理的运行资源组合成图；full takeover 与自动装配互斥，delta 持久化受兼容门禁限制。_assemble_from_features 逐个启用能力，_insert_extra 处理相对位置并校验。SDK 接口不承诺 Gateway 的归属、幂等准入或 SSE 生命周期。

### 5.2.1 create_deerflow_agent

**参数摘要**

```text
    model,
    tools=None,
    system_prompt=None,
    middleware=None,
    features=None,
    extra_middleware=None,
    plan_mode=False,
    state_schema=None,
    checkpoint_channel_mode="full",
    checkpoint_snapshot_frequency=None,
    checkpointer=None,
    name="default",
    subagent_runtime=None,
    pii_redaction_config=None
```

**职责与返回契约**

这是供 Python 调用者直接使用的参数化建图入口：调用者提供模型、工具、系统提示及资源，返回编译后的 Agent 图。建图不等于接收 HTTP 请求，也不会自动取得 Gateway 的线程所有权、持久准入和内部身份。

1. 首先检查装配模式。显式传入 `middleware` 就接管整条链，因此不能同时指定 `features` 或非空 `extra_middleware`。显式子任务 runtime 只能配合已启用的子任务 feature；配置了持久批处理却没有启动 worker 时立即抛错。
2. 拒绝 `delta` 与非空 checkpointer 的组合：这个工厂没有应用入口的模式标记和兼容门禁，直接持久化可能把不同 channel 解释混入同一历史。无 checkpointer 的短期 delta 图允许使用。
3. 复制工具列表，按模式取得或适配 state schema。自动装配调用 `_assemble_from_features`，产生中间件与附加工具；附加工具按名称去重，调用者工具优先。takeover 路径直接复制中间件列表。
4. 统一中间件 schema 后调用 `create_agent`；传入模型、有效工具、系统提示、schema、checkpointer 和名称。资源的启停仍属于调用方。

源码依据：[模式互斥与持久化限制](../backend/packages/harness/deerflow/agents/factory.py#L141-L165) · [工具去重和实际建图](../backend/packages/harness/deerflow/agents/factory.py#L166-L204)

**失败与边界**

校验失败发生在图运行之前。自定义中间件或外部模型的异常没有在工厂中统一恢复；takeover 绕过的是自动装配能力，不能据此认为仍安装了相同的授权、脱敏和生命周期链。

### 5.2.2 _assemble_from_features

**参数摘要**

```text
    feat,
    name="default",
    plan_mode=False,
    extra_middleware=None,
    subagent_runtime=None,
    pii_redaction_config=None
```

按 RuntimeFeatures 依次安装线程、沙箱、工具错误、技能、记忆、压缩和子任务等能力并收集附加工具，处理 PII/plan 与显式 SubagentRuntime；返回中间件和工具两份装配结果，尚不运行图。

源码依据：[_assemble_from_features 实现](../backend/packages/harness/deerflow/agents/factory.py#L205-L473)


### 5.2.3 _insert_extra

参数：`chain, extras`。

合并调用者的额外中间件并应用相对锚点规则，随后校验最终顺序；不以简单列表拼接绕过资源/消费者依赖。

源码依据：[_insert_extra 实现](../backend/packages/harness/deerflow/agents/factory.py#L474-L548)


## 5.3 主 Agent 装配

源码位置：`backend/packages/harness/deerflow/agents/lead_agent/agent.py`。

配置驱动的主装配器：解析 context/configurable、模型授权、技能/工具目录与交互策略，然后建立 middleware/schema/prompt，最后调用 create_agent。模块导入定义 assembly 数据类型与内部常量；运行级资源由外层 worker 持有。graph-only 包装与可观察 assembly 是不同返回契约，详见 4.1.1。

### 5.3.1 辅助函数与局部契约

- **`unwrap_agent_graph`**（`agent_result`）：返回 `agent_result.graph if isinstance(agent_result, LeadAgentAssembly) else agent_result`。

  源码依据：[unwrap_agent_graph 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L109-L125)


### 5.3.2 _subagent_release_policy

参数：`app_config, enabled, max_concurrent, max_total, allowed_subagents=None`。

以 `{ "enabled": enabled, "max_concurrent": max_concurrent, "max_total": max_total, "type_allowlist": [], "runtime_limits": {}, }` 计算 `policy: dict[str, object]`。关键判定为 `not enabled`、`subagent_config is None`。

以 `{ "max_turns": subagent_config.max_turns, "timeout_seconds": subagent_config.timeout_seconds, }` 计算 `runtime_limits[name]`；以 `type_allowlist` 计算 `policy["type_allowlist"]`；其他写入目标为 `policy["runtime_limits"]`。分支返回 `policy`，提前返回结束该路径。

源码依据：[_subagent_release_policy 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L126-L166)


### 5.3.3 辅助函数与局部契约

- **`_resolve_runtime_option`**（`cfg, key, agent_value, default`）：在 `条件 key in cfg` 分支中，返回 `cfg[key]`；在 `条件 agent_value is not None` 分支中，返回 `agent_value`；返回 `default`。

  源码依据：[_resolve_runtime_option 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L167-L182)


- **`_append_named_tools_without_conflicts`**（`tools, new_tools, kind`）：以 `{getattr(tool, "name", None) for tool in tools}` 计算 `existing_names`；在 `循环 for new_tool in new_tools` → `条件 new_tool.name in existing_names` 分支中，进入下一轮循环；在 `循环 for new_tool in new_tools` 分支中，执行 `tools.append(new_tool)`；在 `循环 for new_tool in new_tools` 分支中，执行 `existing_names.add(new_tool.name)`。

  源码依据：[_append_named_tools_without_conflicts 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L183-L193)


- **`_append_memory_tools_without_name_conflicts`**（`tools`）：执行 `_append_named_tools_without_conflicts(tools, get_memory_tools(), kind="Memory")`。

  源码依据：[_append_memory_tools_without_name_conflicts 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L194-L200)


- **`_append_project_document_tools_if_pinned`**（`tools, cfg`）：在 `条件 PROJECT_CONTEXT_KEY not in cfg` 分支中，返回 None，提前结束当前路径；执行 `_append_named_tools_without_conflicts(tools, get_project_document_tools(), kind="Project document")`。

  源码依据：[_append_project_document_tools_if_pinned 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L201-L218)


- **`_get_runtime_config`**（`config`）：以 `dict(config.get("configurable", {}) or {})` 计算 `cfg`；以 `config.get("context", {}) or {}` 计算 `context`；在 `条件 isinstance(context, dict)` 分支中，执行 `cfg.update(context)`；返回 `cfg`。

  源码依据：[_get_runtime_config 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L219-L227)


- **`_resolve_model_name`**（`requested_model_name=None, app_config=None`）：已传且存在的请求模型优先，否则回退全局首个配置；Agent 默认由上游选择后作为 requested_model_name 传入。完全没有模型抛 ValueError，函数体与旧注释不同。

  源码依据：[_resolve_model_name 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L228-L242)


### 5.3.4 _authorize_model_name

参数：`model_name, context, app_config`。

对所选模型执行 model:use 授权；拒绝后寻找可用替代模型，没有替代时依据授权配置 fail-closed 或保留，不能把名称回退理解成权限放宽。

源码依据：[_authorize_model_name 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L243-L337)


### 5.3.5 辅助函数与局部契约

- **`_create_summarization_middleware`**（`app_config=None, run_model_name=None, skip_memory_flush=False, extensions=None`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  create_summarization_middleware( app_config=app_config, run_model_name=run_model_name,
     skip_memory_flush=skip_memory_flush, extensions=extensions, )
  ```


  源码依据：[_create_summarization_middleware 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L338-L358)


### 5.3.6 _create_todo_list_middleware

参数：`is_plan_mode`。

在 `条件 not is_plan_mode` 分支中，返回 `None`；构造多行字符串赋给 `system_prompt`，文本见源码；构造多行字符串赋给 `tool_description`，文本见源码；返回 `TodoMiddleware(system_prompt=system_prompt, tool_description=tool_description)`。

源码依据：[_create_todo_list_middleware 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L359-L484)


### 5.3.7 build_middlewares

**参数摘要**

```text
    config,
    model_name,
    agent_name=None,
    custom_middlewares=None,
    available_skills=None,
    memory_enabled=True,
    owns_agent_skill_projection=True,
    app_config=None,
    deferred_setup=None,
    mcp_routing_middleware=None,
    user_id=None,
    authorization_provider=None,
    skill_authorization=None,
    extensions=None,
    subagent_execution_capacity=None
```

先生成共享链，再条件添加 Lead 能力与终止保护，Clarification 后可加 RunInput；最后与扩展合成并检查顺序，列表决定实际图钩子形态。

源码依据：[build_middlewares 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L485-L822)


### 5.3.8 辅助函数与局部契约

- **`_available_skill_names`**（`agent_config, is_bootstrap`）：在 `条件 is_bootstrap` 分支中，返回 `set(_BOOTSTRAP_SKILL_NAMES)`；在 `条件 agent_config and agent_config.skills is not None` 分支中，返回 `set(agent_config.skills)`；返回 `None`。

  源码依据：[_available_skill_names 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L823-L830)


- **`_insert_hitl_approval_middleware`**（`middlewares, final_tools, app_config, supports_resume, model_name=None`）：以 `getattr(app_config, "hitl_approval", None)` 计算 `hitl_config`；在 `条件 hitl_config is None or hitl_config.enabled is not True` 分支中，返回 None，提前结束当前路径；以 `{tool.name for tool in final_tools}` 计算 `known_tool_names`；以 `set(hitl_config.tools) - known_tool_names` 计算 `unknown_names`；以 `next((i + 1 for i, middleware in enumerate(middlewares) if type(middleware).__name__ == "ArtifactResolutionMiddleware"), 0)` 计算 `index`；执行 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  middlewares.insert(index, ToolApprovalMiddleware(hitl_config, app_config=app_config,
       supports_resume=supports_resume, model_name=model_name))
  ```


  源码依据：[_insert_hitl_approval_middleware 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L831-L853)


- **`_load_enabled_available_skills`**（`available_skills, app_config, user_id=None`）：在 `try` 分支中，以 `get_enabled_skills_for_config(app_config, user_id=user_id)` 计算 `skills`；在 `捕获 Exception` 分支中，向外继续抛出当前异常；在 `条件 available_skills is None` 分支中，返回 `skills`；返回 `[skill for skill in skills if skill.name in available_skills]`。

  源码依据：[_load_enabled_available_skills 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L854-L867)


- **`make_lead_agent`**（`config`）：发布 graph-only 契约，取 assemble_lead_agent(config).graph；不额外执行模型。

  源码依据：[make_lead_agent 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L868-L872)


### 5.3.9 assemble_lead_agent

参数：`config, app_config=None`。

选择配置并冻结 checkpoint 模式和快照 cadence，把内部模式信息写入本次 config，再返回完整 LeadAgentAssembly。

源码依据：[assemble_lead_agent 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L873-L913)


### 5.3.10 辅助函数与局部契约

- **`_make_lead_agent`**（`config, app_config`）：内部 graph-only 包装，直接调用 _assemble_lead_agent；与公开 assemble 的冻结步骤不同，不是每次公开调用的必经层。

  源码依据：[_make_lead_agent 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L914-L923)


### 5.3.11 _complete_assembly

**参数摘要**

```text
    config,
    graph,
    namespace,
    agent_name,
    requested_model,
    effective_model,
    model_config,
    model_overrides=None,
    thinking_enabled,
    reasoning_effort,
    rendered_base_prompt,
    tools,
    middlewares,
    deferred_names,
    enabled_skills,
    effective_policies
```

无 observer/capture 时避免构造 descriptor；需要证据时组装有效工具、中间件、策略描述并通知 observer，始终保留 effective_model。

源码依据：[_complete_assembly 实现](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L924-L989)


### 5.3.12 _assemble_lead_agent

参数：`config, app_config`。

**职责与隐式上下文**

输入为本次 `RunnableConfig` 和已加载的 `AppConfig`，输出 `LeadAgentAssembly`。它把“请求想使用什么”缩窄为“当前身份和部署实际允许什么”，然后建图。它还会修改本次 config 中的有效开关、metadata 和 callbacks；这些修改供后续 worker、记录与装配观察者使用。

1. 合并 `configurable` 与 `context`，解析权威用户身份、checkpoint 模式、交互方式、Agent 名称、子任务并发与总数。scheduler capability 必须是宿主对象、模式相符且属于允许入口；历史字段不能自己恢复这一能力。
2. 加载当前用户的 Agent 配置。请求可关闭子任务，但不能突破 Agent 的 `allowed_subagents=[]` 硬拒绝。Agent 的 skill 白名单与授权 provider 的资源过滤取交集，启用目录仍需经过可见性约束。
3. 选择请求模型、Agent 默认模型或全局模型；显式选择走严格授权，普通默认选择可寻找获准替代模型。thinking 与 effort 按请求、Agent 默认、运行默认解析，再经过模型 reasoning contract 归一化。

源码依据：[身份、能力与 Agent 限制](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L990-L1049) · [技能与模型授权](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L1050-L1155)

4. 将有效设置写入 metadata，按根图挂接回调。加载获准技能；bootstrap 分支使用受限技能和建 Agent 工具，走自己的提示与装配返回路径。
5. 普通分支创建实际 chat model，按工具组、MCP、会话读取器和 scheduler capability 收集工具。Webhook 来源不暴露持久修改 Agent 的 `update_agent`。显式子模型还要独立授权。
6. 先对完整候选工具执行授权，再区分直接暴露工具、延迟发现工具与后加工具；构造路由提示和 middleware。中间件声明的新工具继续缩窄，随后插入审批层。
7. 用同一有效能力集构造系统提示，规范中间件 state schema，再核对声明工具视图。最终以 `get_thread_state_schema(mode)` 和 `context_schema=dict` 调用 `create_agent`，由 `_complete_assembly` 生成可选描述证据。

源码依据：[普通分支工具收集与授权](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L1329-L1401) · [中间件、提示与实际建图](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L1402-L1483)

**设计作用与代价**

工具可见性、提示说明、执行时授权分担不同责任。过滤目录能减少模型看到的无效能力，执行 wrapper 才检查实际动作；提示文本自身不是权限边界。这里的装配依赖配置、扩展、工具目录和身份，不能仅凭导入同一个工厂就断言两个入口拥有相同能力。缺模型、非法显式选择、目录读取或授权错误可能使装配失败，模型尚未开始执行。

## 5.4 线程状态与合并规则

源码位置：`backend/packages/harness/deerflow/agents/thread_state.py`。

这是状态定义及纯合并算法文件。reducer 不持有分布式锁，输入中的 None/空值/不同 ID 各有不同含义；动态 delta schema 用缓存复用类型，以免同配置不断生成新类型。类字段定义与各函数合起来决定持久恢复形态，详见 4.1.3。 导入时执行 delta_messages_field() 建立默认 DeltaChannel 注解和 DeltaThreadState，随后按请求模式/cadence 另选有效 schema；这是类型/channel 定义，不是启动一次图。

源码依据：[thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L531-L560)

### 5.4.1 辅助函数与局部契约

- **`_resolve_snapshot_frequency`**（`snapshot_frequency`）：在 `条件 snapshot_frequency is not None` 分支中，返回 `snapshot_frequency`；返回 `resolve_checkpoint_snapshot_frequency()`。

  源码依据：[_resolve_snapshot_frequency 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L25-L35)


- **`merge_sandbox`**（`existing, new`）：新值 None 保留旧值；无旧值接收新值；同 sandbox_id 幂等返回旧对象；两个不同非空 ID 抛 ValueError，阻止静默切换隔离资源。

  源码依据：[merge_sandbox 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L79-L99)


- **`merge_artifacts`**（`existing, new`）：在 `条件 existing is None` 分支中，返回 `new or []`；在 `条件 new is None` 分支中，返回 `existing`；返回 `list(dict.fromkeys(existing + new))`。

  源码依据：[merge_artifacts 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L103-L112)


- **`merge_viewed_images`**（`existing, new`）：在 `条件 existing is None` 分支中，返回 `new or {}`；在 `条件 new is None` 分支中，返回 `existing`；在 `条件 len(new) == 0` 分支中，返回 `{}`；以 `{key: value for key, value in existing.items() if key not in new}` 计算 `merged`；执行 `merged.update(new)`；返回 `merged`。

  源码依据：[merge_viewed_images 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L113-L138)


- **`merge_todos`**（`existing, new`）：只让新的非 None 完整列表替换旧值，[] 明确清空；None 表示保留，修复后续节点空写覆盖规划的问题。

  源码依据：[merge_todos 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L139-L151)


- **`merge_goal`**（`existing, new`）：在 `条件 new is None` 分支中，返回 `existing`；返回 `new`。

  源码依据：[merge_goal 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L152-L158)


- **`merge_promoted`**（`existing, new`）：以 `new.get("turn_id")` 计算 `new_turn_id`；以 `{ "catalog_hash": new["catalog_hash"], "names": list(dict.fromkeys(names)), }` 计算 `merged: PromotedTools`。关键判定为 `not new`、`not isinstance(existing_names, list) or not all(isinstance(name, str) for name in existing_names)`、`turn_id is not None`，相应动作只在对应分支执行。以 `turn_id` 计算 `merged["turn_id"]`。分支返回 `existing`、`merged`，提前返回结束该路径。

  源码依据：[merge_promoted 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L165-L192)


- **`delegation_key`**（`entry`）：返回 `entry.get("run_id") or None, entry["id"], entry.get("dispatch_message_id") or None`。

  源码依据：[delegation_key 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L226-L234)


- **`find_delegation_key`**（`keys, key`）：在 `条件 key in keys` 分支中，返回 `key`；以 `key` 计算 `run_id, entry_id, dispatch_id`；在 `循环 for prior in reversed(keys)` → `条件 prior[1] != entry_id` 分支中，进入下一轮循环；在 `循环 for prior in reversed(keys)` → `条件 run_id is None` 分支中，返回 `prior`；在 `循环 for prior in reversed(keys)` → `条件 prior[0] == run_id and (dispatch_id is None or prior[2] is None)` 分支中，返回 `prior`；返回 `None`。

  源码依据：[find_delegation_key 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L235-L257)


### 5.4.2 merge_delegations

参数：`existing, new`。

按 run/call/dispatch 三元关联更新，终态不被迟到非终态降级；兼容旧条目并裁剪到50项，不能按 call_id 单独去重。

源码依据：[merge_delegations 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L258-L298)


### 5.4.3 辅助函数与局部契约

- **`_normalize_skill_entry`**（`entry`）：以 `entry.get("description")` 计算 `description`；以 `entry.get("loaded_at")` 计算 `loaded_at`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "name": str(entry.get("name") or ""), "path": str(entry["path"]),
     "description": " ".join(description.split())[:_SKILL_DESCRIPTION_MAX_CHARS] if isinstance(description,
       str) else "", "loaded_at": loaded_at if isinstance(loaded_at,
       int) else 0, }
  ```


  源码依据：[_normalize_skill_entry 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L310-L321)


- **`merge_skill_context`**（`existing, new`）：以 `[_normalize_skill_entry(entry) for entry in existing or []]` 计算 `normalized_existing`；以 `merged[-_SKILL_CONTEXT_MAX_ENTRIES:]` 计算 `merged`。关键判定为 `not new`、`path not in by_path`、`path in by_path`，相应动作只在对应分支执行。以 `entry` 计算 `by_path[path]`；以 `entry` 计算 `by_path[path]`。执行 `order.append(path)`；执行 `order.remove(path)`。分支返回 `normalized_existing`、`merged`，提前返回结束该路径。

  源码依据：[merge_skill_context 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L322-L359)


- **`merge_tool_artifacts`**（`existing, new`）：以 `{}` 计算 `by_handle: dict[str, ArtifactEntry]`；以 `merged[-_ARTIFACT_MAX_ENTRIES_CEILING:]` 计算 `merged`。关键判定为 `not new`、`not isinstance(entry, dict) or "handle" not in entry`、`handle not in by_handle`，相应动作只在对应分支执行。以 `entry` 计算 `by_handle[handle]`。执行 `order.append(handle)`。分支返回 `existing or []`、`merged`，提前返回结束该路径。

  源码依据：[merge_tool_artifacts 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L383-L421)


- **`_normalize_messages`**（`value`）：以 `value if isinstance(value, list) else [value]` 计算 `values`；以 `[message_chunk_to_message(cast(BaseMessageChunk, message)) for message in convert_to_messages(values)]` 计算 `messages`；在 `循环 for message in messages` → `条件 message.id is None` 分支中，以 `str(uuid.uuid4())` 计算 `message.id`；返回 `messages`。

  源码依据：[_normalize_messages 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L444-L452)


- **`_index_messages`**（`messages`）：以 `{}` 计算 `latest_position: dict[str, int]`；以 `{}` 计算 `positions_by_id: dict[str, list[int]]`；在 `循环 for position, message in enumerate(messages)` → `条件 message is None` 分支中，进入下一轮循环；在 `循环 for position, message in enumerate(messages)` 分支中，以 `cast(str, message.id)` 计算 `message_id`；在 `循环 for position, message in enumerate(messages)` 分支中，以 `position` 计算 `latest_position[message_id]`；在 `循环 for position, message in enumerate(messages)` 分支中，执行 `positions_by_id.setdefault(message_id, []).append(position)`；返回 `latest_position, positions_by_id`。

  源码依据：[_index_messages 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L453-L466)


- **`_raise_null_write`**（`has_messages`）：以 `"left" if has_messages else "right"` 计算 `received`；抛出 `ValueError(f"Must specify non-null arguments for both 'left' and 'right'. Only received: '{received}'.")`。

  源码依据：[_raise_null_write 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L467-L473)


### 5.4.4 merge_message_writes

参数：`state, writes`。

按批处理消息增量维护 ID 索引，执行同 ID 替换、RemoveMessage 与全清语义；非法空写显式报错，不将 delta 当作普通追加列表。

源码依据：[merge_message_writes 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L474-L530)


### 5.4.5 辅助函数与局部契约

- **`delta_messages_field`**（`snapshot_frequency=DEFAULT_CHECKPOINT_SNAPSHOT_FREQUENCY`）：返回 `Annotated[ list[AnyMessage], DeltaChannel(merge_message_writes, snapshot_frequency=snapshot_frequency), ]`。

  源码依据：[delta_messages_field 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L531-L538)


- **`get_thread_state_schema`**（`mode, snapshot_frequency=None`）：在 `条件 mode != "delta"` 分支中，返回 `ThreadState`；返回 `_delta_thread_state_schema(_resolve_snapshot_frequency(snapshot_frequency))`。

  源码依据：[get_thread_state_schema 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L564-L569)


- **`_delta_thread_state_schema`**（`snapshot_frequency`）：在 `条件 snapshot_frequency == DEFAULT_CHECKPOINT_SNAPSHOT_FREQUENCY` 分支中，返回 `DeltaThreadState`；以 `get_type_hints(ThreadState, include_extras=True)` 计算 `annotations`；以 `delta_messages_field(snapshot_frequency)` 计算 `annotations["messages"]`；返回 `TypedDict( f"DeltaThreadState_f{snapshot_frequency}", annotations, total=getattr(ThreadState, "__total__", True), )`。

  源码依据：[_delta_thread_state_schema 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L571-L584)


- **`adapt_state_schema_for_mode`**（`schema, mode, snapshot_frequency=None`）：在 `条件 mode == "full"` 分支中，返回 `schema`；返回 `_adapt_state_schema_for_delta(schema, _resolve_snapshot_frequency(snapshot_frequency))`。

  源码依据：[adapt_state_schema_for_mode 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L585-L590)


- **`_adapt_state_schema_for_delta`**（`schema, snapshot_frequency`）：以 `get_type_hints(schema, include_extras=True)` 计算 `annotations`；以 `delta_messages_field(snapshot_frequency)` 计算 `annotations["messages"]`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  TypedDict( f"Delta{schema.__module__.replace('.', '_')}_{schema.__name__}_f{snapshot_frequency}",
     annotations, total=getattr(schema, "__total__", True), )
  ```


  源码依据：[_adapt_state_schema_for_delta 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L592-L601)


- **`normalize_middleware_state_schemas`**（`middleware, mode, snapshot_frequency=None`）：以 `_resolve_snapshot_frequency(snapshot_frequency)` 计算 `resolved_frequency`；以 `[]` 计算 `normalized`。关键判定为 `mode == "full"`、`schema is None`，相应动作只在对应分支执行。执行 `normalized.append(item)`；执行 `normalized.append(adapted)`。分支返回 `list(middleware)`、`normalized`，提前返回结束该路径。

  源码依据：[normalize_middleware_state_schemas 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L602-L616)


## 5.5 工具错误与共享运行链

源码位置：`backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py`。

文件同时承担工具异常转结果和共享运行时链装配。错误转换须保留 framework interrupt/cancellation 的传播，技能读取元数据需解析为规范技能标识；共同链由一处建立，Lead/子图在其上分化，避免两条链悄悄缺少保护。wrapper 的内外层决定进度与 receipt 看到的实际状态。

### 5.5.1 辅助函数与局部契约

- **`_stamp_task_exception_status`**（`message, tool_name, error`）：在 `条件 tool_name != _TASK_TOOL_NAME` 分支中，返回 `message`；以 `format_subagent_result_message("failed", error=error)` 计算 `content, metadata_error`；在 `条件 not content.endswith((".", "!", "?"))` 分支中，对 `content` 执行 `+= "."` 更新；以 `f"{content} {_RECOVERY_HINT}"` 计算 `message.content`；以 `dict(message.additional_kwargs or {})` 计算 `existing`；执行 `existing.update(make_subagent_additional_kwargs("failed", error=metadata_error))`；以 `existing` 计算 `message.additional_kwargs`；返回 `message`。

  源码依据：[_stamp_task_exception_status 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L58-L71)


- **`ToolErrorHandlingMiddleware.__init__`**（`app_config=None, skill_authorization=None, user_id=None`）：执行 `super().__init__()`；以 `app_config` 计算 `self._app_config`；以 `skill_authorization` 计算 `self._skill_authorization`；以 `user_id` 计算 `self._user_id`；在 `条件 app_config is None` 分支中，以 `frozenset(DEFAULT_SKILL_FILE_READ_TOOL_NAMES)` 计算 `self._skill_read_tool_names`；在 `条件 app_config is None` 分支中，以 `DEFAULT_SKILLS_CONTAINER_PATH` 计算 `self._skills_root`；在 `同层否则分支` 分支中，以 `frozenset(app_config.summarization.skill_file_read_tool_names)` 计算 `self._skill_read_tool_names`；在 `同层否则分支` 分支中，以 `app_config.skills.container_path` 计算 `self._skills_root`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L75-L86)


- **`ToolErrorHandlingMiddleware._model_request`**（`request`）：以 `[tool_result_for_model(message) if isinstance(message, ToolMessage) else message for message in request.messages]` 计算 `messages`。关键判定为 `all(projected is original for projected, original in zip(messages, request.messages, strict=True))`、`system is None`、`not system.additional_kwargs.get(_TOOL_RESULT_CONTRACT_KEY)`，相应动作只在对应分支执行。分支返回 `request`、`request.override(messages=messages, system_message=system)`，提前返回结束该路径。

  源码依据：[_model_request 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L88-L99)


- **`ToolErrorHandlingMiddleware.wrap_model_call`**（`request, handler`）：返回 `handler(self._model_request(request))`。

  源码依据：[wrap_model_call 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L101-L103)


- **`ToolErrorHandlingMiddleware.awrap_model_call`**（`request, handler`）：返回 `await handler(self._model_request(request))`。

  源码依据：[awrap_model_call 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L105-L107)


- **`ToolErrorHandlingMiddleware._storage`**（无显式业务参数）：在 `条件 self._user_id is not None` 分支中，返回 `get_or_new_user_skill_storage(self._user_id, app_config=self._app_config)`；在 `条件 self._app_config is not None` 分支中，返回 `get_or_new_skill_storage(app_config=self._app_config)`；返回 `get_or_new_skill_storage()`。

  源码依据：[_storage 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L108-L120)


- **`ToolErrorHandlingMiddleware._canonical_skill_name`**（`skill_md_path`）：在 `try` 分支中，以 `build_container_path_registry(self._storage())` 计算 `registry`；在 `捕获 Exception` 分支中，返回 `_skill_name_from_path(skill_md_path)`；返回 `canonical_skill_name(registry, skill_md_path) or _skill_name_from_path(skill_md_path)`。

  源码依据：[_canonical_skill_name 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L121-L142)


- **`ToolErrorHandlingMiddleware._build_error_message`**（`request, exc`）：以 `str(request.tool_call.get("name") or "unknown_tool")` 计算 `tool_name`。关键判定为 `len(detail) > 500`，相应动作只在对应分支执行。分支返回 `stamp_exception_meta(message, exc)`，提前返回结束该路径。

  源码依据：[_build_error_message 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L143-L163)


### 5.5.2 ToolErrorHandlingMiddleware._stamp_skill_read_metadata

参数：`message, request, tool_name, activation_allowed=None, skill_name=None`。

以 `message.additional_kwargs.get(TOOL_META_KEY)` 计算 `tool_meta`。关键判定为 `tool_name not in self._skill_read_tool_names`、`getattr(message, "status", "success") == "error"`、`isinstance(tool_meta, dict) and tool_meta.get("status") == "error"`。

以 `True` 计算 `existing[SKILL_CONTEXT_DENIED_KEY]`；以 `dict(entry)` 计算 `existing[SKILL_CONTEXT_ENTRY_KEY]`；其他写入目标为 `existing[SKILL_USAGE_KEY]`。分支返回 `message`，提前返回结束该路径。

源码依据：[_stamp_skill_read_metadata 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L164-L240)


### 5.5.3 ToolErrorHandlingMiddleware._amaybe_stamp

参数：`result, request`。

以 `str(request.tool_call.get("name") or "")` 计算 `tool_name`。关键判定为 `isinstance(result, ToolMessage)`、`not isinstance(update, dict)`、`isinstance(messages, ToolMessage)`。等待 `stamp(result)` 完成；等待 `stamp(messages)` 完成。分支返回 `result`，提前返回结束该路径。

源码依据：[_amaybe_stamp 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L241-L288)


### 5.5.4 辅助函数与局部契约

- **`ToolErrorHandlingMiddleware._amaybe_stamp.stamp`**（`message`）：父函数内定义的回调。以 `dict(message.additional_kwargs or {})` 计算 `existing`。关键判定为 `tool_call_id is None or str(message.tool_call_id) == tool_call_id`、`probe is not None and self._skill_authorization is not None`，相应动作只在对应分支执行。执行 `existing.pop(SKILL_CONTEXT_ENTRY_KEY, None)`；执行 `existing.pop(SKILL_USAGE_KEY, None)`。

  源码依据：[stamp 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L256-L273)


- **`ToolErrorHandlingMiddleware._resolve_skill_read_entry`**（`message, request, tool_name`）：在 `条件 tool_name not in self._skill_read_tool_names` 分支中，返回 `None`；在 `条件 getattr(message, "status", "success") == "error"` 分支中，返回 `None`；以 `message.content if isinstance(message.content, str) else None` 计算 `content`；在 `条件 content is None` 分支中，返回 `None`；以 `_tool_call_path(request.tool_call)` 计算 `path`；在 `条件 path is None` 分支中，返回 `None`；返回 `build_skill_entry_metadata_from_read(path, content, skills_root=self._skills_root)`。

  源码依据：[_resolve_skill_read_entry 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L289-L308)


- **`ToolErrorHandlingMiddleware._maybe_stamp`**（`result, request`）：以 `str(request.tool_call.get("name") or "")` 计算 `tool_name`；以 `_tool_call_id(request.tool_call)` 计算 `tool_call_id`；建立局部回调 `def stamp(message: ToolMessage) -> None`，行为在独立条目展开；在 `条件 isinstance(result, ToolMessage)` 分支中，执行 `stamp(result)`；在 `条件 isinstance(result, ToolMessage)` 分支中，返回 `result`；以 `getattr(result, "update", None)` 计算 `update`；在 `条件 not isinstance(update, dict)` 分支中，返回 `result`；以 `update.get("messages")` 计算 `messages`；在 `条件 isinstance(messages, ToolMessage)` 分支中，执行 `stamp(messages)`；在 `条件 isinstance(messages, (list, tuple))` → `循环 for message in messages` → `条件 isinstance(message, ToolMessage)` 分支中，执行 `stamp(message)`；返回 `result`。

  源码依据：[_maybe_stamp 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L309-L338)


- **`ToolErrorHandlingMiddleware._maybe_stamp.stamp`**（`message`）：父函数内定义的回调。以 `dict(message.additional_kwargs or {})` 计算 `existing`；执行 `existing.pop(SKILL_CONTEXT_ENTRY_KEY, None)`；执行 `existing.pop(SKILL_USAGE_KEY, None)`；以 `existing` 计算 `message.additional_kwargs`；在 `条件 tool_call_id is None or str(message.tool_call_id) == tool_call_id` 分支中，执行 `self._stamp_skill_read_metadata(message, request, tool_name=tool_name)`。

  源码依据：[stamp 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L314-L323)


- **`ToolErrorHandlingMiddleware.wrap_tool_call`**（`request, handler`）：在 `try` 分支中，以 `handler(request)` 计算 `result`；在 `捕获 GraphBubbleUp` 分支中，向外继续抛出当前异常；在 `捕获 Exception as exc` 分支中，返回 `self._build_error_message(request, exc)`；返回 `self._maybe_stamp( normalize_tool_result(result, tool_call_id=str(request.tool_call.get("id") or "")), request, )`。

  源码依据：[wrap_tool_call 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L340-L357)


- **`ToolErrorHandlingMiddleware.awrap_tool_call`**（`request, handler`）：在 `try` 分支中，以 `await handler(request)` 计算 `result`；在 `捕获 GraphBubbleUp` 分支中，向外继续抛出当前异常；在 `捕获 Exception as exc` 分支中，返回 `self._build_error_message(request, exc)`；返回 `await self._amaybe_stamp( normalize_tool_result(result, tool_call_id=str(request.tool_call.get("id") or "")), request, )`。

  源码依据：[awrap_tool_call 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L359-L377)


### 5.5.5 _build_runtime_middlewares

**参数摘要**

```text
    app_config,
    include_uploads,
    include_dangling_tool_call_patch,
    lazy_init=True,
    receipts_render_mode="delegation_only",
    authorization_provider=None,
    authorization_infrastructure_tools=(),
    available_skills=None,
    owns_agent_skill_projection=True,
    skill_authorization=None,
    user_id=None
```

以 「表达式摘要 1」 计算 `outer_wrappers: list[AgentMiddleware]`；以 `[*outer_wrappers, *thread_hooks, *tail]` 计算 `middlewares`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
[ InputSanitizationMiddleware(), KnowledgeScopeMiddleware(), ToolOutputBudgetMiddleware.from_app_config(app_config),
   ToolResultSanitizationMiddleware(), ]
```


关键判定为 `app_config.pii_redaction.enabled`、`include_uploads`、`include_dangling_tool_call_patch`。以 `"deerflow"` 计算 `provider_kwargs["framework"]`。执行 `outer_wrappers.append(PiiRedactionMiddleware(app_config.pii_redaction))`；执行 `thread_hooks.append(UploadsMiddleware())`。

捕获 `(ValueError, TypeError)` 的边界见源码，分支返回 `middlewares`，提前返回结束该路径。

源码依据：[_build_runtime_middlewares 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L378-L573)


### 5.5.6 辅助函数与局部契约

- **`build_lead_runtime_middlewares`**（「表达式摘要 1」）：分支返回 「表达式摘要 2」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  app_config, lazy_init=True, authorization_provider=None, deferred_setup=None,
   available_skills=None, owns_agent_skill_projection=True, skill_authorization=None,
   user_id=None
  ```


  **表达式摘要 2**（非完整源码；换行仅为阅读排版）：

  ```text
  _build_runtime_middlewares( app_config=app_config, include_uploads=True,
     include_dangling_tool_call_patch=True, lazy_init=lazy_init, # The lead renders the receipt ledger only while
    processing subagent # results (default "delegation_only"); stamping stays always-on. receipts_render_mode=app_config.verification.receipts_render_mode,
     authorization_provider=authorization_provider, available_skills=available_skills,
     owns_agent_skill_projection=owns_agent_skill_projection, authorization_infrastructure_tools=((deferred_setup.tool_search_tool,
        ) if authorization_provider is not None and deferred_setup is not None and deferred_setup.tool_search_tool
      is not None else ()), skill_authorization=skill_authorization,
     user_id=user_id, )
  ```


  源码依据：[build_lead_runtime_middlewares 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L574-L602)


### 5.5.7 build_subagent_runtime_middlewares

**参数摘要**

```text
    app_config=None,
    model_name=None,
    lazy_init=True,
    deferred_setup=None,
    mcp_routing_middleware=None,
    agent_name=None,
    available_skills=None,
    user_id=None,
    authorization_provider=None,
    skill_authorization=None,
    extensions=None
```

以 `get_app_config()` 计算 `app_config`。关键判定为 `app_config is None`、`deferred_setup is not None and deferred_setup.deferred_names`、`model_name is None and app_config.models`。

执行 「表达式摘要 1」；执行 `middlewares.append(DeferredToolPromotionAuditMiddleware(deferred_setup.deferred_names, deferred_setup.catalog_hash))`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
middlewares.append( SkillActivationMiddleware( available_skills=available_skills,
     app_config=app_config, user_id=user_id, slash_source_owner_token=slash_source_owner_token,
     skill_authorization=skill_authorization, ) )
```


分支返回 `compose_with_extensions(middlewares, AgentScope.SUBAGENT, None, resolved_extensions)`、「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
compose_with_extensions( middlewares, AgentScope.SUBAGENT, AgentBuildContext( scope=AgentScope.SUBAGENT,
     agent_name=agent_name, model_name=model_name, policy=project_host_policy( app_config,
       token_budget_config=token_budget_config, max_subagents_per_run=None,
       ), ), resolved_extensions, )
```


源码依据：[build_subagent_runtime_middlewares 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py#L603-L893)


## 5.6 回合结束后的记忆提交

源码位置：`backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py`。

本文件只负责回合结束后的记忆提交与脱敏，不负责 LLM 提取和文件事务。复制、递归字符串脱敏、捕获用户/agent 身份和异步入队是主职责，避免后台任务再读取已变化的 runtime context。详见 4.1.5。

### 5.6.1 redact_queued_messages

参数：`messages, pii_redaction_config`。

以 `_make_redactor(pii_redaction_config)` 计算 `redactor`；以 `[]` 计算 `redacted`。关键判定为 `pii_redaction_config is None or not pii_redaction_config.enabled`、`changed`、`tool_calls`。以 `content` 计算 `updates["content"]`；以 `rewritten` 计算 `updates[field]`；其他写入目标为 `updates["additional_kwargs"]`。

执行 `rewritten.append(rewritten_call)`；执行 `redacted.append(message.model_copy(update=updates) if updates else message)`。分支返回 `list(messages)`、`redacted`，提前返回结束该路径。

源码依据：[redact_queued_messages 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L29-L91)


### 5.6.2 辅助函数与局部契约

- **`_redact_tool_call`**（`call, redactor`）：以 `dict(call)` 计算 `rewritten`；以 `False` 计算 `changed`；在 `循环 for field in ("args", "error")` 分支中，以 `rewritten.get(field)` 计算 `value`；在 `循环 for field in ("args", "error")` → `条件 value is not None` 分支中，以 `_redact_strings(value, redactor)` 计算 `rewritten[field], field_changed`；在 `循环 for field in ("args", "error")` → `条件 value is not None` 分支中，以 `changed or field_changed` 计算 `changed`；返回 `rewritten, changed`。

  源码依据：[_redact_tool_call 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L92-L103)


### 5.6.3 _redact_strings

参数：`value, redactor`。

以 `redactor.redact(value)` 计算 `redacted`。关键判定为 `isinstance(value, str)`、`not isinstance(value, (dict, list))`、`isinstance(key, str)`。以 `rewritten` 计算 `target[new_key]`。执行 `ancestors.remove(id(source))`；执行 `stack.pop()`。捕获 `StopIteration` 的边界见源码，显式抛出路径涉及 `ValueError`。

分支返回 `redacted, redacted != value`、`value, False`、`rebuilt, changed`，提前返回结束该路径。

源码依据：[_redact_strings 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L104-L153)


### 5.6.4 辅助函数与局部契约

- **`MemoryMiddleware.__init__`**（`agent_name=None, memory_config=None, pii_redaction_config=None`）：执行 `super().__init__()`；以 `agent_name` 计算 `self._agent_name`；以 `memory_config` 计算 `self._memory_config`；以 `pii_redaction_config` 计算 `self._pii_redaction_config`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L172-L195)


- **`MemoryMiddleware._resolve_add_args`**（`state, runtime`）：检查 enabled/thread/messages，捕获用户与 trace 并对复制后的消息脱敏，返回入队参数；不在这里做最终对话筛选和提取。

  源码依据：[_resolve_add_args 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L196-L230)


- **`MemoryMiddleware._redact_queued_messages`**（`messages`）：返回 `redact_queued_messages(messages, self._pii_redaction_config)`。

  源码依据：[_redact_queued_messages 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L231-L234)


- **`MemoryMiddleware.after_agent`**（`state, runtime`）：以 `self._resolve_add_args(state, runtime)` 计算 `add_args`；在 `条件 add_args is None` 分支中，返回 `None`；以 `add_args` 计算 `thread_id, messages, user_id, trace_id`；执行 `get_memory_manager().add( thread_id, messages, agent_name=self._agent_name, user_id=user_id, trace_id=trace_id, )`；返回 `None`。

  源码依据：[after_agent 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L236-L254)


- **`MemoryMiddleware.aafter_agent`**（`state, runtime`）：以 `self._resolve_add_args(state, runtime)` 计算 `add_args`；在 `条件 add_args is None` 分支中，返回 `None`；以 `add_args` 计算 `thread_id, messages, user_id, trace_id`；以 `await asyncio.to_thread(get_memory_manager)` 计算 `manager`；等待 `manager.aadd( thread_id, messages, agent_name=self._agent_name, user_id=user_id, trace_id=trace_id, )` 完成；返回 `None`。

  源码依据：[aafter_agent 实现](../backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py#L256-L271)


## 5.7 单轮委派数量控制

源码位置：`backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py`。

该文件把本条 AI 响应的 task 集合与已有 delegation ledger 做预算比较，超额调用在执行前删除。辅助函数负责内容类型兼容、run 关联与去重；这里没有实际执行 semaphore，详见 4.1.6。

### 5.7.1 辅助函数与局部契约

- **`_clamp_subagent_limit`**（`value`）：返回 `clamp_subagent_concurrency(value)`。

  源码依据：[_clamp_subagent_limit 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L38-L42)


- **`_clamp_total_subagent_limit`**（`value`）：返回 `clamp_total_subagents_per_run(value)`。

  源码依据：[_clamp_total_subagent_limit 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L43-L47)


- **`_append_text`**（`content, text`）：在 `条件 content is None` 分支中，返回 `text`；在 `条件 isinstance(content, str)` → `条件 content` 分支中，返回 `f"{content}\n\n{text}"`；在 `条件 isinstance(content, str)` 分支中，返回 `text`；在 `条件 isinstance(content, list)` 分支中，返回 `[*content, {"type": "text", "text": f"\n\n{text}"}]`；返回 `f"{content}\n\n{text}"`。

  源码依据：[_append_text 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L48-L59)


- **`_delegation_id`**（`entry`）：在 `条件 not isinstance(entry, dict)` 分支中，返回 `None`；以 `entry.get("id")` 计算 `entry_id`；在 `条件 not entry_id` 分支中，返回 `None`；以 `entry.get("dispatch_message_id")` 计算 `dispatch_id`；返回 `str(entry_id), str(dispatch_id) if dispatch_id else None`。

  源码依据：[_delegation_id 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L60-L75)


- **`_delegation_run_id`**（`entry`）：在 `条件 not isinstance(entry, dict)` 分支中，返回 `None`；以 `entry.get("run_id")` 计算 `run_id`；返回 `str(run_id) if run_id else None`。

  源码依据：[_delegation_run_id 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L76-L82)


- **`_runtime_run_id`**（`runtime`）：以 `getattr(runtime, "context", None)` 计算 `context`；在 `条件 not isinstance(context, dict)` 分支中，返回 `None`；以 `context.get("run_id")` 计算 `run_id`；返回 `str(run_id) if run_id else None`。

  源码依据：[_runtime_run_id 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L83-L90)


- **`_count_prior_delegations`**（`delegations, run_id`）：在 `条件 not isinstance(delegations, list)` 分支中，返回 `0`；以 `set()` 计算 `ids`；在 `循环 for entry in delegations` → `条件 run_id is not None and _delegation_run_id(entry) != run_id` 分支中，进入下一轮循环；在 `循环 for entry in delegations` 分支中，以 `_delegation_id(entry)` 计算 `delegation_id`；在 `循环 for entry in delegations` → `条件 delegation_id is not None` 分支中，执行 `ids.add(delegation_id)`；返回 `len(ids)`。

  源码依据：[_count_prior_delegations 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L91-L103)


- **`SubagentLimitMiddleware.__init__`**（`max_concurrent=MAX_CONCURRENT_SUBAGENTS, max_total=DEFAULT_MAX_TOTAL_SUBAGENTS`）：执行 `super().__init__()`；以 `_clamp_subagent_limit(max_concurrent)` 计算 `self.max_concurrent`；以 `_clamp_total_subagent_limit(max_total)` 计算 `self.max_total`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L122-L126)


- **`SubagentLimitMiddleware.release_policy_parameters`**（无显式业务参数）：返回 `{ "max_concurrent": self.max_concurrent, "max_total": self.max_total, }`。

  源码依据：[release_policy_parameters 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L127-L132)


### 5.7.2 SubagentLimitMiddleware._truncate_task_calls

参数：`state, runtime=None`。

以 `state.get("messages", [])` 计算 `messages`。关键判定为 `not messages`、`getattr(last_msg, "type", None) != "ai"`、`not tool_calls`。以 `"subagent_limit_capped"` 计算 `runtime.context["stop_reason"]`。分支返回 `None`、`{"messages": [updated_msg]}`，提前返回结束该路径。

源码依据：[_truncate_task_calls 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L133-L183)


### 5.7.3 辅助函数与局部契约

- **`SubagentLimitMiddleware.after_model`**（`state, runtime`）：返回 `self._truncate_task_calls(state, runtime)`。

  源码依据：[after_model 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L185-L187)


- **`SubagentLimitMiddleware.aafter_model`**（`state, runtime`）：返回 `self._truncate_task_calls(state, runtime)`。

  源码依据：[aafter_model 实现](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L189-L191)


## 5.8 重复调用检测与停止

源码位置：`backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py`。

模块常量定义默认阈值，工具参数规范化函数生成可重复签名；实例维护按 thread/run 划分的有界历史、待注入 warning 和停止原因。程序硬截断与请求内软告警分别发生在 after_model 与 wrap_model_call，详见 4.1.7。

### 5.8.1 辅助函数与局部契约

- **`_stable_args_key`**（`value`）：在 `try` 分支中，返回 `json.dumps(value, sort_keys=True, default=str)`；在 `捕获 (TypeError, ValueError, RecursionError)` 分支中，返回 `None`。

  源码依据：[_stable_args_key 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L116-L123)


- **`_normalize_tool_call_args`**（`raw_args`）：在 `条件 isinstance(raw_args, dict)` 分支中，返回 `raw_args, None`；在 `条件 isinstance(raw_args, str)` → `try` 分支中，以 `json.loads(raw_args)` 计算 `parsed`；在 `条件 isinstance(raw_args, str)` → `捕获 (TypeError, ValueError, json.JSONDecodeError, RecursionError)` 分支中，返回 `{}, raw_args`；在 `条件 isinstance(raw_args, str)` → `条件 isinstance(parsed, dict)` 分支中，返回 `parsed, None`；在 `条件 isinstance(raw_args, str)` 分支中，返回 `{}, _stable_args_key(parsed)`；在 `条件 raw_args is None` 分支中，返回 `{}, None`；返回 `{}, _stable_args_key(raw_args)`。

  源码依据：[_normalize_tool_call_args 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L124-L149)


- **`_coerce_line_number`**（`value`）：在 `try` 分支中，以 `int(value) # type: ignore[arg-type]` 计算 `line`；在 `捕获 (TypeError, ValueError, OverflowError)` 分支中，返回 `None`；返回 `max(line, 1)`。

  源码依据：[_coerce_line_number 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L150-L161)


- **`_normalized_read_range`**（`args`）：以 `_coerce_line_number(args.get("start_line"))` 计算 `start_line`；以 `_coerce_line_number(args.get("end_line"))` 计算 `end_line`；在 `条件 start_line is None` 分支中，以 `1` 计算 `start_line`；在 `条件 end_line is not None and end_line < start_line` 分支中，以 `end_line, start_line` 计算 `start_line, end_line`；返回 `start_line, end_line`。

  源码依据：[_normalized_read_range 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L162-L190)


- **`_stable_tool_key`**（`name, args, fallback_key`）：以 `args.get("path") or ""` 计算 `path`。关键判定为 `name == "read_file" and fallback_key is None`、`not isinstance(path, str)`、`name in {"write_file", "str_replace"}`，相应动作只在对应分支执行。分支返回 `f"{path}:{start_line}-{end_line if end_line is not None else _OPEN_ENDED_READ}"`、`fallback_key`、`_stable_args_key(args) or _UNSERIALIZABLE_ARGS`，提前返回结束该路径。

  源码依据：[_stable_tool_key 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L191-L224)


- **`_hash_tool_calls`**（`tool_calls`）：以 `[]` 计算 `normalized: list[str]`；在 `循环 for tc in tool_calls` 分支中，以 `tc.get("name", "")` 计算 `name`；在 `循环 for tc in tool_calls` 分支中，以 `_normalize_tool_call_args(tc.get("args", {}))` 计算 `args, fallback_key`；在 `循环 for tc in tool_calls` 分支中，以 `_stable_tool_key(name, args, fallback_key)` 计算 `key`；在 `循环 for tc in tool_calls` 分支中，执行 `normalized.append(f"{name}:{key}")`；执行 `normalized.sort()`；以 `json.dumps(normalized, sort_keys=True, default=str)` 计算 `blob`；返回 `hashlib.md5(blob.encode()).hexdigest()[:12]`。

  源码依据：[_hash_tool_calls 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L225-L254)


- **`_LoopDecision.hard_stop`**（无显式业务参数）：返回 `self.action == "hard_stop"`。

  源码依据：[hard_stop 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L290-L293)


### 5.8.2 LoopDetectionMiddleware.__init__

**参数摘要**

```text
    warn_threshold=_DEFAULT_WARN_THRESHOLD,
    hard_limit=_DEFAULT_HARD_LIMIT,
    window_size=_DEFAULT_WINDOW_SIZE,
    max_tracked_threads=_DEFAULT_MAX_TRACKED_THREADS,
    tool_freq_warn=_DEFAULT_TOOL_FREQ_WARN,
    tool_freq_hard_limit=_DEFAULT_TOOL_FREQ_HARD_LIMIT,
    tool_freq_overrides=None
```

以 `warn_threshold` 计算 `self.warn_threshold`；以 `hard_limit` 计算 `self.hard_limit`；其他写入目标为 `self.window_size`、`self.max_tracked_threads`、`self.tool_freq_warn`、`self.tool_freq_hard_limit`、`self._default_tool_freq_thresholds`、`self._tool_freq_overrides: dict[str, tuple[int, int]]`、`self._tool_freq_window`、`self._lock`、`self._fallback_run_ids: OrderedDict[int, tuple[object, str]]`、`self._max_fallback_run_ids`、`self._history: OrderedDict[_RunScopeKey, list[str]]`、`self._warned: dict[_RunScopeKey, set[str]]`、`self._tool_name_history: defaultdict[_RunScopeKey, deque[str]]`、`self._tool_name_counter: defaultdict[_RunScopeKey, Counter[str]]`、`self._tool_freq_warned: dict[_RunScopeKey, set[str]]`、`self._pending_warnings: dict[_RunScopeKey, list[str]]`、`self._pending_warning_touch_order: OrderedDict[_RunScopeKey, None]`、`self._max_pending_warning_keys`、`self._stop_reason: BoundedDict[str | None, str]`、`self._stop_reason_thread_id: BoundedDict[str | None, str]`。

源码依据：[__init__ 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L328-L405)


### 5.8.3 辅助函数与局部契约

- **`LoopDetectionMiddleware.release_policy_parameters`**（无显式业务参数）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "warn_threshold": self.warn_threshold, "hard_limit": self.hard_limit,
     "window_size": self.window_size, "max_tracked_threads": self.max_tracked_threads,
     "tool_freq_warn": self.tool_freq_warn, "tool_freq_hard_limit": self.tool_freq_hard_limit,
     "tool_freq_overrides": self._tool_freq_overrides, }
  ```


  源码依据：[release_policy_parameters 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L406-L416)


- **`LoopDetectionMiddleware.from_config`**（`config`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  cls( warn_threshold=config.warn_threshold, hard_limit=config.hard_limit,
     window_size=config.window_size, max_tracked_threads=config.max_tracked_threads,
     tool_freq_warn=config.tool_freq_warn, tool_freq_hard_limit=config.tool_freq_hard_limit,
     tool_freq_overrides={name: (o.warn, o.hard_limit) for name, o in config.tool_freq_overrides.items()},
     )
  ```


  源码依据：[from_config 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L418-L429)


- **`LoopDetectionMiddleware._get_thread_id`**（`runtime`）：以 `runtime.context.get("thread_id") if runtime.context else None` 计算 `thread_id`；在 `条件 thread_id` 分支中，返回 `str(thread_id)`；返回 `"default"`。

  源码依据：[_get_thread_id 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L430-L436)


### 5.8.4 LoopDetectionMiddleware._get_run_id

参数：`runtime`。

context 中 run_id 键即使为 None 也优先；缺键再取 execution_info.run_id，最后以稳定 control/runtime anchor 映射 UUID token。锁和强引用防并发安装与对象地址重用，LRU 有界，after_agent 释放后备映射。

源码依据：[_get_run_id 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L437-L478)


### 5.8.5 辅助函数与局部契约

- **`LoopDetectionMiddleware._release_fallback_run_id`**（`runtime`）：以 `getattr(runtime, "context", None)` 计算 `ctx`；在 `条件 isinstance(ctx, dict) and "run_id" in ctx` 分支中，返回 None，提前结束当前路径；以 `getattr(runtime, "execution_info", None)` 计算 `execution_info`；在 `条件 getattr(execution_info, "run_id", None) is not None` 分支中，返回 None，提前结束当前路径；以 `getattr(runtime, "control", None)` 计算 `control`；以 `control if control is not None else runtime` 计算 `anchor`；以 `id(anchor)` 计算 `anchor_id`；在 `资源/同步边界 self._lock` 分支中，以 `self._fallback_run_ids.get(anchor_id)` 计算 `existing`；在 `资源/同步边界 self._lock` → `条件 existing is not None and existing[0] is anchor` 分支中，执行 `self._fallback_run_ids.pop(anchor_id, None)`。

  源码依据：[_release_fallback_run_id 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L479-L496)


- **`LoopDetectionMiddleware.consume_stop_reason`**（`run_id`）：在 `资源/同步边界 self._lock` 分支中，以 `self._stop_reason.pop(run_id, None)` 计算 `reason`；在 `资源/同步边界 self._lock` 分支中，执行 `self._stop_reason_thread_id.pop(run_id, None)`；在 `资源/同步边界 self._lock` 分支中，返回 `reason`。

  源码依据：[consume_stop_reason 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L497-L512)


- **`LoopDetectionMiddleware._run_scope_key`**（`runtime`）：返回 `self._get_thread_id(runtime), self._get_run_id(runtime)`。

  源码依据：[_run_scope_key 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L513-L516)


- **`LoopDetectionMiddleware._pending_key`**（`runtime`）：返回 `self._run_scope_key(runtime)`。

  源码依据：[_pending_key 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L517-L520)


- **`LoopDetectionMiddleware._evict_if_needed`**（无显式业务参数）：以 `self._history.popitem(last=False)` 计算 `evicted_key, _`。执行 `self._warned.pop(evicted_key, None)`；执行 `self._tool_name_history.pop(evicted_key, None)`。

  源码依据：[_evict_if_needed 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L521-L537)


- **`LoopDetectionMiddleware._drop_pending_warning_key_locked`**（`key`）：执行 `self._pending_warnings.pop(key, None)`；执行 `self._pending_warning_touch_order.pop(key, None)`。

  源码依据：[_drop_pending_warning_key_locked 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L538-L545)


- **`LoopDetectionMiddleware._touch_pending_warning_key_locked`**（`key`）：以 `None` 计算 `self._pending_warning_touch_order[key]`；执行 `self._pending_warning_touch_order.move_to_end(key)`。

  源码依据：[_touch_pending_warning_key_locked 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L546-L553)


- **`LoopDetectionMiddleware._prune_pending_warning_state_locked`**（`protected_key`）：以 `len(self._pending_warning_touch_order) - self._max_pending_warning_keys` 计算 `overflow`；在 `条件 overflow <= 0` 分支中，返回 None，提前结束当前路径；以 `[key for key in self._pending_warning_touch_order if key != protected_key]` 计算 `candidates`；在 `循环 for key in candidates[:overflow]` 分支中，执行 `self._drop_pending_warning_key_locked(key)`。

  源码依据：[_prune_pending_warning_state_locked 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L554-L566)


- **`LoopDetectionMiddleware._queue_pending_warning`**（`runtime, warning`）：以 `self._pending_key(runtime)` 计算 `pending_key`；在 `资源/同步边界 self._lock` 分支中，以 `self._pending_warnings[pending_key]` 计算 `warnings`；在 `资源/同步边界 self._lock` → `条件 warning not in warnings` 分支中，执行 `warnings.append(warning)`；在 `资源/同步边界 self._lock` → `条件 len(warnings) > _MAX_PENDING_WARNINGS_PER_RUN` 分支中，删除 `warnings[: len(warnings) - _MAX_PENDING_WARNINGS_PER_RUN]`；在 `资源/同步边界 self._lock` 分支中，执行 `self._touch_pending_warning_key_locked(pending_key)`；在 `资源/同步边界 self._lock` 分支中，执行 `self._prune_pending_warning_state_locked(protected_key=pending_key)`。

  源码依据：[_queue_pending_warning 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L567-L578)


### 5.8.6 LoopDetectionMiddleware._track_and_check

参数：`state, runtime`。

以 `state.get("messages", [])` 计算 `messages`；以 「表达式摘要 1」 计算 `warning`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
_LoopDecision( message=_TOOL_FREQ_WARNING_MSG.format(tool_name=name,
     count=freq_count), action="warn", detection_layer="tool_frequency",
   tool_names=(name,), count=freq_count, threshold=eff_warn, )
```


关键判定为 `not messages`、`getattr(last_msg, "type", None) != "ai"`、`not tool_calls`。进入 `self._lock` 边界，退出由对应 context manager 处理。以 `[]` 计算 `self._history[scope_key]`；以 `history[-self.window_size :]` 计算 `history[:]`；其他写入目标为 `name_counter[name]`、`name_counter[old]`。

执行 `history.append(call_hash)`；执行 `self._warned.pop(scope_key, None)`。

分支返回 `None`、「表达式摘要 1」、「表达式摘要 2」、`warning`，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
_LoopDecision( message=_HARD_STOP_MSG, action="hard_stop", detection_layer="identical_call_set",
   tool_names=tuple(tool_names), count=count, threshold=self.hard_limit,
   )
```


**表达式摘要 2**（非完整源码；换行仅为阅读排版）：

```text
_LoopDecision( message=_TOOL_FREQ_HARD_STOP_MSG.format(tool_name=name,
     count=freq_count), action="hard_stop", detection_layer="tool_frequency",
   tool_names=(name,), count=freq_count, threshold=eff_hard, )
```


源码依据：[_track_and_check 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L579-L765)


### 5.8.7 辅助函数与局部契约

- **`LoopDetectionMiddleware._append_text`**（`content, text`）：在 `条件 content is None` 分支中，返回 `text`；在 `条件 isinstance(content, list)` 分支中，返回 `[*content, {"type": "text", "text": f"\n\n{text}"}]`；在 `条件 isinstance(content, str)` 分支中，返回 `content + f"\n\n{text}"`；返回 `str(content) + f"\n\n{text}"`。

  源码依据：[_append_text 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L767-L782)


- **`LoopDetectionMiddleware._record_audit_event`**（`decision, runtime`）：以 `resolve_audit_recorder( getattr(runtime, "context", None), recorder_key=LOOP_DETECTION_RECORDER_CONTEXT_KEY, )` 计算 `recorder, is_subagent, agent_id`；在 `条件 recorder is None` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  recorder.record_middleware( tag=MIDDLEWARE_LOOP_DETECTION_TAG, name=type(self).__name__,
     hook="after_model", action=decision.action, changes={ "is_subagent": is_subagent,
       "agent_id": agent_id, "detection_layer": decision.detection_layer,
       "tool_names": list(decision.tool_names), "count": decision.count,
       "threshold": decision.threshold, }, )
  ```


  源码依据：[_record_audit_event 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L783-L817)


### 5.8.8 LoopDetectionMiddleware._apply

参数：`state, runtime`。

以 `self._track_and_check(state, runtime)` 计算 `decision`。关键判定为 `decision is None`、`decision.hard_stop`、`isinstance(ctx, dict)`。进入 `self._lock` 边界，退出由对应 context manager 处理。以 `"loop_capped"` 计算 `self._stop_reason[run_id]`；以 `thread_id` 计算 `self._stop_reason_thread_id[run_id]`；其他写入目标为 `ctx["stop_reason"]`。

分支返回 `None`、`{"messages": [stripped_msg]}`，提前返回结束该路径。

源码依据：[_apply 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L818-L869)


### 5.8.9 辅助函数与局部契约

- **`LoopDetectionMiddleware._clear_current_run_pending_warnings`**（`runtime`）：以 `self._pending_key(runtime)` 计算 `pending_key`；在 `资源/同步边界 self._lock` 分支中，执行 `self._drop_pending_warning_key_locked(pending_key)`。

  源码依据：[_clear_current_run_pending_warnings 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L870-L875)


- **`LoopDetectionMiddleware._format_warning_message`**（`warnings`）：以 `list(dict.fromkeys(warnings))` 计算 `deduped`；返回 `"\n\n".join(deduped)`。

  源码依据：[_format_warning_message 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L877-L881)


- **`LoopDetectionMiddleware.before_agent`**（`state, runtime`）：返回 `None`。

  源码依据：[before_agent 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L883-L888)


- **`LoopDetectionMiddleware.abefore_agent`**（`state, runtime`）：返回 `None`。

  源码依据：[abefore_agent 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L890-L893)


- **`LoopDetectionMiddleware.after_model`**（`state, runtime`）：返回 `self._apply(state, runtime)`。

  源码依据：[after_model 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L895-L897)


- **`LoopDetectionMiddleware.aafter_model`**（`state, runtime`）：返回 `self._apply(state, runtime)`。

  源码依据：[aafter_model 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L899-L901)


- **`LoopDetectionMiddleware.after_agent`**（`state, runtime`）：执行 `self._clear_current_run_pending_warnings(runtime)`；执行 `self._release_fallback_run_id(runtime)`；返回 `None`。

  源码依据：[after_agent 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L903-L907)


- **`LoopDetectionMiddleware.aafter_agent`**（`state, runtime`）：执行 `self._clear_current_run_pending_warnings(runtime)`；执行 `self._release_fallback_run_id(runtime)`；返回 `None`。

  源码依据：[aafter_agent 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L909-L913)


- **`LoopDetectionMiddleware._drain_pending_warnings`**（`runtime`）：以 `self._pending_key(runtime)` 计算 `pending_key`；在 `资源/同步边界 self._lock` 分支中，以 `self._pending_warnings.pop(pending_key, [])` 计算 `warnings`；在 `资源/同步边界 self._lock` 分支中，执行 `self._pending_warning_touch_order.pop(pending_key, None)`；返回 `warnings`。

  源码依据：[_drain_pending_warnings 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L914-L921)


- **`LoopDetectionMiddleware._restore_pending_warnings`**（`runtime, warnings`）：在 `条件 not warnings` 分支中，返回 None，提前结束当前路径；以 `self._pending_key(runtime)` 计算 `pending_key`；在 `资源/同步边界 self._lock` 分支中，以 `self._pending_warnings[pending_key]` 计算 `queued`；在 `资源/同步边界 self._lock` 分支中，以 `[warning for warning in warnings if warning not in queued]` 计算 `queued[:0]`；在 `资源/同步边界 self._lock` 分支中，删除 `queued[_MAX_PENDING_WARNINGS_PER_RUN:]`；在 `资源/同步边界 self._lock` 分支中，执行 `self._touch_pending_warning_key_locked(pending_key)`；在 `资源/同步边界 self._lock` 分支中，执行 `self._prune_pending_warning_state_locked(protected_key=pending_key)`。

  源码依据：[_restore_pending_warnings 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L922-L939)


- **`LoopDetectionMiddleware._inject_warnings`**（`request, warnings`）：在 `条件 not warnings` 分支中，返回 `request`；以 `[ *request.messages, HumanMessage(content=self._format_warning_message(warnings), name="loop_warning"), ]` 计算 `new_messages`；返回 `request.override(messages=new_messages)`。

  源码依据：[_inject_warnings 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L940-L957)


- **`LoopDetectionMiddleware.wrap_model_call`**（`request, handler`）：以 `self._drain_pending_warnings(request.runtime)` 计算 `warnings`；在 `try` 分支中，返回 `handler(self._inject_warnings(request, warnings))`；在 `捕获 Exception` 分支中，执行 `self._restore_pending_warnings(request.runtime, warnings)`；在 `捕获 Exception` 分支中，向外继续抛出当前异常。

  源码依据：[wrap_model_call 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L959-L970)


- **`LoopDetectionMiddleware.awrap_model_call`**（`request, handler`）：以 `self._drain_pending_warnings(request.runtime)` 计算 `warnings`；在 `try` 分支中，返回 `await handler(self._inject_warnings(request, warnings))`；在 `捕获 Exception` 分支中，执行 `self._restore_pending_warnings(request.runtime, warnings)`；在 `捕获 Exception` 分支中，向外继续抛出当前异常。

  源码依据：[awrap_model_call 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L972-L983)


- **`LoopDetectionMiddleware.reset`**（`thread_id=None`）：以 `set(self._pending_warnings) | set(self._pending_warning_touch_order)` 计算 `pending_keys`。关键判定为 `thread_id`、`key[0] == thread_id`、`key[0] == thread_id`，相应动作只在对应分支执行。进入 `self._lock` 边界，退出由对应 context manager 处理。执行 `mapping.pop(key, None)`；执行 `self._stop_reason.pop(run_id, None)`。

  源码依据：[reset 实现](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L984-L1017)


## 5.9 沙箱提供者与默认绑定

源码位置：`backend/packages/harness/deerflow/sandbox/sandbox_provider.py`。

抽象接口定义 acquire/get/release 及网络策略、技能同步扩展点；模块底部管理进程级 provider 和谁拥有 shutdown 的嵌套语境。没有把 provider release 规定为容器销毁，也没有把 user_id 省略视为自然多用户安全。

### 5.9.1 辅助函数与局部契约

- **`SandboxProvider.acquire`**（`thread_id=None, user_id=None`）：该默认实现/分支不执行额外动作。

  源码依据：[acquire 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L31-L38)


- **`SandboxProvider.acquire_async`**（`thread_id=None, user_id=None`）：返回 `await asyncio.to_thread(self.acquire, thread_id, user_id=user_id)`。

  源码依据：[acquire_async 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L39-L48)


- **`SandboxProvider.sync_agent_skills`**（`sandbox_id, thread_id, user_id, projection`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[sync_agent_skills 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L49-L62)


- **`SandboxProvider.sync_agent_skills_async`**（`sandbox_id, thread_id, user_id, projection`）：等待 `run_sync_lifecycle_operation( self.sync_agent_skills, sandbox_id, thread_id=thread_id, user_id=user_id, projection=projection, )` 完成。

  源码依据：[sync_agent_skills_async 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L63-L79)


- **`SandboxProvider.get`**（`sandbox_id`）：该默认实现/分支不执行额外动作。

  源码依据：[get 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L81-L88)


- **`SandboxProvider.get_scoped`**（`sandbox_id, thread_id, user_id`）：删除 `sandbox_id, thread_id, user_id`；返回 `None`。

  源码依据：[get_scoped 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L89-L104)


- **`SandboxProvider.release`**（`sandbox_id`）：该默认实现/分支不执行额外动作。

  源码依据：[release 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L106-L113)


- **`SandboxProvider.reset`**（无显式业务参数）：该默认实现/分支不执行额外动作。

  源码依据：[reset 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L114-L120)


- **`SandboxProvider.sandbox_network_mode`**（无显式业务参数）：返回 `"open"`。

  源码依据：[sandbox_network_mode 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L121-L124)


- **`SandboxProvider.sandbox_network_temporary_grant_ttl`**（无显式业务参数）：返回 `300`。

  源码依据：[sandbox_network_temporary_grant_ttl 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L125-L127)


- **`SandboxProvider.consume_network_policy_events`**（`sandbox_id`）：删除 `sandbox_id`；返回 `[]`。

  源码依据：[consume_network_policy_events 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L128-L135)


- **`SandboxProvider.consume_network_policy_events_async`**（`sandbox_id`）：返回 `await run_sync_lifecycle_operation( self.consume_network_policy_events, sandbox_id, )`。

  源码依据：[consume_network_policy_events_async 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L136-L141)


- **`SandboxProvider.deny_pending_network_policy_events`**（`sandbox_id`）：删除 `sandbox_id`；返回 `False`。

  源码依据：[deny_pending_network_policy_events 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L142-L146)


- **`SandboxProvider.deny_pending_network_policy_events_async`**（`sandbox_id`）：返回 `await run_sync_lifecycle_operation( self.deny_pending_network_policy_events, sandbox_id, )`。

  源码依据：[deny_pending_network_policy_events_async 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L147-L152)


- **`SandboxProvider.decide_network_policy_request`**（`sandbox_id, request_id, decision`）：删除 `sandbox_id, request_id, decision`；返回 `False`。

  源码依据：[decide_network_policy_request 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L153-L157)


- **`SandboxProvider.decide_network_policy_request_async`**（`sandbox_id, request_id, decision`）：返回 `await run_sync_lifecycle_operation( self.decide_network_policy_request, sandbox_id, request_id, decision, )`。

  源码依据：[decide_network_policy_request_async 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L158-L166)


- **`hold_sandbox_shutdown_ownership`**（无显式业务参数）：使用外层变量 `global _host_shutdown_owners`；在 `资源/同步边界 _provider_lock` 分支中，对 `_host_shutdown_owners` 执行 `+= 1` 更新；在 `try` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾；在 `finally 收尾` → `资源/同步边界 _provider_lock` 分支中，对 `_host_shutdown_owners` 执行 `-= 1` 更新。

  源码依据：[hold_sandbox_shutdown_ownership 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L192-L209)


- **`host_owns_sandbox_shutdown`**（无显式业务参数）：在 `资源/同步边界 _provider_lock` 分支中，返回 `_host_shutdown_owners > 0`。

  源码依据：[host_owns_sandbox_shutdown 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L210-L215)


- **`get_initialized_sandbox_provider`**（无显式业务参数）：在 `资源/同步边界 _provider_lock` 分支中，返回 `_default_sandbox_provider`。

  源码依据：[get_initialized_sandbox_provider 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L216-L221)


- **`get_sandbox_provider`**（`**kwargs`）：使用外层变量 `global _default_sandbox_provider`；在 `资源/同步边界 _provider_lock` → `条件 _default_sandbox_provider is not None` 分支中，返回 `_default_sandbox_provider`；以 `get_app_config()` 计算 `config`；以 `resolve_class(config.sandbox.use, SandboxProvider)` 计算 `cls`；以 `cls(**kwargs)` 计算 `provider`；在 `资源/同步边界 _provider_lock` → `条件 _default_sandbox_provider is None` 分支中，以 `provider` 计算 `_default_sandbox_provider`；在 `资源/同步边界 _provider_lock` → `条件 _default_sandbox_provider is None` 分支中，返回 `provider`；在 `资源/同步边界 _provider_lock` 分支中，以 `_default_sandbox_provider` 计算 `winner`；在 `条件 hasattr(provider, "shutdown")` 分支中，执行 `provider.shutdown()`；返回 `winner`。

  源码依据：[get_sandbox_provider 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L222-L260)


- **`reset_sandbox_provider`**（无显式业务参数）：使用外层变量 `global _default_sandbox_provider`；在 `资源/同步边界 _provider_lock` 分支中，以 `_default_sandbox_provider` 计算 `provider`；在 `资源/同步边界 _provider_lock` 分支中，以 `None` 计算 `_default_sandbox_provider`；在 `条件 provider is not None` 分支中，执行 `discard_sandbox_lease_manager(provider)`；在 `条件 provider is not None` 分支中，执行 `provider.reset()`。

  源码依据：[reset_sandbox_provider 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L261-L290)


- **`shutdown_sandbox_provider`**（无显式业务参数）：以 `_default_sandbox_provider` 计算 `provider`。关键判定为 `provider is not None and hasattr(provider, "shutdown")`、`_default_sandbox_provider is None`，相应动作只在对应分支执行。进入 `_provider_lock` 边界，退出由对应 context manager 处理。捕获 `BaseException` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `当前异常`。

  源码依据：[shutdown_sandbox_provider 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L291-L319)


- **`set_sandbox_provider`**（`provider`）：使用外层变量 `global _default_sandbox_provider`；在 `资源/同步边界 _provider_lock` 分支中，以 `_default_sandbox_provider` 计算 `previous`；在 `资源/同步边界 _provider_lock` 分支中，以 `provider` 计算 `_default_sandbox_provider`；在 `条件 previous is not None and previous is not provider` 分支中，执行 `discard_sandbox_lease_manager(previous)`。

  源码依据：[set_sandbox_provider 实现](../backend/packages/harness/deerflow/sandbox/sandbox_provider.py#L320-L339)


## 5.10 图与沙箱生命周期衔接

源码位置：`backend/packages/harness/deerflow/sandbox/middleware.py`。

图生命周期与工具 runtime 之间的适配器：验证技能投影、获取/retain 使用权、异步预热、恢复/分支处理、网络审批与 Command 更新。after_agent 只释放对应 holder，异常兜底来自外层 execution lease，详见 4.1.4。

### 5.10.1 辅助函数与局部契约

- **`_network_approval_is_non_interactive`**（`context`）：返回 `not resolve_run_interaction_policy({"context": context}).allows_clarification`。

  源码依据：[_network_approval_is_non_interactive 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L45-L48)


- **`SandboxMiddleware.__init__`**（`lazy_init=True, available_skills=None, owns_agent_skill_projection=True`）：执行 `super().__init__()`；以 `lazy_init` 计算 `self._lazy_init`；以 `set(available_skills) if available_skills is not None else None` 计算 `self._available_skills`；以 `owns_agent_skill_projection` 计算 `self._owns_agent_skill_projection`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L69-L91)


- **`SandboxMiddleware._prepare_agent_skill_projection`**（`thread_id, user_id`）：以 `get_sandbox_provider()` 计算 `provider`。关键判定为 `not self._owns_agent_skill_projection`、`self._available_skills is None and not get_paths().thread_skills_view_dir(thread_id, user_id=user_id).exists()`、`not provider.supports_agent_skill_isolation`，相应动作只在对应分支执行。显式抛出路径涉及 `SandboxRuntimeError`。分支返回 `None`、`ensure_thread_skill_projection(storage, thread_id, self._available_skills)`，提前返回结束该路径。

  源码依据：[_prepare_agent_skill_projection 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L92-L125)


- **`SandboxMiddleware._require_projection_support`**（`provider, projection`）：在 `条件 projection is not None and not provider.supports_agent_skill_isolation` 分支中，抛出 `SandboxRuntimeError(f"Sandbox provider {provider.__class__.__name__} cannot enforce per-Agent skill filesystem isolation")`。

  源码依据：[_require_projection_support 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L127-L130)


- **`SandboxMiddleware._can_defer_sandbox_acquisition`**（`state, runtime, projection`）：在 `条件 not self._lazy_init` 分支中，返回 `False`；在 `条件 projection is None` 分支中，返回 `True`；在 `条件 state.get("sandbox") is not None or (runtime.context or {}).get("sandbox_id") is not None` 分支中，返回 `False`；以 `get_sandbox_provider()` 计算 `provider`；执行 `self._require_projection_support(provider, projection)`；返回 `provider.supports_lazy_agent_skill_isolation`。

  源码依据：[_can_defer_sandbox_acquisition 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L131-L145)


- **`SandboxMiddleware._acquire_sandbox`**（`thread_id, user_id, owner_id`）：以 `get_sandbox_provider()` 计算 `provider`；在 `条件 owner_id is None` 分支中，以 `provider.acquire(thread_id, user_id=user_id)` 计算 `sandbox_id`；在 `同层否则分支` 分支中，以 `get_sandbox_lease_manager(provider).acquire( owner_id, thread_id, user_id=user_id, )` 计算 `sandbox_id`；返回 `sandbox_id`。

  源码依据：[_acquire_sandbox 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L146-L164)


- **`SandboxMiddleware._acquire_sandbox_async`**（`thread_id, user_id, owner_id`）：以 `get_sandbox_provider()` 计算 `provider`；在 `条件 owner_id is None` 分支中，以 `await provider.acquire_async(thread_id, user_id=user_id)` 计算 `sandbox_id`；在 `同层否则分支` 分支中，以 `await get_sandbox_lease_manager(provider).acquire_async( owner_id, thread_id, user_id=user_id, )` 计算 `sandbox_id`；返回 `sandbox_id`。

  源码依据：[_acquire_sandbox_async 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L165-L183)


- **`SandboxMiddleware._retain_existing_sandbox`**（`state, thread_id, user_id, owner_id`）：在 `条件 owner_id is None` 分支中，返回 `None`；以 `unwrap_sandbox(state.get("sandbox"))` 计算 `sandbox, fork_restored`；在 `条件 not isinstance(sandbox, dict) or fork_restored` 分支中，返回 `None`；以 `sandbox.get("sandbox_id")` 计算 `sandbox_id`；在 `条件 isinstance(sandbox_id, str)` 分支中，以 `get_sandbox_provider()` 计算 `provider`；在 `条件 isinstance(sandbox_id, str)` 分支中，以 `get_sandbox_lease_manager(provider).reuse_or_acquire( owner_id, sandbox_id, thread_id=thread_id, user_id=user_id, )` 计算 `sandbox_id`；在 `条件 isinstance(sandbox_id, str)` 分支中，返回 `sandbox_id`；返回 `None`。

  源码依据：[_retain_existing_sandbox 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L185-L208)


- **`SandboxMiddleware._retain_existing_sandbox_async`**（`state, thread_id, user_id, owner_id`）：在 `条件 owner_id is None` 分支中，返回 `None`；以 `unwrap_sandbox(state.get("sandbox"))` 计算 `sandbox, fork_restored`；在 `条件 not isinstance(sandbox, dict) or fork_restored` 分支中，返回 `None`；以 `sandbox.get("sandbox_id")` 计算 `sandbox_id`；在 `条件 isinstance(sandbox_id, str)` 分支中，以 `get_sandbox_provider()` 计算 `provider`；在 `条件 isinstance(sandbox_id, str)` 分支中，以 `await get_sandbox_lease_manager(provider).reuse_or_acquire_async( owner_id, sandbox_id, thread_id=thread_id, user_id=user_id, )` 计算 `sandbox_id`；在 `条件 isinstance(sandbox_id, str)` 分支中，返回 `sandbox_id`；返回 `None`。

  源码依据：[_retain_existing_sandbox_async 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L210-L233)


- **`SandboxMiddleware._release_sandbox_async`**（`sandbox_id, owner_id`）：以 `get_sandbox_provider()` 计算 `provider`；在 `条件 owner_id is not None` 分支中，等待 `get_sandbox_lease_manager(provider).release_async(owner_id)` 完成；在 `条件 owner_id is not None` 分支中，返回 None，提前结束当前路径；等待 `await_drained(asyncio.to_thread(provider.release, sandbox_id))` 完成。

  源码依据：[_release_sandbox_async 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L234-L245)


### 5.10.2 SandboxMiddleware.before_agent

参数：`state, runtime`。

同步路径建立技能投影并校验 provider 支持，解释恢复 sandbox 与 fork，按 lazy/eager 决定获取或 retain，必要时用 Overwrite 交付受控重绑定。

源码依据：[before_agent 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L247-L331)


### 5.10.3 辅助函数与局部契约

- **`SandboxMiddleware._apply_network_policy_response`**（`state, runtime`）：以 `self._read_sandbox_id_from_state(state)` 计算 `sandbox_id`。关键判定为 `sandbox_id is None`、`not isinstance(message, HumanMessage)`、`candidate is None or candidate["source"] != NETWORK_POLICY_HUMAN_INPUT_SOURCE`，相应动作只在对应分支执行。执行 `applied.add(marker)`。显式抛出路径涉及 `SandboxRuntimeError`。分支返回 `None`，提前返回结束该路径。

  源码依据：[_apply_network_policy_response 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L332-L365)


### 5.10.4 SandboxMiddleware.abefore_agent

参数：`state, runtime`。

异步执行同样身份、投影、复用与网络回应规则；阻塞 provider/文件操作经异步适配，不把旧 checkpoint 值当当前授权。

源码依据：[abefore_agent 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L367-L444)


### 5.10.5 辅助函数与局部契约

- **`SandboxMiddleware.before_model`**（`state, runtime`）：返回 `super().before_model(state, runtime)`。

  源码依据：[before_model 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L446-L451)


- **`SandboxMiddleware.abefore_model`**（`state, runtime`）：在满足延迟获取条件时发起 sandbox prewarm，返回时不等容器全部准备完成；首个工具共享这次初始化，资源由外层清理 fence 接管。

  源码依据：[abefore_model 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L453-L460)


- **`SandboxMiddleware.after_agent`**（`state, runtime`）：fork-restored 值跳过父容器归还；普通 holder 按 lease 释放，legacy 情形调用 provider.release。完成钩子不是整个进程的容器 shutdown。

  源码依据：[after_agent 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L462-L493)


### 5.10.6 SandboxMiddleware.aafter_agent

参数：`state, runtime`。

以 `unwrap_sandbox(state.get("sandbox"))` 计算 `sandbox, fork_restored`。关键判定为 `sandbox_prewarm_cleanup_registered(runtime.context)`、`sandbox is not None`、`fork_restored`。等待 `close_sandbox_prewarm(runtime)` 完成；等待 `self._release_sandbox_async( sandbox_id, owner_id=sandbox_lease_owner(runtime.context), )` 完成。

分支返回 `await super().aafter_agent(state, runtime)`、`None`，提前返回结束该路径。

源码依据：[aafter_agent 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L495-L542)


### 5.10.7 辅助函数与局部契约

- **`SandboxMiddleware._read_sandbox_id_from_state`**（`state`）：在 `条件 not isinstance(state, dict)` 分支中，返回 `None`；以 `unwrap_sandbox(state.get("sandbox"))` 计算 `sandbox_state, _`；在 `条件 not isinstance(sandbox_state, dict)` 分支中，返回 `None`；以 `sandbox_state.get("sandbox_id")` 计算 `sandbox_id`；返回 `sandbox_id if isinstance(sandbox_id, str) else None`。

  源码依据：[_read_sandbox_id_from_state 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L544-L552)


- **`SandboxMiddleware._attach_sandbox_update`**（`result, sandbox_id, overwrite=False`）：以 `{"sandbox_id": sandbox_id}` 计算 `sandbox_value: object`；在 `条件 overwrite` 分支中，以 `Overwrite(sandbox_value)` 计算 `sandbox_value`；以 `{"sandbox": sandbox_value}` 计算 `sandbox_update`；在 `条件 isinstance(result, ToolMessage)` 分支中，返回 `Command(update={**sandbox_update, "messages": [result]})`；以 `result.update` 计算 `existing_update`；在 `条件 isinstance(existing_update, dict)` 分支中，以 `{**existing_update, **sandbox_update}` 计算 `merged_update`；在 `条件 isinstance(existing_update, dict)` 分支中，返回 `dc_replace(result, update=merged_update)`；返回 `result`。

  源码依据：[_attach_sandbox_update 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L554-L581)


- **`SandboxMiddleware._read_sandbox_id_from_request`**（`request`）：以 `request.runtime` 计算 `runtime`；在 `条件 runtime is None or runtime.state is None` 分支中，返回 `None`；返回 `SandboxMiddleware._read_sandbox_id_from_state(runtime.state)`。

  源码依据：[_read_sandbox_id_from_request 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L583-L589)


- **`SandboxMiddleware.wrap_tool_call`**（`request, handler`）：以 `self._read_sandbox_id_from_request(request)` 计算 `prev_sandbox_id`；以 `handler(request)` 计算 `result`；以 `self._read_sandbox_id_from_request(request)` 计算 `curr_sandbox_id`；在 `条件 curr_sandbox_id is not None and curr_sandbox_id != prev_sandbox_id` 分支中，以 `self._attach_sandbox_update( result, curr_sandbox_id, overwrite=prev_sandbox_id is not None, )` 计算 `result`；返回 `self._maybe_request_network_approval(request, result, curr_sandbox_id or prev_sandbox_id)`。

  源码依据：[wrap_tool_call 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L591-L606)


- **`SandboxMiddleware.awrap_tool_call`**（`request, handler`）：对照调用前后 sandbox ID，执行网络策略审批分支并 await 工具，必要时将 lazy 初始化补入 Command.update；避免覆写不能安全合并的已有 Command 形状。

  源码依据：[awrap_tool_call 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L608-L637)


- **`SandboxMiddleware._maybe_request_network_approval`**（`request, result, sandbox_id`）：在 `条件 sandbox_id is None` 分支中，返回 `result`；以 `getattr(request.runtime, "context", None) or {}` 计算 `context`；以 `get_initialized_sandbox_provider()` 计算 `provider`；在 `条件 provider is None` 分支中，返回 `result`；在 `条件 provider.sandbox_network_mode() != "allowlist"` 分支中，返回 `result`；在 `条件 _network_approval_is_non_interactive(context) or context.get("is_subagent")` 分支中，返回 `result`；以 `provider.consume_network_policy_events(sandbox_id)` 计算 `events`；返回 `self._network_approval_result(request, result, sandbox_id, events)`。

  源码依据：[_maybe_request_network_approval 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L638-L658)


### 5.10.8 SandboxMiddleware._network_approval_result

参数：`request, result, sandbox_id, events`。

以 `events[0]` 计算 `event`。关键判定为 `not events`、`not isinstance(request_id, str) or not isinstance(host, str) or not isinstance(port, int)`、`isinstance(result, Command) and isinstance(result.update, dict)`。分支返回 `result`、`Command(update=update, goto=END)`，提前返回结束该路径。

源码依据：[_network_approval_result 实现](../backend/packages/harness/deerflow/sandbox/middleware.py#L659-L709)


## 5.11 沙箱操作租约与取消排空

源码位置：`backend/packages/harness/deerflow/sandbox/lease.py`。

本地执行权协调器将阻塞 provider 工作、引用计数、按 thread 序列化和取消排空组合起来。它保护的是资源仍在使用时不能被提前归还；与 Redis 容器销毁权、RunManager 持久写权是三套不同协议。

### 5.11.1 辅助函数与局部契约

- **`_drain_task_after_cancellation`**（`task`）：重复取消不能打断对既有 task 的排空；task 已结束时读取结果，未结束继续 shield，生命周期锁不会早于 provider 操作释放。

  源码依据：[_drain_task_after_cancellation 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L45-L59)


- **`run_sync_lifecycle_operation`**（`func, *args, _on_cancel=None, **kwargs`）：将阻塞函数移到线程并 shield；取消时可先发送取消信号，随后排空已开始线程，即使 drain 失败也保留原取消；只有工作结束才允许外层资源释放。

  源码依据：[run_sync_lifecycle_operation 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L60-L88)


- **`SandboxClientLease.release`**（无显式业务参数）：在 `条件 self.owner_id is None` 分支中，返回 None，提前结束当前路径；以 `self.owner_id` 计算 `owner_id`；以 `None` 计算 `self.owner_id`；等待 `get_sandbox_lease_manager(self.provider).release_async(owner_id)` 完成。

  源码依据：[release 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L98-L105)


- **`SandboxClientLease.run_sync`**（`func, *args, **kwargs`）：返回 `await run_sync_lifecycle_operation(func, *args, **kwargs)`。

  源码依据：[run_sync 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L106-L110)


- **`acquire_sandbox_client_lease`**（`provider, thread_id, user_id, owner_prefix, release_on_last=True`）：以 `f"{owner_prefix}:{uuid.uuid4()}"` 计算 `owner_id`；以 `get_sandbox_lease_manager(provider)` 计算 `manager`；以 `await manager.acquire_async( owner_id, thread_id, user_id=user_id, release_on_last=release_on_last, )` 计算 `sandbox_id`；在 `try` 分支中，以 `provider.get(sandbox_id)` 计算 `sandbox`；在 `捕获 BaseException` 分支中，等待 `manager.release_async(owner_id)` 完成；在 `捕获 BaseException` 分支中，向外继续抛出当前异常；返回 `SandboxClientLease( sandbox=sandbox, sandbox_id=sandbox_id, owner_id=owner_id, provider=provider, )`。

  源码依据：[acquire_sandbox_client_lease 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L111-L147)


- **`SandboxClientTeardownInProgressError.__init__`**（`sandbox_id`）：执行 `super().__init__(f"sandbox {sandbox_id} is reserved for teardown; retry after cleanup completes")`；以 `sandbox_id` 计算 `self.sandbox_id`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L164-L168)


- **`SandboxClientUnavailableError.__init__`**（`sandbox_id`）：执行 `super().__init__("Sandbox not found after acquisition", sandbox_id=sandbox_id)`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L176-L179)


- **`SandboxLeaseManager.__init__`**（`provider`）：以 `provider` 计算 `self._provider`；以 `threading.RLock()` 计算 `self._metadata_lock`；以 `AcquireSerializer[tuple[str, str]]( thread_name_prefix="sandbox-execution-lease", )` 计算 `self._serializer`；以 `{}` 计算 `self._bindings_by_owner: dict[str, _LeaseBinding]`；以 `{}` 计算 `self._owners_by_sandbox: dict[str, set[str]]`；以 `set()` 计算 `self._release_pending_by_sandbox: set[str]`；以 `{}` 计算 `self._teardowns_by_sandbox: dict[str, tuple[int, int]]`；以 `{}` 计算 `self._client_lifecycle_observations: dict[str, _ClientLifecycleObservation]`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L188-L199)


- **`SandboxLeaseManager._thread_key`**（`thread_id, user_id`）：返回 `user_id, thread_id`。

  源码依据：[_thread_key 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L201-L203)


- **`SandboxLeaseManager._remove_binding_locked`**（`owner_id, request_release=True`）：以 `self._bindings_by_owner.pop(owner_id, None)` 计算 `binding`。关键判定为 `binding is None`、`request_release and binding.release_on_last`、`owners is None`，相应动作只在对应分支执行。执行 `self._release_pending_by_sandbox.add(binding.sandbox_id)`；执行 `owners.discard(owner_id)`。分支返回 `None, False`、`binding, release_provider`、`binding, False`，提前返回结束该路径。

  源码依据：[_remove_binding_locked 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L204-L229)


### 5.11.2 SandboxLeaseManager._bind_locked

参数：`owner_id, sandbox_id, key, release_on_last`。

以 `self._bindings_by_owner.get(owner_id)` 计算 `existing`。关键判定为 `sandbox_id in self._teardowns_by_sandbox`、`existing is not None and existing.thread_key != key`、`existing is not None and existing.sandbox_id == sandbox_id`。

以 `_LeaseBinding( sandbox_id=sandbox_id, thread_key=key, release_on_last=True, )` 计算 `self._bindings_by_owner[owner_id]`；以 `_LeaseBinding( sandbox_id=sandbox_id, thread_key=key, release_on_last=release_on_last, )` 计算 `self._bindings_by_owner[owner_id]`。

执行 `self._owners_by_sandbox.setdefault(sandbox_id, set()).add(owner_id)`。显式抛出路径涉及 `SandboxClientTeardownInProgressError`、`RuntimeError`。分支返回 `None, False`、`previous, release_previous`，提前返回结束该路径。

源码依据：[_bind_locked 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L230-L269)


### 5.11.3 辅助函数与局部契约

- **`SandboxLeaseManager._release_unbound_acquire`**（`sandbox_id`）：在 `资源/同步边界 self._metadata_lock` 分支中，执行 `self._release_pending_by_sandbox.add(sandbox_id)`；执行 `self._release_if_idle(sandbox_id)`。

  源码依据：[_release_unbound_acquire 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L270-L284)


- **`SandboxLeaseManager.reserve_idle_teardown`**（`sandbox_id`）：以 `threading.get_ident()` 计算 `worker`。关键判定为 `granted`、`current is None and (observation := self._client_lifecycle_observations.get(sandbox_id)) is not None`、`granted`，相应动作只在对应分支执行。进入 `self._metadata_lock` 边界，退出由对应 context manager 处理。以 `(worker, current[1] + 1 if current else 1)` 计算 `self._teardowns_by_sandbox[sandbox_id]`；以 `(worker, depth - 1)` 计算 `self._teardowns_by_sandbox[sandbox_id]`。交付 `bool(granted)` 后暂停生成器，待消费方继续。finally 包含 执行 `with self._metadata_lock:`；以 `self._teardowns_by_sandbox[sandbox_id]` 计算 `_, depth`。

  源码依据：[reserve_idle_teardown 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L286-L312)


- **`SandboxLeaseManager._observe_client_lifecycle`**（`sandbox_id`）：以 `self._client_lifecycle_observations.setdefault(sandbox_id, _ClientLifecycleObservation())` 计算 `observation`。关键判定为 `sandbox_id in self._teardowns_by_sandbox`、`observation.readers == 0`，相应动作只在对应分支执行。进入 `self._metadata_lock` 边界，退出由对应 context manager 处理。交付 `observation, revision` 后暂停生成器，待消费方继续。显式抛出路径涉及 `SandboxClientTeardownInProgressError`。finally 包含 执行 `with self._metadata_lock:`；对 `observation.readers` 执行 `-= 1` 更新。

  源码依据：[_observe_client_lifecycle 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L314-L329)


- **`SandboxLeaseManager._bind_live_client`**（`owner_id, sandbox_id, key, release_on_last, allow_unscoped_borrow`）：在 `资源/同步边界 self._observe_client_lifecycle(sandbox_id) as (observation, revision)` 分支中，以 `self._provider.get(sandbox_id) if allow_unscoped_borrow else self._provider.get_scoped(sandbox_id, thread_id=key[1], user_id=key[0])` 计算 `sandbox`；在 `资源/同步边界 self._observe_client_lifecycle(sandbox_id) as (observation, revision)` → `资源/同步边界 self._metadata_lock` → `条件 sandbox is None or observation.revision != revision` 分支中，返回 `None`；在 `资源/同步边界 self._observe_client_lifecycle(sandbox_id) as (observation, revision)` → `资源/同步边界 self._metadata_lock` 分支中，返回 `self._bind_locked(owner_id, sandbox_id, key, release_on_last=release_on_last)`。

  源码依据：[_bind_live_client 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L330-L347)


- **`SandboxLeaseManager._release_if_idle`**（`sandbox_id`）：在 `资源/同步边界 self.reserve_idle_teardown(sandbox_id) as reserved` → `条件 not reserved` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self.reserve_idle_teardown(sandbox_id) as reserved` → `资源/同步边界 self._metadata_lock` 分支中，执行 `self._release_pending_by_sandbox.discard(sandbox_id)`；在 `资源/同步边界 self.reserve_idle_teardown(sandbox_id) as reserved` 分支中，执行 `self._provider.release(sandbox_id)`。

  源码依据：[_release_if_idle 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L348-L355)


- **`SandboxLeaseManager._active_owner_binding`**（`owner_id, key, release_on_last`）：以 `self._bindings_by_owner.get(owner_id)` 计算 `existing`。关键判定为 `existing is None`、`existing.thread_key != key`、`self._provider.get(existing.sandbox_id) is not None`，相应动作只在对应分支执行。进入 `self._metadata_lock` 边界，退出由对应 context manager 处理。以 `_LeaseBinding( sandbox_id=existing.sandbox_id, thread_key=existing.thread_key, release_on_last=True, )` 计算 `self._bindings_by_owner[owner_id]`。显式抛出路径涉及 `RuntimeError`。分支返回 `None`、`existing.sandbox_id`，提前返回结束该路径。

  源码依据：[_active_owner_binding 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L356-L390)


- **`SandboxLeaseManager._acquire_and_bind`**（`owner_id, thread_id, user_id, key, release_on_last`）：以 `self._provider.acquire(thread_id, user_id=user_id)` 计算 `sandbox_id`；在 `try` 分支中，以 `self._bind_live_client(owner_id, sandbox_id, key, release_on_last=release_on_last, allow_unscoped_borrow=True)` 计算 `binding`；在 `try` → `条件 binding is None` 分支中，抛出 `SandboxClientUnavailableError(sandbox_id)`；在 `try` 分支中，以 `binding` 计算 `previous, release_previous`；在 `捕获 BaseException` 分支中，执行 `self._release_unbound_acquire(sandbox_id)`；在 `捕获 BaseException` 分支中，向外继续抛出当前异常；在 `条件 release_previous and previous is not None` 分支中，执行 `self._release_if_idle(previous.sandbox_id)`；返回 `sandbox_id`。

  源码依据：[_acquire_and_bind 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L391-L414)


### 5.11.4 SandboxLeaseManager._acquire_and_bind_async

参数：`owner_id, thread_id, user_id, key, release_on_last`。

以 `asyncio.create_task(self._provider.acquire_async(thread_id, user_id=user_id))` 计算 `acquire_task`；以 `await _drain_task_after_cancellation(acquire_task)` 计算 `sandbox_id`。关键判定为 `binding is None`、`release_previous and previous is not None`。

以 `asyncio.create_task(self._provider.acquire_async(thread_id, user_id=user_id))` 计算 `acquire_task`；等待 `_drain_task_after_cancellation(rollback_task)` 完成。捕获 `asyncio.CancelledError as cancellation`、`Exception`、`BaseException` 的边界见源码，显式抛出路径涉及 `cancellation`、`SandboxClientUnavailableError`、`当前异常`。

分支返回 `sandbox_id`，提前返回结束该路径。

源码依据：[_acquire_and_bind_async 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L415-L467)


### 5.11.5 辅助函数与局部契约

- **`SandboxLeaseManager.retain`**（`owner_id, sandbox_id, thread_id, user_id, release_on_last=True`）：以 `self._thread_key(thread_id, user_id)` 计算 `key`；在 `资源/同步边界 self._serializer.hold(key)` → `资源/同步边界 self._metadata_lock` 分支中，以 `self._bind_locked( owner_id, sandbox_id, key, release_on_last=release_on_last, )` 计算 `previous, release_previous`；在 `资源/同步边界 self._serializer.hold(key)` → `条件 release_previous and previous is not None` 分支中，执行 `self._release_if_idle(previous.sandbox_id)`。

  源码依据：[retain 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L468-L494)


- **`SandboxLeaseManager.retain_async`**（`owner_id, sandbox_id, thread_id, user_id, release_on_last=True`）：以 `self._thread_key(thread_id, user_id)` 计算 `key`；在 `资源/同步边界 self._serializer.hold_async(key)` → `资源/同步边界 self._metadata_lock` 分支中，以 `self._bind_locked( owner_id, sandbox_id, key, release_on_last=release_on_last, )` 计算 `previous, release_previous`；在 `资源/同步边界 self._serializer.hold_async(key)` → `条件 release_previous and previous is not None` 分支中，等待 `run_sync_lifecycle_operation( self._release_if_idle, previous.sandbox_id, )` 完成。

  源码依据：[retain_async 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L495-L519)


- **`SandboxLeaseManager.acquire`**（`owner_id, thread_id, user_id, release_on_last=True`）：以 `self._thread_key(thread_id, user_id)` 计算 `key`；在 `资源/同步边界 self._serializer.hold(key)` 分支中，以 `self._active_owner_binding( owner_id, key, release_on_last=release_on_last, )` 计算 `existing_sandbox_id`；在 `资源/同步边界 self._serializer.hold(key)` → `条件 existing_sandbox_id is not None` 分支中，返回 `existing_sandbox_id`；在 `资源/同步边界 self._serializer.hold(key)` 分支中，返回 `self._acquire_and_bind( owner_id, thread_id, user_id, key, release_on_last=release_on_last, )`。

  源码依据：[acquire 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L520-L545)


### 5.11.6 SandboxLeaseManager.reuse_or_acquire

**参数摘要**

```text
    owner_id,
    sandbox_id,
    thread_id,
    user_id,
    release_on_last=True,
    acquire_release_on_last=True,
    allow_unscoped_borrow=False
```

以 `self._thread_key(thread_id, user_id)` 计算 `key`；以 `self._active_owner_binding( owner_id, key, release_on_last=release_on_last, )` 计算 `existing_sandbox_id`。关键判定为 `existing_sandbox_id is not None`、`binding is not None`、`release_previous and previous is not None`。

进入 `self._serializer.hold(key)` 边界，退出由对应 context manager 处理。分支返回 `existing_sandbox_id`、`sandbox_id`、`self._acquire_and_bind( owner_id, thread_id, user_id, key, release_on_last=acquire_release_on_last, )`，提前返回结束该路径。

源码依据：[reuse_or_acquire 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L546-L589)


### 5.11.7 辅助函数与局部契约

- **`SandboxLeaseManager.acquire_async`**（`owner_id, thread_id, user_id, release_on_last=True`）：以 `self._thread_key(thread_id, user_id)` 计算 `key`；在 `资源/同步边界 self._serializer.hold_async(key)` 分支中，以 `self._active_owner_binding( owner_id, key, release_on_last=release_on_last, )` 计算 `existing_sandbox_id`；在 `资源/同步边界 self._serializer.hold_async(key)` → `条件 existing_sandbox_id is not None` 分支中，返回 `existing_sandbox_id`；在 `资源/同步边界 self._serializer.hold_async(key)` 分支中，返回 `await self._acquire_and_bind_async( owner_id, thread_id, user_id, key, release_on_last=release_on_last, )`。

  源码依据：[acquire_async 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L590-L615)


### 5.11.8 SandboxLeaseManager.reuse_or_acquire_async

**参数摘要**

```text
    owner_id,
    sandbox_id,
    thread_id,
    user_id,
    release_on_last=True,
    acquire_release_on_last=True,
    allow_unscoped_borrow=False
```

以 `self._thread_key(thread_id, user_id)` 计算 `key`；以 `self._active_owner_binding( owner_id, key, release_on_last=release_on_last, )` 计算 `existing_sandbox_id`。关键判定为 `existing_sandbox_id is not None`、`binding is not None`、`release_previous and previous is not None`。

进入 `self._serializer.hold_async(key)` 边界，退出由对应 context manager 处理。等待 `run_sync_lifecycle_operation( self._release_if_idle, previous.sandbox_id, )` 完成。分支返回 `existing_sandbox_id`、`sandbox_id`、`await self._acquire_and_bind_async( owner_id, thread_id, user_id, key, release_on_last=acquire_release_on_last, )`，提前返回结束该路径。

源码依据：[reuse_or_acquire_async 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L616-L655)


### 5.11.9 辅助函数与局部契约

- **`SandboxLeaseManager.cancel`**（`owner_id, reset=False`）：以 `self._bindings_by_owner.get(owner_id)` 计算 `binding`。关键判定为 `binding is None`、`self._bindings_by_owner.get(owner_id) is not binding`、`sandbox is None`，相应动作只在对应分支执行。进入 `self._metadata_lock`、`self._serializer.hold(binding.thread_key)` 边界，退出由对应 context manager 处理。执行 `sandbox.release_command_scope(owner_id)`。显式抛出路径涉及 `SandboxNotFoundError`。分支返回 `None`，提前返回结束该路径。

  源码依据：[cancel 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L656-L674)


- **`SandboxLeaseManager.cancel_async`**（`owner_id, reset=False`）：等待 `run_sync_lifecycle_operation(self.cancel, owner_id, reset=reset)` 完成。

  源码依据：[cancel_async 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L675-L677)


- **`SandboxLeaseManager.release`**（`owner_id`）：以 `self._bindings_by_owner.get(owner_id)` 计算 `binding`。关键判定为 `binding is None`、`current is None`、`sandbox is not None`，相应动作只在对应分支执行。进入 `self._metadata_lock`、`self._serializer.hold(binding.thread_key)` 边界，退出由对应 context manager 处理。执行 `sandbox.release_command_scope(owner_id)`。分支返回 `None`，提前返回结束该路径。finally 包含 执行 `with self._metadata_lock:`；以 `self._remove_binding_locked(owner_id)` 计算 `binding, release_provider`。

  源码依据：[release 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L678-L703)


- **`SandboxLeaseManager.release_async`**（`owner_id`）：以 `asyncio.create_task(asyncio.to_thread(self.release, owner_id))` 计算 `release_task`；在 `try` 分支中，等待 `asyncio.shield(release_task)` 完成；在 `捕获 asyncio.CancelledError as cancellation` → `try` 分支中，等待 `_drain_task_after_cancellation(release_task)` 完成；在 `捕获 asyncio.CancelledError as cancellation` → `捕获 Exception` 分支中，抛出 `cancellation`；在 `捕获 asyncio.CancelledError as cancellation` 分支中，抛出 `cancellation`。

  源码依据：[release_async 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L704-L721)


- **`SandboxLeaseManager.binding_for`**（`owner_id`）：在 `资源/同步边界 self._metadata_lock` 分支中，以 `self._bindings_by_owner.get(owner_id)` 计算 `binding`；在 `资源/同步边界 self._metadata_lock` 分支中，返回 `binding.sandbox_id if binding is not None else None`。

  源码依据：[binding_for 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L722-L727)


- **`SandboxLeaseManager.close`**（无显式业务参数）：执行 `self._serializer.close()`。

  源码依据：[close 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L728-L732)


- **`get_sandbox_lease_manager`**（`provider`）：以 `id(provider)` 计算 `provider_id`；在 `资源/同步边界 _manager_lock` 分支中，以 `_managers.get(provider_id)` 计算 `entry`；在 `资源/同步边界 _manager_lock` → `条件 entry is not None and entry[0] is provider` 分支中，返回 `entry[1]`；在 `资源/同步边界 _manager_lock` 分支中，以 `SandboxLeaseManager(provider)` 计算 `manager`；在 `资源/同步边界 _manager_lock` 分支中，以 `(provider, manager)` 计算 `_managers[provider_id]`；在 `资源/同步边界 _manager_lock` 分支中，返回 `manager`。

  源码依据：[get_sandbox_lease_manager 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L737-L755)


- **`discard_sandbox_lease_manager`**（`provider`）：以 `id(provider)` 计算 `provider_id`；在 `资源/同步边界 _manager_lock` 分支中，以 `_managers.get(provider_id)` 计算 `entry`；在 `资源/同步边界 _manager_lock` → `条件 entry is None or entry[0] is not provider` 分支中，以 `None` 计算 `manager`；在 `资源/同步边界 _manager_lock` → `同层否则分支` 分支中，以 `_managers.pop(provider_id)` 计算 `_, manager`；在 `条件 manager is not None` 分支中，执行 `manager.close()`。

  源码依据：[discard_sandbox_lease_manager 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L756-L768)


- **`register_sandbox_prewarm_cleanup`**（`context`）：以 `_PREWARM_CLEANUP_MARKER` 计算 `context[SANDBOX_PREWARM_CLEANUP_CONTEXT_KEY]`；执行 `ensure_sandbox_lease_owner(context)`。

  源码依据：[register_sandbox_prewarm_cleanup 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L769-L774)


- **`sandbox_prewarm_cleanup_registered`**（`context`）：返回 `isinstance(context, dict) and context.get(SANDBOX_PREWARM_CLEANUP_CONTEXT_KEY) is _PREWARM_CLEANUP_MARKER`。

  源码依据：[sandbox_prewarm_cleanup_registered 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L775-L779)


- **`ensure_sandbox_lease_owner`**（`context`）：在 `条件 not isinstance(context, dict)` 分支中，返回 `None`；以 `context.get(SANDBOX_LEASE_OWNER_CONTEXT_KEY)` 计算 `existing`；在 `条件 isinstance(existing, str) and existing` 分支中，返回 `existing`；以 `f"agent:{uuid.uuid4()}"` 计算 `owner_id`；以 `owner_id` 计算 `context[SANDBOX_LEASE_OWNER_CONTEXT_KEY]`；返回 `owner_id`。

  源码依据：[ensure_sandbox_lease_owner 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L780-L791)


- **`sandbox_lease_owner`**（`context`）：在 `条件 not isinstance(context, dict)` 分支中，返回 `None`；以 `context.get(SANDBOX_LEASE_OWNER_CONTEXT_KEY)` 计算 `owner_id`；返回 `owner_id if isinstance(owner_id, str) and owner_id else None`。

  源码依据：[sandbox_lease_owner 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L792-L799)


- **`sandbox_command_scope`**（`context`）：在 `条件 not isinstance(context, dict)` 分支中，返回 `None`；以 `context.get(SANDBOX_COMMAND_SCOPE_CONTEXT_KEY)` 计算 `scope_id`；返回 `scope_id if isinstance(scope_id, str) and scope_id else None`。

  源码依据：[sandbox_command_scope 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L800-L807)


- **`_sandbox_execution_lease`**（`context`）：以 `sandbox_lease_owner(context)` 计算 `owner_id`；在 `条件 owner_id is None or not isinstance(context, dict)` 分支中，返回 `None`；以 `context.get("sandbox_id")` 计算 `sandbox_id`；在 `条件 not isinstance(sandbox_id, str) or not sandbox_id` 分支中，返回 `None`；返回 `owner_id, sandbox_id`。

  源码依据：[_sandbox_execution_lease 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L808-L818)


- **`cancel_sandbox_execution_lease_async`**（`context, reset=False`）：以 `_sandbox_execution_lease(context)` 计算 `lease`；在 `条件 lease is None` 分支中，返回 None，提前结束当前路径；以 `lease` 计算 `owner_id, _sandbox_id`；等待 `get_sandbox_lease_manager(get_sandbox_provider()).cancel_async(owner_id, reset=reset)` 完成。

  源码依据：[cancel_sandbox_execution_lease_async 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L819-L829)


- **`release_sandbox_execution_lease`**（`context`）：以 `_sandbox_execution_lease(context)` 计算 `lease`；在 `条件 lease is None` 分支中，返回 None，提前结束当前路径；以 `lease` 计算 `owner_id, _sandbox_id`；以 `get_sandbox_provider()` 计算 `provider`；执行 `get_sandbox_lease_manager(provider).release(owner_id)`。

  源码依据：[release_sandbox_execution_lease 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L830-L844)


- **`release_sandbox_execution_lease_async`**（`context`）：在 `条件 isinstance(context, dict)` 分支中，执行 `context.pop(SANDBOX_PREWARM_CLEANUP_CONTEXT_KEY, None)`；在 `try` 分支中，等待 `close_sandbox_prewarm_context(context)` 完成；在 `finally 收尾` 分支中，以 `_sandbox_execution_lease(context)` 计算 `lease`；在 `finally 收尾` → `条件 lease is not None` 分支中，以 `lease` 计算 `owner_id, _sandbox_id`；在 `finally 收尾` → `条件 lease is not None` 分支中，以 `get_sandbox_provider()` 计算 `provider`；在 `finally 收尾` → `条件 lease is not None` 分支中，等待 `get_sandbox_lease_manager(provider).release_async(owner_id)` 完成。

  源码依据：[release_sandbox_execution_lease_async 实现](../backend/packages/harness/deerflow/sandbox/lease.py#L845-L865)


## 5.12 本地沙箱实例管理

源码位置：`backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py`。

该 provider 缓存按用户/线程映射的 LocalSandbox，并构造线程目录/技能投影挂载视图。缓存有锁和 LRU 上限，release 不删除主机目录；主机 bash 开关不能被虚拟路径映射替代。

### 5.12.1 辅助函数与局部契约

- **`LocalSandboxProvider.supports_agent_skill_isolation`**（无显式业务参数）：在 `try` 分支中，返回 `not is_host_bash_allowed()`；在 `捕获 Exception` 分支中，返回 `False`。

  源码依据：[supports_agent_skill_isolation 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L77-L85)


- **`LocalSandboxProvider.__init__`**（`max_cached_threads=DEFAULT_MAX_CACHED_THREAD_SANDBOXES`）：以 `DEFAULT_SKILLS_CONTAINER_PATH` 计算 `self._skills_container_path`；以 `self._setup_path_mappings()` 计算 `self._path_mappings`；以 `None` 计算 `self._generic_sandbox: LocalSandbox | None`；以 `OrderedDict()` 计算 `self._thread_sandboxes: OrderedDict[tuple[str, str], LocalSandbox]`；以 `max_cached_threads` 计算 `self._max_cached_threads`；以 `threading.Lock()` 计算 `self._lock`；以 `self._load_sandbox_environment()` 计算 `self._environment`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L86-L101)


- **`LocalSandboxProvider._load_sandbox_environment`**（无显式业务参数）：在 `try` 分支中，以 `get_app_config().sandbox` 计算 `sandbox_config`；在 `捕获 Exception` 分支中，返回 `{}`；以 `getattr(sandbox_config, "environment", None) or {}` 计算 `env_config`；返回 `dict(env_config)`。

  源码依据：[_load_sandbox_environment 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L102-L121)


### 5.12.2 LocalSandboxProvider._setup_path_mappings

无显式业务参数。

以 `[]` 计算 `mappings: list[PathMapping]`。关键判定为 `public_skills_path.exists()`、`sandbox_config and sandbox_config.mounts`、`not host_path.is_absolute()`。以 `container_path.rstrip("/")` 计算 `self._skills_container_path`。

执行 `mappings.append( PathMapping( container_path=f"{container_path}/public", local_path=str(public_skills_path), read_only=True, ) )`；执行 `mappings.append( PathMapping( container_path=container_path, local_path=str(host_path.resolve()), read_only=mount.read_only, ) )`。

执行 `if not container_path.startswith("/"):`；执行 「表达式摘要 1」。捕获 `Exception as e` 的边界见源码，分支返回 `mappings`，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
if container_path in _RESERVED_CONTAINER_PATHS or any(container_path == p or container_path.startswith(p + "/")
  for p in _RESERVED_CONTAINER_PREFIXES):
```


源码依据：[_setup_path_mappings 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L122-L250)


### 5.12.3 辅助函数与局部契约

- **`LocalSandboxProvider._effective_acquire_user_id`**（`user_id`）：返回 `user_id or get_effective_user_id()`。

  源码依据：[_effective_acquire_user_id 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L252-L256)


- **`LocalSandboxProvider._thread_key`**（`thread_id, user_id`）：返回 `(user_id, thread_id)`。

  源码依据：[_thread_key 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L258-L260)


- **`LocalSandboxProvider._ensure_skills_projection`**（`user_id=None, thread_id=None`）：在 `try` 分支中，以 `get_app_config()` 计算 `config`；在 `try` → `条件 user_id is None` 分支中，以 `get_or_new_skill_storage(app_config=config)` 计算 `storage`；在 `try` → `同层否则分支` 分支中，以 `get_or_new_user_skill_storage(user_id, app_config=config)` 计算 `storage`；在 `try` → `同层否则分支` → `条件 thread_id is not None and thread_skill_projection_exists(storage, thread_id)` 分支中，返回 `get_thread_skill_projection_paths(storage, thread_id)`；在 `try` 分支中，返回 `ensure_skill_projections(storage)`；在 `捕获 Exception as exc` 分支中，返回 `None`。

  源码依据：[_ensure_skills_projection 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L262-L297)


- **`LocalSandboxProvider._append_public_skill_mapping`**（`mappings, projection`）：在 `条件 projection is None` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，以 `get_app_config().skills.container_path.rstrip("/")` 计算 `container_path`；在 `try` 分支中，以 `f"{container_path}/public"` 计算 `public_container_path`；在 `try` → `条件 any(mapping.container_path.rstrip("/") == public_container_path for mapping in mappings)` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 `mappings.append( PathMapping( container_path=public_container_path, local_path=str(projection.public), read_only=True, ) )`。

  源码依据：[_append_public_skill_mapping 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L299-L318)


- **`LocalSandboxProvider._sandbox_id_for_thread`**（`thread_id, user_id`）：返回 `f"local:{user_id}:{thread_id}"`。

  源码依据：[_sandbox_id_for_thread 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L320-L322)


- **`LocalSandboxProvider._key_from_sandbox_id`**（`sandbox_id`）：在 `条件 not sandbox_id.startswith("local:")` 分支中，返回 `None`；以 `sandbox_id[len("local:") :]` 计算 `value`；以 `value.partition(":")` 计算 `user_id, separator, thread_id`；在 `条件 not separator or not user_id or not thread_id` 分支中，返回 `None`；返回 `(user_id, thread_id)`。

  源码依据：[_key_from_sandbox_id 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L324-L332)


### 5.12.4 LocalSandboxProvider._build_thread_path_mappings

参数：`thread_id, user_id=None, skill_projection=None`。

以 `get_paths()` 计算 `paths`；以 `` [ # Aggregate parent mapping so ``ls /mnt/user-data`「表达式摘要 1」`/mnt/user-data/workspace/...`` # because ``_find_path_mapping`「表达式摘要 2」` 计算 `mappings`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
 and other # parent-level operations behave the same as inside AIO (where the # parent directory is real and contains
  the three subdirs). Longer # subpath mappings below still win for
```


**表达式摘要 2**（非完整源码；换行仅为阅读排版）：

```text
 sorts by container_path length. PathMapping( container_path=_USER_DATA_VIRTUAL_PREFIX,
   local_path=str(paths.sandbox_user_data_dir(thread_id, user_id=effective_user_id)),
   read_only=False, ), PathMapping( container_path=f"{_USER_DATA_VIRTUAL_PREFIX}/workspace",
   local_path=str(paths.sandbox_work_dir(thread_id, user_id=effective_user_id)),
   read_only=False, ), PathMapping( container_path=f"{_USER_DATA_VIRTUAL_PREFIX}/uploads",
   local_path=str(paths.sandbox_uploads_dir(thread_id, user_id=effective_user_id)),
   read_only=False, ), PathMapping( container_path=f"{_USER_DATA_VIRTUAL_PREFIX}/outputs",
   local_path=str(paths.sandbox_outputs_dir(thread_id, user_id=effective_user_id)),
   read_only=False, ), PathMapping( container_path=_ACP_WORKSPACE_VIRTUAL_PREFIX,
   local_path=str(paths.acp_workspace_dir(thread_id, user_id=effective_user_id)),
   read_only=False, ), ]
```


关键判定为 `projection is not None`、`projection.public.parent == thread_projection_root`。

执行 `mappings.append( PathMapping( container_path=skills_container_path, local_path=str(thread_projection_root), read_only=True, ) )`；执行 「表达式摘要 1」。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
mappings.extend( [ PathMapping( container_path=f"{skills_container_path}/public",
       local_path=str(projection.public), read_only=True, ), PathMapping( container_path=f"{skills_container_path}/custom",
       local_path=str(projection.custom), read_only=True, ), PathMapping( container_path=f"{skills_container_path}/legacy",
       local_path=str(projection.legacy), read_only=True, ), PathMapping( container_path=f"{skills_container_path}/integrations",
       local_path=str(projection.integrations), read_only=True, ),
     ] )
```


捕获 `Exception as exc` 的边界见源码，分支返回 `mappings`，提前返回结束该路径。

源码依据：[_build_thread_path_mappings 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L334-L432)


### 5.12.5 辅助函数与局部契约

- **`LocalSandboxProvider._without_managed_skill_mappings`**（`mappings, policy_scoped`）：以 `self._skills_container_path` 计算 `root`；以 `tuple(f"{root}/{category}" for category in ("public", "custom", "legacy", "integrations"))` 计算 `managed_categories`；建立局部回调 `def conflicts(mapping: PathMapping) -> bool`，行为在独立条目展开；返回 `[mapping for mapping in mappings if not conflicts(mapping)]`。

  源码依据：[_without_managed_skill_mappings 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L433-L457)


- **`LocalSandboxProvider._without_managed_skill_mappings.conflicts`**（`mapping`）：父函数内定义的回调。以 `mapping.container_path.rstrip("/")` 计算 `path`；在 `条件 path == root` 分支中，返回 `True`；以 `(root,) if policy_scoped else managed_categories` 计算 `prefixes`；返回 `any(path == prefix or path.startswith(prefix + "/") for prefix in prefixes)`。

  源码依据：[conflicts 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L449-L455)


### 5.12.6 LocalSandboxProvider.acquire

参数：`thread_id=None, user_id=None`。

按用户/线程构造映射与技能投影，锁内复查缓存，未命中创建 LocalSandbox 并淘汰 LRU；release 不清同一缓存，跨轮可复用。

源码依据：[acquire 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L458-L540)


### 5.12.7 辅助函数与局部契约

- **`LocalSandboxProvider._evict_until_within_cap_locked`**（无显式业务参数）：在 `循环 while len(self._thread_sandboxes) > self._max_cached_threads` 分支中，以 `self._thread_sandboxes.popitem(last=False)` 计算 `evicted_key, _`。

  源码依据：[_evict_until_within_cap_locked 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L541-L554)


- **`LocalSandboxProvider.get`**（`sandbox_id`）：以 `self._generic_sandbox` 计算 `generic`；以 `self._thread_sandboxes.get(key)` 计算 `cached`。关键判定为 `sandbox_id == "local"`、`generic is None`、`isinstance(sandbox_id, str) and sandbox_id.startswith("local:")`，相应动作只在对应分支执行。进入 `self._lock` 边界，退出由对应 context manager 处理。执行 `self.acquire()`；执行 `if isinstance(sandbox_id, str) and sandbox_id.startswith("local:"):`。分支返回 `self._generic_sandbox`、`generic`、`None`、`cached`，提前返回结束该路径。

  源码依据：[get 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L555-L577)


- **`LocalSandboxProvider.release`**（`sandbox_id`）：该默认实现/分支不执行额外动作。

  源码依据：[release 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L578-L588)


- **`LocalSandboxProvider.reset`**（无显式业务参数）：使用外层变量 `global _singleton`；在 `资源/同步边界 self._lock` 分支中，以 `None` 计算 `self._generic_sandbox`；在 `资源/同步边界 self._lock` 分支中，执行 `self._thread_sandboxes.clear()`；在 `资源/同步边界 self._lock` 分支中，以 `None` 计算 `_singleton`。

  源码依据：[reset 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L589-L602)


- **`LocalSandboxProvider.shutdown`**（无显式业务参数）：执行 `self.reset()`。

  源码依据：[shutdown 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py#L603-L608)


## 5.13 宿主文件与命令执行

源码位置：`backend/packages/harness/deerflow/sandbox/local/local_sandbox.py`。

LocalSandbox 实际执行宿主文件和可选命令操作。文件路径按最具体映射解析并约束只读挂载，读写还处理内容中虚拟路径、文件 revision 和输出反向映射。命令分别使用 Windows 进程树或 POSIX process group，管道有界捕获和超时清理；这不是内核级隔离环境。

### 5.13.1 辅助函数与局部契约

- **`_existing_regular_file_head`**（`path`）：在 `try` 分支中，以 `os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))` 计算 `fd`；在 `捕获 OSError` 分支中，返回 `b""`；在 `try` → `条件 not stat.S_ISREG(os.fstat(fd).st_mode)` 分支中，返回 `b""`；在 `try` 分支中，返回 `os.read(fd, 2)`；在 `捕获 OSError` 分支中，返回 `b""`；在 `finally 收尾` → `资源/同步边界 suppress(OSError)` 分支中，执行 `os.close(fd)`。

  源码依据：[_existing_regular_file_head 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L52-L76)


- **`_content_path_translation_applies`**（`path, content, append, resolved_path`）：以 `PurePosixPath(path.replace("\\", "/"))` 计算 `posix`；在 `条件 is_code_path(path) or posix.suffix.lower() in _INTERPRETER_SCRIPT_SUFFIXES` 分支中，返回 `True`；在 `条件 posix.suffix` 分支中，返回 `False`；以 `content[:2].encode("utf-8", "surrogatepass")` 计算 `head`；在 `条件 append and not head.startswith(b"#!")` 分支中，以 `_existing_regular_file_head(resolved_path) or head` 计算 `head`；返回 `is_code_file(path, head)`。

  源码依据：[_content_path_translation_applies 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L77-L105)


- **`_publish_file_revision`**（`path, content`）：以 `Path(os.path.realpath(path))` 计算 `target`；在 `try` 分支中，以 `os.open(target, os.O_WRONLY | getattr(os, "O_NONBLOCK", 0))` 计算 `fd`；在 `捕获 FileNotFoundError` 分支中，以 `None` 计算 `fd`；在 `try` → `条件 fd is not None` → `条件 not stat.S_ISREG(os.fstat(fd).st_mode)` 分支中，抛出 `OSError(errno.EINVAL, "Text revisions require a regular file", path)`；在 `try` 分支中，执行 `publish_bytes_atomically(target, content.encode("utf-8"), temporary_prefix=".deerflow-write-", preserve_metadata_from=fd)`；在 `finally 收尾` → `条件 fd is not None` → `try` 分支中，执行 `os.close(fd)`。

  源码依据：[_publish_file_revision 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L106-L127)


- **`_BoundedPipeCapture.__init__`**（`limit_bytes=_COMMAND_CAPTURE_LIMIT_BYTES, encoding="utf-8", fallback_encoding=None, normalize_newlines=False`）：以 `limit_bytes` 计算 `self._limit_bytes`；以 `encoding` 计算 `self._encoding`；以 `fallback_encoding` 计算 `self._fallback_encoding`；以 `normalize_newlines` 计算 `self._normalize_newlines`；以 `[]` 计算 `self._chunks: list[bytes]`；以 `0` 计算 `self._kept_bytes`；以 `0` 计算 `self._total_bytes`；以 `False` 计算 `self._split_tail`；以 `threading.Lock()` 计算 `self._lock`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L131-L153)


- **`_BoundedPipeCapture.mark_split_tail`**（无显式业务参数）：在 `资源/同步边界 self._lock` 分支中，以 `True` 计算 `self._split_tail`。

  源码依据：[mark_split_tail 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L154-L159)


- **`_BoundedPipeCapture._decode`**（`data, allow_trim`）：以 `_trim_trailing_partial_utf8(data)` 计算 `trimmed`。关键判定为 `self._fallback_encoding is None`、`allow_trim and self._encoding.replace("_", "-").lower() == "utf-8"`、`len(trimmed) != len(data)`，相应动作只在对应分支执行。执行 `if allow_trim and self._encoding.replace("_", "-").lower() == "utf-8":`。捕获 `UnicodeDecodeError` 的边界见源码，捕获不等于已恢复。分支返回 `data.decode(self._encoding, errors="replace")`、`data.decode(self._encoding)`、`trimmed.decode(self._encoding)`、`data.decode(self._fallback_encoding, errors="replace")`，提前返回结束该路径。

  源码依据：[_decode 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L160-L193)


- **`_BoundedPipeCapture.append`**（`chunk`）：在 `资源/同步边界 self._lock` 分支中，对 `self._total_bytes` 执行 `+= len(chunk)` 更新；在 `资源/同步边界 self._lock` → `条件 self._kept_bytes >= self._limit_bytes` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._lock` 分支中，以 `self._limit_bytes - self._kept_bytes` 计算 `remaining`；在 `资源/同步边界 self._lock` 分支中，以 `chunk[:remaining]` 计算 `kept`；在 `资源/同步边界 self._lock` 分支中，执行 `self._chunks.append(kept)`；在 `资源/同步边界 self._lock` 分支中，对 `self._kept_bytes` 执行 `+= len(kept)` 更新。

  源码依据：[append 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L194-L203)


- **`_BoundedPipeCapture.read`**（无显式业务参数）：以 `b"".join(self._chunks)` 计算 `data`；对 `output` 执行 `+= notice` 更新。关键判定为 `self._normalize_newlines`、`truncated`，相应动作只在对应分支执行。进入 `self._lock` 边界，退出由对应 context manager 处理。以 `b"".join(self._chunks)` 计算 `data`；以 `output.replace("\r\n", "\n").replace("\r", "\n")` 计算 `output`。分支返回 `output`，提前返回结束该路径。

  源码依据：[read 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L204-L225)


- **`_trim_trailing_partial_utf8`**（`data`）：在 `循环 for offset in range(min(len(data), 4))` 分支中，以 `data[len(data) - 1 - offset]` 计算 `byte`；在 `循环 for offset in range(min(len(data), 4))` → `条件 byte < 0x80 or 0xC0 <= byte <= 0xC1 or byte > 0xF4` 分支中，退出当前循环；在 `循环 for offset in range(min(len(data), 4))` → `条件 byte >= 0xC2` 分支中，以 `2 if byte < 0xE0 else (3 if byte < 0xF0 else 4)` 计算 `expected`；在 `循环 for offset in range(min(len(data), 4))` → `条件 byte >= 0xC2` → `条件 offset + 1 < expected` 分支中，返回 `data[: len(data) - 1 - offset]`；在 `循环 for offset in range(min(len(data), 4))` → `条件 byte >= 0xC2` 分支中，退出当前循环；返回 `data`。

  源码依据：[_trim_trailing_partial_utf8 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L226-L250)


- **`_match_ends_before_a_colon`**（`match`）：返回 `match.end() < len(match.string) and match.string[match.end()] == ":"`。

  源码依据：[_match_ends_before_a_colon 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L251-L263)


### 5.13.2 escape_powershell_path

参数：`match, path`。

以 `None` 计算 `quote`；以 `path[:-1]` 计算 `path`。

关键判定为 `escaped`、`` char == "`" and quote != "'" ``、`quote is None and prefix[index : index + 2] == "<#"`。执行 `subexpressions.append(quote)`；执行 `subexpressions.append(None)`。以 `match.string[: match.start()]` 计算 `prefix`。分支返回 `path`、`re.sub(r"['\u2018\u2019]", lambda char: char.group() * 2, path) + closing_quote`、`` re.sub(characters, lambda char: "`" + char.group(), path) + closing_quote ``，提前返回结束该路径。

源码依据：[escape_powershell_path 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L281-L330)


### 5.13.3 辅助函数与局部契约

- **`LocalSandbox.cancel_command_scope`**（`scope_id`）：以 `self._command_scopes.find(scope_id)` 计算 `scope`。关键判定为 `scope is None`、`process.poll() is None`、`not pids`，相应动作只在对应分支执行。进入 `scope.lock`、`suppress(ProcessLookupError)` 边界，退出由对应 context manager 处理。以 `",".join(x for x in members.stdout.split() if x.isdecimal())` 计算 `pids`。分支返回 `None`，提前返回结束该路径。

  源码依据：[cancel_command_scope 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L334-L357)


- **`LocalSandbox._shell_name`**（`shell`）：返回 `shell.replace("\\", "/").rsplit("/", 1)[-1].lower()`。

  源码依据：[_shell_name 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L361-L364)


- **`LocalSandbox._is_powershell`**（`shell`）：返回 `LocalSandbox._shell_name(shell) in {"powershell", "powershell.exe", "pwsh", "pwsh.exe"}`。

  源码依据：[_is_powershell 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L366-L369)


- **`LocalSandbox._is_cmd_shell`**（`shell`）：返回 `LocalSandbox._shell_name(shell) in {"cmd", "cmd.exe"}`。

  源码依据：[_is_cmd_shell 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L371-L374)


- **`LocalSandbox._is_msys_shell`**（`shell`）：以 `shell.replace("\\", "/").lower()` 计算 `normalized`；以 `LocalSandbox._shell_name(shell)` 计算 `shell_name`；返回 `shell_name in {"sh.exe", "bash.exe"} and any(part in normalized for part in ("/git/", "/mingw", "/msys"))`。

  源码依据：[_is_msys_shell 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L376-L381)


- **`LocalSandbox._msys_path_conversion_exclusions`**（无显式业务参数）：以 `{}` 计算 `safe_roots: dict[str, None]`；在 `循环 for mapping in self.path_mappings` 分支中，以 `mapping.container_path.rstrip("/")` 计算 `root`；在 `循环 for mapping in self.path_mappings` → `条件 not root or not root.startswith("/") or ";" in root or "*" in root` 分支中，进入下一轮循环；在 `循环 for mapping in self.path_mappings` 分支中，以 `None` 计算 `safe_roots[root]`；返回 `";".join(safe_roots)`。

  源码依据：[_msys_path_conversion_exclusions 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L382-L400)


- **`LocalSandbox._find_first_available_shell`**（`candidates`）：在 `循环 for shell in candidates` → `条件 os.path.isabs(shell)` → `条件 os.path.isfile(shell) and os.access(shell, os.X_OK)` 分支中，返回 `shell`；在 `循环 for shell in candidates` → `条件 os.path.isabs(shell)` 分支中，进入下一轮循环；在 `循环 for shell in candidates` 分支中，以 `shutil.which(shell)` 计算 `shell_from_path`；在 `循环 for shell in candidates` → `条件 shell_from_path is not None` 分支中，返回 `shell_from_path`；返回 `None`。

  源码依据：[_find_first_available_shell 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L402-L415)


- **`LocalSandbox._format_timeout_duration`**（`timeout`）：以 `float(timeout)` 计算 `seconds`；在 `条件 seconds.is_integer()` 分支中，以 `str(int(seconds))` 计算 `amount`；在 `同层否则分支` 分支中，以 `f"{seconds:g}"` 计算 `amount`；以 `"second" if seconds == 1 else "seconds"` 计算 `unit`；返回 `f"{amount} {unit}"`。

  源码依据：[_format_timeout_duration 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L417-L425)


- **`LocalSandbox._format_timeout_notice`**（`timeout`）：返回 `「表达式摘要 1」your-command > /mnt/user-data/workspace/server.log 2>&1 &`." ) ``。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
   ( f"Command timed out after {LocalSandbox._format_timeout_duration(timeout)} and was terminated. " "To run a long-lived process such as a web server, start it in the background "
    "and redirect its output, e.g.
  ```


  源码依据：[_format_timeout_notice 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L427-L433)


- **`LocalSandbox._drain_pipe`**（`fd, capture`）：在 `try` → `循环 while chunk := os.read(fd, 8192)` 分支中，执行 `capture.append(chunk)`；在 `finally 收尾` → `try` 分支中，执行 `os.close(fd)`；在 `finally 收尾` → `捕获 OSError` 分支中，该默认实现/分支不执行额外动作。

  源码依据：[_drain_pipe 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L435-L447)


- **`LocalSandbox._start_pipe_drain`**（`fd, name, encoding="utf-8", fallback_encoding=None, normalize_newlines=False`）：以 `_BoundedPipeCapture(encoding=encoding, fallback_encoding=fallback_encoding, normalize_newlines=normalize_newlines)` 计算 `capture`；以 `threading.Thread(target=LocalSandbox._drain_pipe, args=(fd, capture), name=name, daemon=True)` 计算 `thread`；执行 `thread.start()`；返回 `capture, thread`。

  源码依据：[_start_pipe_drain 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L449-L461)


- **`LocalSandbox._process_group_exists`**（`pgid`）：在 `条件 pgid is None` 分支中，返回 `False`；在 `try` 分支中，执行 `os.killpg(pgid, 0)`；在 `try` 分支中，返回 `True`；在 `捕获 ProcessLookupError` 分支中，返回 `False`；在 `捕获 PermissionError` 分支中，返回 `True`；在 `捕获 OSError` 分支中，返回 `False`。

  源码依据：[_process_group_exists 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L463-L475)


- **`LocalSandbox.__init__`**（`id, path_mappings=None, environment=None`）：执行 `super().__init__(id)`；以 `path_mappings or []` 计算 `self.path_mappings`；以 `dict(environment) if environment else {}` 计算 `environment`；执行 `_validate_extra_env(environment)`；以 `environment` 计算 `self.environment: dict[str, str]`；以 `set()` 计算 `self._agent_written_paths: set[str]`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L476-L513)


- **`LocalSandbox._command_pattern`**（无显式业务参数）：以 `sorted(self.path_mappings, key=lambda m: len(m.container_path), reverse=True)` 计算 `mappings`；在 `条件 not mappings` 分支中，返回 `None`；以 `[PATH_START_BOUNDARY + re.escape(m.container_path) + r"(?=/|$|[\s\"';&|<>()])(?:/[^\s\"';&|<>():]*)?" for m in mappings]` 计算 `patterns`；返回 `re.compile("|".join(f"({p})" for p in patterns))`。

  源码依据：[_command_pattern 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L515-L529)


- **`LocalSandbox._content_pattern`**（无显式业务参数）：以 `sorted(self.path_mappings, key=lambda m: len(m.container_path), reverse=True)` 计算 `mappings`；在 `条件 not mappings` 分支中，返回 `None`；以 `[PATH_START_BOUNDARY + re.escape(m.container_path) + r"(?=/|$|[^\w./-])(?:/[^\s\"';&|<>():]*)?" for m in mappings]` 计算 `patterns`；返回 `re.compile("|".join(f"({p})" for p in patterns))`。

  源码依据：[_content_pattern 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L531-L542)


- **`LocalSandbox._resolved_local_paths`**（无显式业务参数）：返回 `{m: os.path.realpath(m.local_path) for m in self.path_mappings}`。

  源码依据：[_resolved_local_paths 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L544-L547)


- **`LocalSandbox._mappings_by_container_specificity`**（无显式业务参数）：返回 `sorted(self.path_mappings, key=lambda m: len(m.container_path.rstrip("/") or "/"), reverse=True)`。

  源码依据：[_mappings_by_container_specificity 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L549-L552)


- **`LocalSandbox._mappings_by_local_specificity`**（无显式业务参数）：返回 `sorted(self.path_mappings, key=lambda m: len(m.local_path), reverse=True)`。

  源码依据：[_mappings_by_local_specificity 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L554-L557)


- **`LocalSandbox._is_read_only_path`**（`resolved_path`）：以 `os.path.realpath(resolved_path)` 计算 `resolved`。关键判定为 `resolved == local_resolved or resolved.startswith(local_resolved + os.sep)`、`prefix_len > best_prefix_len`、`best_mapping is None`，相应动作只在对应分支执行。执行 `if resolved == local_resolved or resolved.startswith(local_resolved + os.sep):`。分支返回 `False`、`best_mapping.read_only`，提前返回结束该路径。

  源码依据：[_is_read_only_path 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L558-L582)


- **`LocalSandbox._find_path_mapping`**（`path`）：以 `str(path)` 计算 `path_str`。关键判定为 `container_path == "/"`、`path_str.startswith("/")`、`path_str == container_path or path_str.startswith(container_path + "/")`，相应动作只在对应分支执行。执行 `if path_str.startswith("/"):`；执行 `if path_str == container_path or path_str.startswith(container_path + "/"):`。分支返回 `mapping, path_str.lstrip("/")`、`mapping, relative`、`None`，提前返回结束该路径。

  源码依据：[_find_path_mapping 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L583-L598)


- **`LocalSandbox._resolve_path_with_mapping`**（`path`）：以 `str(path)` 计算 `path_str`。关键判定为 `mapping_match is None`、`not inside_root`，相应动作只在对应分支执行。以 `os.path.realpath(os.path.join(local_root, relative)) if relative else local_root` 计算 `resolved_path`。捕获 `ValueError` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `PermissionError`。分支返回 `ResolvedPath(path_str, None)`、`ResolvedPath(resolved_path, mapping)`，提前返回结束该路径。

  源码依据：[_resolve_path_with_mapping 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L599-L626)


- **`LocalSandbox._resolve_path`**（`path`）：返回 `self._resolve_path_with_mapping(path).path`。

  源码依据：[_resolve_path 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L627-L629)


- **`LocalSandbox._resolve_path_textually`**（`path`）：执行 `self._resolve_path(path)`；以 `self._find_path_mapping(path)` 计算 `mapping_match`；在 `条件 mapping_match is None` 分支中，返回 `path`；以 `mapping_match` 计算 `mapping, relative`；以 `self._resolved_local_paths[mapping]` 计算 `root`；返回 `f"{root}/{relative}" if relative else root`。

  源码依据：[_resolve_path_textually 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L630-L650)


- **`LocalSandbox._is_resolved_path_read_only`**（`resolved`）：返回 `bool(resolved.mapping and resolved.mapping.read_only) or self._is_read_only_path(resolved.path)`。

  源码依据：[_is_resolved_path_read_only 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L651-L653)


- **`LocalSandbox._reverse_resolve_path`**（`path`）：以 `path.replace("\\", "/")` 计算 `normalized_path`；以 `os.path.realpath(normalized_path)` 计算 `path_str`；以 `self._container_path_for_local(path_str)` 计算 `container_path`；在 `条件 container_path is None` 分支中，以 `self._container_path_for_local(os.path.normpath(normalized_path))` 计算 `container_path`；在 `条件 container_path is not None` 分支中，返回 `container_path`；返回 `path_str`。

  源码依据：[_reverse_resolve_path 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L654-L679)


- **`LocalSandbox._container_path_for_local`**（`local_path`）：在 `循环 for mapping in self._mappings_by_local_specificity` 分支中，以 `self._resolved_local_paths[mapping]` 计算 `local_path_resolved`；在 `循环 for mapping in self._mappings_by_local_specificity` → `条件 local_path == local_path_resolved or local_path.startswith(local_path_resolved + os.sep)` 分支中，以 `local_path[len(local_path_resolved) :].lstrip(os.sep).replace(os.sep, "/")` 计算 `relative`；在 `循环 for mapping in self._mappings_by_local_specificity` → `条件 local_path == local_path_resolved or local_path.startswith(local_path_resolved + os.sep)` 分支中，返回 `f"{mapping.container_path}/{relative}" if relative else mapping.container_path`；返回 `None`。

  源码依据：[_container_path_for_local 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L680-L701)


- **`LocalSandbox._reverse_resolve_paths_in_output`**（`output`）：以 `output` 计算 `result`；在 `循环 for mapping in self._mappings_by_local_specificity` 分支中，以 `replace_output_path_matches( result, self._resolved_local_paths[mapping], self._reverse_resolve_path, separator_agnostic=True, )` 计算 `result`；返回 `result`。

  源码依据：[_reverse_resolve_paths_in_output 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L702-L731)


- **`LocalSandbox._resolve_paths_in_command`**（`command, shell=None`）：以 `self._command_pattern` 计算 `pattern`；在 `条件 pattern is None` 分支中，返回 `command`；建立局部回调 `def replace_match(match: re.Match) -> str`，行为在独立条目展开；返回 `pattern.sub(replace_match, command)`。

  源码依据：[_resolve_paths_in_command 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L732-L765)


- **`LocalSandbox._resolve_paths_in_command.replace_match`**（`match`）：父函数内定义的回调。以 `match.group(0)` 计算 `matched_path`；在 `条件 _match_ends_before_a_colon(match)` 分支中，以 `self._resolve_path_textually(matched_path).replace("\\", "/")` 计算 `resolved`；在 `同层否则分支` 分支中，以 `self._resolve_path(matched_path).replace("\\", "/")` 计算 `resolved`；在 `条件 self._is_powershell(shell or "")` 分支中，返回 `escape_powershell_path(match, resolved)`；返回 `resolved`。

  源码依据：[replace_match 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L747-L763)


- **`LocalSandbox._resolve_paths_in_content`**（`content`）：以 `self._content_pattern` 计算 `pattern`；在 `条件 pattern is None` 分支中，返回 `content`；建立局部回调 `def replace_match(match: re.Match) -> str`，行为在独立条目展开；返回 `pattern.sub(replace_match, content)`。

  源码依据：[_resolve_paths_in_content 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L766-L799)


- **`LocalSandbox._resolve_paths_in_content.replace_match`**（`match`）：父函数内定义的回调。以 `match.group(0)` 计算 `matched_path`；在 `条件 _match_ends_before_a_colon(match)` 分支中，返回 `self._resolve_path_textually(matched_path).replace("\\", "/")`；以 `self._resolve_path(matched_path)` 计算 `resolved`；返回 `resolved.replace("\\", "/")`。

  源码依据：[replace_match 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L785-L797)


- **`LocalSandbox._get_cmd_output_encoding`**（无显式业务参数）：以 `ctypes.WinDLL("kernel32", use_last_error=True)` 计算 `kernel32`。关键判定为 `code_page`，相应动作只在对应分支执行。捕获 `(AttributeError, OSError)` 的边界见源码，捕获不等于已恢复。分支返回 `f"cp{code_page}"`、`locale.getencoding()`，提前返回结束该路径。

  源码依据：[_get_cmd_output_encoding 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L801-L817)


- **`LocalSandbox._get_shell`**（无显式业务参数）：以 `LocalSandbox._find_first_available_shell(("/bin/zsh", "/bin/bash", "/bin/sh", "sh"))` 计算 `shell`；以 「表达式摘要 1」 计算 `shell`。关键判定为 `shell is not None`、`os.name == "nt"`、`shell is not None`，相应动作只在对应分支执行。以 「表达式摘要 2」 计算 `shell`。显式抛出路径涉及 `RuntimeError`。分支返回 `shell`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  LocalSandbox._find_first_available_shell( ( "pwsh", "pwsh.exe", "powershell",
       "powershell.exe", ntpath.join(system_root, "System32", "WindowsPowerShell",
         "v1.0", "powershell.exe"), "cmd.exe", ) )
  ```


  **表达式摘要 2**（非完整源码；换行仅为阅读排版）：

  ```text
  LocalSandbox._find_first_available_shell( ( "pwsh", "pwsh.exe", "powershell",
       "powershell.exe", ntpath.join(system_root, "System32", "WindowsPowerShell",
         "v1.0", "powershell.exe"), "cmd.exe", ) )
  ```


  源码依据：[_get_shell 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L819-L843)


### 5.13.4 LocalSandbox.execute_command

参数：`command, env=None, timeout=None`。

校验 env 键并映射命令中的虚拟路径，合并经过过滤的宿主环境与请求注入项，按 Windows/POSIX 选择 runner；输出反向映射路径并形成超时结果。默认主机 bash 的禁用发生在 sandbox.tools 的工具入口，直接调用本方法不重复该开关，也不承诺 shell 写入只读映射被内核阻止。

源码依据：[execute_command 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L844-L965)


### 5.13.5 LocalSandbox._run_windows_command

参数：`args, timeout, env=None, encoding=None, fallback_encoding=None`。

启动宿主命令并分别排空 stdout/stderr，有界捕获输出；超时或取消使用进程树终止路径，等待清理后再形成结果，停止等待不能等价于命令没运行。

源码依据：[_run_windows_command 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L967-L1053)


### 5.13.6 辅助函数与局部契约

- **`LocalSandbox._terminate_windows_process_tree`**（`process`）：以 `os.environ.get("SystemRoot", r"C:\Windows")` 计算 `system_root`；以 `ntpath.join(system_root, "System32", "taskkill.exe")` 计算 `taskkill`；在 `try` 分支中，以 「表达式摘要 1」 计算 `result`；在 `try` → `条件 result.returncode != 0 and process.poll() is None` → `try` 分支中，执行 `process.kill()`；在 `捕获 (OSError, subprocess.TimeoutExpired)` → `条件 process.poll() is None` → `try` 分支中，执行 `process.kill()`；在 `try` 分支中，执行 `process.wait(timeout=10)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  subprocess.run( [taskkill, "/PID", str(process.pid), "/T", "/F"],
     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
     timeout=10, check=False, )
  ```


  源码依据：[_terminate_windows_process_tree 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1055-L1085)


### 5.13.7 LocalSandbox._run_posix_command

参数：`args, timeout, env=None`。

用独立 process group 管理命令，管道在后台排空，超时/取消进入组终止；进程后代是否仍存在需组级检查，而不能只看主进程 returncode。

源码依据：[_run_posix_command 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1087-L1194)


### 5.13.8 辅助函数与局部契约

- **`LocalSandbox._terminate_process_group`**（`process, process_group_id`）：在 `try` 分支中，执行 `os.killpg(process_group_id, signal.SIGKILL)`；在 `捕获 (ProcessLookupError, PermissionError, OSError)` → `try` 分支中，执行 `process.kill()`；在 `try` 分支中，执行 `process.wait(timeout=10)`。

  源码依据：[_terminate_process_group 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1196-L1217)


### 5.13.9 LocalSandbox.list_dir

参数：`path, max_depth=2`。

以 `self._resolve_path(path)` 计算 `resolved_path`。关键判定为 `not mapping.container_path.startswith(container_path + "/")`、`"/" in child_rel`、`os.path.isdir(self._resolved_local_paths[mapping])`。

执行 `virtual_children.append(mapping)`；执行 `result.append(f"{reversed_entry}/" if is_dir and not reversed_entry.endswith("/") else reversed_entry)`。执行 `if not mapping.container_path.startswith(container_path + "/"):`。捕获 `OSError`、`FileNotFoundError`、`OSError as e` 的边界见源码，显式抛出路径涉及 `当前异常`、`type`。

分支返回 `sorted(result)`，提前返回结束该路径。

源码依据：[list_dir 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1218-L1279)


### 5.13.10 辅助函数与局部契约

- **`LocalSandbox.read_file`**（`path, start_line=None, end_line=None`）：以 `self._resolve_path(path)` 计算 `resolved_path`；以 `self._reverse_resolve_paths_in_output(content)` 计算 `content`。关键判定为 `not should_slice`、`should_slice`、`line_number < start`，相应动作只在对应分支执行。进入 `open(resolved_path, encoding="utf-8", newline="\n") as f` 边界，退出由对应 context manager 处理。执行 `selected.append(line.rstrip("\r\n"))`。以 `f.read()` 计算 `content`；以 `"\n".join(selected)` 计算 `content`。捕获 `OSError as e` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `type`。分支返回 `content`，提前返回结束该路径。

  源码依据：[read_file 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1280-L1317)


- **`LocalSandbox.download_file`**（`path, max_bytes=None`）：以 `download_limit(max_bytes, DEFAULT_DOWNLOAD_LIMIT)` 计算 `max_download_size`；以 `f.read(max_download_size + 1)` 计算 `data`。关键判定为 `stripped_path != allowed_prefix and not stripped_path.startswith(f"{allowed_prefix}/")`、`file_size > max_download_size`、`len(data) > max_download_size`，相应动作只在对应分支执行。进入 `open(resolved_path, "rb") as f` 边界，退出由对应 context manager 处理。以 `path.replace("\\", "/")` 计算 `normalised`；执行 `if stripped_path != allowed_prefix and not stripped_path.startswith(f"{allowed_prefix}/"):`。捕获 `OSError as e` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `PermissionError`、`OSError`、`type`。分支返回 `data`，提前返回结束该路径。

  源码依据：[download_file 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1318-L1340)


- **`LocalSandbox.write_file`**（`path, content, append=False`）：以 `self._resolve_path_with_mapping(path)` 计算 `resolved`。关键判定为 `self._is_resolved_path_read_only(resolved)`、`dir_path`、`append or sys.platform not in {"darwin", "linux"}`，相应动作只在对应分支执行。进入 `open(resolved_path, "a" if append else "w", encoding="utf-8", newline="") as f` 边界，退出由对应 context manager 处理。执行 `self._agent_written_paths.add(resolved_path)`。执行 `f.write(resolved_content)`。捕获 `OSError as e` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `OSError`、`type`。

  源码依据：[write_file 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1341-L1368)


### 5.13.11 LocalSandbox._virtual_search_mounts

参数：`path`。

以 `path.rstrip("/") + "/"` 计算 `prefix`；以 `[]` 计算 `mounts`。关键判定为 `self._find_path_mapping(path) is not None`、`not mapping.container_path.startswith(prefix)`、`not child or "/" in child`。执行 `mounts.append((child, root))`。执行 `if not mapping.container_path.startswith(prefix):`。

捕获 `OSError`、`FileNotFoundError` 的边界见源码，显式抛出路径涉及 `NotADirectoryError`、`当前异常`。分支返回 `None`、`mounts`，提前返回结束该路径。

源码依据：[_virtual_search_mounts 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1369-L1408)


### 5.13.12 辅助函数与局部契约

- **`LocalSandbox.glob`**（`path, pattern, include_dirs=False, max_results=200`）：以 `self._virtual_search_mounts(path)` 计算 `mounts`。关键判定为 `mounts is not None`、`include_dirs and path_matches(pattern, child)`、`len(results) <= max_results`，相应动作只在对应分支执行。执行 `results.append(virtual_dir)`；执行 `results.extend(f"{virtual_dir}/{Path(match).relative_to(root).as_posix()}" for match in matches)`。捕获 `OSError as e` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `当前异常`、`type`。分支返回 `results[:max_results], True`、`results, False`、`[self._reverse_resolve_path(match) for match in matches], truncated`，提前返回结束该路径。

  源码依据：[glob 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1409-L1436)


### 5.13.13 LocalSandbox.grep

参数：`path, pattern, glob=None, literal=False, case_sensitive=False, max_results=100`。

以 `self._virtual_search_mounts(path)` 计算 `mounts`。关键判定为 `mounts is not None`、`len(results) > max_results`、`e.filename is None`。

执行 「表达式摘要 1」。捕获 `OSError as e` 的边界见源码，显式抛出路径涉及 `当前异常`、`type`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
results.extend(GrepMatch(path=f"{path.rstrip('/')}/{child}/{Path(match.path).relative_to(root).as_posix()}",
     line_number=match.line_number, line=match.line) for match in matches)
```


分支返回 `results[:max_results], True`、`results, False`、「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
[ GrepMatch( path=self._reverse_resolve_path(match.path), line_number=match.line_number,
     line=match.line, ) for match in matches ], truncated
```


源码依据：[grep 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1437-L1489)


### 5.13.14 辅助函数与局部契约

- **`LocalSandbox.update_file`**（`path, content`）：以 `self._resolve_path_with_mapping(path)` 计算 `resolved`；以 `resolved.path` 计算 `resolved_path`；在 `条件 self._is_resolved_path_read_only(resolved)` 分支中，抛出 `OSError(errno.EROFS, "Read-only file system", path)`；在 `try` 分支中，以 `os.path.dirname(resolved_path)` 计算 `dir_path`；在 `try` → `条件 dir_path` 分支中，执行 `os.makedirs(dir_path, exist_ok=True)`；在 `try` → `资源/同步边界 open(resolved_path, "wb") as f` 分支中，执行 `f.write(content)`；在 `捕获 OSError as e` 分支中，抛出 `type(e)(e.errno, e.strerror, path) from None`。

  源码依据：[update_file 实现](../backend/packages/harness/deerflow/sandbox/local/local_sandbox.py#L1490-L1504)


## 5.14 温池生命周期决策

源码位置：`backend/packages/harness/deerflow/community/warm_pool_lifecycle.py`。

Mixin 只提供可复用的 warm LRU/过期和 idle checker 框架，具体 active 计数与销毁由 provider 实现。replicas 是软目标，线程停止、超时和锁的责任与实际 destroy 分开。

### 5.14.1 辅助函数与局部契约

- **`WarmPoolLifecycleMixin._active_count_locked`**（无显式业务参数）：抛出 `NotImplementedError`。

  源码依据：[_active_count_locked 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L31-L34)


- **`WarmPoolLifecycleMixin._destroy_warm_entry`**（`sandbox_id, entry, reason`）：抛出 `NotImplementedError`。

  源码依据：[_destroy_warm_entry 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L35-L38)


- **`WarmPoolLifecycleMixin._replica_count`**（无显式业务参数）：以 `int(self._config.get("replicas", DEFAULT_REPLICAS))` 计算 `replicas`；在 `资源/同步边界 self._lock` 分支中，以 `self._active_count_locked() + len(self._warm_pool)` 计算 `total`；返回 `replicas, total`。

  源码依据：[_replica_count 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L39-L45)


- **`WarmPoolLifecycleMixin._log_replicas_soft_cap`**（`replicas, sandbox_id, evicted`）：在 `条件 evicted is not None` 分支中，返回 None，提前结束当前路径。

  源码依据：[_log_replicas_soft_cap 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L46-L57)


- **`WarmPoolLifecycleMixin._evict_oldest_warm`**（无显式业务参数）：在 `资源/同步边界 self._lock` → `条件 not self._warm_pool` 分支中，返回 `None`；在 `资源/同步边界 self._lock` 分支中，以 `min(self._warm_pool.items(), key=lambda item: item[1][1])` 计算 `sandbox_id, (entry, _)`；在 `资源/同步边界 self._lock` 分支中，执行 `self._warm_pool.pop(sandbox_id)`；执行 `self._destroy_warm_entry(sandbox_id, entry, reason="replica_enforcement")`；返回 `sandbox_id`。

  源码依据：[_evict_oldest_warm 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L58-L68)


- **`WarmPoolLifecycleMixin._reap_expired_warm`**（`idle_timeout=None`）：以 `float(self._config.get("idle_timeout", DEFAULT_IDLE_TIMEOUT) if idle_timeout is None else idle_timeout)` 计算 `timeout`；在 `条件 timeout <= 0` 分支中，返回 None，提前结束当前路径；以 `time.monotonic()` 计算 `now`；以 `[]` 计算 `expired: list[tuple[str, WarmEntryT]]`；在 `资源/同步边界 self._lock` → `循环 for sandbox_id, (entry, timestamp) in self._warm_pool.items()` → `条件 now - timestamp > timeout` 分支中，执行 `expired.append((sandbox_id, entry))`；在 `资源/同步边界 self._lock` → `循环 for sandbox_id, _ in expired` 分支中，执行 `self._warm_pool.pop(sandbox_id, None)`；在 `循环 for sandbox_id, entry in expired` 分支中，执行 `self._destroy_warm_entry(sandbox_id, entry, reason="idle_timeout")`。

  源码依据：[_reap_expired_warm 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L69-L88)


- **`WarmPoolLifecycleMixin._start_idle_checker`**（无显式业务参数）：在 `条件 self._idle_checker_thread is not None and self._idle_checker_thread.is_alive()` 分支中，返回 None，提前结束当前路径；执行 `self._idle_checker_stop.clear()`；以 `threading.Thread( target=self._idle_checker_loop, name=self._idle_checker_thread_name, daemon=True, )` 计算 `self._idle_checker_thread`；执行 `self._idle_checker_thread.start()`。

  源码依据：[_start_idle_checker 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L89-L102)


- **`WarmPoolLifecycleMixin._stop_idle_checker`**（无显式业务参数）：执行 `self._idle_checker_stop.set()`；以 `self._idle_checker_thread` 计算 `thread`；在 `条件 thread is not None and thread.is_alive() and thread is not threading.current_thread()` 分支中，执行 `thread.join(timeout=5)`；在 `条件 thread is not None and thread.is_alive() and thread is not threading.current_thread()` → `条件 thread.is_alive()` 分支中，抛出 `RuntimeError("warm-pool idle checker is still running after stop timeout")`。

  源码依据：[_stop_idle_checker 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L103-L111)


- **`WarmPoolLifecycleMixin._idle_checker_loop`**（无显式业务参数）：以 `float(self._config.get("idle_timeout", DEFAULT_IDLE_TIMEOUT))` 计算 `idle_timeout`；在 `循环 while not self._idle_checker_stop.wait(self.IDLE_CHECK_INTERVAL)` → `try` 分支中，执行 `self._cleanup_idle_resources(idle_timeout)`。

  源码依据：[_idle_checker_loop 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L112-L120)


- **`WarmPoolLifecycleMixin._cleanup_idle_resources`**（`idle_timeout`）：执行 `self._reap_expired_warm(idle_timeout)`。

  源码依据：[_cleanup_idle_resources 实现](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L121-L125)


## 5.15 容器沙箱分配与回收

源码位置：`backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py`。

AIO provider 把 backend 容器动作、作用域身份、创建序列化、active/warm 池、跨实例所有权和失败销毁隔离联结起来。同步/异步入口共享生命周期规则但等待方式不同；所有权服务响应可能迟到，所以本地 reservation 与 epoch 也必须参与判断。详见 4.1.4。

### 5.15.1 辅助函数与局部契约

- **`SandboxBeingDestroyedError.__init__`**（`sandbox_id`）：执行 `super().__init__(f"sandbox {sandbox_id} is being destroyed by another instance")`；以 `sandbox_id` 计算 `self.sandbox_id`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L106-L110)


- **`SandboxPolicyReplacementDeferredError.__init__`**（`sandbox_id, reason=None`）：以 `reason or "has an incompatible provisioning policy; replacement is deferred until its current owner releases it"` 计算 `detail`；执行 `super().__init__(f"sandbox {sandbox_id} {detail}")`；以 `sandbox_id` 计算 `self.sandbox_id`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L114-L119)


- **`SandboxBrokerModeUnverifiedError.__init__`**（`sandbox_id`）：执行 「表达式摘要 1」；以 `sandbox_id` 计算 `self.sandbox_id`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  super().__init__(f"sandbox {sandbox_id} Lark broker mode is unverified; update the provisioner before retrying; the existing runtime is preserved")
  ```


  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L123-L127)


- **`SandboxIdentityCollisionError.__init__`**（`sandbox_id, stored_key, requested_key`）：执行 `super().__init__(f"sandbox ID collision for {sandbox_id}: tracked identity is {stored_key!r}, requested identity is {requested_key!r}")`；以 `sandbox_id` 计算 `self.sandbox_id`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L131-L140)


- **`_run_started_acquire_worker`**（`executor, func, *args, **kwargs`）：以 `asyncio.get_running_loop()` 计算 `loop`。关键判定为 `worker.cancelled() or worker.cancel()`，相应动作只在对应分支执行。执行 `if worker.cancelled() or worker.cancel():`；等待 `asyncio.shield(done.wait())` 完成。捕获 `asyncio.CancelledError as cancellation`、`asyncio.CancelledError`、`Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `当前异常`、`cancellation`。分支返回 `await wrapped`，提前返回结束该路径。

  源码依据：[_run_started_acquire_worker 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L141-L176)


- **`_lock_file_exclusive`**（`lock_file`）：在 `条件 fcntl is not None` 分支中，执行 `fcntl.flock(lock_file, fcntl.LOCK_EX)`；在 `条件 fcntl is not None` 分支中，返回 None，提前结束当前路径；执行 `lock_file.seek(0)`；执行 `msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)`。

  源码依据：[_lock_file_exclusive 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L177-L185)


- **`_try_lock_file_exclusive`**（`lock_file`）：在 `try` → `条件 fcntl is not None` 分支中，执行 `fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)`；在 `try` → `同层否则分支` 分支中，执行 `lock_file.seek(0)`；在 `try` → `同层否则分支` 分支中，执行 `msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)`；在 `捕获 OSError as error` → `条件 error.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK)` 分支中，返回 `False`；在 `捕获 OSError as error` 分支中，向外继续抛出当前异常；返回 `True`。

  源码依据：[_try_lock_file_exclusive 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L186-L200)


- **`_unlock_file`**（`lock_file`）：在 `条件 fcntl is not None` 分支中，执行 `fcntl.flock(lock_file, fcntl.LOCK_UN)`；在 `条件 fcntl is not None` 分支中，返回 None，提前结束当前路径；执行 `lock_file.seek(0)`；执行 `msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)`。

  源码依据：[_unlock_file 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L201-L209)


- **`_open_lock_file`**（`lock_path`）：返回 `open(lock_path, "a", encoding="utf-8")`。

  源码依据：[_open_lock_file 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L210-L213)


- **`AioSandboxProvider._positive_float`**（`name, value, default`）：在 `try` 分支中，以 `float(default if value is None else value)` 计算 `resolved`；在 `捕获 (TypeError, ValueError) as exc` 分支中，抛出 `ValueError(f"sandbox.{name} must be positive") from exc`；在 `条件 not math.isfinite(resolved) or resolved <= 0` 分支中，抛出 `ValueError(f"sandbox.{name} must be positive")`；返回 `resolved`。

  源码依据：[_positive_float 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L251-L259)


### 5.15.2 AioSandboxProvider.__init__

无显式业务参数。

关键判定为 `not self._ownership.supports_cross_process`、`self._config.get("idle_timeout", DEFAULT_IDLE_TIMEOUT) > 0`。

以 `threading.Lock()` 计算 `self._lock`；以 `{} # sandbox_id -> AioSandbox instance` 计算 `self._sandboxes: dict[str, AioSandbox]`；其他写入目标为 `self._sandbox_infos: dict[str, SandboxInfo]`、`self._thread_sandboxes: dict[tuple[str, str], str]`、`self._acquire_serializer: AcquireSerializer[tuple[str, str]]`、`self._acquire_worker_executor`、`self._last_activity: dict[str, float]`、`self._warm_pool: dict[str, tuple[SandboxInfo, float]]`、`self._active_sandbox_identity: dict[str, tuple[str, str] | None]`、`self._warm_pool_identity: dict[str, tuple[str, str] | None]`、`self._unowned_since: dict[str, float]`、`self._local_teardown: set[str]`、`self._acquire_epoch: dict[str, int]`、`self._acquire_epoch_counter`、`self._acquire_inflight: dict[str, int]`、`self._shutdown_called`、`self._admission_closed`、`self._cleanup_retry_ids: set[str]`、`self._idle_checker_stop`、`self._idle_checker_thread: threading.Thread | None`、`self._renewal_stop`、`self._renewal_thread: threading.Thread | None`、`self._owner_id`、`self._config`、`self._ownership_config`、`self._ownership: SandboxOwnershipStore`、`self._backend: SandboxBackend`。

源码依据：[__init__ 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L260-L332)


### 5.15.3 辅助函数与局部契约

- **`AioSandboxProvider.uses_thread_data_mounts`**（无显式业务参数）：以 `self._config.get("thread_data_mounts")` 计算 `override`；在 `条件 override is not None` 分支中，返回 `override`；返回 `isinstance(self._backend, LocalContainerBackend)`。

  源码依据：[uses_thread_data_mounts 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L334-L348)


- **`AioSandboxProvider._create_backend`**（无显式业务参数）：以 `self._config.get("provisioner_url")` 计算 `provisioner_url`。关键判定为 `provisioner_url`、`self.sandbox_network_mode() != "open"`，相应动作只在对应分支执行。显式抛出路径涉及 `RuntimeError`。分支返回 「表达式摘要 1」、「表达式摘要 2」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  RemoteSandboxBackend( provisioner_url=provisioner_url, api_key=api_key,
     max_shell_sessions=self._config.get("max_shell_sessions"), max_bash_sessions=self._config.get("max_bash_sessions",
       50), required_shell_sessions=self._config.get("required_shell_sessions",
       0), )
  ```


  **表达式摘要 2**（非完整源码；换行仅为阅读排版）：

  ```text
  LocalContainerBackend( image=self._config["image"], base_port=self._config["port"],
     container_prefix=self._config["container_prefix"], config_mounts=self._config["mounts"],
     environment=self._config["environment"], network_config=self._config["network"],
     required_shell_sessions=self._config.get("required_shell_sessions",
       0), )
  ```


  源码依据：[_create_backend 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L349-L384)


### 5.15.4 AioSandboxProvider._load_config

无显式业务参数。

以 `get_app_config()` 计算 `config`。关键判定为 `not isinstance(configured_skills_path, str)`、`max_bash_sessions <= 0`、`configured_shell_sessions is None`。以 `str(max_bash_sessions)` 计算 `environment["AIO_BASH_MAX_SESSIONS"]`；以 `str(max_shell_sessions)` 计算 `environment["MAX_SHELL_SESSIONS"]`。

捕获 `(TypeError, ValueError) as exc` 的边界见源码，显式抛出路径涉及 `ValueError`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
{ "image": sandbox_config.image or DEFAULT_IMAGE, "port": sandbox_config.port or DEFAULT_PORT,
   "container_prefix": sandbox_config.container_prefix or DEFAULT_CONTAINER_PREFIX,
   "idle_timeout": idle_timeout if idle_timeout is not None else DEFAULT_IDLE_TIMEOUT,
   "command_timeout": command_timeout, "replicas": replicas if replicas is not None else DEFAULT_REPLICAS,
   "mounts": sandbox_config.mounts or [], "thread_data_mounts": getattr(sandbox_config,
     "thread_data_mounts", None), "environment": environment, "max_shell_sessions": max_shell_sessions,
   "max_bash_sessions": max_bash_sessions, "required_shell_sessions": required_shell_sessions,
   "network": sandbox_config.network.model_dump(), "ownership": getattr(sandbox_config,
     "ownership", None), # A redis stream bridge means the deployment is multi-instance,
   which # is what the ownership store must default to. Read the same source # the bridge's own resolver reads, not just its env var. "stream_bridge": getattr(config, "stream_bridge", None), # provisioner URL for dynamic pod management (e.g. http://provisioner:8002) "provisioner_url": getattr(sandbox_config, "provisioner_url", None) or "", "provisioner_api_key": getattr(sandbox_config, "provisioner_api_key", None) or "", "skills_container_path": _normalize_skills_container_path( configured_skills_path, ), }
```


源码依据：[_load_config 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L385-L458)


### 5.15.5 辅助函数与局部契约

- **`AioSandboxProvider.sandbox_network_mode`**（无显式业务参数）：返回 `str(self._config.get("network", {}).get("mode", "open"))`。

  源码依据：[sandbox_network_mode 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L459-L461)


- **`AioSandboxProvider.sandbox_network_temporary_grant_ttl`**（无显式业务参数）：返回 `int(self._config.get("network", {}).get("temporary_grant_ttl", 300))`。

  源码依据：[sandbox_network_temporary_grant_ttl 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L462-L464)


- **`AioSandboxProvider.consume_network_policy_events`**（`sandbox_id`）：在 `条件 not isinstance(self._backend, LocalContainerBackend)` 分支中，返回 `[]`；返回 `self._backend.consume_network_policy_events(sandbox_id)`。

  源码依据：[consume_network_policy_events 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L465-L469)


- **`AioSandboxProvider.deny_pending_network_policy_events`**（`sandbox_id`）：在 `条件 not isinstance(self._backend, LocalContainerBackend)` 分支中，返回 `True`；返回 `self._backend.deny_pending_network_policy_events(sandbox_id)`。

  源码依据：[deny_pending_network_policy_events 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L470-L474)


- **`AioSandboxProvider.decide_network_policy_request`**（`sandbox_id, request_id, decision`）：在 `条件 not isinstance(self._backend, LocalContainerBackend)` 分支中，返回 `False`；返回 `self._backend.decide_network_policy_request(sandbox_id, request_id, decision)`。

  源码依据：[decide_network_policy_request 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L475-L481)


### 5.15.6 AioSandboxProvider._publish_ownership

参数：`sandbox_id`。

向 store 发布持有权前后核对本地 teardown/epoch，拒绝将迟到响应应用到已重建的资源；ownership 不足时不能悄悄继续拥有销毁权。

源码依据：[_publish_ownership 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L482-L554)


### 5.15.7 辅助函数与局部契约

- **`AioSandboxProvider._reserve_local_teardown`**（`sandbox_id, still_reapable`）：在 `资源/同步边界 self._lock` → `条件 sandbox_id in self._local_teardown or not still_reapable()` 分支中，返回 `False`；在 `资源/同步边界 self._lock` 分支中，执行 `self._local_teardown.add(sandbox_id)`；在 `资源/同步边界 self._lock` 分支中，返回 `True`。

  源码依据：[_reserve_local_teardown 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L555-L577)


- **`AioSandboxProvider._finish_local_teardown`**（`sandbox_id`）：在 `资源/同步边界 self._lock` 分支中，执行 `self._local_teardown.discard(sandbox_id)`。

  源码依据：[_finish_local_teardown 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L578-L581)


- **`AioSandboxProvider._being_torn_down_locally`**（`sandbox_id`）：返回 `sandbox_id in self._local_teardown`。

  源码依据：[_being_torn_down_locally 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L582-L588)


- **`AioSandboxProvider._acquire_epoch_of`**（`sandbox_id`）：在 `资源/同步边界 self._lock` 分支中，返回 `self._acquire_epoch.get(sandbox_id, 0)`。

  源码依据：[_acquire_epoch_of 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L589-L600)


- **`AioSandboxProvider._claim_ownership`**（`sandbox_id, for_destroy=False`）：在 `try` 分支中，返回 `self._ownership.claim(sandbox_id, for_destroy=for_destroy)`；在 `捕获 OwnershipBackendError as e` 分支中，返回 `False`。

  源码依据：[_claim_ownership 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L601-L618)


- **`AioSandboxProvider._release_ownership`**（`sandbox_id`）：在 `try` 分支中，执行 `self._ownership.release(sandbox_id)`。

  源码依据：[_release_ownership 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L619-L626)


- **`AioSandboxProvider._refresh_ownership`**（`sandbox_id`）：在 `try` 分支中，以 `self._ownership.renew(sandbox_id)` 计算 `outcome`；在 `捕获 OwnershipBackendError as e` 分支中，返回 `True`；在 `条件 outcome is RenewOutcome.RENEWED` 分支中，返回 `True`；在 `条件 outcome is RenewOutcome.LAPSED` → `try` → `条件 self._ownership.claim(sandbox_id)` 分支中，返回 `True`；在 `条件 outcome is RenewOutcome.LAPSED` → `捕获 OwnershipBackendError as e` 分支中，返回 `True`；在 `条件 outcome is RenewOutcome.LAPSED` 分支中，返回 `False`；返回 `False`。

  源码依据：[_refresh_ownership 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L627-L662)


### 5.15.8 AioSandboxProvider._held_teardown_lease

参数：`sandbox_id`。

以 `threading.Event()` 计算 `stop`；建立局部回调 `def beat() -> None`，行为在独立条目展开；以 `threading.Thread(target=beat, name="sandbox-teardown-lease", daemon=True)` 计算 `beater`；执行 `beater.start()`；在 `try` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾；在 `finally 收尾` 分支中，执行 `stop.set()`；在 `finally 收尾` 分支中，执行 `beater.join(timeout=self._TEARDOWN_JOIN_TIMEOUT_SECONDS)`。

源码依据：[_held_teardown_lease 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L664-L751)


### 5.15.9 辅助函数与局部契约

- **`AioSandboxProvider._held_teardown_lease.beat`**（无显式业务参数）：父函数内定义的回调。以 `self._ownership_config.renewal_interval_seconds` 计算 `interval`；在 `try` → `循环 while not stop.wait(interval)` → `try` → `条件 not self._ownership.claim(sandbox_id, for_destroy=True)` 分支中，返回 None，提前结束当前路径；在 `finally 收尾` 分支中，执行 `self._release_ownership(sandbox_id)`。

  源码依据：[beat 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L703-L730)


### 5.15.10 AioSandboxProvider._adoptable_after_grace

参数：`sandbox_id, now`。

在 `条件 not self._ownership.supports_cross_process` 分支中，返回 `True`；在 `try` 分支中，以 `self._ownership.owner(sandbox_id)` 计算 `current_owner`；在 `捕获 OwnershipBackendError as e` 分支中，返回 `False`；在 `条件 current_owner is not None` 分支中，执行 `self._unowned_since.pop(sandbox_id, None)`；在 `条件 current_owner is not None` 分支中，返回 `False`；以 `self._unowned_since.setdefault(sandbox_id, now)` 计算 `first_seen`；返回 `now - first_seen >= compute_lease_ttl(self._ownership_config)`。

源码依据：[_adoptable_after_grace 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L752-L795)


### 5.15.11 辅助函数与局部契约

- **`AioSandboxProvider._replace_incompatible_sandbox`**（`info, now`）：关键判定为 `not info.requires_replacement`、`not self._adoptable_after_grace(info.sandbox_id, now)`、「表达式摘要 1」，相应动作只在对应分支执行。进入 `self._held_teardown_lease(info.sandbox_id)` 边界，退出由对应 context manager 处理。执行 `self._unowned_since.pop(info.sandbox_id, None)`。捕获 `Exception as e` 的边界见源码，捕获不等于已恢复。分支返回 `False`、`True`，提前返回结束该路径。finally 包含 执行 `self._finish_local_teardown(info.sandbox_id)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  not self._reserve_local_teardown( info.sandbox_id, lambda: info.sandbox_id not in self._sandboxes and info.sandbox_id
    not in self._sandbox_infos and info.sandbox_id not in self._warm_pool,
     )
  ```


  源码依据：[_replace_incompatible_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L796-L831)


### 5.15.12 AioSandboxProvider._reconcile_orphans

无显式业务参数。

以 `self._backend.list_running()` 计算 `running`。关键判定为 `not running`、`self._quarantine_store().contains(info)`、`info.requires_replacement`。进入 `self._lock` 边界，退出由对应 context manager 处理。

以 `{sid: seen for sid, seen in self._unowned_since.items() if sid in running_ids}` 计算 `self._unowned_since`；以 `(info, time.monotonic())` 计算 `self._warm_pool[info.sandbox_id]`；其他写入目标为 `self._warm_pool_identity[info.sandbox_id]`。执行 `self._unowned_since.pop(info.sandbox_id, None)`。捕获 `Exception as e` 的边界见源码，

分支返回 `None`，提前返回结束该路径。

源码依据：[_reconcile_orphans 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L832-L935)


### 5.15.13 辅助函数与局部契约

- **`AioSandboxProvider._effective_acquire_user_id`**（`user_id`）：返回 `user_id or get_effective_user_id()`。

  源码依据：[_effective_acquire_user_id 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L937-L939)


- **`AioSandboxProvider._thread_key`**（`thread_id, user_id`）：返回 `(user_id, thread_id)`。

  源码依据：[_thread_key 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L941-L943)


- **`AioSandboxProvider._deterministic_sandbox_id`**（`thread_id, user_id`）：返回 `derive_sandbox_scope_token(user_id=user_id, thread_id=thread_id)`。

  源码依据：[_deterministic_sandbox_id 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L945-L956)


- **`AioSandboxProvider._thread_skill_projection_active`**（`thread_id, user_id`）：返回 `get_paths().thread_skills_view_dir(thread_id, user_id=user_id).exists()`。

  源码依据：[_thread_skill_projection_active 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L958-L960)


- **`AioSandboxProvider._policy_scoped_sandbox_id`**（`thread_id, user_id, skills_container_path`）：以 `_normalize_skills_container_path(skills_container_path)` 计算 `normalized_root`；以 `b"agent-skills-v2\0" + user_id.encode() + b"\0" + thread_id.encode() + b"\0" + normalized_root.encode()` 计算 `seed`；返回 `hashlib.sha256(seed).hexdigest()[:16]`。

  源码依据：[_policy_scoped_sandbox_id 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L962-L971)


- **`AioSandboxProvider._custom_root_sandbox_id`**（`thread_id, user_id, skills_container_path`）：以 `_normalize_skills_container_path(skills_container_path)` 计算 `normalized_root`；以 `b"skills-root-v1\0" + user_id.encode() + b"\0" + thread_id.encode() + b"\0" + normalized_root.encode()` 计算 `seed`；返回 `hashlib.sha256(seed).hexdigest()[:16]`。

  源码依据：[_custom_root_sandbox_id 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L973-L982)


- **`AioSandboxProvider._assert_active_identity_available_locked`**（`sandbox_id, requested_key`）：在 `条件 sandbox_id not in self._sandboxes and sandbox_id not in self._sandbox_infos` 分支中，返回 None，提前结束当前路径；以 `self._active_sandbox_identity.get(sandbox_id)` 计算 `stored_key`；在 `条件 stored_key is None` 分支中，以 `[key for key, mapped_id in self._thread_sandboxes.items() if mapped_id == sandbox_id]` 计算 `matching_keys`；在 `条件 stored_key is None` → `条件 len(matching_keys) == 1` 分支中，以 `matching_keys[0]` 计算 `stored_key`；在 `条件 stored_key != requested_key` 分支中，抛出 `SandboxIdentityCollisionError(sandbox_id, stored_key, requested_key)`。

  源码依据：[_assert_active_identity_available_locked 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L983-L999)


- **`AioSandboxProvider._assert_warm_identity_available_locked`**（`sandbox_id, requested_key`）：在 `条件 sandbox_id not in self._warm_pool` 分支中，返回 None，提前结束当前路径；以 `self._warm_pool_identity.get(sandbox_id)` 计算 `stored_key`；在 `条件 stored_key is not None and stored_key != requested_key` 分支中，抛出 `SandboxIdentityCollisionError(sandbox_id, stored_key, requested_key)`。

  源码依据：[_assert_warm_identity_available_locked 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1000-L1014)


### 5.15.14 AioSandboxProvider._get_extra_mounts

参数：`thread_id, user_id=None`。

以 `[]` 计算 `mounts: list[tuple[str, str, bool]]`。关键判定为 `thread_id`、`skills_mounts`、`user_skill_mounts`。执行 `mounts.extend(self._get_thread_mounts(thread_id, user_id=user_id))`；执行 `mounts.extend(skills_mounts)`。分支返回 `self._dedupe_mounts_by_container_path(mounts)`，提前返回结束该路径。

源码依据：[_get_extra_mounts 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1015-L1059)


### 5.15.15 辅助函数与局部契约

- **`AioSandboxProvider._local_config_mount_exclusion_root`**（`thread_id, user_id`）：在 `条件 not isinstance(self._backend, LocalContainerBackend) or not thread_id` 分支中，返回 `None`；在 `条件 not self._thread_skill_projection_active(thread_id, user_id)` 分支中，返回 `None`；返回 `self._configured_skills_container_path()`。

  源码依据：[_local_config_mount_exclusion_root 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1060-L1072)


- **`AioSandboxProvider._configured_skills_container_path`**（无显式业务参数）：以 `getattr(self, "_config", None)` 计算 `config`；在 `条件 not isinstance(config, dict)` 分支中，以 `self._load_config()` 计算 `config`；返回 `_normalize_skills_container_path( config.get( "skills_container_path", DEFAULT_SKILLS_CONTAINER_PATH, ) )`。

  源码依据：[_configured_skills_container_path 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1073-L1087)


- **`AioSandboxProvider._dedupe_mounts_by_container_path`**（`mounts`）：以 `set()` 计算 `seen: set[str]`；以 `[]` 计算 `deduped: list[tuple[str, str, bool]]`；在 `循环 for host_path, container_path, read_only in mounts` → `条件 container_path in seen` 分支中，进入下一轮循环；在 `循环 for host_path, container_path, read_only in mounts` 分支中，执行 `seen.add(container_path)`；在 `循环 for host_path, container_path, read_only in mounts` 分支中，执行 `deduped.append((host_path, container_path, read_only))`；返回 `deduped`。

  源码依据：[_dedupe_mounts_by_container_path 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1089-L1110)


- **`AioSandboxProvider._get_thread_mounts`**（`thread_id, user_id=None`）：以 `get_paths()` 计算 `paths`。分支返回 「表达式摘要 1」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  [ (paths.host_sandbox_work_dir(thread_id, user_id=effective_user_id),
       f"{VIRTUAL_PATH_PREFIX}/workspace", False), (paths.host_sandbox_uploads_dir(thread_id,
         user_id=effective_user_id), f"{VIRTUAL_PATH_PREFIX}/uploads",
       False), (paths.host_sandbox_outputs_dir(thread_id, user_id=effective_user_id),
       f"{VIRTUAL_PATH_PREFIX}/outputs", False), # ACP workspace: read-only inside the sandbox (lead agent reads
      results; # the ACP subprocess writes from the host side, not from within the container). (paths.host_acp_workspace_dir(thread_id,
         user_id=effective_user_id), "/mnt/acp-workspace", True), ]
  ```


  源码依据：[_get_thread_mounts 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1112-L1131)


### 5.15.16 AioSandboxProvider._get_skills_mounts

参数：`thread_id=None, user_id=None, skills_container_path=None`。

以 `[]` 计算 `mounts: list[tuple[str, str, bool]]`。关键判定为 `thread_id and AioSandboxProvider._thread_skill_projection_active( thread_id, effective_user_id, )`。

执行 `mounts.append( ( join_host_path(host_base_dir, "skills_view", "public"), f"{container_path}/public", True, ) )`；执行 `mounts.append( ( host_user_custom, f"{container_path}/custom", True, ) )`。捕获 `Exception as e` 的边界见源码，

分支返回 `[ ( join_host_path(host_root, category.value), f"{container_path}/{category.value}", True, ) for category in SkillCategory ]`、`mounts`，提前返回结束该路径。

源码依据：[_get_skills_mounts 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1133-L1217)


### 5.15.17 辅助函数与局部契约

- **`AioSandboxProvider._ensure_skills_projection`**（`user_id`）：在 `try` 分支中，以 `get_or_new_user_skill_storage(user_id, app_config=get_app_config())` 计算 `storage`；在 `try` 分支中，返回 `ensure_skill_projections(storage)`；在 `捕获 Exception as exc` 分支中，返回 `None`。

  源码依据：[_ensure_skills_projection 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1219-L1236)


- **`AioSandboxProvider._get_user_skill_mounts`**（`user_id=None, skills_container_path=None`）：在 `try` 分支中，以 `get_app_config()` 计算 `config`；在 `try` 分支中，以 `get_paths()` 计算 `paths`；在 `try` 分支中，以 `_normalize_skills_container_path(skills_container_path or config.skills.container_path)` 计算 `resolved_skills_container_path`；在 `try` 分支中，以 `AioSandboxProvider._effective_acquire_user_id(user_id)` 计算 `effective_user_id`；在 `try` 分支中，执行 `AioSandboxProvider._ensure_skills_projection(effective_user_id)`；在 `try` 分支中，返回 「表达式摘要 1」；在 `捕获 Exception as e` 分支中，返回 `[]`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  [ ( join_host_path( str(paths.host_base_dir), "users", effective_user_id,
         "skills_view", "integrations", ), f"{resolved_skills_container_path}/integrations",
       True, ), ]
  ```


  源码依据：[_get_user_skill_mounts 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1238-L1271)


- **`AioSandboxProvider._lark_integration_active`**（`user_id=None`）：在 `try` 分支中，以 `AioSandboxProvider._effective_acquire_user_id(user_id)` 计算 `effective_user_id`；在 `try` 分支中，返回 `lark_skills_installed(effective_user_id)`；在 `捕获 Exception as e` 分支中，返回 `False`。

  源码依据：[_lark_integration_active 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1273-L1286)


- **`AioSandboxProvider._lark_broker_active`**（`user_id=None, observed_mode=None`）：在 `条件 not AioSandboxProvider._lark_integration_active(user_id)` 分支中，返回 `False`；返回 `sandbox_lark_broker_active(observed_mode=observed_mode) if observed_mode is not None else sandbox_lark_broker_active()`。

  源码依据：[_lark_broker_active 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1288-L1301)


- **`AioSandboxProvider._quarantine_store`**（无显式业务参数）：以 `getattr(self, "_quarantine", None)` 计算 `store`；在 `条件 store is None` 分支中，以 `(self._config.get("provisioner_url") or "").rstrip("/") or f"local:{self._config.get('container_prefix', DEFAULT_CONTAINER_PREFIX)}"` 计算 `scope`；在 `条件 store is None` 分支中，以 `SandboxQuarantine(get_paths().base_dir / "sandbox-quarantine", scope)` 计算 `store`；在 `条件 store is None` 分支中，以 `store` 计算 `self._quarantine`；返回 `store`。

  源码依据：[_quarantine_store 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1302-L1309)


- **`AioSandboxProvider._check_sandbox_reuse_policy`**（`info, user_id`）：以 `True` 计算 `info.requires_replacement`。关键判定为 `self._quarantine_store().contains(info)`、`info.requires_replacement`、`isinstance(self._backend, RemoteSandboxBackend) and self._lark_integration_active(user_id)`，相应动作只在对应分支执行。捕获 `LarkBrokerCapabilityUnknownError` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `SandboxBrokerModeUnverifiedError`。分支返回 `None`，提前返回结束该路径。

  源码依据：[_check_sandbox_reuse_policy 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1310-L1339)


### 5.15.18 AioSandboxProvider._get_lark_cli_runtime_mounts

参数：`user_id=None`。

以 `get_paths()` 计算 `paths`；以 「表达式摘要 1」 计算 `mounts`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
[ (config_dir, LARK_CLI_SANDBOX_CONFIG_DIR, True), (join_host_path(config_dir,
       "locks"), LARK_CLI_SANDBOX_LOCKS_DIR, False), (paths.host_user_integration_data_dir(effective_user_id,
       LARK_CLI_INTEGRATION_ID), LARK_CLI_SANDBOX_DATA_DIR, False),
   ]
```


关键判定为 `runtime_dir.is_dir()`。执行 「表达式摘要 1」。捕获 `Exception as e` 的边界见源码，分支返回 `mounts`、`[]`，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
mounts.append( ( join_host_path(str(paths.host_base_dir), "integrations",
       LARK_CLI_INTEGRATION_ID, "sandbox-cli"), LARK_CLI_SANDBOX_RUNTIME_DIR,
     True, ) )
```


源码依据：[_get_lark_cli_runtime_mounts 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1341-L1389)


### 5.15.19 辅助函数与局部契约

- **`AioSandboxProvider._cleanup_idle_resources`**（`idle_timeout`）：执行 `self._reconcile_orphans()`；执行 `self._cleanup_idle_sandboxes(idle_timeout)`。

  源码依据：[_cleanup_idle_resources 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1390-L1397)


- **`AioSandboxProvider._start_lease_renewal`**（无显式业务参数）：在 `条件 self._renewal_thread is not None and self._renewal_thread.is_alive()` 分支中，返回 None，提前结束当前路径；执行 `self._renewal_stop.clear()`；以 `threading.Thread( target=self._lease_renewal_loop, name="sandbox-lease-renewal", daemon=True, )` 计算 `self._renewal_thread`；执行 `self._renewal_thread.start()`。

  源码依据：[_start_lease_renewal 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1398-L1422)


- **`AioSandboxProvider._stop_lease_renewal`**（无显式业务参数）：执行 `self._renewal_stop.set()`；以 `self._renewal_thread` 计算 `thread`；在 `条件 thread is None or thread is threading.current_thread()` 分支中，返回 None，提前结束当前路径；在 `条件 thread.is_alive()` 分支中，执行 `thread.join(timeout=5)`；在 `条件 thread.is_alive()` 分支中，抛出 `RuntimeError("Sandbox lease-renewal thread is still running after stop timeout")`。

  源码依据：[_stop_lease_renewal 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1423-L1432)


- **`AioSandboxProvider._lease_renewal_loop`**（无显式业务参数）：以 `self._ownership_config.renewal_interval_seconds` 计算 `interval`；在 `循环 while not self._renewal_stop.wait(interval)` → `try` 分支中，执行 `self._renew_owned_leases()`。

  源码依据：[_lease_renewal_loop 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1433-L1440)


- **`AioSandboxProvider._renew_owned_leases`**（无显式业务参数）：快照 active/warm 资源逐一续约；RENEWED 保持，LAPSED 尝试重建，LOST 丢本地引用但不停止对方容器，避免误销毁。

  源码依据：[_renew_owned_leases 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1441-L1464)


### 5.15.20 AioSandboxProvider._forget_lost_sandbox

参数：`sandbox_id, expected_epoch=None`。

以 `self._sandboxes.pop(sandbox_id, None)` 计算 `sandbox`。关键判定为 `sandbox_id in self._local_teardown`、`sandbox_id in self._acquire_inflight`、`expected_epoch is not None and self._acquire_epoch.get(sandbox_id, 0) != expected_epoch`。进入 `self._lock` 边界，退出由对应 context manager 处理。

执行 `self._sandbox_infos.pop(sandbox_id, None)`；执行 `self._active_sandbox_identity.pop(sandbox_id, None)`。执行 `sandbox.close()`。捕获 `Exception as e` 的边界见源码，分支返回 `None`，提前返回结束该路径。

源码依据：[_forget_lost_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1465-L1525)


### 5.15.21 AioSandboxProvider._cleanup_idle_sandboxes

参数：`idle_timeout`。

以 `time.monotonic()` 计算 `current_time`；以 `[]` 计算 `active_to_destroy`；在 `资源/同步边界 self._lock` → `循环 for sandbox_id, last_activity in self._last_activity.items()` 分支中，以 `current_time - last_activity` 计算 `idle_duration`；在 `资源/同步边界 self._lock` → `循环 for sandbox_id, last_activity in self._last_activity.items()` → `条件 idle_duration > idle_timeout` 分支中，执行 `active_to_destroy.append(sandbox_id)`；建立局部回调 `def still_idle(sandbox_id: str) -> bool`，行为在独立条目展开；在 `循环 for sandbox_id in active_to_destroy` → `try` 分支中，执行 `self._destroy_tracked(sandbox_id, still_reapable=lambda sid=sandbox_id: still_idle(sid))`；执行 `self._reap_expired_warm(idle_timeout)`。

源码依据：[_cleanup_idle_sandboxes 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1526-L1567)


### 5.15.22 辅助函数与局部契约

- **`AioSandboxProvider._cleanup_idle_sandboxes.still_idle`**（`sandbox_id`）：父函数内定义的回调。以 `self._last_activity.get(sandbox_id)` 计算 `last_activity`；在 `条件 last_activity is None` 分支中，返回 `False`；在 `条件 (time.monotonic() - last_activity) < idle_timeout` 分支中，返回 `False`；返回 `True`。

  源码依据：[still_idle 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1547-L1558)


- **`AioSandboxProvider._reap_expired_warm`**（`idle_timeout=None`）：以 `float(self._config.get("idle_timeout", DEFAULT_IDLE_TIMEOUT) if idle_timeout is None else idle_timeout)` 计算 `timeout`；在 `条件 timeout <= 0` 分支中，返回 None，提前结束当前路径；以 `time.monotonic()` 计算 `now`；以 `[]` 计算 `expired: list[tuple[str, SandboxInfo]]`；在 `资源/同步边界 self._lock` → `循环 for sandbox_id, (entry, timestamp) in self._warm_pool.items()` → `条件 now - timestamp > timeout` 分支中，执行 `expired.append((sandbox_id, entry))`；在 `循环 for sandbox_id, entry in expired` 分支中，执行 `self._destroy_warm_entry(sandbox_id, entry, reason="idle_timeout", still_reapable=lambda sid=sandbox_id: sid in self._warm_pool)`。

  源码依据：[_reap_expired_warm 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1568-L1588)


- **`AioSandboxProvider._evict_oldest_warm`**（无显式业务参数）：在 `资源/同步边界 self._lock` → `条件 not self._warm_pool` 分支中，返回 `None`；在 `资源/同步边界 self._lock` 分支中，以 `[(sandbox_id, entry) for sandbox_id, (entry, _) in sorted(self._warm_pool.items(), key=lambda item: item[1][1])]` 计算 `candidates`；在 `循环 for sandbox_id, entry in candidates` → 「表达式摘要 1」 分支中，进入下一轮循环；在 `循环 for sandbox_id, entry in candidates` 分支中，返回 `sandbox_id`；返回 `None`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 not self._destroy_warm_entry(sandbox_id, entry, reason="replica_enforcement",
     still_reapable=lambda sid=sandbox_id: sid in self._warm_pool)
  ```


  源码依据：[_evict_oldest_warm 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1589-L1611)


### 5.15.23 AioSandboxProvider._register_signal_handlers

无显式业务参数。

以 `signal.getsignal(signal.SIGTERM)` 计算 `self._original_sigterm`；以 `signal.getsignal(signal.SIGINT)` 计算 `self._original_sigint`；以 `signal.getsignal(signal.SIGHUP) if hasattr(signal, "SIGHUP") else None` 计算 `self._original_sighup`；以 `[] if host_owns_sandbox_shutdown() else [signal.SIGTERM, signal.SIGINT]` 计算 `signums`；在 `条件 hasattr(signal, "SIGHUP")` 分支中，执行 `signums.append(signal.SIGHUP)`；建立局部回调 `def signal_handler(signum, frame)`，行为在独立条目展开；在 `try` → `循环 for signum in signums` 分支中，执行 `signal.signal(signum, signal_handler)`。

源码依据：[_register_signal_handlers 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1612-L1653)


### 5.15.24 辅助函数与局部契约

- **`AioSandboxProvider._register_signal_handlers.signal_handler`**（`signum, frame`）：父函数内定义的回调。在 `try` 分支中，执行 `self.shutdown()`；在 `条件 signum == signal.SIGTERM` 分支中，以 `self._original_sigterm` 计算 `original`；在 `条件 hasattr(signal, "SIGHUP") and signum == signal.SIGHUP` 分支中，以 `self._original_sighup` 计算 `original`；在 `同层否则分支` 分支中，以 `self._original_sigint` 计算 `original`；在 `条件 callable(original)` 分支中，执行 `original(signum, frame)`；在 `条件 original == signal.SIG_DFL` 分支中，执行 `signal.signal(signum, signal.SIG_DFL)`；在 `条件 original == signal.SIG_DFL` 分支中，执行 `signal.raise_signal(signum)`。

  源码依据：[signal_handler 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1629-L1645)


- **`AioSandboxProvider._sandbox_id_for_thread`**（`thread_id, user_id`）：在 `条件 not thread_id` 分支中，返回 `str(uuid.uuid4())[:8]`；以 `self._effective_acquire_user_id(user_id)` 计算 `effective_user_id`；以 `self._configured_skills_container_path()` 计算 `skills_container_path`；在 `条件 self._thread_skill_projection_active(thread_id, effective_user_id)` 分支中，返回 `self._policy_scoped_sandbox_id( thread_id, effective_user_id, skills_container_path, )`；在 `条件 skills_container_path != DEFAULT_SKILLS_CONTAINER_PATH` 分支中，返回 `self._custom_root_sandbox_id( thread_id, effective_user_id, skills_container_path, )`；返回 `self._deterministic_sandbox_id(thread_id, effective_user_id)`。

  源码依据：[_sandbox_id_for_thread 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1654-L1676)


### 5.15.25 AioSandboxProvider._reuse_in_process_sandbox

参数：`thread_id, user_id=None, post_lock=False`。

以 `self._effective_acquire_user_id(user_id)` 计算 `effective_user_id`；以 `self._thread_sandboxes[key]` 计算 `existing_id`。关键判定为 `thread_id is None`、`key not in self._thread_sandboxes`、`root_scoped_identity and existing_id != expected_id`。进入 `self._lock` 边界，退出由对应 context manager 处理。

以 `time.monotonic()` 计算 `self._last_activity[existing_id]`。执行 `self._thread_sandboxes.pop(key, None)`。捕获 `SandboxBeingDestroyedError` 的边界见源码，显式抛出路径涉及 `SandboxPolicyReplacementDeferredError`。分支返回 `None`、`existing_id`，提前返回结束该路径。

源码依据：[_reuse_in_process_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1677-L1804)


### 5.15.26 辅助函数与局部契约

- **`AioSandboxProvider._recycle_fenced_tracked_sandbox`**（`sandbox_id`）：以 `self._sandboxes.get(sandbox_id)` 计算 `sandbox`。关键判定为 `sandbox is None`、`getattr(sandbox, "requires_container_recycle", False) is True`、`info is None`，相应动作只在对应分支执行。进入 `self._lock` 边界，退出由对应 context manager 处理。捕获 `Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `RuntimeError`。分支返回 `True`、`self._destroy_tracked(sandbox_id, still_reapable=lambda: self._sandboxes.get(sandbox_id) is sandbox)`、`False`，提前返回结束该路径。

  源码依据：[_recycle_fenced_tracked_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1805-L1837)


### 5.15.27 AioSandboxProvider._reclaim_warm_pool_sandbox

参数：`thread_id, sandbox_id, user_id=None, post_lock=False`。

以 `self._effective_acquire_user_id(user_id)` 计算 `effective_user_id`。关键判定为 `thread_id is None`、`sandbox_id not in self._warm_pool`、`self._being_torn_down_locally(sandbox_id)`。进入 `self._lock` 边界，退出由对应 context manager 处理。

以 `sandbox` 计算 `self._sandboxes[sandbox_id]`；以 `info` 计算 `self._sandbox_infos[sandbox_id]`；其他写入目标为 `self._active_sandbox_identity[sandbox_id]`、`self._last_activity[sandbox_id]`、`self._thread_sandboxes[key]`。执行 `self._warm_pool.pop(sandbox_id)`；执行 `self._warm_pool_identity.pop(sandbox_id, None)`。

捕获 `SandboxBeingDestroyedError` 的边界见源码，显式抛出路径涉及 `SandboxPolicyReplacementDeferredError`。分支返回 `None`、`sandbox_id`，提前返回结束该路径。

源码依据：[_reclaim_warm_pool_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1838-L1937)


### 5.15.28 辅助函数与局部契约

- **`AioSandboxProvider._recheck_cached_sandbox`**（`thread_id, sandbox_id, user_id`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  self._reuse_in_process_sandbox(thread_id, user_id=user_id, post_lock=True) or self._reclaim_warm_pool_sandbox(
    thread_id, sandbox_id, user_id=user_id, post_lock=True, )
  ```


  源码依据：[_recheck_cached_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1938-L1946)


### 5.15.29 AioSandboxProvider._register_discovered_sandbox

参数：`thread_id, info, user_id`。

以 `self._thread_key(thread_id, user_id)` 计算 `key`。关键判定为 `info.requires_replacement`、`self._being_torn_down_locally(info.sandbox_id)`、`info.requires_replacement`。进入 `self._lock` 边界，退出由对应 context manager 处理。

以 `sandbox` 计算 `self._sandboxes[info.sandbox_id]`；以 `info` 计算 `self._sandbox_infos[info.sandbox_id]`；其他写入目标为 `self._active_sandbox_identity[info.sandbox_id]`、`self._last_activity[info.sandbox_id]`、`self._thread_sandboxes[key]`。

执行 `self._warm_pool.pop(info.sandbox_id, None)`；执行 `self._warm_pool_identity.pop(info.sandbox_id, None)`。执行 `sandbox.close()`。捕获 `Exception`、`Exception as e` 的边界见源码，显式抛出路径涉及 `SandboxPolicyReplacementDeferredError`、`SandboxBeingDestroyedError`、`当前异常`。分支返回 `info.sandbox_id`，提前返回结束该路径。

源码依据：[_register_discovered_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L1947-L2024)


### 5.15.30 AioSandboxProvider._register_created_sandbox

参数：`thread_id, sandbox_id, info, user_id=None`。

以 `None` 计算 `sandbox: AioSandbox | None`。关键判定为 `info.requires_replacement`、`key is not None`、`key is not None`。进入 `self._lock` 边界，退出由对应 context manager 处理。

以 `sandbox` 计算 `self._sandboxes[sandbox_id]`；以 `info` 计算 `self._sandbox_infos[sandbox_id]`；其他写入目标为 `self._active_sandbox_identity[sandbox_id]`、`self._last_activity[sandbox_id]`、`self._thread_sandboxes[key]`。执行 `self._warm_pool.pop(sandbox_id, None)`；执行 `self._warm_pool_identity.pop(sandbox_id, None)`。

执行 `sandbox.close()`。捕获 `Exception`、`Exception as e` 的边界见源码，显式抛出路径涉及 `SandboxPolicyReplacementDeferredError`、`当前异常`。分支返回 `sandbox_id`，提前返回结束该路径。

源码依据：[_register_created_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2025-L2081)


### 5.15.31 辅助函数与局部契约

- **`AioSandboxProvider._check_tracked_sandbox_alive`**（`sandbox_id, info`）：在 `try` 分支中，返回 `self._backend.is_alive(info)`；在 `捕获 Exception as e` 分支中，返回 `None`。

  源码依据：[_check_tracked_sandbox_alive 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2082-L2089)


- **`AioSandboxProvider._remove_tracked_sandbox`**（`sandbox_id, expected_info=None`）：以 `[]` 计算 `thread_keys_to_remove: list[tuple[str, str]]`。关键判定为 `expected_info is not None and active_info is not expected_info and warm_info is not expected_info`、`info is None and sandbox_id in self._warm_pool`，相应动作只在对应分支执行。进入 `self._lock` 边界，退出由对应 context manager 处理。执行 `self._active_sandbox_identity.pop(sandbox_id, None)`；执行 `self._last_activity.pop(sandbox_id, None)`。分支返回 `None, None, False`、`sandbox, info, True`，提前返回结束该路径。

  源码依据：[_remove_tracked_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2090-L2127)


- **`AioSandboxProvider._drop_unhealthy_sandbox`**（`sandbox_id, reason, expected_info=None`）：在 `条件 not self._reserve_local_teardown(sandbox_id, lambda: True)` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 `self._drop_unhealthy_reserved(sandbox_id, reason, expected_info=expected_info)`；在 `finally 收尾` 分支中，执行 `self._finish_local_teardown(sandbox_id)`。

  源码依据：[_drop_unhealthy_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2128-L2141)


- **`AioSandboxProvider._drop_unhealthy_reserved`**（`sandbox_id, reason, expected_info=None`）：以 `self._remove_tracked_sandbox(sandbox_id, expected_info=expected_info)` 计算 `sandbox, info, removed`；在 `条件 not removed` 分支中，返回 None，提前结束当前路径；在 `条件 sandbox is not None` → `try` 分支中，执行 `sandbox.close()`；在 `条件 info is not None` → `条件 self._claim_ownership(sandbox_id, for_destroy=True)` → `try` → `资源/同步边界 self._held_teardown_lease(sandbox_id)` 分支中，执行 `self._backend.destroy(info)`。

  源码依据：[_drop_unhealthy_reserved 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2142-L2174)


- **`AioSandboxProvider._active_count_locked`**（无显式业务参数）：返回 `len(self._sandboxes)`。

  源码依据：[_active_count_locked 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2175-L2178)


### 5.15.32 AioSandboxProvider._destroy_warm_entry

参数：`sandbox_id, entry, reason, still_reapable`。

warm 条目移出池后仍须取得销毁 claim、校验本地 reservation/代际，再执行 backend destroy；失败进入隔离/记账，不能当作容器已不存在。

源码依据：[_destroy_warm_entry 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2179-L2269)


### 5.15.33 辅助函数与局部契约

- **`AioSandboxProvider._raise_if_shutting_down`**（无显式业务参数）：在 `条件 not getattr(self, "_admission_closed", False)` 分支中，返回 None，提前结束当前路径；抛出 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  SandboxCapacityExceededError( "Sandbox provider is shutting down; cannot acquire sandbox",
     active=len(self._sandboxes), warm=len(self._warm_pool), replicas=int(self._config.get("replicas",
         DEFAULT_REPLICAS)), retry_after_seconds=30.0, reason="shutdown",
     )
  ```


  源码依据：[_raise_if_shutting_down 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2270-L2282)


- **`AioSandboxProvider.acquire`**（`thread_id=None, user_id=None`）：执行 `self._raise_if_shutting_down()`；以 `self._effective_acquire_user_id(user_id)` 计算 `effective_user_id`；在 `条件 thread_id` → `资源/同步边界 self._acquire_serializer.hold(self._thread_key(thread_id, effective_user_id))` 分支中，返回 `self._acquire_internal(thread_id, user_id=effective_user_id)`；返回 `self._acquire_internal(thread_id, user_id=effective_user_id)`。

  源码依据：[acquire 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2283-L2304)


- **`AioSandboxProvider.acquire_async`**（`thread_id=None, user_id=None`）：执行 `self._raise_if_shutting_down()`；以 `self._effective_acquire_user_id(user_id)` 计算 `effective_user_id`；在 `条件 thread_id` → `资源/同步边界 self._acquire_serializer.hold_async(self._thread_key(thread_id, effective_user_id))` 分支中，返回 `await self._acquire_internal_async(thread_id, user_id=effective_user_id)`；返回 `await self._acquire_internal_async(thread_id, user_id=effective_user_id)`。

  源码依据：[acquire_async 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2305-L2318)


- **`AioSandboxProvider._acquire_internal`**（`thread_id, user_id`）：在当前 scope 的 AcquireSerializer 内依次尝试 active、warm、发现/创建；策略和身份校验先于复用，创建前优先回收 warm 但 replicas 只作软目标。

  源码依据：[_acquire_internal 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2319-L2354)


### 5.15.34 AioSandboxProvider._acquire_internal_async

参数：`thread_id, user_id`。

遵循相同池与身份规则，通过异步 serializer/文件锁和 async backend 等待；已开始 provider 工作的取消仍须排空，不能提前释放创建 fence。

源码依据：[_acquire_internal_async 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2355-L2398)


### 5.15.35 辅助函数与局部契约

- **`AioSandboxProvider._discover_or_create_with_lock`**（`thread_id, sandbox_id, user_id=None`）：以 `get_paths()` 计算 `paths`；以 `self._recheck_cached_sandbox(thread_id, sandbox_id, user_id=effective_user_id)` 计算 `cached_id`。关键判定为 `cached_id is not None`、`discovered is not None`、`discovered.requires_replacement`，相应动作只在对应分支执行。进入 `open(lock_path, "a", encoding="utf-8") as lock_file` 边界，退出由对应 context manager 处理。显式抛出路径涉及 `SandboxPolicyReplacementDeferredError`。分支返回 `cached_id`、`self._register_discovered_sandbox(thread_id, discovered, user_id=effective_user_id)`、`self._create_sandbox(thread_id, sandbox_id, user_id=effective_user_id)`，提前返回结束该路径。finally 包含 执行 `_unlock_file(lock_file)`。

  源码依据：[_discover_or_create_with_lock 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2399-L2435)


### 5.15.36 AioSandboxProvider._discover_or_create_with_lock_async

参数：`thread_id, sandbox_id, user_id=None`。

以 `get_paths()` 计算 `paths`；以 `await run_sync_lifecycle_operation(self._recheck_cached_sandbox, thread_id, sandbox_id, user_id=effective_user_id)` 计算 `cached_id`。关键判定为 `not locked`、`cached_id is not None`、`discovered is not None`。

等待 `asyncio.to_thread(paths.ensure_thread_dirs, thread_id, user_id=effective_user_id)` 完成；等待 `asyncio.sleep(0.02)` 完成。显式抛出路径涉及 `SandboxPolicyReplacementDeferredError`。

分支返回 `cached_id`、`await run_sync_lifecycle_operation(self._register_discovered_sandbox, thread_id, discovered, user_id=effective_user_id)`、`await await_drained(self._create_sandbox_async(thread_id, sandbox_id, user_id=effective_user_id))`，提前返回结束该路径。

finally 包含 等待 `run_sync_lifecycle_operation(_unlock_file, lock_file)` 完成；执行 `finally:`。

源码依据：[_discover_or_create_with_lock_async 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2436-L2494)


### 5.15.37 辅助函数与局部契约

- **`AioSandboxProvider._discover_or_create_with_lock_async._lock_path`**（无显式业务参数）：父函数内定义的回调。返回 `paths.thread_dir(thread_id, user_id=effective_user_id) / f"{sandbox_id}.lock"`。

  源码依据：[_lock_path 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2442-L2447)


### 5.15.38 AioSandboxProvider._destroy_unready_sandbox

参数：`sandbox_id, info`。

在 「表达式摘要 1」 分支中，返回 None，提前结束当前路径；在 `try` → `条件 not self._claim_ownership(sandbox_id, for_destroy=True)` 分支中，返回 None，提前结束当前路径；在 `try` → `try` → `资源/同步边界 self._held_teardown_lease(sandbox_id)` 分支中，执行 `self._backend.destroy(info)`；在 `finally 收尾` 分支中，执行 `self._finish_local_teardown(sandbox_id)`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
条件 not self._reserve_local_teardown( sandbox_id, lambda: sandbox_id not in self._sandboxes and sandbox_id not
  in self._sandbox_infos and sandbox_id not in self._warm_pool, )
```


源码依据：[_destroy_unready_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2495-L2549)


### 5.15.39 辅助函数与局部契约

- **`AioSandboxProvider._retire_absent_quarantine`**（`sandbox_id`）：以 `SandboxInfo(sandbox_id, "")` 计算 `info`。关键判定为 `not store.contains(info)`、「表达式摘要 1」、`not self._claim_ownership(sandbox_id, for_destroy=True)`，相应动作只在对应分支执行。进入 `self._held_teardown_lease(sandbox_id)` 边界，退出由对应 context manager 处理。显式抛出路径涉及 `SandboxPolicyReplacementDeferredError`。分支返回 `None`，提前返回结束该路径。finally 包含 执行 `self._finish_local_teardown(sandbox_id)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  not self._reserve_local_teardown( sandbox_id, lambda: sandbox_id not in self._sandboxes and sandbox_id not in
    self._sandbox_infos and sandbox_id not in self._warm_pool, )
  ```


  源码依据：[_retire_absent_quarantine 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2550-L2574)


- **`AioSandboxProvider._remember_failed_teardown`**（`sandbox_id, info`）：在 `资源/同步边界 self._lock` 分支中，以 `getattr(self, "_cleanup_retry_ids", set()) | {sandbox_id}` 计算 `self._cleanup_retry_ids`；以 `True` 计算 `info.requires_replacement`；执行 `self._quarantine_store().mark(info)`。

  源码依据：[_remember_failed_teardown 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2575-L2581)


### 5.15.40 AioSandboxProvider._complete_quarantined_teardown

参数：`sandbox_id, store, retire_replaced`。

以 `getattr(self._backend, "inspect_runtime", None)` 计算 `inspect_runtime`。关键判定为 `self._backend.is_absent(sandbox_id) is not True`、`not callable(inspect_runtime)`、`current is not None`。进入 `self._lock` 边界，退出由对应 context manager 处理。

执行 `getattr(self, "_cleanup_retry_ids", set()).discard(sandbox_id)`；执行 `getattr(self, "_cleanup_retry_ids", set()).discard(sandbox_id)`。显式抛出路径涉及 `RuntimeError`、`SandboxPolicyReplacementDeferredError`。分支返回 `False`、`True`，提前返回结束该路径。

源码依据：[_complete_quarantined_teardown 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2582-L2622)


### 5.15.41 AioSandboxProvider._create_sandbox

参数：`thread_id, sandbox_id, user_id=None`。

以 `self._effective_acquire_user_id(user_id)` 计算 `effective_user_id`。关键判定为 `total >= replicas`、`config_mount_exclusion_root is not None`、`isinstance(self._backend, RemoteSandboxBackend)`。

以 `config_mount_exclusion_root` 计算 `create_kwargs["config_mount_exclusion_root"]`；以 `self._configured_skills_container_path()` 计算 `create_kwargs["skills_container_path"]`。捕获 `SandboxCreationError as exc` 的边界见源码，显式抛出路径涉及 `当前异常`、`RuntimeError`。

分支返回 `self._register_created_sandbox(thread_id, sandbox_id, info, user_id=effective_user_id)`，提前返回结束该路径。

源码依据：[_create_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2623-L2683)


### 5.15.42 AioSandboxProvider._create_sandbox_async

参数：`thread_id, sandbox_id, user_id=None`。

以 `self._effective_acquire_user_id(user_id)` 计算 `effective_user_id`。关键判定为 `total >= replicas`、`config_mount_exclusion_root is not None`、`isinstance(self._backend, RemoteSandboxBackend)`。

以 `config_mount_exclusion_root` 计算 `create_kwargs["config_mount_exclusion_root"]`；以 `self._configured_skills_container_path()` 计算 `create_kwargs["skills_container_path"]`。

等待 `run_sync_lifecycle_operation(self._retire_absent_quarantine, sandbox_id)` 完成；等待 `run_sync_lifecycle_operation(self._destroy_unready_sandbox, sandbox_id, exc.info)` 完成。捕获 `SandboxCreationError as exc` 的边界见源码，显式抛出路径涉及 `当前异常`、`RuntimeError`。

分支返回 `await run_sync_lifecycle_operation(self._register_created_sandbox, thread_id, sandbox_id, info, user_id=effective_user_id)`，提前返回结束该路径。

源码依据：[_create_sandbox_async 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2684-L2741)


### 5.15.43 辅助函数与局部契约

- **`AioSandboxProvider.get`**（`sandbox_id`）：在 `资源/同步边界 self._lock` → `条件 sandbox_id in getattr(self, "_cleanup_retry_ids", set())` 分支中，返回 `None`；在 `资源/同步边界 self._lock` 分支中，以 `self._sandboxes.get(sandbox_id)` 计算 `sandbox`；在 `资源/同步边界 self._lock` → `条件 sandbox is not None` 分支中，以 `time.monotonic()` 计算 `self._last_activity[sandbox_id]`；返回 `sandbox`。

  源码依据：[get 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2742-L2765)


- **`AioSandboxProvider.get_scoped`**（`sandbox_id, thread_id, user_id`）：以 `self._thread_key(thread_id, user_id)` 计算 `key`；在 `资源/同步边界 self._lock` → `条件 sandbox_id in getattr(self, "_cleanup_retry_ids", set())` 分支中，返回 `None`；在 `资源/同步边界 self._lock` → `条件 self._thread_sandboxes.get(key) != sandbox_id` 分支中，返回 `None`；在 `资源/同步边界 self._lock` → `条件 self._active_sandbox_identity.get(sandbox_id) != key` 分支中，返回 `None`；在 `资源/同步边界 self._lock` 分支中，以 `self._sandboxes.get(sandbox_id)` 计算 `sandbox`；在 `资源/同步边界 self._lock` → `条件 sandbox is not None` 分支中，以 `time.monotonic()` 计算 `self._last_activity[sandbox_id]`；在 `资源/同步边界 self._lock` 分支中，返回 `sandbox`。

  源码依据：[get_scoped 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2766-L2786)


- **`AioSandboxProvider.release`**（`sandbox_id`）：协调本地引用/池转移，释放只是使用权归还，未使用容器可进 warm；回收错误按既有 best-effort 路径记录，不反向修改已完成 run。

  源码依据：[release 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2787-L2806)


### 5.15.44 AioSandboxProvider._release_idle_sandbox

参数：`sandbox_id`。

以 `self._sandboxes.get(sandbox_id)` 计算 `recycle_sandbox`。关键判定为 「表达式摘要 1」、`info and sandbox_id not in self._warm_pool`、`sandbox is not None`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
recycle_sandbox is not None and (getattr(recycle_sandbox, "has_pending_session_creates",
     False) is True or recycle_sandbox.requires_container_recycle)
```


进入 `self._lock` 边界，退出由对应 context manager 处理。以 `(info, time.monotonic())` 计算 `self._warm_pool[sandbox_id]`；以 `thread_keys_to_remove[0] if thread_keys_to_remove else active_identity` 计算 `self._warm_pool_identity[sandbox_id]`。执行 `self._last_activity.pop(sandbox_id, None)`。执行 `sandbox.close()`。

捕获 `Exception`、`Exception as e` 的边界见源码，分支返回 `None`，提前返回结束该路径。

源码依据：[_release_idle_sandbox 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2807-L2875)


### 5.15.45 辅助函数与局部契约

- **`AioSandboxProvider.destroy`**（`sandbox_id`）：返回 `self._destroy_tracked(sandbox_id, still_reapable=lambda: True)`。

  源码依据：[destroy 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2876-L2896)


- **`AioSandboxProvider._destroy_tracked`**（`sandbox_id, still_reapable, force=False`）：以 `contextlib.nullcontext(True) if force else get_sandbox_lease_manager(self).reserve_idle_teardown(sandbox_id)` 计算 `reservation`；在 `资源/同步边界 reservation as reserved` → `条件 not reserved` 分支中，返回 `False`；在 `资源/同步边界 reservation as reserved` → `条件 not self._reserve_local_teardown(sandbox_id, still_reapable)` 分支中，返回 `False`；在 `资源/同步边界 reservation as reserved` → `try` 分支中，返回 `self._destroy_reserved(sandbox_id)`；在 `资源/同步边界 reservation as reserved` → `finally 收尾` 分支中，执行 `self._finish_local_teardown(sandbox_id)`。

  源码依据：[_destroy_tracked 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2897-L2917)


### 5.15.46 AioSandboxProvider._destroy_reserved

参数：`sandbox_id`。

以 `self._sandboxes.get(sandbox_id)` 计算 `sandbox`。

关键判定为 `info is None and (warm := self._warm_pool.get(sandbox_id)) is not None`、「表达式摘要 1」、`info is None`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
getattr(sandbox, "has_pending_session_creates", False) is True or getattr(sandbox,
   "requires_container_recycle", False) is True or (retry_cleanup and info is not None)
```


进入 `self._lock`、`self._held_teardown_lease(sandbox_id)` 边界，退出由对应 context manager 处理。执行 `sandbox.close()`；执行 `sandbox.close()`。捕获 `Exception as e`、`Exception` 的边界见源码，显式抛出路径涉及 `RuntimeError`、`当前异常`。分支返回 `False`、`self._complete_quarantined_teardown(sandbox_id, store, retire_replaced=False)`、`True`，提前返回结束该路径。

源码依据：[_destroy_reserved 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2918-L2992)


### 5.15.47 辅助函数与局部契约

- **`AioSandboxProvider.reset`**（无显式业务参数）：执行 `discard_sandbox_lease_manager(self)`；执行 `self._acquire_serializer.close()`；执行 `self._acquire_worker_executor.shutdown(wait=False, cancel_futures=True)`。

  源码依据：[reset 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2993-L2998)


### 5.15.48 AioSandboxProvider.shutdown

无显式业务参数。

以 `getattr(self, "_cleanup_retry_ids", set())` 计算 `pending`。关键判定为 `self._shutdown_called`、`not destroyed`、`not destroyed`。进入 `self._lock` 边界，退出由对应 context manager 处理。以 `True` 计算 `self._admission_closed`；以 `True` 计算 `self._shutdown_called`；其他写入目标为 `self._shutdown_called`、`self._cleanup_retry_ids`。

执行 `failed.add(sandbox_id)`；执行 `failed.add(sandbox_id)`。执行 `self._ownership.close()`；执行 `self._acquire_serializer.close()`。捕获 `Exception`、`Exception as e`、`OwnershipBackendError` 的边界见源码，显式抛出路径涉及 `当前异常`、`RuntimeError`。分支返回 `None`，提前返回结束该路径。

源码依据：[shutdown 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2999-L3084)


## 5.16 容器所有权契约

源码位置：`backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py`。

只声明销毁所有权协议和续约三态，不在抽象文件实施 Redis 或线程同步。own:/del: 两类值使正常使用与正在销毁能被区分；协议本身不授予用户访问容器的权限。

### 5.16.1 辅助函数与局部契约

- **`SandboxOwnershipStore.owner_id`**（无显式业务参数）：抽象属性：返回本实例所有权标识，调用者用它解释 lease；这里没有存储实现。

  源码依据：[owner_id 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L85-L87)


- **`SandboxOwnershipStore.take`**（`sandbox_id`）：抽象获取协议：发布自己的普通拥有权，可取代 peer own:，但不得覆盖 del: 销毁标记；返回是否取得，具体原子性由实现提供。

  源码依据：[take 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L89-L109)


### 5.16.2 SandboxOwnershipStore.claim

参数：`sandbox_id, for_destroy=False`。

抽象销毁/接管协议：只在空缺或同 owner 时成功；for_destroy 转为 del:，阻止他人 take；不是无条件 SET。

源码依据：[claim 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L111-L151)


### 5.16.3 辅助函数与局部契约

- **`SandboxOwnershipStore.renew`**（`sandbox_id`）：抽象续约协议：返回 RENEWED/LAPSED/LOST，分别区分续约、已过期和被别的 owner 持有，调用者必须分别处理。

  源码依据：[renew 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L153-L163)


- **`SandboxOwnershipStore.release`**（`sandbox_id`）：抽象释放协议：只删除属于当前实例的 lease，不能移除 peer 的新 token；容器销毁由 provider/backend 负责。

  源码依据：[release 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L165-L174)


- **`SandboxOwnershipStore.owner`**（`sandbox_id`）：抽象只读查询：返回当前拥有者或空值，用于生命周期判断；查询后资源可能变化，不能替代原子 claim。

  源码依据：[owner 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L176-L186)


- **`SandboxOwnershipStore.close`**（无显式业务参数）：抽象资源关闭：释放 store 客户端，具体内存/Redis 实现不同，不代表销毁所有容器。

  源码依据：[close 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L187-L189)


## 5.17 进程内容器所有权

源码位置：`backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py`。

内存实现用线程锁和到期时间模拟同样协议，适用于进程内协调；与 Redis 共享名称相同并不意味着跨 Gateway 共享存储。

### 5.17.1 辅助函数与局部契约

- **`MemoryOwnershipStore.__init__`**（`owner_id, ttl_seconds, time_source=time.monotonic`）：以 `owner_id` 计算 `self._owner_id`；以 `float(ttl_seconds)` 计算 `self._ttl`；以 `time_source` 计算 `self._now`；以 `{}` 计算 `self._leases: dict[str, _Lease]`；以 `threading.Lock()` 计算 `self._lock`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L35-L43)


- **`MemoryOwnershipStore.owner_id`**（无显式业务参数）：返回 `self._owner_id`。

  源码依据：[owner_id 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L45-L47)


- **`MemoryOwnershipStore._live_lease_locked`**（`sandbox_id`）：以 `self._leases.get(sandbox_id)` 计算 `lease`；在 `条件 lease is None` 分支中，返回 `None`；在 `条件 self._now() >= lease.expires_at` 分支中，删除 `self._leases[sandbox_id]`；在 `条件 self._now() >= lease.expires_at` 分支中，返回 `None`；返回 `lease`。

  源码依据：[_live_lease_locked 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L48-L56)


- **`MemoryOwnershipStore._write_locked`**（`sandbox_id, destroying`）：以 `_Lease(owner_id=self._owner_id, expires_at=self._now() + self._ttl, destroying=destroying)` 计算 `self._leases[sandbox_id]`。

  源码依据：[_write_locked 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L57-L59)


- **`MemoryOwnershipStore.take`**（`sandbox_id`）：在 `资源/同步边界 self._lock` 分支中，以 `self._live_lease_locked(sandbox_id)` 计算 `lease`；在 `资源/同步边界 self._lock` → `条件 lease is not None and lease.destroying` 分支中，返回 `False`；在 `资源/同步边界 self._lock` 分支中，执行 `self._write_locked(sandbox_id, destroying=False)`；在 `资源/同步边界 self._lock` 分支中，返回 `True`。

  源码依据：[take 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L60-L69)


- **`MemoryOwnershipStore.claim`**（`sandbox_id, for_destroy=False`）：在 `资源/同步边界 self._lock` 分支中，以 `self._live_lease_locked(sandbox_id)` 计算 `lease`；在 `资源/同步边界 self._lock` → `条件 lease is not None and lease.owner_id != self._owner_id` 分支中，返回 `False`；在 `资源/同步边界 self._lock` → `条件 not for_destroy and lease is not None and lease.destroying` 分支中，返回 `False`；在 `资源/同步边界 self._lock` 分支中，执行 `self._write_locked(sandbox_id, destroying=for_destroy)`；在 `资源/同步边界 self._lock` 分支中，返回 `True`。

  源码依据：[claim 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L70-L82)


- **`MemoryOwnershipStore.renew`**（`sandbox_id`）：在 `资源/同步边界 self._lock` 分支中，以 `self._live_lease_locked(sandbox_id)` 计算 `lease`；在 `资源/同步边界 self._lock` → `条件 lease is None` 分支中，返回 `RenewOutcome.LAPSED`；在 `资源/同步边界 self._lock` → `条件 lease.owner_id != self._owner_id or lease.destroying` 分支中，返回 `RenewOutcome.LOST`；在 `资源/同步边界 self._lock` 分支中，执行 `self._write_locked(sandbox_id, destroying=False)`；在 `资源/同步边界 self._lock` 分支中，返回 `RenewOutcome.RENEWED`。

  源码依据：[renew 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L83-L92)


- **`MemoryOwnershipStore.release`**（`sandbox_id`）：在 `资源/同步边界 self._lock` 分支中，以 `self._live_lease_locked(sandbox_id)` 计算 `lease`；在 `资源/同步边界 self._lock` → `条件 lease is not None and lease.owner_id == self._owner_id` 分支中，删除 `self._leases[sandbox_id]`。

  源码依据：[release 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L93-L98)


- **`MemoryOwnershipStore.owner`**（`sandbox_id`）：在 `资源/同步边界 self._lock` 分支中，以 `self._live_lease_locked(sandbox_id)` 计算 `lease`；在 `资源/同步边界 self._lock` 分支中，返回 `None if lease is None else lease.owner_id`。

  源码依据：[owner 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L99-L103)


- **`MemoryOwnershipStore.close`**（无显式业务参数）：在 `资源/同步边界 self._lock` 分支中，执行 `self._leases.clear()`。

  源码依据：[close 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/memory.py#L104-L107)


## 5.18 跨进程容器所有权

源码位置：`backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py`。

文件顶层 Lua 脚本是实际原子判定点，Python 方法传 key/token/TTL 并解释返回值。比较后删除、续约和销毁 claim 必须在 Redis 内完成，不能由两次独立 GET/SET 代替。

### 5.18.1 辅助函数与局部契约

- **`RedisOwnershipStore.__init__`**（`owner_id, redis_url, ttl_seconds, key_prefix="deerflow:sandbox:owner", client=None`）：以 `owner_id` 计算 `self._owner_id`；以 `max(1, math.ceil(float(ttl_seconds) * 1000))` 计算 `self._ttl_ms`；其他写入目标为 `self._key_prefix`、`self._redis`、`self._owns_client`、`self._take`、`self._claim`、`self._renew`、`self._release`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L126-L159)


- **`RedisOwnershipStore.owner_id`**（无显式业务参数）：返回 `self._owner_id`。

  源码依据：[owner_id 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L161-L163)


- **`RedisOwnershipStore._key`**（`sandbox_id`）：返回 `f"{self._key_prefix}:{sandbox_id}"`。

  源码依据：[_key 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L164-L166)


- **`RedisOwnershipStore.take`**（`sandbox_id`）：在 `try` 分支中，以 `self._take(keys=[self._key(sandbox_id)], args=[self._owner_id, self._ttl_ms])` 计算 `result`；在 `捕获 RedisError as e` 分支中，抛出 `OwnershipBackendError(f"failed to publish sandbox ownership for {sandbox_id}: {e}") from e`；返回 `bool(result)`。

  源码依据：[take 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L167-L173)


- **`RedisOwnershipStore.claim`**（`sandbox_id, for_destroy=False`）：在 `try` 分支中，以 `self._claim(keys=[self._key(sandbox_id)], args=[self._owner_id, self._ttl_ms, "1" if for_destroy else "0"])` 计算 `result`；在 `捕获 RedisError as e` 分支中，抛出 `OwnershipBackendError(f"failed to claim sandbox ownership for {sandbox_id}: {e}") from e`；返回 `bool(result)`。

  源码依据：[claim 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L174-L180)


- **`RedisOwnershipStore.renew`**（`sandbox_id`）：在 `try` 分支中，以 `int(self._renew(keys=[self._key(sandbox_id)], args=[self._owner_id, self._ttl_ms]))` 计算 `result`；在 `捕获 RedisError as e` 分支中，抛出 `OwnershipBackendError(f"failed to renew sandbox ownership for {sandbox_id}: {e}") from e`；在 `条件 result == 1` 分支中，返回 `RenewOutcome.RENEWED`；在 `条件 result == -1` 分支中，返回 `RenewOutcome.LAPSED`；返回 `RenewOutcome.LOST`。

  源码依据：[renew 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L181-L191)


- **`RedisOwnershipStore.release`**（`sandbox_id`）：在 `try` 分支中，执行 `self._release(keys=[self._key(sandbox_id)], args=[self._owner_id])`；在 `捕获 RedisError as e` 分支中，抛出 `OwnershipBackendError(f"failed to release sandbox ownership for {sandbox_id}: {e}") from e`。

  源码依据：[release 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L192-L197)


- **`RedisOwnershipStore.owner`**（`sandbox_id`）：在 `try` 分支中，以 `self._redis.get(self._key(sandbox_id))` 计算 `value`；在 `捕获 RedisError as e` 分支中，抛出 `OwnershipBackendError(f"failed to read sandbox ownership for {sandbox_id}: {e}") from e`；在 `条件 value is None` 分支中，返回 `None`；以 `value.decode("utf-8") if isinstance(value, bytes) else value` 计算 `text`；在 `条件 text.startswith(_OWN) or text.startswith(_DEL)` 分支中，返回 `text[4:]`；返回 `text`。

  源码依据：[owner 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L198-L210)


- **`RedisOwnershipStore.close`**（无显式业务参数）：在 `条件 not self._owns_client` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 `self._redis.close()`。

  源码依据：[close 实现](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/redis.py#L211-L218)


## 5.19 记忆后端契约与选择

源码位置：`backend/packages/harness/deerflow/agents/memory/manager.py`。

MemoryManager 是可替换后端契约，默认/no-op/不支持的接口并不代表 DeerMem 已执行。工厂扫描注册后端、解析 manager_class，注入宿主 LLM、路径、trace/判断钩子；singleton 与 reload 时 judge 连接签名决定共享资源。后台 worker 依赖这里的身份及读取失败策略。

### 5.19.1 辅助函数与局部契约

- **`context_query_kwargs`**（`get_context, query`）：在 `条件 query is None` 分支中，返回 `{}`；在 `try` 分支中，以 `inspect.signature(get_context).parameters.values()` 计算 `parameters`；在 `捕获 (TypeError, ValueError)` 分支中，返回 `{}`；在 「表达式摘要 1」 分支中，返回 `{"query": query}`；返回 `{}`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 any(parameter.kind == inspect.Parameter.VAR_KEYWORD or (parameter.name == "query" and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD,
         inspect.Parameter.KEYWORD_ONLY)) for parameter in parameters)
  ```


  源码依据：[context_query_kwargs 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L56-L72)


- **`MemoryCallbacks.on_memory_llm_call`**（`invoke_config, thread_id, user_id, trace_id, model_name`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[on_memory_llm_call 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L81-L92)


- **`MemoryCallbacks.on_memory_llm_result`**（`invoke_config, prompt, response, error, duration_ms, model_name`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[on_memory_llm_result 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L93-L111)


- **`MemoryManager._coerce_backend_config`**（`value`）：返回 `value or {}`。

  源码依据：[_coerce_backend_config 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L187-L197)


- **`MemoryManager.read_failures_are_fatal_for_config`**（`backend_config`）：以 `backend_config.get("failure_policy") if isinstance(backend_config, dict) else None` 计算 `failure_policy`；返回 `isinstance(failure_policy, dict) and failure_policy.get("read") == "fail_closed"`。

  源码依据：[read_failures_are_fatal_for_config 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L213-L221)


- **`MemoryManager.read_failures_are_fatal`**（无显式业务参数）：返回 `type(self).read_failures_are_fatal_for_config(self.backend_config)`。

  源码依据：[read_failures_are_fatal 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L223-L233)


- **`MemoryManager._check_invariants`**（无显式业务参数）：以 `type(self).search is not MemoryManager.search` 计算 `search_overridden`。关键判定为 `type(self).supports_search != search_overridden`、`self.mode == "tool" and not search_overridden`，相应动作只在对应分支执行。显式抛出路径涉及 `ValueError`。分支返回 `self`，提前返回结束该路径。

  源码依据：[_check_invariants 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L235-L267)


- **`MemoryManager.add`**（`thread_id, messages, agent_name=None, user_id=None, trace_id=None`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[add 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L269-L288)


- **`MemoryManager.get_context`**（`user_id, agent_name=None, thread_id=None, query=None`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[get_context 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L290-L325)


- **`MemoryManager.add_nowait`**（`thread_id, messages, agent_name=None, user_id=None`）：执行 `self.add(thread_id, messages, agent_name=agent_name, user_id=user_id)`。

  源码依据：[add_nowait 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L326-L342)


- **`MemoryManager.search`**（`query, top_k=5, user_id=None, agent_name=None, category=None`）：抛出 `NotImplementedError(f"search not supported by {type(self).__name__}")`。

  源码依据：[search 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L343-L359)


- **`MemoryManager.get_memory`**（`user_id=None, agent_name=None`）：抛出 `NotImplementedError(f"get_memory not supported by {type(self).__name__}")`。

  源码依据：[get_memory 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L360-L368)


- **`MemoryManager.delete_memory`**（`user_id=None, agent_name=None`）：抛出 `NotImplementedError(f"delete_memory not supported by {type(self).__name__}")`。

  源码依据：[delete_memory 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L369-L378)


- **`MemoryManager.clear_memory`**（`user_id=None, agent_name=None`）：抛出 `NotImplementedError(f"clear_memory not supported by {type(self).__name__}")`。

  源码依据：[clear_memory 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L379-L393)


- **`MemoryManager.cancel_by_agent`**（`agent_name=None, user_id=None`）：返回 `0`。

  源码依据：[cancel_by_agent 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L394-L423)


- **`MemoryManager.import_memory`**（`memory_data, user_id=None, agent_name=None`）：抛出 `NotImplementedError(f"import_memory not supported by {type(self).__name__}")`。

  源码依据：[import_memory 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L424-L438)


- **`MemoryManager.export_memory`**（`user_id=None, agent_name=None`）：抛出 `NotImplementedError(f"export_memory not supported by {type(self).__name__}")`。

  源码依据：[export_memory 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L439-L448)


- **`MemoryManager.shutdown_flush`**（`timeout`）：返回 `True`。

  源码依据：[shutdown_flush 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L449-L480)


- **`MemoryManager.warm`**（无显式业务参数）：返回 `None`。

  源码依据：[warm 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L481-L496)


- **`MemoryManager.reload_memory`**（`user_id=None, agent_name=None`）：抛出 `NotImplementedError(f"reload_memory not supported by {type(self).__name__}")`。

  源码依据：[reload_memory 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L497-L507)


- **`MemoryManager.create_fact`**（`content, category="context", confidence=0.5, agent_name=None, user_id=None`）：抛出 `NotImplementedError(f"create_fact not supported by {type(self).__name__}")`。

  源码依据：[create_fact 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L508-L520)


- **`MemoryManager.delete_fact`**（`fact_id, agent_name=None, user_id=None`）：抛出 `NotImplementedError(f"delete_fact not supported by {type(self).__name__}")`。

  源码依据：[delete_fact 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L521-L530)


- **`MemoryManager.update_fact`**（`fact_id, content=None, category=None, confidence=None, agent_name=None, user_id=None`）：抛出 `NotImplementedError(f"update_fact not supported by {type(self).__name__}")`。

  源码依据：[update_fact 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L531-L548)


- **`MemoryManager.on_pre_compress`**（`messages`）：返回 `""`。

  源码依据：[on_pre_compress 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L549-L554)


- **`MemoryManager.on_turn_start`**（`turn_number, message, **kwargs`）：返回 `None`。

  源码依据：[on_turn_start 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L555-L558)


- **`MemoryManager.refresh_judge`**（`judge`）：返回 `None`。

  源码依据：[refresh_judge 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L559-L573)


- **`MemoryManager.aadd`**（`thread_id, messages, agent_name=None, user_id=None, trace_id=None`）：返回 `self.add(thread_id, messages, agent_name=agent_name, user_id=user_id, trace_id=trace_id)`。

  源码依据：[aadd 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L574-L584)


- **`MemoryManager.aget_context`**（`user_id, agent_name=None, thread_id=None, query=None`）：返回 `self.get_context(user_id, agent_name=agent_name, thread_id=thread_id, **context_query_kwargs(self.get_context, query))`。

  源码依据：[aget_context 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L585-L594)


- **`MemoryManager.asearch`**（`query, top_k=5, user_id=None, agent_name=None, category=None`）：返回 `self.search(query, top_k, user_id=user_id, agent_name=agent_name, category=category)`。

  源码依据：[asearch 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L595-L606)


- **`MemoryManager.from_config`**（`backend_config, mode="middleware", **host_hooks`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[from_config 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L609-L627)


- **`MemoryManager.close`**（无显式业务参数）：返回 `None`。

  源码依据：[close 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L628-L638)


### 5.19.2 _scan_backends

无显式业务参数。

以 `{}` 计算 `registry: dict[str, type[MemoryManager]]`；以 `registry` 计算 `_backends_cache`。关键判定为 `_backends_cache is not None`、`not _BACKENDS_DIR.is_dir()`、`not entry.is_dir() or entry.name.startswith(("_", "."))`。以 `cls` 计算 `registry[entry.name]`。

执行 `if not entry.is_dir() or entry.name.startswith(("_", ".")):`。捕获 `Exception: # noqa: BLE001 - a broken backend must not break the factory` 的边界见源码，分支返回 `_backends_cache`、`registry`，提前返回结束该路径。

源码依据：[_scan_backends 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L639-L684)


### 5.19.3 _resolve_manager_class

参数：`manager_class, allow_discovery=True`。

以 `_scan_backends() if allow_discovery else (_backends_cache or {})` 计算 `registry`；以 `vars(module).get(attr) if module is not None else None` 计算 `cls`。关键判定为 `manager_class in registry`、`":" in manager_class`、`module_path and attr`。捕获 `ImportError as e` 的边界见源码，显式抛出路径涉及 `ValueError`。

分支返回 `registry[manager_class]`、`cls`，提前返回结束该路径。

源码依据：[_resolve_manager_class 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L685-L736)


### 5.19.4 辅助函数与局部契约

- **`backend_requires_passive_writes_in_tool_mode`**（`manager_class`）：返回 `_resolve_manager_class(manager_class).requires_passive_writes_in_tool_mode`。

  源码依据：[backend_requires_passive_writes_in_tool_mode 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L737-L759)


- **`LangfuseMemoryCallbacks.__init__`**（`extensions=None`）：在 `条件 extensions is None` 分支中，以 `get_loaded_extensions()` 计算 `extensions`；以 `extensions` 计算 `self._extensions`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L771-L777)


- **`LangfuseMemoryCallbacks.on_memory_llm_call`**（`invoke_config, thread_id, user_id, trace_id, model_name`）：执行 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  inject_langfuse_metadata( invoke_config, thread_id=thread_id, user_id=user_id,
     assistant_id="memory_agent", model_name=model_name, environment=os.environ.get("DEER_FLOW_ENV") or os.environ.get("ENVIRONMENT"),
     deerflow_trace_id=trace_id, )
  ```


  源码依据：[on_memory_llm_call 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L778-L798)


### 5.19.5 LangfuseMemoryCallbacks.on_memory_llm_result

参数：`invoke_config, prompt, response, error, duration_ms, model_name`。

以 `self._extensions` 计算 `extensions`；在 `条件 not extensions.has_system_model_observers` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 「表达式摘要 1」。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
dispatch_system_model_observation( notify_system_model_call( extensions,
     task_store_for_system_call(invoke_config), SystemOperationKind.MEMORY,
     SystemModelRequest( messages=prompt, model_name=model_name, invoke_config=invoke_config,
       ), SystemModelResult( response=response, error=error, duration_ms=duration_ms,
       ), ), SystemOperationKind.MEMORY.value, )
```


源码依据：[on_memory_llm_result 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L799-L853)


### 5.19.6 辅助函数与局部契约

- **`_host_default_should_keep_hidden_message`**（`additional_kwargs`）：返回 `read_human_input_response(additional_kwargs) is not None`。

  源码依据：[_host_default_should_keep_hidden_message 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L854-L867)


- **`_host_default_llm`**（无显式业务参数）：在 `try` 分支中，返回 `create_chat_model(name=None)`；在 `捕获 Exception` 分支中，返回 `None`。

  源码依据：[_host_default_llm 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L868-L886)


### 5.19.7 _host_default_extraction_callback

参数：`payload`。

在 `条件 not isinstance(payload, dict)` 分支中，返回 None，提前结束当前路径。

源码依据：[_host_default_extraction_callback 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L887-L977)


### 5.19.8 辅助函数与局部契约

- **`_collect_host_hooks`**（无显式业务参数）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "project_memory": prepare_project_extraction, "callbacks": LangfuseMemoryCallbacks(),
     "should_keep_hidden_message": _host_default_should_keep_hidden_message,
     "trace_context_manager": ensure_trace_context, "host_llm_factory": _host_default_llm,
     "extraction_callback": _host_default_extraction_callback, "judge": _host_default_judge(),
     }
  ```


  源码依据：[_collect_host_hooks 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L978-L1009)


- **`_host_default_judge`**（无显式业务参数）：返回 `build_memory_judge()`。

  源码依据：[_host_default_judge 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1010-L1016)


- **`_judging_config_signature`**（`cfg`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  json.dumps( { "prescreen": cfg.prescreen.model_dump(mode="json"),
       "signal_classification": cfg.signal_classification.model_dump(mode="json"),
       "typesafe": typesafe_defaults(), "effective_connections": _effective_connection_signature(cfg),
       }, sort_keys=True, )
  ```


  源码依据：[_judging_config_signature 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1017-L1040)


- **`_effective_connection_signature`**（`cfg`）：以 `typesafe_defaults()` 计算 `defaults`；以 `{}` 计算 `signature: dict[str, Any]`。以 `{"unresolved": True}` 计算 `signature[name]`；以 `None if connection is None else {**connection.public_parameters(), "credential": connection.credential_fingerprint()}` 计算 `signature[name]`。捕获 `Exception` 的边界见源码，捕获不等于已恢复。分支返回 `signature`，提前返回结束该路径。

  源码依据：[_effective_connection_signature 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1041-L1070)


- **`_refresh_judge_for_reloaded_config`**（`manager`）：使用外层变量 `global _memory_judge_signature`；以 `_judging_config_signature(get_memory_config())` 计算 `signature`；在 `条件 signature == _memory_judge_signature` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，以 `_host_default_judge()` 计算 `judge`；在 `捕获 Exception` → `资源/同步边界 _manager_lock` 分支中，以 `signature` 计算 `_memory_judge_signature`；在 `捕获 Exception` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 _manager_lock` → `条件 _judging_config_signature(get_memory_config()) != signature` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 _manager_lock` 分支中，执行 `manager.refresh_judge(judge)`；在 `资源/同步边界 _manager_lock` 分支中，以 `signature` 计算 `_memory_judge_signature`。

  源码依据：[_refresh_judge_for_reloaded_config 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1071-L1107)


- **`resolve_deermem_storage_path`**（`backend_config`）：以 `str(backend_config.get("storage_path") or "")` 计算 `storage_path`；在 `条件 not storage_path` 分支中，返回 `str(runtime_home())`；在 `条件 not Path(storage_path).is_absolute()` 分支中，返回 `str((Path(runtime_home()) / storage_path).resolve())`；返回 `storage_path`。

  源码依据：[resolve_deermem_storage_path 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1108-L1129)


### 5.19.9 get_memory_manager

无显式业务参数。

以 `get_memory_config()` 计算 `cfg`；以 `cls.from_config(backend_config, mode=cfg.mode, **host_hooks)` 计算 `_memory_manager`。关键判定为 `_memory_manager is not None`、`_memory_manager is not None`。进入 `_manager_lock` 边界，退出由对应 context manager 处理。

以 `resolve_deermem_storage_path(backend_config)` 计算 `backend_config["storage_path"]`。分支返回 `_memory_manager`，提前返回结束该路径。

源码依据：[get_memory_manager 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1130-L1180)


### 5.19.10 辅助函数与局部契约

- **`memory_read_failures_are_fatal`**（`manager_class, backend_config, resolved_only=False`）：在 `try` 分支中，以 `_resolve_manager_class(manager_class, allow_discovery=not resolved_only)` 计算 `cls`；在 `捕获 Exception` → `条件 resolved_only` 分支中，返回 `None`；在 `捕获 Exception` 分支中，返回 `True`；在 `try` 分支中，返回 `cls.read_failures_are_fatal_for_config(backend_config)`；在 `捕获 Exception` 分支中，返回 `True`。

  源码依据：[memory_read_failures_are_fatal 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1181-L1207)


- **`reset_memory_manager`**（无显式业务参数）：使用外层变量 `global _memory_manager, _backends_cache, _memory_judge_signature`；在 `资源/同步边界 _manager_lock` 分支中，以 `None` 计算 `_memory_manager`；在 `资源/同步边界 _manager_lock` 分支中，以 `None` 计算 `_backends_cache`；在 `资源/同步边界 _manager_lock` 分支中，以 `None` 计算 `_memory_judge_signature`。

  源码依据：[reset_memory_manager 实现](../backend/packages/harness/deerflow/agents/memory/manager.py#L1208-L1219)


## 5.20 默认记忆后端

源码位置：`backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py`。

默认后端把消息筛选、signal 检测、队列和 updater/storage 组合起来；读上下文可走 FTS5、词法 relevance 或子串回退，写入/CRUD 均显式传 agent/user。warm 检索和 close/flush 的意义不同，详见 4.1.5。

### 5.20.1 辅助函数与局部契约

- **`_resolve_agent_name`**（`agent_name`）：返回 `agent_name.lower() if agent_name is not None else DEFAULT_AGENT_BUCKET`。

  源码依据：[_resolve_agent_name 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L53-L57)


- **`_call_backend`**（`operation`）：在 `try` 分支中，返回 `operation()`；在 `捕获 MemoryRevisionConflict as exc` 分支中，抛出 `MemoryConflictError(str(exc)) from exc`；在 `捕获 MemoryStorageCorruption as exc` 分支中，抛出 `MemoryCorruptionError(str(exc)) from exc`；在 `捕获 DuplicateFactIdError as exc` 分支中，抛出 `MemoryDuplicateFactIdError(str(exc)) from exc`。

  源码依据：[_call_backend 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L58-L69)


- **`_legacy_source_value`**（`source`）：在 `条件 isinstance(source, str)` 分支中，返回 `source`；在 `条件 not isinstance(source, dict)` 分支中，返回 `"unknown"`；以 `source.get("type")` 计算 `source_type`；以 `source.get("threadId")` 计算 `thread_id`；在 `条件 source_type == "conversation" and isinstance(thread_id, str) and thread_id` 分支中，返回 `thread_id`；在 `条件 isinstance(source_type, str) and source_type` 分支中，返回 `source_type`；在 `条件 isinstance(thread_id, str) and thread_id` 分支中，返回 `thread_id`；返回 `"unknown"`。

  源码依据：[_legacy_source_value 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L70-L86)


- **`_compat_document`**（`memory_data`）：以 `copy.deepcopy(memory_data)` 计算 `result`；在 `循环 for fact in result.get("facts", [])` → `条件 isinstance(fact, dict)` 分支中，以 `_legacy_source_value(fact.get("source"))` 计算 `fact["source"]`；返回 `result`。

  源码依据：[_compat_document 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L87-L95)


### 5.20.2 DeerMem.model_post_init

参数：`__context`。

以 `DeerMemConfig.from_backend_config(self.backend_config)` 计算 `self._config`；以 `create_storage(self._config)` 计算 `self._storage`；以 `self._config.host_llm if self._config.host_llm is not None else build_llm(self._config.model)` 计算 `self._llm`；以 `MemoryUpdater(self._config, self._storage, self._llm, prompts_dir=self._config.prompts_dir, callbacks=self.callbacks)` 计算 `self._updater`；以 `threading.RLock()` 计算 `self._retrieval_lock`；以 `set()` 计算 `self._retrieval_warmed_scopes: set[tuple[str | None, str | None]]`；以 `False` 计算 `self._retrieval_fully_warmed`；以 `MemoryUpdateQueue(self._config, self._updater)` 计算 `self._queue`。

源码依据：[model_post_init 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L116-L158)


### 5.20.3 DeerMem.from_config

参数：`backend_config=None, mode="middleware", **host_hooks`。

以 `dict(backend_config or {})` 计算 `config_dict`；以 `cls(backend_config=config_dict, mode=mode, callbacks=host_hooks.get("callbacks"))` 计算 `instance`。关键判定为 `key not in config_dict and key in host_hooks`、`"host_llm" not in config_dict`、`not (isinstance(model_cfg, dict) and model_cfg.get("model"))`。

以 `host_hooks[key]` 计算 `config_dict[key]`；以 `host_llm_factory()` 计算 `config_dict["host_llm"]`。分支返回 `instance`，提前返回结束该路径。

源码依据：[from_config 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L160-L199)


### 5.20.4 辅助函数与局部契约

- **`DeerMem.refresh_judge`**（`judge`）：以 `judge` 计算 `self._config.judge`。

  源码依据：[refresh_judge 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L200-L210)


- **`DeerMem.add`**（`thread_id, messages, agent_name=None, user_id=None, trace_id=None`）：过滤后要求同时有 user 和 final AI，检测 signal 并提交 debounce 队列；QueueFull 被捕获为日志/跳过，水位不前移，下轮累积历史可再次馈入；其他异常不在这里通用吞掉。

  源码依据：[add 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L211-L248)


- **`DeerMem.add_nowait`**（`thread_id, messages, agent_name=None, user_id=None`）：以 `self._prepare_update(messages)` 计算 `prepared`；在 `条件 prepared is None` 分支中，返回 None，提前结束当前路径；以 `prepared` 计算 `filtered, signals`；在 `try` 分支中，执行 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  self._queue.add_nowait( thread_id=thread_id, messages=filtered, agent_name=_resolve_agent_name(agent_name),
     user_id=user_id, signals=signals, )
  ```


  源码依据：[add_nowait 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L249-L280)


- **`DeerMem._prepare_update`**（`messages`）：解析 agent/user、过滤可提取消息并检测 correction/reinforcement signal；空有效对话不排队，真实的 user+final AI 规则来自消息处理器。

  源码依据：[_prepare_update 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L281-L302)


### 5.20.5 DeerMem.get_context

参数：`user_id, agent_name=None, thread_id=None, query=None`。

以 `None if self.mode == "tool" else _resolve_agent_name(agent_name)` 计算 `injection_agent`。关键判定为 `relevance_enabled and self._config.retrieval_relevance_weight > 0`、`isinstance(facts, list) and facts`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
format_memory_for_injection( memory_data, max_tokens=self._config.max_injection_tokens,
   use_tiktoken=(self._config.token_counting == "tiktoken"), guaranteed_categories=self._config.guaranteed_categories,
   guaranteed_token_budget=self._config.guaranteed_token_budget, query=query if relevance_enabled else None,
   relevance_weight=self._config.retrieval_relevance_weight if relevance_enabled else None,
   diversity_weight=self._config.retrieval_diversity_weight if relevance_enabled else None,
   idf=corpus_idf, )
```


源码依据：[get_context 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L303-L349)


### 5.20.6 DeerMem.search

参数：`query, top_k=5, user_id=None, agent_name=None, category=None`。

以 `_resolve_agent_name(agent_name)` 计算 `resolved_agent_name`；以 `indexed or self._substring_search( query, top_k=top_k, user_id=user_id, agent_name=resolved_agent_name, category=category, )` 计算 `results`。

关键判定为 `not query or not query.strip() or top_k <= 0`、`self._config.retrieval_relevance_enabled`、`results and (self._config.fact_eviction_policy == EVICTION_POLICY_HYBRID_V1 or self._config.fact_eviction_shadow_enabled)`。捕获 `Exception` 的边界见源码，分支返回 `[]`、`results`，提前返回结束该路径。

源码依据：[search 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L350-L400)


### 5.20.7 辅助函数与局部契约

- **`DeerMem._fts5_search`**（`query, top_k, user_id, agent_name, category`）：以 `_resolve_agent_name(agent_name)` 计算 `agent_name`；以 `getattr(self._storage, "search_facts", None)` 计算 `search_facts`；以 `[{"userId": user_id, "agentName": agent_name}]` 计算 `scopes`；在 `try` 分支中，执行 `self._ensure_retrieval_scopes(scopes)`；在 `try` 分支中，以 「表达式摘要 1」 计算 `indexed`；在 `捕获 Exception` 分支中，以 `[]` 计算 `indexed`；在 `条件 indexed` 分支中，返回 `[_compat_document({"facts": [result.get("fact", result)]})["facts"][0] for result in indexed]`；返回 `[]`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ( search_facts( query, scopes=scopes, top_k=top_k, mode="hybrid",
       filters={"category": category} if category else None, ) if callable(search_facts) else [] )
  ```


  源码依据：[_fts5_search 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L401-L434)


- **`DeerMem._substring_search`**（`query, top_k, user_id, agent_name, category`）：以 `query.strip().lower()` 计算 `query_lower`；以 `_call_backend(lambda: self._updater.get_memory_data(agent_name=agent_name, user_id=user_id))` 计算 `memory_data`；以 「表达式摘要 1」 计算 `matched`；执行 `matched.sort(key=_coerce_source_confidence, reverse=True)`；返回 `_compat_document({"facts": matched[:top_k]})["facts"]`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  [fact for fact in memory_data.get("facts", []) if isinstance(fact.get("content"),
       str) and query_lower in fact["content"].lower() and (category is None or fact.get("category") == category)]
  ```


  源码依据：[_substring_search 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L435-L449)


- **`DeerMem._relevance_search`**（`query, top_k, user_id, agent_name, category`）：以 `_call_backend(lambda: self._updater.get_memory_data(agent_name=agent_name, user_id=user_id))` 计算 `memory_data`；以 「表达式摘要 1」 计算 `facts`；在 `条件 not facts` 分支中，返回 `[]`；以 `build_idf([tokenize(fact["content"]) for fact in facts])` 计算 `corpus_idf`；以 「表达式摘要 2」 计算 `ranked`；返回 `_compat_document({"facts": ranked})["facts"]`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  [fact for fact in memory_data.get("facts", []) if isinstance(fact.get("content"),
       str) and (category is None or fact.get("category") == category)]
  ```


  **表达式摘要 2**（非完整源码；换行仅为阅读排版）：

  ```text
  order_facts_for_query( facts, query, relevance_weight=self._config.retrieval_relevance_weight,
     diversity_weight=self._config.retrieval_diversity_weight, idf=corpus_idf,
     limit=top_k, )
  ```


  源码依据：[_relevance_search 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L450-L481)


- **`DeerMem._ensure_retrieval_scopes`**（`scopes`）：以 `getattr(self._storage, "rebuild_index", None)` 计算 `rebuild`。关键判定为 `not hasattr(self, "_retrieval_lock")`、`not hasattr(self, "_retrieval_warmed_scopes")`、`not hasattr(self, "_retrieval_fully_warmed")`，相应动作只在对应分支执行。进入 `self._retrieval_lock` 边界，退出由对应 context manager 处理。以 `threading.RLock()` 计算 `self._retrieval_lock`；以 `set()` 计算 `self._retrieval_warmed_scopes`；其他写入目标为 `self._retrieval_fully_warmed`。执行 `self._retrieval_warmed_scopes.update((scope.get("userId"), scope.get("agentName")) for scope in scopes)`；执行 `self._retrieval_warmed_scopes.add(key)`。捕获 `Exception` 的边界见源码，捕获不等于已恢复。分支返回 `None`，提前返回结束该路径。

  源码依据：[_ensure_retrieval_scopes 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L482-L512)


- **`DeerMem.get_memory`**（`user_id=None, agent_name=None`）：以 `_call_backend(lambda: self._updater.get_memory_data(agent_name=_resolve_agent_name(agent_name), user_id=user_id))` 计算 `memory_data`；返回 `_compat_document(memory_data)`。

  源码依据：[get_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L513-L525)


- **`DeerMem.cancel_by_agent`**（`agent_name=None, user_id=None`）：在 `条件 agent_name is None` 分支中，返回 `self._queue.cancel_by_agent(user_id=user_id, all_agents=True)`；返回 `self._queue.cancel_by_agent( _resolve_agent_name(agent_name), user_id=user_id, all_agents=False, )`。

  源码依据：[cancel_by_agent 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L526-L544)


- **`DeerMem.clear_memory`**（`user_id=None, agent_name=None`）：执行 `self.cancel_by_agent(agent_name, user_id=user_id)`；在 `条件 agent_name is None` 分支中，以 `_call_backend(lambda: self._updater.clear_all_memory_data(user_id=user_id))` 计算 `memory_data`；在 `同层否则分支` 分支中，以 `_call_backend(lambda: self._updater.clear_memory_data(agent_name=_resolve_agent_name(agent_name), user_id=user_id))` 计算 `memory_data`；执行 `self.cancel_by_agent(agent_name, user_id=user_id)`；返回 `_compat_document(memory_data)`。

  源码依据：[clear_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L545-L560)


- **`DeerMem.import_memory`**（`memory_data, user_id=None, agent_name=None`）：以 「表达式摘要 1」 计算 `imported`；返回 `_compat_document(imported)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  _call_backend( lambda: self._updater.import_memory_data( memory_data,
       agent_name=_resolve_agent_name(agent_name), user_id=user_id,
       replace_shared_summaries=agent_name is None, ) )
  ```


  源码依据：[import_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L561-L578)


- **`DeerMem.shutdown_flush`**（`timeout`）：返回 `self._queue.flush_sync(timeout)`。

  源码依据：[shutdown_flush 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L579-L590)


- **`DeerMem.close`**（无显式业务参数）：执行 `self._storage.close()`。

  源码依据：[close 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L591-L595)


- **`DeerMem.warm`**（无显式业务参数）：在 `条件 self._config.retrieval_relevance_enabled` 分支中，执行 `warm_tokenizer()`；在 `条件 self._config.token_counting == "char"` 分支中，返回 `True`；返回 `warm_tiktoken_cache()`。

  源码依据：[warm 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L596-L612)


- **`DeerMem.warm_retrieval`**（无显式业务参数）：以 `getattr(self._storage, "rebuild_index", None)` 计算 `rebuild`；在 `条件 not callable(rebuild)` 分支中，返回 `True`；在 `try` 分支中，以 `rebuild()` 计算 `result`；在 `try` 分支中，以 `not bool(result.get("fatal"))` 计算 `index_ok`；在 `try` 分支中，以 `int(result.get("failed") or 0)` 计算 `failed`；在 `try` → `条件 index_ok` → `资源/同步边界 self._retrieval_lock` 分支中，以 `True` 计算 `self._retrieval_fully_warmed`；在 `try` → `条件 index_ok` → `资源/同步边界 self._retrieval_lock` 分支中，执行 `self._retrieval_warmed_scopes.clear()`；在 `try` 分支中，返回 `index_ok`；在 `捕获 Exception` 分支中，返回 `False`。

  源码依据：[warm_retrieval 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L613-L635)


- **`DeerMem.reload_memory`**（`user_id=None, agent_name=None`）：以 `_call_backend( lambda: self._updater.reload_memory_data( agent_name=_resolve_agent_name(agent_name), user_id=user_id, ) )` 计算 `memory_data`；返回 `_compat_document(memory_data)`。

  源码依据：[reload_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L636-L650)


- **`DeerMem.create_fact`**（`content, category="context", confidence=0.5, agent_name=None, user_id=None`）：以 「表达式摘要 1」 计算 `memory_data, fact_id`；返回 `_compat_document(memory_data), fact_id`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  _call_backend( lambda: self._updater.create_memory_fact( content,
       category=category, confidence=confidence, agent_name=_resolve_agent_name(agent_name),
       user_id=user_id, ) )
  ```


  源码依据：[create_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L651-L670)


- **`DeerMem.delete_fact`**（`fact_id, agent_name=None, user_id=None`）：以 `_call_backend( lambda: self._updater.delete_memory_fact( fact_id, agent_name=_resolve_agent_name(agent_name), user_id=user_id, ) )` 计算 `memory_data`；返回 `_compat_document(memory_data)`。

  源码依据：[delete_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L671-L686)


- **`DeerMem.update_fact`**（`fact_id, content=None, category=None, confidence=None, agent_name=None, user_id=None`）：以 「表达式摘要 1」 计算 `memory_data`；返回 `_compat_document(memory_data)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  _call_backend( lambda: self._updater.update_memory_fact( fact_id,
       content=content, category=category, confidence=confidence, agent_name=_resolve_agent_name(agent_name),
       user_id=user_id, ) )
  ```


  源码依据：[update_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deer_mem.py#L687-L708)


## 5.21 可提取对话筛选

源码位置：`backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py`。

该文件把框架消息转换为适合长期提取的对话，加载 correction/reinforcement 模式并检测 signal。澄清响应使用结构字段辨认，不能用可伪造的一段普通文本无条件认定。

### 5.21.1 load_patterns

参数：`name, patterns_dir=None`。

以 `(name, patterns_dir)` 计算 `cache_key`；以 `_PATTERN_CACHE.get(cache_key)` 计算 `cached`；以 `[]` 计算 `compiled: list[re.Pattern[str]]`。关键判定为 `cached is not None`、`not path.exists()`、`patterns_dir is not None`。进入 `path.open(encoding="utf-8") as f` 边界，退出由对应 context manager 处理。

以 `[]` 计算 `_PATTERN_CACHE[cache_key]`；以 `[]` 计算 `_PATTERN_CACHE[cache_key]`；其他写入目标为 `_PATTERN_CACHE[cache_key]`。执行 `compiled.append(re.compile(pattern_text, flags))`。捕获 `yaml.YAMLError as e`、`OSError as e`、`re.error as e` 的边界见源码，显式抛出路径涉及 `FileNotFoundError`、`ValueError`、`OSError`。

分支返回 `cached`、`[]`、`compiled`，提前返回结束该路径。

源码依据：[load_patterns 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L21-L96)


### 5.21.2 辅助函数与局部契约

- **`extract_message_text`**（`message`）：以 `getattr(message, "content", "")` 计算 `content`；在 `条件 content is None` 分支中，返回 `""`；在 `条件 isinstance(content, list)` 分支中，以 `[]` 计算 `text_parts: list[str]`；在 `条件 isinstance(content, list)` → `循环 for part in content` → `条件 isinstance(part, str)` 分支中，执行 `text_parts.append(part)`；在 `条件 isinstance(content, list)` → `循环 for part in content` → `条件 isinstance(part, dict)` 分支中，以 `part.get("text")` 计算 `text_val`；在 `条件 isinstance(content, list)` → `循环 for part in content` → `条件 isinstance(part, dict)` → `条件 isinstance(text_val, str)` 分支中，执行 `text_parts.append(text_val)`；在 `条件 isinstance(content, list)` 分支中，返回 `" ".join(text_parts)`；返回 `str(content)`。

  源码依据：[extract_message_text 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L97-L119)


- **`_non_empty_str`**（`value`）：返回 `value if isinstance(value, str) and value.strip() else None`。

  源码依据：[_non_empty_str 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L120-L124)


- **`_is_human_clarification_response`**（`additional_kwargs`）：在 `条件 not isinstance(additional_kwargs, Mapping)` 分支中，返回 `False`；以 `additional_kwargs.get("human_input_response")` 计算 `raw`；在 `条件 not isinstance(raw, Mapping)` 分支中，返回 `False`；在 `条件 raw.get("version") != 1 or raw.get("kind") != "human_input_response"` 分支中，返回 `False`；在 `条件 _non_empty_str(raw.get("source")) is None or _non_empty_str(raw.get("request_id")) is None or _non_empty_str(raw.get("value")) is None` 分支中，返回 `False`；以 `raw.get("response_kind")` 计算 `response_kind`；在 `条件 response_kind == "text"` 分支中，返回 `True`；在 `条件 response_kind == "option"` 分支中，返回 `_non_empty_str(raw.get("option_id")) is not None`；返回 `False`。

  源码依据：[_is_human_clarification_response 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L125-L156)


### 5.21.3 filter_messages_for_memory

参数：`messages, should_keep_hidden_message=None`。

排除系统、工具反馈、含工具调用的 AI 中间步骤及隐藏控制内容，保留最终回答与用户内容；结构化澄清和上传回合有特殊分支，目的是降低临时过程污染长期事实。

源码依据：[filter_messages_for_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L157-L220)


### 5.21.4 辅助函数与局部契约

- **`detect_correction`**（`messages, patterns=None`）：在 `条件 patterns is None` 分支中，以 `load_patterns("correction")` 计算 `patterns`；以 `[msg for msg in messages[-6:] if getattr(msg, "type", None) == "human"]` 计算 `recent_user_msgs`；在 `循环 for msg in recent_user_msgs` 分支中，以 `extract_message_text(msg).strip()` 计算 `content`；在 `循环 for msg in recent_user_msgs` → `条件 content and any(pattern.search(content) for pattern in patterns)` 分支中，返回 `True`；返回 `False`。

  源码依据：[detect_correction 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L221-L240)


- **`detect_reinforcement`**（`messages, patterns=None`）：在 `条件 patterns is None` 分支中，以 `load_patterns("reinforcement")` 计算 `patterns`；以 `[msg for msg in messages[-6:] if getattr(msg, "type", None) == "human"]` 计算 `recent_user_msgs`；在 `循环 for msg in recent_user_msgs` 分支中，以 `extract_message_text(msg).strip()` 计算 `content`；在 `循环 for msg in recent_user_msgs` → `条件 content and any(pattern.search(content) for pattern in patterns)` 分支中，返回 `True`；返回 `False`。

  源码依据：[detect_reinforcement 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L241-L264)


- **`detect_signals`**（`messages, patterns_dir=None, window=_SIGNAL_WINDOW`）：以 `messages if window is None else messages[-window:]` 计算 `candidates`；以 `set()` 计算 `hits: set[str]`。关键判定为 `not recent_user_msgs`、`not patterns`、`content and any(pattern.search(content) for pattern in patterns)`，相应动作只在对应分支执行。执行 `hits.add(name)`。分支返回 `set()`、`hits`，提前返回结束该路径。

  源码依据：[detect_signals 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/message_processing.py#L281-L314)


## 5.22 记忆存储路径与隔离

源码位置：`backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py`。

所有 canonical 路径与 user/agent 规范化集中于此；函数返回 Path 不意味着文件已创建。用户摘要、agent 事实、元数据、水位和检索索引有不同目录层级，不能按旧 per-agent memory.json 布局解释当前写入。

### 5.22.1 辅助函数与局部契约

- **`safe_user_id`**（`raw`）：在 `条件 not raw` 分支中，抛出 `ValueError("user_id must be a non-empty string.")`；以 `_UNSAFE_USER_ID_CHAR_RE.sub("-", raw)` 计算 `sanitized`；在 `条件 sanitized == raw` 分支中，返回 `raw`；以 `hashlib.sha256(raw.encode("utf-8")).hexdigest()[:_SAFE_USER_ID_DIGEST_HEX_LEN]` 计算 `digest`；返回 `f"{sanitized}-{digest}"`。

  源码依据：[safe_user_id 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L39-L55)


- **`validate_agent_name`**（`name`）：在 `条件 not name` 分支中，抛出 `ValueError("Agent name must be a non-empty string.")`；在 `条件 name != DEFAULT_AGENT_BUCKET and not AGENT_NAME_PATTERN.fullmatch(name)` 分支中，抛出 `ValueError(f"Invalid agent name {name!r}: names must match {AGENT_NAME_PATTERN.pattern}")`。

  源码依据：[validate_agent_name 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L56-L63)


- **`_default_root`**（无显式业务参数）：以 `os.environ.get("DEERMEM_DATA_DIR")` 计算 `env`；在 `条件 env` 分支中，返回 `Path(env)`；返回 `Path.home() / ".deermem"`。

  源码依据：[_default_root 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L64-L71)


- **`retrieval_index_directory`**（`storage_path, retrieval_index_path=None`）：以 `str(retrieval_index_path or "")` 计算 `configured`；以 `Path(storage_path) if storage_path else None` 计算 `root`；在 `条件 configured` 分支中，以 `Path(configured)` 计算 `path`；在 `条件 configured` → `条件 path.is_absolute()` 分支中，返回 `path`；在 `条件 configured` 分支中，返回 `(root if root is not None else _default_root()) / path`；在 `条件 root is None` 分支中，返回 `None`；返回 `root / RETRIEVAL_INDEX_DIRNAME`。

  源码依据：[retrieval_index_directory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L75-L96)


- **`memory_file_path`**（`config, agent_name=None, user_id=None`）：以 `Path(config.storage_path) if config.storage_path else _default_root()` 计算 `root`。关键判定为 `config.strict_user_scope and user_id is None`、`Path(manifest_filename).name != manifest_filename or not manifest_filename.endswith(".json")`、`user_id is not None`，相应动作只在对应分支执行。显式抛出路径涉及 `ValueError`。分支返回 `bucket / manifest_filename`，提前返回结束该路径。

  源码依据：[memory_file_path 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L97-L129)


- **`agent_facts_directory`**（`memory_path, agent_name`）：执行 `validate_agent_name(agent_name)`；返回 `memory_path.parent / "agents" / agent_name.lower() / "facts"`。

  源码依据：[agent_facts_directory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L130-L135)


- **`agent_metadata_directory`**（`memory_path, agent_name`）：执行 `validate_agent_name(agent_name)`；返回 `memory_path.parent / "agents" / agent_name.lower() / ".metadata"`。

  源码依据：[agent_metadata_directory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L136-L141)


- **`agent_usage_path`**（`memory_path, agent_name`）：返回 `agent_metadata_directory(memory_path, agent_name) / "fact-usage.json"`。

  源码依据：[agent_usage_path 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L142-L146)


- **`agent_eviction_audit_path`**（`memory_path, agent_name`）：返回 `agent_metadata_directory(memory_path, agent_name) / "eviction-audit.json"`。

  源码依据：[agent_eviction_audit_path 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L147-L151)


- **`agent_watermark_path`**（`memory_path, agent_name`）：返回 `agent_metadata_directory(memory_path, agent_name) / "watermarks.json"`。

  源码依据：[agent_watermark_path 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L152-L156)


- **`fact_file_path`**（`memory_path, fact_id, agent_name`）：在 `条件 not fact_id or not re.fullmatch(r"[A-Za-z0-9_-]+", fact_id)` 分支中，抛出 `ValueError("Fact id may contain only letters, numbers, '_' and '-'.")`；以 `hashlib.sha256(fact_id.encode("utf-8")).hexdigest()[:2]` 计算 `prefix`；返回 `agent_facts_directory(memory_path, agent_name) / prefix / f"{fact_id}.md"`。

  源码依据：[fact_file_path 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/paths.py#L157-L163)


## 5.23 记忆防抖队列

源码位置：`backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py`。

线程锁保护待处理集合和 timer 状态，deadline 采用单调时钟。普通累积历史按键替换，emergency 快照独立，消费者锁外做模型工作；队列和 timer 均非持久资源。

### 5.23.1 辅助函数与局部契约

- **`queue_key`**（`thread_id, user_id, agent_name`）：返回 `(thread_id, user_id, agent_name)`。

  源码依据：[queue_key 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L52-L60)


- **`MemoryUpdateQueue.__init__`**（`config, updater`）：以 `config` 计算 `self._config`；以 `updater` 计算 `self._updater`；以 `[]` 计算 `self._items: list[ConversationContext]`；以 `threading.Lock()` 计算 `self._lock`；以 `None` 计算 `self._timer: threading.Timer | None`；以 `None` 计算 `self._timer_due_at: float | None`；以 `False` 计算 `self._processing`；以 `None` 计算 `self._processing_thread: threading.Thread | None`；以 `False` 计算 `self._reprocess_pending`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L92-L107)


- **`MemoryUpdateQueue.add`**（`thread_id, messages, agent_name=None, user_id=None, trace_id=None, signals=None`）：在 `资源/同步边界 self._lock` 分支中，执行 「表达式摘要 1」；在 `资源/同步边界 self._lock` 分支中，执行 `self._rearm_timer_locked()`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  self._enqueue_locked( thread_id=thread_id, messages=messages, agent_name=agent_name,
     user_id=user_id, trace_id=trace_id, signals=frozenset(signals) if signals else frozenset(),
     bypass_watermark=False, )
  ```


  源码依据：[add 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L108-L145)


- **`MemoryUpdateQueue.add_nowait`**（`thread_id, messages, agent_name=None, user_id=None, trace_id=None, signals=None`）：在 `资源/同步边界 self._lock` 分支中，执行 「表达式摘要 1」；在 `资源/同步边界 self._lock` 分支中，执行 `self._schedule_timer(0)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  self._enqueue_locked( thread_id=thread_id, messages=messages, agent_name=agent_name,
     user_id=user_id, trace_id=trace_id, signals=frozenset(signals) if signals else frozenset(),
     bypass_watermark=True, )
  ```


  源码依据：[add_nowait 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L146-L169)


### 5.23.2 MemoryUpdateQueue._enqueue_locked

参数：`thread_id, messages, agent_name, user_id, trace_id, signals, bypass_watermark=False`。

**输入与锁前提**

调用者必须持有队列锁。函数按 `(thread_id, user_id, agent_name)` 关联对话，输入为累积消息、signal、trace 及 `bypass_watermark`；返回新增的 `ConversationContext`，副作用是更新内存待处理表。

1. 普通更新查找同键普通项，以新累积历史替换旧项，并保留最早到达时间和 signal 并集。紧急压缩快照彼此独立，不能被下一份可能不相交的快照覆盖。
2. 达到 `queue_max_depth` 时，仅拒绝新建的、无 signal 的普通项；同键合并、signal 项和紧急快照继续准入。深度参数因此不能作为所有输入下的绝对内存上界。
3. 普通项截止时间取 `min(now+debounce, first_at+max_wait)`；紧急项立即到期。已被强制 flush 的旧 deadline 不会因后续同键消息被推迟。
4. 移除被替换的普通项并追加新 context；大规模紧急积压按递增阈值记日志。这里不调用模型、不落盘，也没有持久消息确认。

源码依据：[合并、容量与截止时间](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L170-L239)

**规模影响**

当前同键查找和替换都扫描列表，队列长度为 Q 时单次入队为 O(Q)。保留紧急快照能减少压缩丢失信息的窗口，代价是持续高压下队列仍可增长；没有负载数据可以量化其实际内存或延迟。

### 5.23.3 辅助函数与局部契约

- **`MemoryUpdateQueue._rearm_timer_locked`**（无显式业务参数）：在 `条件 not self._items` 分支中，返回 None，提前结束当前路径；在 `条件 self._processing` 分支中，以 `True` 计算 `self._reprocess_pending`；在 `条件 self._processing` 分支中，返回 None，提前结束当前路径；以 `time.monotonic()` 计算 `now`；以 `min(c.due_at for c in self._items)` 计算 `due_at`；在 `条件 self._timer is not None and self._timer_due_at is not None and self._timer_due_at <= due_at` 分支中，返回 None，提前结束当前路径；执行 `self._schedule_timer(max(0.05, due_at - now))`。

  源码依据：[_rearm_timer_locked 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L240-L254)


- **`MemoryUpdateQueue._schedule_timer`**（`delay_seconds`）：在 `条件 self._timer is not None` 分支中，执行 `self._timer.cancel()`；以 `threading.Timer( delay_seconds, self._process_queue, )` 计算 `self._timer`；以 `min((c.due_at for c in self._items), default=time.monotonic() + delay_seconds)` 计算 `self._timer_due_at`；以 `True` 计算 `self._timer.daemon`；执行 `self._timer.start()`。

  源码依据：[_schedule_timer 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L255-L270)


### 5.23.4 MemoryUpdateQueue._process_queue

参数：`skip_inter_item_delay=False, due_only=True`。

**调度与状态变化**

Timer 和显式 flush 都到这里。默认只取到期项；`due_only=False` 则尝试排出当前全部 pending。`skip_inter_item_delay` 是关闭排空的特殊路径。

1. 在锁内取消当前 Timer。如果已有消费者运行，只设置重处理标志并返回，避免不断制造零延迟 Timer。没有项直接结束；有项但尚未到期则重新设定唤醒。
2. 记录 processing 和当前线程，把本批项从共享列表摘走；随后释放锁。模型调用与文件写入都在锁外依次执行，新消息可继续入队。
3. 每项调用 `update_memory`，传原 user、agent、trace、signal 和 bypass 标记。关闭排空时关闭预筛 judge 并跳过常规批项间 0.5 秒间隔，预算优先用于保存。
4. 返回假或抛 Exception 都计入 failed，记录后继续下一项；已摘下的失败项不在此自动重新入队。finally 清 processing 及线程引用，并在锁内重排 Timer，避免与并发 add 互相覆盖 Timer。

源码依据：[锁内选批与锁外消费](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L271-L366)

**对外含义**

队列 drained 只说明消费结束，不能证明每项提取成功。普通项未推进 watermark 时，后续重新馈入累积历史可能再次提取；紧急快照即将从图历史移除，硬退出或持续失败时没有同样的重馈保证。

### 5.23.5 辅助函数与局部契约

- **`MemoryUpdateQueue.flush`**（`skip_inter_item_delay=False`）：在 `资源/同步边界 self._lock` → `条件 self._timer is not None` 分支中，执行 `self._timer.cancel()`；在 `资源/同步边界 self._lock` → `条件 self._timer is not None` 分支中，以 `None` 计算 `self._timer`；在 `资源/同步边界 self._lock` → `条件 self._timer is not None` 分支中，以 `None` 计算 `self._timer_due_at`；在 `资源/同步边界 self._lock` 分支中，以 `time.monotonic()` 计算 `now`；在 `资源/同步边界 self._lock` → `循环 for context in self._items` 分支中，以 `min(context.due_at, now)` 计算 `context.due_at`；执行 `self._process_queue(skip_inter_item_delay=skip_inter_item_delay, due_only=False)`。

  源码依据：[flush 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L367-L389)


### 5.23.6 MemoryUpdateQueue.flush_sync

参数：`timeout`。

**职责与预算**

供优雅关闭调用的同步、有时限排空，返回布尔值。它只限制调用方等待总时长；同步 LLM 调用本身不能由这个 timeout 强制杀死。

1. 用 monotonic 时钟建立统一 deadline，先取正在执行的消费者线程并在剩余预算内 join。这样即使共享列表已空，也不会忽略已经出队、正在提取的批次。
2. 若此时 pending 为零且没有 processing，直接返回真。
3. 启动 daemon 线程调用 `flush(skip_inter_item_delay=True)`，用 Event 在同一剩余预算内等待。内部 `_run` 回调只在 flush 正常返回时置 success，并在 finally 设置 done。
4. 超时返回假；否则同时要求 success、没有 processing、pending 为零。逐项提取失败已被消费者吞为计数，所以返回真仍不代表每项都保存成功。

源码依据：[完整排空与等待边界](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L390-L459)

**资源边界**

等待超时后 daemon worker 可能继续运行，直到完成或进程退出；内存队列没有独立持久消费日志。不能从 graceful flush 推出硬退出不丢记忆。

### 5.23.7 辅助函数与局部契约

- **`MemoryUpdateQueue.flush_sync._run`**（无显式业务参数）：父函数内定义的回调。使用外层变量 `nonlocal success`；在 `try` 分支中，执行 `self.flush(skip_inter_item_delay=True)`；在 `try` 分支中，以 `True` 计算 `success`；在 `finally 收尾` 分支中，执行 `done.set()`。

  源码依据：[_run 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L442-L451)


- **`MemoryUpdateQueue.flush_nowait`**（无显式业务参数）：在 `资源/同步边界 self._lock` 分支中，以 `time.monotonic()` 计算 `now`；在 `资源/同步边界 self._lock` → `循环 for context in self._items` 分支中，以 `min(context.due_at, now)` 计算 `context.due_at`；在 `资源/同步边界 self._lock` 分支中，执行 `self._schedule_timer(0)`。

  源码依据：[flush_nowait 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L460-L469)


### 5.23.8 MemoryUpdateQueue.cancel_by_agent

参数：`agent_name=None, user_id=None, all_agents=False`。

在 `资源/同步边界 self._lock` 分支中，以 `len(self._items)` 计算 `before`；在 `资源/同步边界 self._lock` 分支中，建立局部回调 `def _keep(context: ConversationContext) -> bool`，行为在独立条目展开；在 `资源/同步边界 self._lock` 分支中，以 `[context for context in self._items if _keep(context)]` 计算 `self._items`；在 `资源/同步边界 self._lock` 分支中，以 `before - len(self._items)` 计算 `removed`；在 `资源/同步边界 self._lock` → `条件 removed and not self._items and self._timer is not None` 分支中，执行 `self._timer.cancel()`；在 `资源/同步边界 self._lock` → `条件 removed and not self._items and self._timer is not None` 分支中，以 `None` 计算 `self._timer`；在 `资源/同步边界 self._lock` 分支中，返回 `removed`。

源码依据：[cancel_by_agent 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L470-L514)


### 5.23.9 辅助函数与局部契约

- **`MemoryUpdateQueue.cancel_by_agent._keep`**（`context`）：父函数内定义的回调。在 `条件 not all_agents and context.agent_name != agent_name` 分支中，返回 `True`；返回 `context.user_id != user_id`。

  源码依据：[_keep 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L501-L507)


- **`MemoryUpdateQueue.clear`**（无显式业务参数）：在 `资源/同步边界 self._lock` → `条件 self._timer is not None` 分支中，执行 `self._timer.cancel()`；在 `资源/同步边界 self._lock` → `条件 self._timer is not None` 分支中，以 `None` 计算 `self._timer`；在 `资源/同步边界 self._lock` 分支中，以 `[]` 计算 `self._items`；在 `资源/同步边界 self._lock` 分支中，以 `False` 计算 `self._processing`；在 `资源/同步边界 self._lock` 分支中，以 `None` 计算 `self._processing_thread`；在 `资源/同步边界 self._lock` 分支中，以 `False` 计算 `self._reprocess_pending`。

  源码依据：[clear 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L515-L528)


- **`MemoryUpdateQueue.pending_count`**（无显式业务参数）：在 `资源/同步边界 self._lock` 分支中，返回 `len(self._items)`。

  源码依据：[pending_count 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L530-L534)


- **`MemoryUpdateQueue.is_processing`**（无显式业务参数）：在 `资源/同步边界 self._lock` 分支中，返回 `self._processing`。

  源码依据：[is_processing 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L536-L540)


## 5.24 记忆提取与变更归并

源码位置：`backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py`。

这是提取、规范化、置信度与来源门控、合并/去重、过期/容量选择及提交协调的核心。对提取结果的应用与文件库提交分开；revision 冲突重新读取再应用，不能把旧快照删除无条件重放。手动 CRUD、自动批次、水位和项目记忆各有边界。 顶层建立 max_workers=4 的 _SYNC_MEMORY_UPDATER_EXECUTOR，并注册 atexit lambda 调 shutdown(wait=False)。它供同步 update_memory 在检测到活动 loop 时提交工作；aupdate_memory 另用 asyncio.to_thread。这是另一执行资源，不能把 queue 单消费者误读成所有提取路径绝无并发，也不能把 wait=False 当作强制中止已运行任务。

源码依据：[updater.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L47-L56)

### 5.24.1 辅助函数与局部契约

- **`_validate_confidence`**（`confidence`）：在 `条件 not math.isfinite(confidence) or confidence < 0 or confidence > 1` 分支中，抛出 `ValueError("confidence")`；返回 `confidence`。

  源码依据：[_validate_confidence 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L63-L69)


- **`_coerce_source_confidence`**（`fact`）：以 `fact.get("confidence")` 计算 `raw`；在 `条件 raw is None or isinstance(raw, bool)` 分支中，返回 `0.5`；在 `try` 分支中，以 `float(raw)` 计算 `val`；在 `捕获 (TypeError, ValueError, OverflowError)` 分支中，返回 `0.5`；返回 `max(0.0, min(val, 1.0)) if math.isfinite(val) else 0.5`。

  源码依据：[_coerce_source_confidence 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L70-L87)


- **`_extract_text`**（`content`）：以 `[]` 计算 `pieces: list[str]`。关键判定为 `isinstance(content, str)`、`isinstance(content, list)`、`isinstance(block, str)`，相应动作只在对应分支执行。执行 `pending_str_parts.append(block)`；执行 `pieces.append(text_val)`。分支返回 `content`、`"\n".join(pieces)`、`str(content)`，提前返回结束该路径。

  源码依据：[_extract_text 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L88-L124)


- **`_extract_text.flush_pending_str_parts`**（无显式业务参数）：父函数内定义的回调。在 `条件 pending_str_parts` 分支中，执行 `pieces.append("".join(pending_str_parts))`；在 `条件 pending_str_parts` 分支中，执行 `pending_str_parts.clear()`。

  源码依据：[flush_pending_str_parts 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L106-L110)


- **`_normalize_gate_label`**（`value`）：在 `条件 not isinstance(value, str)` 分支中，返回 `None`；以 `value.strip().lower()` 计算 `normalized`；返回 `normalized or None`。

  源码依据：[_normalize_gate_label 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L129-L136)


- **`_fact_scope_gate_reason`**（`fact`）：在 `条件 any(_normalize_gate_label(fact.get(field)) is None for field in _FACT_CLASSIFICATION_FIELDS)` 分支中，返回 `"missing"`；在 `条件 _normalize_gate_label(fact.get("scope")) != "user"` 分支中，返回 `"scope"`；在 `条件 _normalize_gate_label(fact.get("durability")) != "durable"` 分支中，返回 `"durability"`；在 `条件 _normalize_gate_label(fact.get("authority")) != "descriptive"` 分支中，返回 `"authority"`；返回 `None`。

  源码依据：[_fact_scope_gate_reason 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L137-L149)


- **`_summary_scope_gate_reason`**（`section_data`）：以 `_normalize_gate_label(section_data.get("scope"))` 计算 `scope`；以 `_normalize_gate_label(section_data.get("authority"))` 计算 `authority`；在 `条件 scope is None or authority is None` 分支中，返回 `"missing"`；在 `条件 scope != "user"` 分支中，返回 `"scope"`；在 `条件 authority != "descriptive"` 分支中，返回 `"authority"`；返回 `None`。

  源码依据：[_summary_scope_gate_reason 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L150-L162)


- **`_removal_scope_gate_reason`**（`removal`）：以 `_normalize_gate_label(removal.get("scope"))` 计算 `scope`；以 `removal.get("reason")` 计算 `reason`；在 `条件 scope is None or not isinstance(reason, str) or not reason.strip()` 分支中，返回 `"missing"`；在 `条件 scope != "user"` 分支中，返回 `"scope"`；返回 `None`。

  源码依据：[_removal_scope_gate_reason 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L163-L173)


- **`_log_dropped_confidence`**（`raw`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[_log_dropped_confidence 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L174-L181)


- **`_log_defaulted_consolidation_confidence`**（`raw`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[_log_defaulted_consolidation_confidence 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L182-L189)


### 5.24.2 _normalize_memory_update_fact

参数：`fact`。

以 `fact.get("content")` 计算 `raw_content`；以 `{ "content": content, "category": category, "confidence": raw_confidence, }` 计算 `normalized_fact`。关键判定为 `not isinstance(fact, dict)`、`not isinstance(raw_content, str)`、`not content`。

以 `normalized_source_error` 计算 `normalized_fact["sourceError"]`；以 `evd` 计算 `normalized_fact["expected_valid_days"]`；其他写入目标为 `normalized_fact[field]`。捕获 `(OverflowError, ValueError)` 的边界见源码，分支返回 `None`、`normalized_fact`，提前返回结束该路径。

源码依据：[_normalize_memory_update_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L190-L267)


### 5.24.3 _normalize_memory_update_data

参数：`update_data`。

以 `update_data.get("user")` 计算 `user`。关键判定为 `isinstance(facts_to_reinforce, list)`、`not isinstance(entry, dict)`、`not isinstance(raw_id, str) or not raw_id.strip()`。

以 `scope` 计算 `normalized_entry["scope"]`；以 `reason.strip()` 计算 `normalized_entry["reason"]`；其他写入目标为 `normalized_removal["scope"]`、`normalized_removal["reason"]`、`normalized_removal["replacementFactIndex"]`。

执行 `normalized_facts_to_reinforce.append(normalized_entry)`；执行 `normalized_facts_to_remove.append({"id": fact_id})`。捕获 `OverflowError` 的边界见源码，显式抛出路径涉及 `json.JSONDecodeError`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
{ "user": user if isinstance(user, dict) else {}, "history": history if isinstance(history,
     dict) else {}, "newFacts": normalized_new_facts, "factsToReinforce": normalized_facts_to_reinforce,
   "factsToRemove": normalized_facts_to_remove, "staleFactsToRemove": normalized_stale_removals,
   "staleFactsToExtend": normalized_stale_extensions, "factsToConsolidate": normalized_consolidation,
   }
```


源码依据：[_normalize_memory_update_data 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L268-L447)


### 5.24.4 辅助函数与局部契约

- **`_parse_memory_update_response`**（`response_content`）：以 `_extract_text(response_content).strip()` 计算 `response_text`；以 `json.JSONDecoder()` 计算 `decoder`；在 `循环 for match in re.finditer(r"\{", response_text)` → `try` 分支中，以 `decoder.raw_decode(response_text[match.start() :])` 计算 `parsed, _end`；在 `循环 for match in re.finditer(r"\{", response_text)` → `捕获 json.JSONDecodeError` 分支中，进入下一轮循环；在 `循环 for match in re.finditer(r"\{", response_text)` → `条件 isinstance(parsed, dict) and _REQUIRED_MEMORY_UPDATE_TOP_LEVEL_KEYS.issubset(parsed)` 分支中，返回 `_normalize_memory_update_data(parsed)`；抛出 `json.JSONDecodeError("No valid memory update JSON object found", response_text, 0)`。

  源码依据：[_parse_memory_update_response 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L448-L468)


- **`_strip_upload_mentions_from_memory`**（`memory_data`）：返回 `memory_data`。

  源码依据：[_strip_upload_mentions_from_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L469-L473)


- **`_fact_content_key`**（`content`）：在 `条件 not isinstance(content, str)` 分支中，返回 `None`；以 `content.strip()` 计算 `stripped`；在 `条件 not stripped` 分支中，返回 `None`；返回 `stripped.casefold()`。

  源码依据：[_fact_content_key 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L474-L482)


- **`_fact_content_tokens`**（`content`）：以 `content.strip().lower()` 计算 `lowered`；在 `条件 not lowered` 分支中，返回 `[]`；以 `[]` 计算 `tokens: list[str]`；在 `循环 for match in _FACT_TOKEN_RE.finditer(lowered)` 分支中，以 `match.group()` 计算 `run`；在 `循环 for match in _FACT_TOKEN_RE.finditer(lowered)` → `条件 run[0].isascii()` 分支中，执行 `tokens.append(run)`；在 `循环 for match in _FACT_TOKEN_RE.finditer(lowered)` → `同层否则分支` 分支中，以 `"".join(run.split())` 计算 `run`；在 `循环 for match in _FACT_TOKEN_RE.finditer(lowered)` → `同层否则分支` 分支中，执行 `tokens.extend([run] if len(run) == 1 else (run[index : index + 2] for index in range(len(run) - 1)))`；返回 `tokens or lowered.split()`。

  源码依据：[_fact_content_tokens 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L488-L508)


- **`_fact_content_similarity`**（`left, right`）：以 `set(_fact_content_tokens(left)[:_FACT_SIMILARITY_TOKEN_BUDGET])` 计算 `left_set`；以 `set(_fact_content_tokens(right)[:_FACT_SIMILARITY_TOKEN_BUDGET])` 计算 `right_set`；在 `条件 not left_set or not right_set` 分支中，返回 `0.0`；返回 `len(left_set & right_set) / len(left_set | right_set)`。

  源码依据：[_fact_content_similarity 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L509-L517)


- **`_find_dedup_merge_target`**（`content, category, facts, threshold`）：以 `None` 计算 `best_target: dict[str, Any] | None`；以 `fact` 计算 `best_target`。关键判定为 `not isinstance(fact, dict)`、`not isinstance(candidate_content, str) or not candidate_content.strip()`、`fact.get("category", "context") != category`，相应动作只在对应分支执行。分支返回 `best_target`，提前返回结束该路径。

  源码依据：[_find_dedup_merge_target 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L518-L548)


- **`_raise_if_duplicate_fact_content`**（`memory_data, content_key`）：在 `条件 content_key is None` 分支中，返回 None，提前结束当前路径；在 `循环 for fact in memory_data.get("facts", [])` → `条件 isinstance(fact, dict) and _fact_content_key(fact.get("content")) == content_key` 分支中，抛出 `ValueError("Duplicate fact")`。

  源码依据：[_raise_if_duplicate_fact_content 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L549-L565)


- **`_parse_fact_datetime`**（`raw`）：在 `条件 not raw` 分支中，返回 `None`；在 `try` 分支中，以 `datetime.fromisoformat(raw)` 计算 `result`；在 `try` → `条件 result.tzinfo is None` 分支中，以 `result.replace(tzinfo=UTC)` 计算 `result`；在 `try` 分支中，返回 `result`；在 `捕获 (ValueError, TypeError)` 分支中，返回 `None`。

  源码依据：[_parse_fact_datetime 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L566-L583)


- **`_read_expected_valid_days`**（`fact`）：以 `fact.get("expected_valid_days")` 计算 `raw`；在 `条件 isinstance(raw, bool)` 分支中，返回 `None`；在 `条件 isinstance(raw, int)` 分支中，以 `raw # arbitrary-precision int; never routed through float()` 计算 `evd`；在 `条件 isinstance(raw, float) and math.isfinite(raw)` 分支中，以 `int(raw) # coerce before the positivity guard` 计算 `evd`；在 `同层否则分支` 分支中，返回 `None`；返回 `evd if evd > 0 else None`。

  源码依据：[_read_expected_valid_days 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L584-L615)


- **`_safe_add_days`**（`dt, days`）：在 `try` 分支中，返回 `dt + timedelta(days=days)`；在 `捕获 (OverflowError, ValueError)` 分支中，返回 `None`。

  源码依据：[_safe_add_days 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L616-L631)


- **`_effective_fact_staleness_age`**（`fact, config`）：以 `_read_expected_valid_days(fact)` 计算 `evd`；返回 `evd if evd is not None else config.staleness_age_days`。

  源码依据：[_effective_fact_staleness_age 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L632-L647)


- **`_select_stale_candidates`**（`current_memory, config`）：以 `datetime.now(UTC)` 计算 `now`；以 `[]` 计算 `candidates: list[dict[str, Any]]`。关键判定为 `not isinstance(fact, dict)`、`isinstance(category, str) and category in protected`、`review_reference is None`，相应动作只在对应分支执行。执行 `candidates.append(fact)`。分支返回 `candidates`，提前返回结束该路径。

  源码依据：[_select_stale_candidates 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L648-L684)


- **`_build_staleness_section`**（`stale_candidates, config, prompts_dir=None, agent_name=None`）：以 `[]` 计算 `lines: list[str]`。关键判定为 `not stale_candidates`，相应动作只在对应分支执行。执行 `lines.append(f'- [{fid} | {cat} | {conf:.2f} | {reviewed_short} | valid:{effective_age}d] "{content}"')`。分支返回 `""`、`load_prompt("staleness_review", prompts_dir=prompts_dir, agent_name=agent_name).format(stale_facts="\n".join(lines))`，提前返回结束该路径。

  源码依据：[_build_staleness_section 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L685-L719)


- **`_select_consolidation_candidates`**（`current_memory, config`）：以 `current_memory.get("facts", [])` 计算 `facts`；在 `条件 not facts` 分支中，返回 `{}`；以 `{}` 计算 `by_category: dict[str, list[dict[str, Any]]]`；在 `循环 for fact in facts` → `条件 not isinstance(fact, dict)` 分支中，进入下一轮循环；在 `循环 for fact in facts` 分支中，以 `fact.get("category", "context")` 计算 `cat`；在 `循环 for fact in facts` → `条件 isinstance(cat, str) and cat.strip()` 分支中，执行 `by_category.setdefault(cat.strip(), []).append(fact)`；以 `config.consolidation_min_facts` 计算 `threshold`；以 `set(config.staleness_protected_categories)` 计算 `protected`；返回 `{cat: group for cat, group in by_category.items() if len(group) >= threshold and cat not in protected}`。

  源码依据：[_select_consolidation_candidates 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L720-L745)


- **`_build_consolidation_section`**（`candidates, max_groups=3, max_sources=8, prompts_dir=None, agent_name=None`）：以 `sorted(candidates.items(), key=lambda kv: (-len(kv[1]), kv[0]))` 计算 `sorted_candidates`。关键判定为 `not candidates`，相应动作只在对应分支执行。执行 `lines.append(f'- [{fid} | {conf:.2f}] "{content}"')`；执行 「表达式摘要 1」。执行 「表达式摘要 2」。分支返回 `""`、「表达式摘要 3」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  parts.append(f'<consolidation_candidates category="{html.escape(cat)}" count="{shown}">\n' + "\n".join(lines)
    + "\n</consolidation_candidates>")
  ```


  **表达式摘要 2**（非完整源码；换行仅为阅读排版）：

  ```text
  parts.append(f'<consolidation_candidates category="{html.escape(cat)}" count="{shown}">\n' + "\n".join(lines)
    + "\n</consolidation_candidates>")
  ```


  **表达式摘要 3**（非完整源码；换行仅为阅读排版）：

  ```text
  load_prompt("consolidation", prompts_dir=prompts_dir, agent_name=agent_name).format(consolidation_groups="\n\n".join(parts),
     max_groups=max_groups)
  ```


  源码依据：[_build_consolidation_section 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L746-L776)


- **`_escape_memory_for_prompt`**（`memory`）：在 `条件 isinstance(memory, str)` 分支中，返回 `html.escape(memory)`；在 `条件 isinstance(memory, dict)` 分支中，返回 `{key: _escape_memory_for_prompt(value) for key, value in memory.items()}`；在 `条件 isinstance(memory, list)` 分支中，返回 `[_escape_memory_for_prompt(item) for item in memory]`；返回 `memory`。

  源码依据：[_escape_memory_for_prompt 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L777-L804)


- **`_memory_with_manual_markers`**（`memory`）：以 `copy.deepcopy(memory)` 计算 `display`。关键判定为 `not isinstance(display, dict)`、`not isinstance(fact, dict)`、`src_type == "manual"`，相应动作只在对应分支执行。以 `"[MANUAL] " + content` 计算 `fact["content"]`。执行 `if isinstance(content, str) and not content.startswith("[MANUAL]"):`。分支返回 `display`，提前返回结束该路径。

  源码依据：[_memory_with_manual_markers 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L805-L828)


- **`_message_identity`**（`msg`）：以 `getattr(msg, "id", None)` 计算 `mid`；在 `条件 isinstance(mid, str) and mid` 分支中，返回 `("id", mid)`；以 `extract_message_text(msg)` 计算 `text`；在 `条件 not text` 分支中，返回 `None`；以 `getattr(msg, "type", "") or ""` 计算 `msg_type`；返回 `("content", msg_type, hashlib.sha256(text.encode("utf-8")).hexdigest())`。

  源码依据：[_message_identity 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L829-L853)


- **`_memory_language_preference`**（`messages`）：在 `循环 for message in reversed(messages)` 分支中，以 `(getattr(message, "additional_kwargs", {}) or {}).get("deerflow_ui_locale")` 计算 `locale`；在 `循环 for message in reversed(messages)` → `条件 isinstance(locale, str) and locale in _MEMORY_UI_LANGUAGES` 分支中，返回 `f"Preferred memory language: {_MEMORY_UI_LANGUAGES[locale]} ({locale})"`；返回 `""`。

  源码依据：[_memory_language_preference 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L857-L864)


- **`MemoryUpdater.__init__`**（`config, storage, llm=None, prompts_dir=None, callbacks=None`）：以 `config` 计算 `self._config`；以 `storage` 计算 `self._storage`；以 `llm` 计算 `self._llm`；以 `prompts_dir` 计算 `self._prompts_dir`；以 `callbacks` 计算 `self._callbacks`；以 `OrderedDict()` 计算 `self._watermarks: OrderedDict[tuple[str | None, str | None, str | None], tuple[str, ...] | None]`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L868-L897)


- **`MemoryUpdater._save_memory_to_file`**（`memory_data, agent_name=None, user_id=None, expected_revision=None`）：以 `{"user_id": user_id}` 计算 `kwargs: dict[str, Any]`；在 `条件 expected_revision is not None` 分支中，以 `expected_revision` 计算 `kwargs["expected_revision"]`；返回 `self._storage.save(memory_data, agent_name, **kwargs)`。

  源码依据：[_save_memory_to_file 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L898-L911)


- **`MemoryUpdater.get_memory_data`**（`agent_name=None, user_id=None`）：返回 `self._storage.load(agent_name, user_id=user_id)`。

  源码依据：[get_memory_data 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L912-L915)


- **`MemoryUpdater.reload_memory_data`**（`agent_name=None, user_id=None`）：返回 `self._storage.reload(agent_name, user_id=user_id)`。

  源码依据：[reload_memory_data 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L916-L919)


### 5.24.5 MemoryUpdater._select_for_capacity

参数：`facts, agent_name, user_id`。

以 `self._config.fact_eviction_policy == EVICTION_POLICY_HYBRID_V1 or self._config.fact_eviction_shadow_enabled` 计算 `uses_hybrid_scoring`。关键判定为 `len(facts) <= self._config.max_facts`、`self._config.fact_eviction_shadow_enabled and self._config.fact_eviction_policy == EVICTION_POLICY_CONFIDENCE`。

分支返回 `facts, None, None`、`decision.kept, decision, shadow_decision`，提前返回结束该路径。

源码依据：[_select_for_capacity 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L920-L965)


### 5.24.6 辅助函数与局部契约

- **`MemoryUpdater._record_capacity_decision`**（`decision, shadow_decision, agent_name, user_id`）：在 `条件 decision is None or agent_name is None` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  self._storage.record_capacity_eviction( decision, max_facts=self._config.max_facts,
     agent_name=agent_name, user_id=user_id, shadow_decision=shadow_decision,
     )
  ```


  源码依据：[_record_capacity_decision 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L966-L987)


### 5.24.7 MemoryUpdater.import_memory_data

参数：`memory_data, agent_name=None, user_id=None, replace_shared_summaries=True`。

以 `memory_data.get("facts")` 计算 `raw_facts`。

关键判定为 `not isinstance(memory_data, dict)`、「表达式摘要 1」、`isinstance(revision, bool) or not isinstance(revision, int) or revision < 1`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
not isinstance(raw_facts, list) or any(not isinstance(fact, dict) or not isinstance(fact.get("content"),
     str) or not fact["content"].strip() for fact in raw_facts)
```


以 `str(fact.get("id") or f"fact_{uuid.uuid4().hex}")` 计算 `fact["id"]`；以 `_coerce_source_confidence(fact)` 计算 `fact["confidence"]`；其他写入目标为 `fact["revision"]`、`change_set["summaries"]`、`memory_data["facts"]`、`fact["id"]`、`fact["confidence"]`、`memory_data["facts"], capacity_decision, shadow_decision`。

执行 `seen_import_ids.add(fact_id)`；执行 `normalized_facts.append(fact)`。执行 `if not self._storage.save(memory_data, agent_name, user_id=user_id):`。显式抛出路径涉及 `ValueError`、`DuplicateFactIdError`、`OSError`。分支返回 `self._storage.load(agent_name, user_id=user_id)`，提前返回结束该路径。

源码依据：[import_memory_data 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L988-L1092)


### 5.24.8 辅助函数与局部契约

- **`MemoryUpdater.clear_memory_data`**（`agent_name=None, user_id=None`）：以 `self.get_memory_data(agent_name, user_id=user_id) if attempt == 0 else self.reload_memory_data(agent_name, user_id=user_id)` 计算 `current`；以 `copy.deepcopy(current)` 计算 `cleared_memory`。关键判定为 `agent_name is not None and getattr(type(self._storage), "apply_changes", None) is not MemoryStorage.apply_changes`、`attempt == 2`、`not self._save_memory_to_file(cleared_memory, agent_name, user_id=user_id, expected_revision=int(current.get("revision") or 0))`，相应动作只在对应分支执行。以 `[]` 计算 `cleared_memory["facts"]`。捕获 `MemoryManifestRevisionConflict` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `当前异常`、`AssertionError`、`OSError`。分支返回 `self.reload_memory_data(agent_name, user_id=user_id)`、`cleared_memory`，提前返回结束该路径。

  源码依据：[clear_memory_data 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1093-L1130)


- **`MemoryUpdater.clear_all_memory_data`**（`user_id=None`）：在 `条件 getattr(type(self._storage), "clear_all", None) is not MemoryStorage.clear_all` 分支中，返回 `self._storage.clear_all(user_id=user_id)`；以 `self.get_memory_data(user_id=user_id)` 计算 `current`；以 `create_empty_memory()` 计算 `cleared_memory`；在 `条件 not self._save_memory_to_file( cleared_memory, user_id=user_id, expected_revision=int(current.get("revision") or 0), )` 分支中，抛出 `OSError("Failed to save cleared memory data")`；返回 `cleared_memory`。

  源码依据：[clear_all_memory_data 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1131-L1144)


### 5.24.9 MemoryUpdater.create_memory_fact

参数：`content, category="context", confidence=0.5, agent_name=None, user_id=None`。

以 `content.strip()` 计算 `normalized_content`。关键判定为 `agent_name is None`、`not normalized_content`、`getattr(type(self._storage), "apply_changes", None) is not MemoryStorage.apply_changes`。

以 `self._select_for_capacity( [*memory_data.get("facts", []), copy.deepcopy(candidate)], agent_name=agent_name, user_id=user_id, )` 计算 `updated_memory["facts"], capacity_decision, shadow_decision`；以 `self._select_for_capacity( [*memory_data.get("facts", []), copy.deepcopy(candidate)], agent_name=agent_name, user_id=user_id, )` 计算 `updated_memory["facts"], capacity_decision, shadow_decision`。

捕获 `MemoryManifestRevisionConflict` 的边界见源码，显式抛出路径涉及 `ValueError`、`当前异常`、`AssertionError`、`OSError`。分支返回 `fresh_memory, (fact_id if stored else None)`、`updated_memory, (fact_id if stored else None)`，提前返回结束该路径。

源码依据：[create_memory_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1145-L1256)


### 5.24.10 辅助函数与局部契约

- **`MemoryUpdater.delete_memory_fact`**（`fact_id, agent_name=None, user_id=None`）：以 `self._storage.get_fact(fact_id, agent_name=agent_name, user_id=user_id)` 计算 `deleted`；以 `dict(memory_data)` 计算 `updated_memory`。关键判定为 `agent_name is None`、`getattr(type(self._storage), "apply_changes", None) is not MemoryStorage.apply_changes and hasattr(self._storage, "get_fact")`、`deleted is None`，相应动作只在对应分支执行。以 `updated_facts` 计算 `updated_memory["facts"]`。显式抛出路径涉及 `ValueError`、`KeyError`、`OSError`。分支返回 `self.get_memory_data(agent_name, user_id=user_id)`、`updated_memory`，提前返回结束该路径。

  源码依据：[delete_memory_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1257-L1294)


### 5.24.11 MemoryUpdater.update_memory_fact

参数：`fact_id, content=None, category=None, confidence=None, agent_name=None, user_id=None`。

以 `self._storage.get_fact(fact_id, agent_name=agent_name, user_id=user_id)` 计算 `updated_fact`；以 `dict(memory_data)` 计算 `updated_memory`。

关键判定为 `agent_name is None`、`getattr(type(self._storage), "apply_changes", None) is not MemoryStorage.apply_changes and hasattr(self._storage, "get_fact")`、`updated_fact is None`。

以 `normalized_content` 计算 `updated_fact["content"]`；以 `category.strip() or "context"` 计算 `updated_fact["category"]`；其他写入目标为 `updated_fact["confidence"]`、`updated_fact["content"]`、`updated_fact["category"]`、`updated_memory["facts"]`。执行 `updated_facts.append(updated_fact)`；执行 `updated_facts.append(fact)`。

显式抛出路径涉及 `ValueError`、`KeyError`、`OSError`。分支返回 `self.get_memory_data(agent_name, user_id=user_id)`、`updated_memory`，提前返回结束该路径。

源码依据：[update_memory_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1295-L1357)


### 5.24.12 辅助函数与局部契约

- **`MemoryUpdater._build_signal_hints`**（`signals`）：在 `条件 not signals` 分支中，返回 `""`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ( "Some lexical patterns may be present in this conversation. They are not evidence of user intent. " "Examine the full context, including quotations, negation and whether the user is discussing someone else. "
    "Store only durable, descriptive user-level facts supported by the conversation, not transient current task or thread state."
    )
  ```


  源码依据：[_build_signal_hints 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1358-L1367)


### 5.24.13 MemoryUpdater._prepare_update_prompt

参数：`messages, agent_name, signals, user_id=None, conversation_text=None`。

以 `self._config` 计算 `config`。关键判定为 `not messages`、`conversation_text is None`、`not conversation_text.strip()`。以 `message.model_copy(update={"content": f"{language_preference}\n\n{message.content}"})` 计算 `prompt[index]`；以 `message.model_copy(update={"content": content})` 计算 `prompt[index]`。

以 `"\n\n".join(part for part in (config.prompt_prepend, message.content, config.prompt_append) if part)` 计算 `content`。显式抛出路径涉及 `ValueError`。分支返回 `None`、`current_memory, prompt`，提前返回结束该路径。

源码依据：[_prepare_update_prompt 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1368-L1449)


### 5.24.14 辅助函数与局部契约

- **`MemoryUpdater._has_manual_facts`**（`memory`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  any(isinstance(f, dict) and isinstance(f.get("source"), dict) and f.get("source",
       {}).get("type") == "manual" for f in memory.get("facts", []))
  ```


  源码依据：[_has_manual_facts 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1450-L1453)


- **`MemoryUpdater._emit_extraction_metrics`**（`metrics, thread_id, user_id, trace_id, model_name, response, success`）：以 `self._config.extraction_callback` 计算 `callback`；在 `条件 callback is None` 分支中，返回 None，提前结束当前路径；以 `getattr(response, "usage_metadata", None)` 计算 `usage`；以 「表达式摘要 1」 计算 `payload: dict[str, Any]`；执行 `payload.update(metrics)`；在 `try` 分支中，执行 `callback(payload)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "thread_id": thread_id, "user_id": user_id, "trace_id": trace_id,
     "model_name": model_name, "success": success, "token_usage": usage if isinstance(usage,
       dict) else None, }
  ```


  源码依据：[_emit_extraction_metrics 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1454-L1488)


### 5.24.15 MemoryUpdater._judge_batch

参数：`hook, batch_text, signals, thread_id, agent_name, user_id, trace_id, bypass_watermark, message_count`。

在 `条件 hook is None` 分支中，返回 `None`；在 `try` 分支中，返回 「表达式摘要 1」；在 `捕获 Exception` 分支中，返回 `None`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
hook( { "batch_text": batch_text, "signals": signals, "staleness_review_enabled": bool(self._config.staleness_review_enabled),
     "consolidation_enabled": bool(self._config.consolidation_enabled),
     "bypass_watermark": bypass_watermark, "thread_id": thread_id,
     "user_id": user_id, "agent_name": agent_name, "trace_id": trace_id,
     "message_count": message_count, } )
```


源码依据：[_judge_batch 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1489-L1532)


### 5.24.16 MemoryUpdater._finalize_update

**参数摘要**

```text
    current_memory,
    response_content,
    thread_id,
    agent_name,
    user_id=None,
    metrics=None,
    signals=frozenset()
```

**职责与契约**

把一次提取模型返回的内容转成受约束记忆变更，返回是否保存成功。输入含提取前记忆快照、模型内容、thread/user/agent 和 signals；提取结果是待解析数据，不能直接当事实库。

1. 解析模型响应并记录提取条目数量。实际置信度、来源、scope、durability 等检查在 `_apply_updates` 完成。
2. 若 storage 实现了真实 `apply_changes`，最多进行三轮提交。每轮先深拷贝当前文档再应用完整提取结果，避免原地修改仍被共享缓存引用的对象。
3. 用 ID 索引对比前后事实，生成 upserts、deletes、各 fact 的预期 revision；摘要发生变化才带 summaries。提交同时携带 manifest 预期 revision。
4. 仅在 `MemoryManifestRevisionConflict` 时重新加载最新文档，再重新应用整个提取结果；不是直接对新版本重放旧的点写，因为裁剪、删除、合并依赖全局快照。第三次冲突向外抛出。
5. storage 没有增量实现时走兼容 save，也传 expected revision。只有保存成功才记录容量决策。

源码依据：[差量构造、深拷贝和冲突重算](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1533-L1634)

**收益和限制**

版本冲突重算保护并发提取不会无条件覆盖更新；它没有在此重新调用提取模型。最多三轮也不等于最终一定成功。模型置信度与事实真实性不是同一性质的保证。

### 5.24.17 辅助函数与局部契约

- **`MemoryUpdater.aupdate_memory`**（`messages, thread_id=None, agent_name=None, signals=frozenset(), user_id=None, trace_id=None, bypass_watermark=False, judge=True`）：await asyncio.to_thread(_do_update_memory_sync,...)，不另建 asyncio loop，复用同步 invoke 提取路径；与同步 update_memory 内 future.result() 的等待方式不同。

  源码依据：[aupdate_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1635-L1666)


### 5.24.18 MemoryUpdater._do_update_memory_sync

**参数摘要**

```text
    messages,
    thread_id=None,
    agent_name=None,
    signals=frozenset(),
    user_id=None,
    trace_id=None,
    bypass_watermark=False,
    judge=True
```

以 `self._config.trace_context_manager` 计算 `cm`；在 `条件 cm is not None and trace_id is not None` → `资源/同步边界 cm(trace_id)` 分支中，返回 「表达式摘要 1」；返回 「表达式摘要 2」。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
self._do_update_memory_sync_impl( messages=messages, thread_id=thread_id,
   agent_name=agent_name, signals=signals, user_id=user_id, trace_id=trace_id,
   bypass_watermark=bypass_watermark, judge=judge, )
```


**表达式摘要 2**（非完整源码；换行仅为阅读排版）：

```text
self._do_update_memory_sync_impl( messages=messages, thread_id=thread_id,
   agent_name=agent_name, signals=signals, user_id=user_id, trace_id=trace_id,
   bypass_watermark=bypass_watermark, judge=judge, )
```


源码依据：[_do_update_memory_sync 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1667-L1713)


### 5.24.19 辅助函数与局部契约

- **`MemoryUpdater._watermark_get`**（`key`）：在 `条件 key not in self._watermarks` 分支中，返回 `None`；执行 `self._watermarks.move_to_end(key)`；返回 `self._watermarks[key]`。

  源码依据：[_watermark_get 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1714-L1724)


- **`MemoryUpdater._watermark_set`**（`key, value`）：以 `value` 计算 `self._watermarks[key]`；执行 `self._watermarks.move_to_end(key)`；以 `self._config.watermark_max_keys` 计算 `cap`；在 `条件 cap > 0 and len(self._watermarks) > cap` 分支中，执行 `self._watermarks.popitem(last=False)`；执行 `self._store_persisted_watermark(key, value)`。

  源码依据：[_watermark_set 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1725-L1743)


- **`MemoryUpdater._persisted_watermark_scope`**（`key`）：以 `key` 计算 `thread_id, user_id, agent_name`；在 `条件 not isinstance(thread_id, str) or not thread_id or agent_name is None` 分支中，返回 `None`；返回 `thread_id, user_id, agent_name`。

  源码依据：[_persisted_watermark_scope 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1745-L1752)


- **`MemoryUpdater._load_persisted_watermark`**（`key`）：以 `self._persisted_watermark_scope(key)` 计算 `scope`；以 `getattr(self._storage, "get_extraction_watermark", None)` 计算 `getter`；在 `条件 scope is None or getter is None` 分支中，返回 `None`；以 `scope` 计算 `thread_id, user_id, agent_name`；在 `try` 分支中，以 `getter(thread_id, agent_name=agent_name, user_id=user_id)` 计算 `value`；在 `捕获 Exception` 分支中，返回 `None`；在 `条件 isinstance(value, list | tuple) and value and all(isinstance(part, str) for part in value)` 分支中，返回 `tuple(value)`；返回 `None`。

  源码依据：[_load_persisted_watermark 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1753-L1772)


- **`MemoryUpdater._store_persisted_watermark`**（`key, value`）：以 `self._persisted_watermark_scope(key)` 计算 `scope`；以 `getattr(self._storage, "set_extraction_watermark", None)` 计算 `setter`；在 `条件 value is None or scope is None or setter is None` 分支中，返回 None，提前结束当前路径；以 `scope` 计算 `thread_id, user_id, agent_name`；在 `try` 分支中，执行 `setter(thread_id, list(value), agent_name=agent_name, user_id=user_id)`。

  源码依据：[_store_persisted_watermark 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1773-L1796)


- **`MemoryUpdater._feed_after_watermark`**（`watermark_key, messages`）：以 `{self._watermark_get(watermark_key), self._load_persisted_watermark(watermark_key)} - {None}` 计算 `candidates`；在 `条件 not candidates` 分支中，返回 `messages`；以 `[_message_identity(msg) for msg in messages]` 计算 `identities`；以 `0` 计算 `start`；在 `循环 for last_id in candidates` → `条件 last_id in identities` 分支中，以 `max(start, identities.index(last_id) + 1)` 计算 `start`；返回 `messages[start:]`。

  源码依据：[_feed_after_watermark 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1797-L1824)


### 5.24.20 MemoryUpdater._do_update_memory_sync_impl

**参数摘要**

```text
    messages,
    thread_id=None,
    agent_name=None,
    signals=frozenset(),
    user_id=None,
    trace_id=None,
    bypass_watermark=False,
    judge=True
```

从水位裁出未消费对话，读取当前用户/agent 记忆构造 prompt，同步 invoke 提取；归一化并提交后推进普通水位，再处理项目记忆等后续阶段，emergency 不推进水位。

源码依据：[_do_update_memory_sync_impl 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1825-L2050)


### 5.24.21 辅助函数与局部契约

- **`MemoryUpdater._notify_llm_result`**（`invoke_config, prompt, response, error, started, model_name`）：在 `条件 self._callbacks is None` 分支中，返回 None，提前结束当前路径；以 `getattr(self._callbacks, "on_memory_llm_result", None)` 计算 `hook`；在 `条件 hook is None` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  hook( invoke_config, prompt=prompt, response=response, error=error,
     duration_ms=(time.monotonic() - started) * 1000, model_name=model_name,
     )
  ```


  源码依据：[_notify_llm_result 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L2051-L2081)


### 5.24.22 MemoryUpdater.update_memory

**参数摘要**

```text
    messages,
    thread_id=None,
    agent_name=None,
    signals=frozenset(),
    user_id=None,
    trace_id=None,
    bypass_watermark=False,
    judge=True
```

无活动 event loop 时直接同步提取；有活动 loop 时提交模块级4-worker线程池，但随即 future.result()，调用线程仍等待，与“不阻塞 caller loop”的注释冲突。offload 分支异常记录并返回 False；after_agent 的普通队列不使用此同步等待分支。

源码依据：[update_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L2082-L2156)


### 5.24.23 MemoryUpdater._apply_updates

**参数摘要**

```text
    current_memory,
    update_data,
    thread_id=None,
    metrics=None,
    signals=frozenset(),
    agent_name=None,
    user_id=None,
    capacity_decisions=None
```

按置信度、scope、durability/authority 判断事实与删除/合并，执行去重、修订、摘要变化及容量选择；它计算新文档，不负责把多个文件瞬间原子替换。

源码依据：[_apply_updates 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L2157-L2717)


### 5.24.24 辅助函数与局部契约

- **`MemoryUpdater._apply_updates.reject_by_scope_gate`**（`kind, reason`）：父函数内定义的回调。对 `scope_gate_rejections[kind][reason]` 执行 `+= 1` 更新。

  源码依据：[reject_by_scope_gate 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L2198-L2205)


## 5.25 事实文件、版本与恢复日志

源码位置：`backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py`。

文件库维护用户 manifest、agent Markdown facts、usage/watermark sidecar 及检索通知。进程内 scope 锁、进程文件锁、版本与 journal 共同覆盖提交；检索索引是派生状态，通知失败不改变已提交文件的事实。

### 5.25.1 辅助函数与局部契约

- **`RetrievalPort.upsert`**（「表达式摘要 1」）：此默认钩子没有额外执行体，具体能力取决于实现类。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  fact, scope, path, fact_id, scope, query, scopes, top_k, mode, filters,
   scopes=None) -> None: ... def rebuild(self, records, scopes, dict[str,
       Any] | str, str | None] ScopedRetrievalNotifications = tuple[str,
     list[RetrievalNotification]] def utc_now_iso_z(
  ```


  源码依据：[upsert 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L78-L93)


- **`RetrievalPort.remove`**（「表达式摘要 1」）：此默认钩子没有额外执行体，具体能力取决于实现类。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  fact_id, scope, query, scopes, top_k, mode, filters, scopes=None) -> None: ... def rebuild(self,
     records, scopes, dict[str, Any] | str, str | None] ScopedRetrievalNotifications = tuple[str,
     list[RetrievalNotification]] def utc_now_iso_z(
  ```


  源码依据：[remove 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L80-L93)


- **`RetrievalPort.search`**（「表达式摘要 1」）：此默认钩子没有额外执行体，具体能力取决于实现类。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  query, scopes, top_k, mode, filters, scopes=None) -> None: ... def rebuild(self,
     records, scopes, dict[str, Any] | str, str | None] ScopedRetrievalNotifications = tuple[str,
     list[RetrievalNotification]] def utc_now_iso_z(
  ```


  源码依据：[search 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L82-L93)


- **`RetrievalPort.clear`**（「表达式摘要 1」）：此默认钩子没有额外执行体，具体能力取决于实现类。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  scopes=None) -> None: ... def rebuild(self, records, scopes, dict[str,
       Any] | str, str | None] ScopedRetrievalNotifications = tuple[str,
     list[RetrievalNotification]] def utc_now_iso_z(
  ```


  源码依据：[clear 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L84-L93)


- **`RetrievalPort.rebuild`**（`records, scopes, dict[str, Any] | str, str | None] ScopedRetrievalNotifications = tuple[str, list[RetrievalNotification]] def utc_now_iso_z(`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[rebuild 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L86-L93)


- **`utc_now_iso_z`**（无显式业务参数）：返回 `datetime.now(UTC).isoformat().removesuffix("+00:00") + "Z"`。

  源码依据：[utc_now_iso_z 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L93-L96)


- **`create_empty_memory`**（无显式业务参数）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "version": "1.0", "revision": 0, "lastUpdated": utc_now_iso_z(),
     "user": { "workContext": {"summary": "", "updatedAt": ""}, "personalContext": {"summary": "",
         "updatedAt": ""}, "topOfMind": {"summary": "", "updatedAt": ""},
       "cognitiveStyle": {"summary": "", "updatedAt": ""}, }, "history": { "recentMonths": {"summary": "",
         "updatedAt": ""}, "earlierContext": {"summary": "", "updatedAt": ""},
       "longTermBackground": {"summary": "", "updatedAt": ""}, }, "facts": [],
     }
  ```


  源码依据：[create_empty_memory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L97-L117)


- **`_normalize_context_section`**（`value`）：在 `条件 not isinstance(value, dict)` 分支中，返回 `{"summary": "", "updatedAt": ""}`；以 `copy.deepcopy(value)` 计算 `section`；以 `value.get("summary") if isinstance(value.get("summary"), str) else ""` 计算 `section["summary"]`；以 `value.get("updatedAt") if isinstance(value.get("updatedAt"), str) else ""` 计算 `section["updatedAt"]`；返回 `section`。

  源码依据：[_normalize_context_section 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L118-L127)


- **`_normalize_legacy_import_fact`**（`value`）：以 `value.get("content")` 计算 `content`；以 `copy.deepcopy(value)` 计算 `fact`。关键判定为 `not isinstance(value, dict)`、`not isinstance(content, str) or not content.strip()`、`isinstance(confidence, bool)`，相应动作只在对应分支执行。以 `content.strip()` 计算 `fact["content"]`；以 `fact_id.strip() if isinstance(fact_id, str) and fact_id.strip() else f"fact_{uuid.uuid4().hex[:8]}"` 计算 `fact["id"]`；其他写入目标为 `fact["category"]`、`fact["confidence"]`、`fact["createdAt"]`、`fact["source"]`。执行 `fact.pop("sourceError")`。捕获 `(TypeError, ValueError, OverflowError)` 的边界见源码，捕获不等于已恢复。分支返回 `None`、`fact`，提前返回结束该路径。

  源码依据：[_normalize_legacy_import_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L128-L161)


- **`_normalize_memory_summaries`**（`data`）：以 `create_empty_memory()` 计算 `empty`；以 `{}` 计算 `summaries: dict[str, Any]`。以 `_normalize_context_section(value) if key in section_keys else copy.deepcopy(value)` 计算 `complete[key]`；以 `_normalize_context_section(complete.get(key))` 计算 `complete[key]`；其他写入目标为 `summaries[section_name]`。分支返回 `summaries`，提前返回结束该路径。

  源码依据：[_normalize_memory_summaries 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L162-L181)


- **`normalize_memory_data`**（`data`）：以 `copy.deepcopy(data) if isinstance(data, dict) else {}` 计算 `normalized`；执行 `normalized.update(_normalize_memory_summaries(normalized))`；以 `normalized.get("facts")` 计算 `facts`；以 `[fact for value in facts if (fact := _normalize_legacy_import_fact(value)) is not None] if isinstance(facts, list) else []` 计算 `normalized["facts"]`；在 `条件 not isinstance(normalized.get("version"), str)` 分支中，以 `"1.0"` 计算 `normalized["version"]`；在 `条件 not isinstance(normalized.get("lastUpdated"), str)` 分支中，以 `""` 计算 `normalized["lastUpdated"]`；返回 `normalized`。

  源码依据：[normalize_memory_data 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L182-L195)


- **`_has_meaningful_data`**（`value`）：在 `条件 isinstance(value, dict)` 分支中，返回 `any(_has_meaningful_data(item) for item in value.values())`；在 `条件 isinstance(value, (list, tuple, set))` 分支中，返回 `any(_has_meaningful_data(item) for item in value)`；返回 `value not in (None, "", False)`。

  源码依据：[_has_meaningful_data 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L196-L204)


- **`_merge_legacy_summary_section`**（`canonical, legacy, section, legacy_path`）：在 `条件 canonical == legacy or not _has_meaningful_data(legacy)` 分支中，返回 `copy.deepcopy(canonical)`；在 `条件 not _has_meaningful_data(canonical)` 分支中，返回 `copy.deepcopy(legacy)`；抛出 `MemoryStorageCorruption(f"Legacy {section} summary migration conflict in {legacy_path}; the legacy file was kept")`。

  源码依据：[_merge_legacy_summary_section 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L205-L213)


- **`_scope_dict`**（`user_id, agent_name`）：返回 `{"userId": user_id, "agentName": agent_name}`。

  源码依据：[_scope_dict 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L214-L217)


- **`_content_hash`**（`raw`）：返回 `f"sha256:{hashlib.sha256(raw).hexdigest()}"`。

  源码依据：[_content_hash 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L218-L221)


- **`_file_signature`**（`path`）：在 `try` 分支中，以 `path.stat()` 计算 `stat`；在 `try` 分支中，返回 `(stat.st_mtime_ns, stat.st_size)`；在 `捕获 OSError` 分支中，返回 `None`。

  源码依据：[_file_signature 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L222-L230)


- **`_ensure_migration_backup`**（`source_path`）：以 `source_path.with_name(f"{source_path.name}.v1.bak")` 计算 `backup_path`。关键判定为 `backup_path.exists()`、`backup_path.read_bytes() != source_bytes`，相应动作只在对应分支执行。以 `source_path.read_bytes()` 计算 `source_bytes`；执行 `if backup_path.read_bytes() != source_bytes:`。捕获 `MemoryStorageCorruption`、`OSError as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `MemoryStorageCorruption`、`当前异常`、`OSError`。分支返回 `backup_path`，提前返回结束该路径。

  源码依据：[_ensure_migration_backup 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L231-L247)


- **`_normalize_category`**（`fact`）：以 `fact.get("category", "context")` 计算 `raw_category`；在 `条件 not isinstance(raw_category, str)` 分支中，抛出 `ValueError("fact.category must be a string")`；以 `raw_category or "context"` 计算 `fact["category"]`；在 `条件 fact["category"] != "other"` 分支中，执行 `fact.pop("categoryExtension", None)`。

  源码依据：[_normalize_category 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L248-L261)


- **`_category_comparison_key`**（`fact`）：以 `fact.get("category")` 计算 `category`；以 `fact.get("categoryExtension")` 计算 `extension`；在 `条件 category == "other" and isinstance(extension, str) and extension` 分支中，返回 `extension, None`；返回 `category, extension`。

  源码依据：[_category_comparison_key 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L262-L278)


- **`_require_string_list`**（`fact, field`）：以 `fact.get(field, [])` 计算 `value`；在 `条件 not isinstance(value, list) or any(not isinstance(item, str) for item in value)` 分支中，抛出 `ValueError(f"fact.{field} must be a list of strings")`；以 `value` 计算 `fact[field]`。

  源码依据：[_require_string_list 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L279-L285)


### 5.25.2 _normalize_fact

参数：`fact, scope, existing=None`。

以 `copy.deepcopy(fact)` 计算 `normalized`。关键判定为 `not isinstance(fact, dict)`、「表达式摘要 1」、`not isinstance(normalized.get("content"), str)`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
not normalized["id"] or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
  for character in normalized["id"])
```


以 `str(normalized.get("id") or f"fact_{uuid.uuid4().hex}")` 计算 `normalized["id"]`；以 `2` 计算 `normalized["schemaVersion"]`；其他写入目标为 `normalized["content"]`、`normalized["confidence"]`、`normalized["status"]`、`normalized["scope"]`、`normalized["source"]`、`normalized["title"]`、`normalized["revision"]`、`normalized["createdAt"]`、`normalized["updatedAt"]`。

显式抛出路径涉及 `ValueError`、`MemoryStorageCorruption`、`MemoryFactRevisionConflict`。分支返回 `normalized`，提前返回结束该路径。

源码依据：[_normalize_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L286-L374)


### 5.25.3 辅助函数与局部契约

- **`_safe_relative_path`**（`root, relative, label`）：以 `Path(relative)` 计算 `candidate`；在 `条件 candidate.is_absolute()` 分支中，抛出 `MemoryStorageCorruption(f"{label} path escapes the user memory directory: {relative!r}")`；以 `root.resolve()` 计算 `root_resolved`；以 `(root / candidate).resolve()` 计算 `resolved`；在 `try` 分支中，执行 `resolved.relative_to(root_resolved)`；在 `捕获 ValueError as exc` 分支中，抛出 `MemoryStorageCorruption(f"{label} path escapes the user memory directory: {relative!r}") from exc`；返回 `resolved`。

  源码依据：[_safe_relative_path 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L375-L388)


- **`_fact_title`**（`fact`）：以 `str(fact.get("title") or "").strip()` 计算 `explicit`；在 `条件 explicit` 分支中，返回 `explicit.replace("\n", " ")[:160]`；以 `str(fact.get("content") or "Memory fact").splitlines()[0].strip()` 计算 `first`；返回 `(first or "Memory fact")[:160]`。

  源码依据：[_fact_title 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L389-L396)


- **`_render_fact_markdown`**（`fact`）：以 `{key: copy.deepcopy(value) for key, value in fact.items() if key not in {"content", "title"}}` 计算 `metadata`；以 `metadata.pop("scope", {})` 计算 `scope`；在 `条件 isinstance(scope, dict)` 分支中，以 `scope.get("userId")` 计算 `metadata["user_id"]`；在 `条件 isinstance(scope, dict)` 分支中，以 `scope.get("agentName")` 计算 `metadata["agent_name"]`；以 `yaml.safe_dump(metadata, allow_unicode=True, sort_keys=False).strip()` 计算 `front_matter`；以 `f"---\n{front_matter}\n---\n\n# {_fact_title(fact)}\n\n{fact['content'].rstrip()}\n"` 计算 `text`；返回 `text.encode("utf-8")`。

  源码依据：[_render_fact_markdown 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L397-L407)


- **`_parse_fact_markdown`**（`path`）：以 `path.read_text(encoding="utf-8-sig")` 计算 `text`；以 `yaml.safe_load(front) or {}` 计算 `metadata`。关键判定为 `not text.startswith("---\n")`、`not isinstance(metadata, dict)`、`lines and lines[0].startswith("# ")`，相应动作只在对应分支执行。以 `title` 计算 `metadata["title"]`；以 `"\n".join(lines).rstrip("\n")` 计算 `metadata["content"]`；其他写入目标为 `metadata["scope"]`。执行 `lines.pop(0)`。以 `path.read_text(encoding="utf-8-sig")` 计算 `text`；执行 `if not text.startswith("---\n"):`。捕获 `(OSError, UnicodeError, ValueError, yaml.YAMLError) as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `ValueError`、`MemoryStorageCorruption`。分支返回 `metadata`，提前返回结束该路径。

  源码依据：[_parse_fact_markdown 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L408-L439)


- **`_parse_listed_fact`**（`path`）：在 `try` 分支中，返回 `_parse_fact_markdown(path)`；在 `捕获 MemoryStorageCorruption` → `条件 os.path.lexists(path)` 分支中，向外继续抛出当前异常；在 `捕获 MemoryStorageCorruption` 分支中，返回 `None`。

  源码依据：[_parse_listed_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L440-L459)


- **`_is_extraction_watermark`**（`value`）：返回 `isinstance(value, list) and bool(value) and all(isinstance(part, str) for part in value)`。

  源码依据：[_is_extraction_watermark 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L460-L464)


- **`_fsync_parent_directory`**（`directory`）：在 `条件 os.name == "nt"` 分支中，返回 None，提前结束当前路径；以 `os.open(directory, os.O_RDONLY)` 计算 `descriptor`；在 `try` 分支中，执行 `os.fsync(descriptor)`；在 `finally 收尾` 分支中，执行 `os.close(descriptor)`。

  源码依据：[_fsync_parent_directory 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L465-L475)


- **`_atomic_write`**（`path, raw`）：在目标目录写临时文件并 flush/fsync，replace 后同步父目录，异常清理临时文件；只保证该文件替换边界，多文件提交依靠上层 journal。

  源码依据：[_atomic_write 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L476-L492)


### 5.25.4 _process_file_lock

参数：`lock_path, timeout_seconds`。

以 `time.monotonic() + timeout_seconds` 计算 `deadline`。关键判定为 `os.name == "nt"`、`handle.tell() == 0`、`handle is not None`。执行 `lock_path.parent.mkdir(parents=True, exist_ok=True)`；执行 `handle.write(b"0")`。捕获 `OSError` 的边界见源码，显式抛出路径涉及 `TimeoutError`。

finally 包含 执行 `handle.seek(0)`；执行 `msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)`。

源码依据：[_process_file_lock 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L494-L547)


### 5.25.5 辅助函数与局部契约

- **`MemoryStorage.load`**（「表达式摘要 1」）：抛出 `NotImplementedError`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  agent_name=None, user_id=None) -> dict[str, Any]: ... @abc.abstractmethod def reload(self,
     agent_name=None, user_id=None) -> dict[str, Any]: ... @abc.abstractmethod def save( self,
     memory_data, agent_name=None, user_id=None, expected_revision=None,
     ) -> bool: ... def apply_changes(self, change_set, **scope
  ```


  源码依据：[load 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L550-L568)


- **`MemoryStorage.reload`**（「表达式摘要 1」）：抛出 `NotImplementedError`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  agent_name=None, user_id=None) -> dict[str, Any]: ... @abc.abstractmethod def save( self,
     memory_data, agent_name=None, user_id=None, expected_revision=None,
     ) -> bool: ... def apply_changes(self, change_set, **scope
  ```


  源码依据：[reload 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L553-L568)


- **`MemoryStorage.save`**（`memory_data, agent_name=None, user_id=None, expected_revision=None, ) -> bool: ... def apply_changes(self, change_set, **scope`）：抛出 `NotImplementedError`。

  源码依据：[save 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L556-L568)


- **`MemoryStorage.apply_changes`**（`change_set, **scope`）：抛出 `NotImplementedError`。

  源码依据：[apply_changes 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L565-L568)


- **`MemoryStorage.clear_all`**（`user_id=None`）：抛出 `NotImplementedError`。

  源码依据：[clear_all 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L569-L572)


- **`MemoryStorage.get_fact_usage`**（`agent_name, user_id=None`）：返回 `{}`。

  源码依据：[get_fact_usage 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L573-L581)


- **`MemoryStorage.record_fact_accesses`**（`fact_ids, agent_name, user_id=None, accessed_at=None`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[record_fact_accesses 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L582-L591)


- **`MemoryStorage.record_capacity_eviction`**（`decision, max_facts, agent_name, user_id=None, occurred_at=None, shadow_decision=None`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[record_capacity_eviction 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L592-L603)


- **`MemoryStorage.clear_fact_metadata`**（`agent_name, user_id=None, fact_ids=None`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[clear_fact_metadata 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L604-L612)


- **`MemoryStorage.get_extraction_watermark`**（`thread_id, agent_name, user_id=None`）：返回 `None`。

  源码依据：[get_extraction_watermark 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L613-L622)


- **`MemoryStorage.set_extraction_watermark`**（`thread_id, watermark, agent_name, user_id=None`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[set_extraction_watermark 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L623-L632)


- **`MemoryStorage.close`**（无显式业务参数）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[close 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L633-L636)


- **`FileMemoryStorage.__init__`**（`config, retrieval=None`）：以 `config` 计算 `self._config`；以 `retrieval` 计算 `self._retrieval`；其他写入目标为 `self._memory_cache: dict[tuple[str | None, str | None], tuple[dict[str, Any], tuple[Any, ...]]]`、`self._cache_lock`、`self._scope_locks: weakref.WeakValueDictionary[tuple[str | None, str | None], threading.RLock]`、`self._retrieval_dirty_scopes: set[tuple[str | None, str | None]]`、`self._retrieval_synced_signatures: dict[tuple[str | None, str | None], tuple[Any, ...]]`、`self._retrieval_index_generation`、`self._retrieval_scope_generations: dict[tuple[str | None, str | None], int]`、`self._retrieval_publish_lock`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L638-L661)


- **`FileMemoryStorage.close`**（无显式业务参数）：在 `条件 self._retrieval is not None` 分支中，执行 `self._retrieval.close()`。

  源码依据：[close 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L662-L666)


- **`FileMemoryStorage._read_json_sidecar`**（`path, expected_type`）：在 `条件 not path.exists()` 分支中，返回 `expected_type()`；在 `try` 分支中，以 `json.loads(path.read_text(encoding="utf-8"))` 计算 `value`；在 `捕获 (OSError, json.JSONDecodeError)` 分支中，返回 `expected_type()`；在 `条件 not isinstance(value, expected_type)` 分支中，返回 `expected_type()`；返回 `value`。

  源码依据：[_read_json_sidecar 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L668-L680)


- **`FileMemoryStorage._read_watermark_sidecar`**（`path`）：在 `try` 分支中，以 `path.read_bytes()` 计算 `raw`；在 `捕获 FileNotFoundError` 分支中，返回 `{}`；在 `try` 分支中，以 `json.loads(raw.decode("utf-8"))` 计算 `value`；在 `捕获 (UnicodeDecodeError, json.JSONDecodeError)` 分支中，返回 `{}`；在 `条件 not isinstance(value, dict)` 分支中，返回 `{}`；返回 `value`。

  源码依据：[_read_watermark_sidecar 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L682-L704)


- **`FileMemoryStorage.get_fact_usage`**（`agent_name, user_id=None`）：以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`；以 `self._cache_key(agent_name, user_id=user_id)` 计算 `key`；在 「表达式摘要 1」 分支中，以 `self._read_json_sidecar( agent_usage_path(path, agent_name), expected_type=dict, )` 计算 `raw`；返回 `{str(fact_id): copy.deepcopy(entry) for fact_id, entry in raw.items() if isinstance(fact_id, str) and isinstance(entry, dict)}`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  资源/同步边界 ( self._scope_lock(key), _process_file_lock( path.parent / ".memory.lock",
       float(getattr(self._config, "file_lock_timeout_seconds", 10)),
       ), )
  ```


  源码依据：[get_fact_usage 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L705-L725)


### 5.25.6 FileMemoryStorage.record_fact_accesses

参数：`fact_ids, agent_name, user_id=None, accessed_at=None`。

以 `list(dict.fromkeys(fact_id for fact_id in fact_ids if fact_id))` 计算 `unique_ids`。关键判定为 `not unique_ids`、`not existing_ids`、`not math.isfinite(heat) or heat < 0`。

进入 「表达式摘要 1」 边界，退出由对应 context manager 处理。以 `{ "accessHeat": heat + 1.0, "accessCount": count + 1, "lastAccessedAt": now.isoformat().removesuffix("+00:00") + "Z", }` 计算 `usage[fact_id]`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
( self._scope_lock(key), _process_file_lock( path.parent / ".memory.lock",
     float(getattr(self._config, "file_lock_timeout_seconds", 10)),
     ), )
```


以 `datetime.fromisoformat(str(last_accessed).replace("Z", "+00:00"))` 计算 `parsed`；以 `parsed.replace(tzinfo=UTC)` 计算 `parsed`。捕获 `(TypeError, ValueError)` 的边界见源码，分支返回 `None`，提前返回结束该路径。

源码依据：[record_fact_accesses 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L726-L786)


### 5.25.7 FileMemoryStorage.record_capacity_eviction

参数：`decision, max_facts, agent_name, user_id=None, occurred_at=None, shadow_decision=None`。

以 `(occurred_at or datetime.now(UTC)).astimezone(UTC)` 计算 `now`。关键判定为 `not decision.evicted or self._config.eviction_audit_max_entries == 0`、`shadow_decision is not None`。

进入 「表达式摘要 1」 边界，退出由对应 context manager 处理。以 `{ "policyVersion": shadow_decision.policy, "wouldEvict": sorted(shadow_ids), "disagrees": actual_ids != shadow_ids, }` 计算 `event["shadow"]`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
( self._scope_lock(key), _process_file_lock( path.parent / ".memory.lock",
     float(getattr(self._config, "file_lock_timeout_seconds", 10)),
     ), )
```


执行 `events.append(event)`。分支返回 `None`，提前返回结束该路径。

源码依据：[record_capacity_eviction 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L787-L842)


### 5.25.8 FileMemoryStorage.clear_fact_metadata

参数：`agent_name, user_id=None, fact_ids=None`。

以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`。关键判定为 `fact_ids is None`、`metadata_path.exists()`、`not removed_ids`。

进入 「表达式摘要 1」 边界，退出由对应 context manager 处理。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
( self._scope_lock(key), _process_file_lock( path.parent / ".memory.lock",
     float(getattr(self._config, "file_lock_timeout_seconds", 10)),
     ), )
```


以 `[item for item in evicted if isinstance(item, dict) and isinstance(item.get("factId"), str) and item["factId"] not in removed_ids]` 计算 `event["evicted"]`；以 `[fact_id for fact_id in would_evict if isinstance(fact_id, str) and fact_id not in removed_ids]` 计算 `shadow["wouldEvict"]`；其他写入目标为 `shadow["disagrees"]`。

执行 `event.pop("shadow", None)`；执行 `event.pop("shadow")`。执行 `usage_path.unlink(missing_ok=True)`；执行 `audit_path.unlink(missing_ok=True)`。捕获 `OSError` 的边界见源码，分支返回 `None`，提前返回结束该路径。

源码依据：[clear_fact_metadata 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L843-L918)


### 5.25.9 辅助函数与局部契约

- **`FileMemoryStorage.get_extraction_watermark`**（`thread_id, agent_name, user_id=None`）：以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`；以 `self._cache_key(agent_name, user_id=user_id)` 计算 `key`；在 「表达式摘要 1」 分支中，以 `self._read_watermark_sidecar(agent_watermark_path(path, agent_name))` 计算 `raw`；以 `raw.get(thread_id)` 计算 `value`；返回 `list(value) if _is_extraction_watermark(value) else None`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  资源/同步边界 ( self._scope_lock(key), _process_file_lock( path.parent / ".memory.lock",
       float(getattr(self._config, "file_lock_timeout_seconds", 10)),
       ), )
  ```


  源码依据：[get_extraction_watermark 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L919-L938)


### 5.25.10 FileMemoryStorage.set_extraction_watermark

参数：`thread_id, watermark, agent_name, user_id=None`。

以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`。关键判定为 `watermark is not None`、`cap > 0 and len(watermarks) > cap`、`watermarks`。

进入 「表达式摘要 1」 边界，退出由对应 context manager 处理。以 `list(watermark)` 计算 `watermarks[thread_id]`。执行 `sidecar_path.unlink(missing_ok=True)`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
( self._scope_lock(key), _process_file_lock( path.parent / ".memory.lock",
     float(getattr(self._config, "file_lock_timeout_seconds", 10)),
     ), )
```


源码依据：[set_extraction_watermark 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L939-L980)


### 5.25.11 辅助函数与局部契约

- **`FileMemoryStorage._cache_key`**（`agent_name=None, user_id=None`）：返回 `(user_id, agent_name)`。

  源码依据：[_cache_key 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L982-L984)


- **`FileMemoryStorage._scope_lock`**（`key`）：在 `资源/同步边界 self._cache_lock` 分支中，返回 `self._scope_locks.setdefault(key, threading.RLock())`。

  源码依据：[_scope_lock 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L985-L988)


- **`FileMemoryStorage._get_memory_file_path`**（`agent_name=None, user_id=None`）：返回 `memory_file_path(self._config, agent_name, user_id=user_id)`。

  源码依据：[_get_memory_file_path 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L989-L991)


- **`FileMemoryStorage._scope_signature`**（`path, agent_name`）：以 `_file_signature(path)` 计算 `file_signature`；在 `条件 file_signature is None` 分支中，返回 `(None, None, None)`；以 `self._load_memory_file(path)` 计算 `memory_file`；以 `int((memory_file or {}).get("revision") or 0)` 计算 `revision`；返回 `(*file_signature, revision)`。

  源码依据：[_scope_signature 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L992-L1007)


### 5.25.12 FileMemoryStorage._dispatch_retrieval_notifications

参数：`notifications, user_id, agent_name, committed=None`。

以 `_scope_dict(user_id, agent_name)` 计算 `scope`。关键判定为 `self._retrieval is None`、`committed is not None`、`action == "upsert"`。进入 `self._cache_lock` 边界，退出由对应 context manager 处理。以 `signature` 计算 `self._retrieval_synced_signatures[synced_key]`。

执行 `self._retrieval.remove(str(value), scope=scope)`；执行 `self._retrieval_dirty_scopes.add(key)`。捕获 `Exception` 的边界见源码，分支返回 `None`，提前返回结束该路径。

源码依据：[_dispatch_retrieval_notifications 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1008-L1086)


### 5.25.13 辅助函数与局部契约

- **`FileMemoryStorage._dispatch_retrieval_notifications.provably_compatible`**（`synced_key`）：父函数内定义的回调。在 `条件 committed is None or fence is None or publication_moved` 分支中，返回 `False`；以 `self._retrieval_synced_signatures.get(synced_key)` 计算 `synced`；在 `条件 synced is None or (synced[2] or 0) != committed[0]` 分支中，返回 `False`；返回 `self._retrieval_scope_generations.get(synced_key, 0) == fence[1].get(synced_key, 0)`。

  源码依据：[provably_compatible 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1068-L1075)


- **`FileMemoryStorage._validate_loaded_fact`**（`fact, fact_path, user_id, agent_name`）：以 `_scope_dict(user_id, agent_name)` 计算 `expected_scope`。关键判定为 `str(fact.get("id")) != fact_path.stem`、`not scope_matches`，相应动作只在对应分支执行。捕获 `ValueError as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `MemoryStorageCorruption`。分支返回 `_normalize_fact(fact, scope=copy.deepcopy(actual_scope))`，提前返回结束该路径。

  源码依据：[_validate_loaded_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1088-L1106)


- **`FileMemoryStorage._load_agent_facts`**（`path, agent_name, user_id`）：在 `条件 agent_name is None` 分支中，返回 `[]`；以 `[]` 计算 `facts: list[dict[str, Any]]`；在 `循环 for fact_path in sorted(agent_facts_directory(path, agent_name).glob("**/*.md"))` 分支中，以 `_parse_listed_fact(fact_path)` 计算 `fact`；在 `循环 for fact_path in sorted(agent_facts_directory(path, agent_name).glob("**/*.md"))` → `条件 fact is None` 分支中，进入下一轮循环；在 `循环 for fact_path in sorted(agent_facts_directory(path, agent_name).glob("**/*.md"))` 分支中，执行 `facts.append(self._validate_loaded_fact(fact, fact_path, user_id=user_id, agent_name=agent_name))`；返回 `sorted(facts, key=lambda fact: str(fact["id"]))`。

  源码依据：[_load_agent_facts 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1107-L1122)


- **`FileMemoryStorage._read_fact`**（`path, fact_id, user_id, agent_name`）：以 `fact_file_path(path, fact_id, agent_name=agent_name)` 计算 `fact_path`；在 `条件 not fact_path.exists()` 分支中，返回 `None, fact_path`；以 `_parse_fact_markdown(fact_path)` 计算 `fact`；返回 `self._validate_loaded_fact(fact, fact_path, user_id=user_id, agent_name=agent_name), fact_path`。

  源码依据：[_read_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1123-L1129)


- **`FileMemoryStorage._agent_entries`**（`path, agent_name, user_id`）：以 `{}` 计算 `entries: dict[str, dict[str, str]]`。关键判定为 `agent_name is None`、`not fact_id or fact_id != fact_path.stem`，相应动作只在对应分支执行。以 `{"path": fact_path.relative_to(path.parent).as_posix()}` 计算 `entries[fact_id]`。显式抛出路径涉及 `MemoryStorageCorruption`。分支返回 `{}`、`entries`，提前返回结束该路径。

  源码依据：[_agent_entries 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1130-L1142)


- **`FileMemoryStorage._legacy_agent_memory_path`**（`path, agent_name`）：返回 `path.parent / "agents" / agent_name.lower() / path.name`。

  源码依据：[_legacy_agent_memory_path 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1143-L1145)


- **`FileMemoryStorage._global_json_needs_migration`**（`path`）：以 `self._load_memory_file(path)` 计算 `memory_file`；返回 `memory_file is not None and ("facts" in memory_file or memory_file.get("version") != DOCUMENT_VERSION)`。

  源码依据：[_global_json_needs_migration 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1146-L1149)


### 5.25.14 FileMemoryStorage._migrate_previous_default_bucket_locked

参数：`path, user_id`。

以 `"lead-agent"` 计算 `legacy_agent_name`。关键判定为 `not legacy_agent_dir.exists() or (legacy_agent_dir / "config.yaml").is_file()`、`legacy_facts_dir.exists()`、`unexpected`。

执行 `unexpected.extend(child for child in legacy_facts_dir.glob("**/*") if child.is_file() and child.suffix.lower() != ".md")`；执行 `upserts.append(candidate)`。以 `", ".join(sorted(child.name for child in unexpected))` 计算 `names`；执行 `fact_path.unlink(missing_ok=True)`。捕获 `OSError` 的边界见源码，

显式抛出路径涉及 `MemoryStorageCorruption`。分支返回 `False, []`、`True, notifications`，提前返回结束该路径。

源码依据：[_migrate_previous_default_bucket_locked 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1150-L1220)


### 5.25.15 辅助函数与局部契约

- **`FileMemoryStorage._run_read_migrations_locked`**（`path, agent_name, user_id`）：以 `[]` 计算 `notifications_by_agent: list[ScopedRetrievalNotifications]`。关键判定为 `self._global_json_needs_migration(path)`、`notifications`、`agent_name is not None`，相应动作只在对应分支执行。执行 `notifications_by_agent.append((DEFAULT_AGENT_BUCKET, notifications))`；执行 `notifications_by_agent.append((agent_name, notifications))`。分支返回 `notifications_by_agent`，提前返回结束该路径。

  源码依据：[_run_read_migrations_locked 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1221-L1251)


- **`FileMemoryStorage._migration_equivalent`**（`left, right`）：以 `{"revision", "createdAt", "updatedAt", "scope", "title", "schemaVersion", "category", "categoryExtension"}` 计算 `ignored`；以 `{key: value for key, value in left.items() if key not in ignored}` 计算 `left_material`；以 `{key: value for key, value in right.items() if key not in ignored}` 计算 `right_material`；返回 `left_material == right_material and _category_comparison_key(left) == _category_comparison_key(right)`。

  源码依据：[_migration_equivalent 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1253-L1258)


- **`FileMemoryStorage._load_memory_file`**（`path`）：在 `条件 not path.exists()` 分支中，返回 `None`；在 `try` 分支中，以 `json.loads(path.read_text(encoding="utf-8-sig"))` 计算 `value`；在 `捕获 (json.JSONDecodeError, OSError, UnicodeError) as exc` 分支中，抛出 `MemoryStorageCorruption(f"Failed to load global memory JSON {path}: {exc}") from exc`；在 `条件 not isinstance(value, dict)` 分支中，抛出 `MemoryStorageCorruption(f"Global memory JSON {path} is not an object")`；返回 `value`。

  源码依据：[_load_memory_file 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1259-L1273)


### 5.25.16 FileMemoryStorage._recover_if_needed

参数：`path`。

**职责与调用前提**

在调用方同时持有进程内 scope 锁和跨进程文件锁时，读取本用户目录的 `.memory.journal.json`。无 journal 直接返回；有 journal 时验证 operation ID 是单一路径分量，并验证状态、agent 与旧路径信息。

- `prepared`：若有旧 manifest 备份则原子恢复；初次创建且原 revision 为零时删除新 manifest。移除本次新建事实，并从 recovery 目录恢复旧事实，逐项校验 ID 和相对路径。
- `committed`：保留新数据，只清 recovery 目录和 journal。
- 解析错误、非法路径或未知状态：抛 `MemoryStorageCorruption`，不把损坏日志当空文档继续覆盖。

源码依据：[日志校验与恢复分支](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1274-L1321)

**边界**

恢复按 journal 提交标记决定回退或保留，不会重新执行模型或外部工具。这里只处理此存储管理的 manifest 与 fact 文件；跨机器锁语义仍取决于部署文件系统。

### 5.25.17 FileMemoryStorage._commit_changes_locked

**参数摘要**

```text
    path,
    user_id,
    agent_name,
    upserts,
    deletes,
    summaries,
    expected_revision,
    delete_revisions=None,
    upsert_revisions=None
```

**职责、前提与数据归属**

调用方已取得两层锁并完成 journal 恢复。函数提交当前用户的共享 summary manifest，以及指定 agent 桶中被寻址的事实文件；返回新 manifest 和检索通知。它不是每次加载、重写所有 Markdown 事实。

1. 读取当前 manifest，核对 expected revision；有事实变更而没有 agent 名称时拒绝。对 upsert 的重复 ID、对象类型、已有 fact revision 与 scope 逐项校验，只把实际变化的事实纳入 prepared。
2. 规范删除 ID，拒绝重复删除以及与变化 upsert 冲突的 ID；不存在事实的删除跳过，带 revision 的删除必须匹配当前版本。合并 user/history 摘要，若没有真实变化或格式清理需求，直接返回原版本而不推进 revision。

源码依据：[manifest 与单事实版本校验](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1322-L1421)

3. 为下一 revision 建立唯一 operation ID，备份旧 manifest 和涉及的旧事实，再用原子写保存 `prepared` journal。
4. 逐个原子写入变动事实、删除指定事实，积累检索 upsert/remove 通知；最后原子替换 manifest，写入 `committed` journal。
5. 清理 recovery 目录和 journal，失效当前用户所有受影响 cache entry，返回通知。检索通知由外层锁外派发；文件提交与索引更新不构成同一原子动作。

源码依据：[备份、提交标记和缓存失效](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1422-L1474)

**失败窗口**

prepared 以后、committed 标记以前失败，下次受控读取根据备份恢复旧数据；committed 后清理失败，下次保留新数据并完成清理。单个文件原子替换与多文件日志恢复共同工作；这不是操作系统一次性替换所有文件，也不包含远端检索服务的事务。

### 5.25.18 FileMemoryStorage._migrate_locked

参数：`path, agent_name, user_id, include_global, adopt_legacy_summaries=True`。

以 `[]` 计算 `sources: list[tuple[Path, dict[str, Any]]]`。关键判定为 `legacy_memory is not None`、`include_global and global_memory is not None and ("facts" in global_memory or global_memory.get("version") != DOCUMENT_VERSION)`、`not sources`。

以 「表达式摘要 1」 计算 `migrated_summaries[section]`；以 `candidate` 计算 `pending[candidate["id"]]`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
_merge_legacy_summary_section( canonical=migrated_summaries[section],
   legacy=legacy_summaries[section], section=section, legacy_path=legacy_path,
   )
```


执行 `sources.append((legacy_path, legacy_memory))`；执行 `sources.append((path, global_memory))`。执行 `legacy_path.unlink(missing_ok=True)`。显式抛出路径涉及 `MemoryStorageCorruption`。分支返回 `False, from_version, []`、`True, from_version, notifications`，提前返回结束该路径。

源码依据：[_migrate_locked 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1475-L1554)


### 5.25.19 FileMemoryStorage._document_from_memory_file

参数：`memory_file, path, agent_name, user_id, allow_legacy_facts=False`。

以 `memory_file.get("facts")` 计算 `legacy_facts`；以 `{key: copy.deepcopy(value) for key, value in memory_file.items() if key != "facts"}` 计算 `result`。关键判定为 `contains_owned_legacy_facts and not allow_legacy_facts`、`isinstance(legacy_facts, list)`、`isinstance(legacy_facts, dict)`。

以 `summary_view["user"]` 计算 `result["user"]`；以 `summary_view["history"]` 计算 `result["history"]`；其他写入目标为 `result["facts"]`。执行 `facts.append(fact)`。执行 `if entry.get("contentHash") and entry.get("contentHash") != _content_hash(fact_path.read_bytes()):`。显式抛出路径涉及 `MemoryStorageCorruption`。分支返回 `result`，提前返回结束该路径。

源码依据：[_document_from_memory_file 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1555-L1603)


### 5.25.20 辅助函数与局部契约

- **`FileMemoryStorage._read_document`**（`path, agent_name, user_id`）：以 `self._load_memory_file(path)` 计算 `memory_file`；在 `条件 memory_file is None` 分支中，以 `create_empty_memory()` 计算 `result`；在 `条件 memory_file is None` 分支中，以 `self._load_agent_facts(path, agent_name, user_id=user_id)` 计算 `result["facts"]`；在 `条件 memory_file is None` 分支中，返回 `result`；返回 `self._document_from_memory_file(memory_file, path, agent_name, user_id=user_id)`。

  源码依据：[_read_document 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1604-L1611)


- **`FileMemoryStorage.load`**（`agent_name=None, user_id=None`）：以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`。关键判定为 `needs_migration`、`cached is not None and cached[1] == signature`，相应动作只在对应分支执行。进入 `self._scope_lock(key), _process_file_lock(path.parent / ".memory.lock", float(getattr(self._config, "file_lock_timeout_seconds", 10)))`、`self._cache_lock` 边界，退出由对应 context manager 处理。以 `(copy.deepcopy(document), signature)` 计算 `self._memory_cache[key]`。分支返回 `copy.deepcopy(cached[0])`、`copy.deepcopy(document)`，提前返回结束该路径。

  源码依据：[load 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1612-L1639)


- **`FileMemoryStorage.reload`**（`agent_name=None, user_id=None, _rebuild_retrieval=True`）：以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`。关键判定为 `needs_migration`、`_rebuild_retrieval and agent_name is not None and self._retrieval is not None`，相应动作只在对应分支执行。进入 `self._scope_lock(key), _process_file_lock(path.parent / ".memory.lock", float(getattr(self._config, "file_lock_timeout_seconds", 10)))`、`self._cache_lock` 边界，退出由对应 context manager 处理。以 `(copy.deepcopy(document), signature)` 计算 `self._memory_cache[key]`。分支返回 `copy.deepcopy(document)`，提前返回结束该路径。

  源码依据：[reload 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1640-L1667)


- **`FileMemoryStorage.migrate`**（`user_id=None, agent_name=None`）：以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`。关键判定为 `agent_name is None`，相应动作只在对应分支执行。进入 `self._scope_lock(key), _process_file_lock(path.parent / ".memory.lock", float(getattr(self._config, "file_lock_timeout_seconds", 10)))` 边界，退出由对应 context manager 处理。显式抛出路径涉及 `ValueError`。分支返回 `{ "migrated": migrated, "fromVersion": from_version, "toVersion": document.get("version"), "revision": document.get("revision", 0), }`，提前返回结束该路径。

  源码依据：[migrate 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1668-L1690)


### 5.25.21 FileMemoryStorage.save

参数：`memory_data, agent_name=None, user_id=None, expected_revision=None`。

以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`。关键判定为 `not isinstance(memory_data, dict)`、`agent_name is not None and "facts" not in memory_data`、`not isinstance(facts_raw, list)`。

进入 `self._scope_lock(key), _process_file_lock(lock_path, float(getattr(self._config, "file_lock_timeout_seconds", 10)))`、`self._cache_lock` 边界，退出由对应 context manager 处理。以 `(copy.deepcopy(document), signature)` 计算 `self._memory_cache[key]`。

捕获 `MemoryRevisionConflict`、`(OSError, ValueError, MemoryStorageCorruption) as exc` 的边界见源码，显式抛出路径涉及 `ValueError`、`当前异常`。分支返回 `False`、`True`，提前返回结束该路径。

源码依据：[save 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1691-L1766)


### 5.25.22 FileMemoryStorage.clear_all

参数：`user_id=None`。

以 `self._get_memory_file_path(user_id=user_id)` 计算 `path`。关键判定为 `agents_root.exists()`、`legacy_path.exists()`、`migration_notifications`。

进入 「表达式摘要 1」 边界，退出由对应 context manager 处理。执行 `metadata_agents.append(agent_name)`；执行 `notifications_by_agent.append((agent_name, migration_notifications))`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
( self._scope_lock(key), _process_file_lock( path.parent / ".memory.lock",
     float(getattr(self._config, "file_lock_timeout_seconds", 10)),
     ), )
```


分支返回 `self.reload(DEFAULT_AGENT_BUCKET, user_id=user_id)`，提前返回结束该路径。

源码依据：[clear_all 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1767-L1831)


### 5.25.23 辅助函数与局部契约

- **`FileMemoryStorage._scope_kwargs`**（`scope`）：以 `{}` 计算 `kwargs: dict[str, str]`；在 `条件 scope.get("userId") is not None` 分支中，以 `str(scope["userId"])` 计算 `kwargs["user_id"]`；在 `条件 scope.get("agentName") is not None` 分支中，以 `str(scope["agentName"])` 计算 `kwargs["agent_name"]`；返回 `kwargs`。

  源码依据：[_scope_kwargs 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1833-L1840)


- **`FileMemoryStorage.get_fact`**（`fact_id, user_id=None, agent_name=None`）：以 `self._get_memory_file_path(agent_name, user_id=user_id)` 计算 `path`。关键判定为 `agent_name is None`、`legacy_path.exists()`，相应动作只在对应分支执行。进入 `self._scope_lock(key), _process_file_lock(path.parent / ".memory.lock", float(getattr(self._config, "file_lock_timeout_seconds", 10)))` 边界，退出由对应 context manager 处理。显式抛出路径涉及 `ValueError`。分支返回 `copy.deepcopy(fact)`，提前返回结束该路径。

  源码依据：[get_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1841-L1861)


- **`FileMemoryStorage.list_facts`**（`user_id=None, agent_name=None, filters=None, cursor=0, limit=100`）：在 `条件 cursor < 0 or limit < 1` 分支中，抛出 `ValueError("cursor must be >= 0 and limit must be >= 1")`；以 `self.load(agent_name, user_id=user_id).get("facts", [])` 计算 `facts`；以 `filters or {}` 计算 `filters`；以 `[fact for fact in facts if all(key in fact and fact.get(key) == value for key, value in filters.items())]` 计算 `matched`；返回 `copy.deepcopy(matched[cursor : cursor + limit])`。

  源码依据：[list_facts 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1862-L1877)


### 5.25.24 FileMemoryStorage.apply_changes

**参数摘要**

```text
    change_set,
    user_id=None,
    agent_name=None,
    expected_manifest_revision=None,
    allow_manifest_rebase=False
```

装载当前文档、验证 manifest 与各事实 expected revision，将 upsert/delete/摘要更新合成一次库内提交；返回新文档并在锁外派发检索通知，索引不与文件组成同一事务。

源码依据：[apply_changes 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1878-L1977)


### 5.25.25 辅助函数与局部契约

- **`FileMemoryStorage.upsert_fact`**（`fact, user_id=None, agent_name=None, expected_manifest_revision=None, expected_fact_revision=None`）：在 `条件 agent_name is None` 分支中，抛出 `ValueError("agent_name is required to upsert a fact")`；以 `copy.deepcopy(fact)` 计算 `incoming`；以 `str(incoming.get("id") or f"fact_{uuid.uuid4().hex}")` 计算 `incoming["id"]`；以 `incoming["id"]` 计算 `fact_id`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  self.apply_changes( {"upserts": [incoming], "upsertRevisions": {fact_id: expected_fact_revision}},
     user_id=user_id, agent_name=agent_name, expected_manifest_revision=expected_manifest_revision,
     allow_manifest_rebase=True, )
  ```


  源码依据：[upsert_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1978-L1999)


- **`FileMemoryStorage.delete_fact`**（`fact_id, user_id=None, agent_name=None, expected_manifest_revision=None, expected_fact_revision=None`）：在 `条件 agent_name is None` 分支中，抛出 `ValueError("agent_name is required to delete a fact")`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  self.apply_changes( { "deletes": [fact_id], "deleteRevisions": ({fact_id: expected_fact_revision} if expected_fact_revision
        is not None else None), }, user_id=user_id, agent_name=agent_name,
     expected_manifest_revision=expected_manifest_revision, allow_manifest_rebase=True,
     )
  ```


  源码依据：[delete_fact 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2000-L2021)


- **`FileMemoryStorage.get_summaries`**（`user_id=None, agent_name=None`）：以 `self.load(agent_name, user_id=user_id)` 计算 `document`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  {"user": copy.deepcopy(document.get("user", {})), "history": copy.deepcopy(document.get("history",
         {})), "revision": document.get("revision", 0)}
  ```


  源码依据：[get_summaries 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2022-L2030)


- **`FileMemoryStorage.update_summaries`**（`summaries, user_id=None, agent_name=None, expected_revision=None`）：以 `self.load(user_id=user_id)` 计算 `document`；执行 `document.update({key: copy.deepcopy(value) for key, value in summaries.items() if key in {"user", "history"}})`；以 `int(document.get("revision") or 0) if expected_revision is None else expected_revision` 计算 `expected`；在 `条件 not self.save(document, user_id=user_id, expected_revision=expected)` 分支中，抛出 `MemoryStorageError("Failed to update global memory summaries")`；返回 `self.reload(user_id=user_id)`。

  源码依据：[update_summaries 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2031-L2046)


- **`FileMemoryStorage.notify_fact_upsert`**（`fact, path=""`）：在 `条件 self._retrieval is None` 分支中，返回 `False`；以 `fact.get("scope") if isinstance(fact.get("scope"), dict) else {}` 计算 `scope`；执行 `self._retrieval.upsert(copy.deepcopy(fact), scope=copy.deepcopy(scope), path=path)`；返回 `True`。

  源码依据：[notify_fact_upsert 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2047-L2053)


- **`FileMemoryStorage.notify_fact_remove`**（`fact_id, scope`）：在 `条件 self._retrieval is None` 分支中，返回 `False`；执行 `self._retrieval.remove(fact_id, scope=copy.deepcopy(scope))`；返回 `True`。

  源码依据：[notify_fact_remove 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2054-L2059)


### 5.25.26 FileMemoryStorage.search_facts

参数：`query, scopes, top_k=10, mode="hybrid", filters=None`。

以 `{self._cache_key(self._scope_kwargs(scope).get("agent_name"), user_id=self._scope_kwargs(scope).get("user_id")) for scope in scopes}` 计算 `requested_keys`。关键判定为 `self._retrieval is not None`、`agent_name is None`、`synced[key] != live_signature`。进入 `self._cache_lock` 边界，退出由对应 context manager 处理。

执行 `dirty.add(key)`。捕获 `(OSError, ValueError, MemoryStorageCorruption) as exc` 的边界见源码，分支返回 `self._search_substring(query, scopes=scopes, top_k=top_k, filters=filters)`、`self._retrieval.search(query, scopes=scopes, top_k=top_k, mode=mode, filters=filters)`，提前返回结束该路径。

源码依据：[search_facts 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2060-L2100)


### 5.25.27 辅助函数与局部契约

- **`FileMemoryStorage._search_substring`**（`query, scopes, top_k, filters`）：以 `query.strip().lower()` 计算 `query_lower`。关键判定为 `not query_lower or top_k <= 0`、`filters and not all(key in fact and fact.get(key) == value for key, value in filters.items())`、`isinstance(content, str) and query_lower in content.lower()`，相应动作只在对应分支执行。执行 `results.append({"fact": fact, "score": score, "matchType": "substring"})`。执行 `for fact in self.load(**self._scope_kwargs(scope)).get("facts", []):`。分支返回 `[]`、`results[:top_k]`，提前返回结束该路径。

  源码依据：[_search_substring 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2101-L2127)


### 5.25.28 FileMemoryStorage.rebuild_index

参数：`scopes=None`。

以 `[]` 计算 `records: list[tuple[dict[str, Any], dict[str, str | None], str]]`。关键判定为 `self._retrieval is None`、`scopes is None`、`"agents" in relative_parts[:-1]`。进入 `self._retrieval_publish_lock` 边界，退出由对应 context manager 处理。

以 `self._scope_signature(manifest_path, relative_parts[agents_index + 1])` 计算 `manifest_signatures[manifest_path]`；以 `self._scope_signature(memory_path, agent_name)` 计算 `synced_signatures[self._cache_key(agent_name, user_id=kwargs.get("user_id"))]`。

执行 `records.append((fact, _scope_dict(original_user, expected_agent), str(path)))`；执行 `records.append((fact, _scope_dict(kwargs.get("user_id"), agent_name), str(fact_file_path(memory_path, fact["id"], agent_name=agent_name))))`。

以 `root.joinpath(*relative_parts[:agents_index]) / self._config.manifest_filename` 计算 `manifest_path`；执行 `for fact in self.load(**kwargs).get("facts", []):`。捕获 `Exception` 的边界见源码，显式抛出路径涉及 `MemoryStorageCorruption`。

分支返回 `{"supported": False, "indexed": 0, "failed": 0, "reason": "retrieval_not_configured"}`、`{"supported": True, "indexed": indexed, "failed": failed, "fatal": True}`、`{"supported": True, "indexed": indexed, "failed": failed}`，提前返回结束该路径。

源码依据：[rebuild_index 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2128-L2263)


### 5.25.29 辅助函数与局部契约

- **`FileMemoryStorage.rebuild_index.bump_generations`**（无显式业务参数）：父函数内定义的回调。在 `条件 requested_keys is None` 分支中，对 `self._retrieval_index_generation` 执行 `+= 1` 更新；在 `同层否则分支` → `循环 for key in requested_keys` 分支中，以 `self._retrieval_scope_generations.get(key, 0) + 1` 计算 `self._retrieval_scope_generations[key]`。

  源码依据：[bump_generations 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2192-L2199)


- **`FileMemoryStorage.rebuild_index.mark_synced`**（无显式业务参数）：父函数内定义的回调。在 `资源/同步边界 self._cache_lock` 分支中，执行 `bump_generations()`；在 `资源/同步边界 self._cache_lock` → `条件 requested_keys is None` 分支中，执行 `self._retrieval_dirty_scopes.clear()`；在 `资源/同步边界 self._cache_lock` → `条件 requested_keys is None` 分支中，以 `synced_signatures` 计算 `self._retrieval_synced_signatures`；在 `资源/同步边界 self._cache_lock` → `同层否则分支` 分支中，执行 `self._retrieval_dirty_scopes.difference_update(requested_keys)`；在 `资源/同步边界 self._cache_lock` → `同层否则分支` 分支中，执行 `self._retrieval_synced_signatures.update(synced_signatures)`。

  源码依据：[mark_synced 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2200-L2209)


- **`FileMemoryStorage.rebuild_index.forget_synced`**（无显式业务参数）：父函数内定义的回调。在 `资源/同步边界 self._cache_lock` 分支中，执行 `bump_generations()`；在 `资源/同步边界 self._cache_lock` → `条件 requested_keys is None` 分支中，执行 `self._retrieval_synced_signatures.clear()`；在 `资源/同步边界 self._cache_lock` → `同层否则分支` → `循环 for key in requested_keys` 分支中，执行 `self._retrieval_synced_signatures.pop(key, None)`。

  源码依据：[forget_synced 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2210-L2234)


- **`FileMemoryStorage.retrieval_status`**（无显式业务参数）：返回 `{ "configured": self._retrieval is not None, "mode": "external" if self._retrieval is not None else "substring_fallback", }`。

  源码依据：[retrieval_status 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2264-L2269)


- **`FileMemoryStorage.capabilities`**（无显式业务参数）：以 `{"file", "markdown-facts", "global-summary-json", "revision", "journal", "fact-repository", "substring-fallback"}` 计算 `capabilities`；在 `条件 self._retrieval is not None` 分支中，执行 `capabilities.add("retrieval")`；返回 `capabilities`。

  源码依据：[capabilities 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2270-L2276)


- **`create_storage`**（`config, retrieval=None`）：以 `create_fts5_retrieval` 计算 `factory`。关键判定为 `retrieval is None and config.retrieval_adapter`、`config.retrieval_adapter == "fts5"`、`not storage_class_path or storage_class_path == "file"`，相应动作只在对应分支执行。捕获 `Exception as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `ValueError`、`TypeError`。分支返回 `FileMemoryStorage(config, retrieval=retrieval)`、`MarkdownMemoryStorage(config, retrieval=retrieval)`、`storage_class(config)`，提前返回结束该路径。

  源码依据：[create_storage 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L2277-L2305)


## 5.26 子 Agent 建图与执行

源码位置：`backend/packages/harness/deerflow/subagents/executor.py`。

文件负责子图构建、执行结果与后台注册。result 终态、cancel 请求、execution_done 分离；同步调用通过隔离 loop 执行，后台执行还用 supervisor 追踪真实结束。父子共享 sandbox 需独立 holder；返回文本、receipt、bash 证据和 token 数据各有采集器，详见 4.1.6。 模块重载时若发现旧 shutdown 函数，会先 unregister 并调用它，避免旧 loop 遗留；正常隔离 loop 由首次获取时建立，启动失败仍保留存活 worker 的所有权，不能马上再建第二个。 [子任务终态集合](../backend/packages/harness/deerflow/subagents/executor.py#L95-L98) [子任务状态与控制字段](../backend/packages/harness/deerflow/subagents/executor.py#L737-L789) atexit 注册 _shutdown_isolated_subagent_loop；这覆盖正常解释器退出的尝试，不保证硬杀进程时执行。

源码依据：[executor.py 实现](../backend/packages/harness/deerflow/subagents/executor.py#L734-L734)

### 5.26.1 辅助函数与局部契约

- **`_utcnow`**（无显式业务参数）：返回 `datetime.now(UTC)`。

  源码依据：[_utcnow 实现](../backend/packages/harness/deerflow/subagents/executor.py#L89-L94)


- **`SubagentStatus.is_terminal`**（无显式业务参数）：返回 `self in { type(self).COMPLETED, type(self).FAILED, type(self).CANCELLED, type(self).TIMED_OUT, }`。

  源码依据：[is_terminal 实现](../backend/packages/harness/deerflow/subagents/executor.py#L112-L120)


- **`SubagentResult.__post_init__`**（无显式业务参数）：在 `条件 self.ai_messages is None` 分支中，以 `[]` 计算 `self.ai_messages`；在 `条件 self.status.is_terminal or self.completed_at is not None` 分支中，执行 `self.execution_done_event.set()`。

  源码依据：[__post_init__ 实现](../backend/packages/harness/deerflow/subagents/executor.py#L190-L196)


- **`SubagentResult.update_token_usage_records`**（`records`）：在 `资源/同步边界 self._state_lock` → `条件 not self.status.is_terminal or self.status in { SubagentStatus.CANCELLED, SubagentStatus.TIMED_OUT, }` 分支中，以 `list(records)` 计算 `self.token_usage_records`。

  源码依据：[update_token_usage_records 实现](../backend/packages/harness/deerflow/subagents/executor.py#L197-L205)


- **`SubagentResult.update_tool_receipts`**（`receipts`）：在 `条件 receipts is None` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._state_lock` → `条件 not self.status.is_terminal` 分支中，以 `[dict(receipt) for receipt in receipts]` 计算 `self.tool_receipts`。

  源码依据：[update_tool_receipts 实现](../backend/packages/harness/deerflow/subagents/executor.py#L206-L213)


- **`SubagentResult.update_bash_executions`**（`executions`）：在 `条件 executions is None` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._state_lock` → `条件 self.status.is_terminal` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._state_lock` 分支中，以 `{_bash_evidence_key(entry): entry for entry in (self.bash_executions or [])}` 计算 `merged`；在 `资源/同步边界 self._state_lock` → `循环 for execution in executions` 分支中，以 `dict(execution)` 计算 `merged[_bash_evidence_key(execution)]`；在 `资源/同步边界 self._state_lock` 分支中，以 `list(merged.values())[-_BASH_EVIDENCE_MAX_ENTRIES:]` 计算 `self.bash_executions`。

  源码依据：[update_bash_executions 实现](../backend/packages/harness/deerflow/subagents/executor.py#L214-L236)


- **`SubagentResult.snapshot_tool_receipts`**（无显式业务参数）：在 `资源/同步边界 self._state_lock` → `条件 self.tool_receipts is None` 分支中，返回 `None`；在 `资源/同步边界 self._state_lock` 分支中，返回 `[dict(receipt) for receipt in self.tool_receipts]`。

  源码依据：[snapshot_tool_receipts 实现](../backend/packages/harness/deerflow/subagents/executor.py#L237-L243)


- **`SubagentResult.update_knowledge_sources`**（`messages`）：以 `0` 计算 `start`。关键判定为 `self.status.is_terminal`、`messages is None`、`previous != current`，相应动作只在对应分支执行。进入 `self._state_lock` 边界，退出由对应 context manager 处理。以 `list(messages)` 计算 `self._knowledge_messages`；以 `source.model_dump(mode="json", exclude_none=True)` 计算 `merged[source.id]`；其他写入目标为 `self.knowledge_sources`。分支返回 `None`，提前返回结束该路径。

  源码依据：[update_knowledge_sources 实现](../backend/packages/harness/deerflow/subagents/executor.py#L244-L266)


### 5.26.2 SubagentResult.try_set_terminal

**参数摘要**

```text
    status,
    result=None,
    error=None,
    stop_reason=None,
    completed_at=None,
    ai_messages=None,
    token_usage_records=None,
    admission_failure=False,
    tool_receipts=None
```

在 result 的状态锁内只允许首次终态转换，并同时保存结果、错误和证据；迟到的成功/失败不能覆盖已终止状态，真实执行结束另由 event 表达。

源码依据：[try_set_terminal 实现](../backend/packages/harness/deerflow/subagents/executor.py#L267-L310)


### 5.26.3 辅助函数与局部契约

- **`_terminal_direct_results`**（`final_state, return_direct_tools`）：以 `final_state.get("messages", [])` 计算 `messages`。关键判定为 `not final_state or not return_direct_tools`、`not isinstance(message, AIMessage)`、`calls and all(call["name"] in return_direct_tools for call in calls)`，相应动作只在对应分支执行。分支返回 `None`、`[results[call["id"]] for call in calls]`，提前返回结束该路径。

  源码依据：[_terminal_direct_results 实现](../backend/packages/harness/deerflow/subagents/executor.py#L311-L328)


### 5.26.4 _extract_final_result

参数：`final_state, trace_id, name, return_direct_tools=None`。

以 `final_state.get("messages", [])` 计算 `messages`。关键判定为 `final_state is None`、`direct_results is not None`、`last_ai_message is not None`。分支返回 `"No response generated"`、`"\n\n".join(text for text in texts if text) or "No response generated"`、`text if text else "No response generated"`，提前返回结束该路径。

源码依据：[_extract_final_result 实现](../backend/packages/harness/deerflow/subagents/executor.py#L329-L372)


### 5.26.5 _extract_llm_error_fallback

参数：`final_state`。

以 `message.additional_kwargs` 计算 `metadata`；以 `message_content_to_text(message.content).strip()` 计算 `content`。关键判定为 `final_state is None`、`not isinstance(message, AIMessage)`、`metadata.get("deerflow_error_fallback") is not True`。分支返回 `None`、`content`、`detail.strip()`、`"LLM request failed"`，提前返回结束该路径。

源码依据：[_extract_llm_error_fallback 实现](../backend/packages/harness/deerflow/subagents/executor.py#L373-L424)


### 5.26.6 _harvest_tool_receipts

参数：`final_state, prefer_citing_turn=False`。

以 `final_state.get("messages") if isinstance(final_state, dict) else None` 计算 `messages`。关键判定为 `not final_state`、`not messages`、`prefer_citing_turn`。捕获 `Exception` 的边界见源码，分支返回 `None`、`[dict(receipt) for receipt in receipts]`，提前返回结束该路径。

源码依据：[_harvest_tool_receipts 实现](../backend/packages/harness/deerflow/subagents/executor.py#L435-L479)


### 5.26.7 辅助函数与局部契约

- **`_bash_evidence_status`**（`content, meta_status`）：返回 `bash_exit_status(content) or (meta_status, None)`。

  源码依据：[_bash_evidence_status 实现](../backend/packages/harness/deerflow/subagents/executor.py#L488-L499)


- **`_harvest_shell_persistence`**（`final_state`）：在 `try` 分支中，以 `unwrap_sandbox(final_state.get("sandbox")) if isinstance(final_state, dict) else (None, False)` 计算 `sandbox_state, _`；在 `try` 分支中，以 `sandbox_state.get("sandbox_id") if isinstance(sandbox_state, dict) else None` 计算 `sandbox_id`；在 `try` → `条件 not isinstance(sandbox_id, str)` 分支中，返回 `None`；在 `try` 分支中，以 `get_sandbox_provider().get(sandbox_id)` 计算 `sandbox`；在 `try` → `条件 sandbox is None` 分支中，返回 `None`；在 `try` 分支中，以 `getattr(sandbox, "persistent_shell_sessions", None)` 计算 `declared`；在 `try` 分支中，返回 `None if declared is None else bool(declared)`；在 `捕获 Exception` 分支中，返回 `None`。

  源码依据：[_harvest_shell_persistence 实现](../backend/packages/harness/deerflow/subagents/executor.py#L500-L535)


- **`_bash_evidence_key`**（`execution`）：以 `execution.get("result_id")` 计算 `result_id`；在 `条件 isinstance(result_id, str) and result_id` 分支中，返回 `("result", result_id)`；返回 `("call", str(execution.get("tool_call_id")))`。

  源码依据：[_bash_evidence_key 实现](../backend/packages/harness/deerflow/subagents/executor.py#L536-L547)


### 5.26.8 _harvest_bash_executions

参数：`final_state, summary_cache=None`。

以 `final_state.get("messages") if isinstance(final_state, dict) else None` 计算 `messages`。关键判定为 `not final_state`、`summary_cache is not None`、`not messages`。以 `(content, test_summary)` 计算 `next_cache[cache_key]`；以 `shell_persistent` 计算 `execution["shell_persistent"]`。

执行 `summary_cache.clear()`；执行 `summary_cache.clear()`。捕获 `Exception` 的边界见源码，分支返回 `None`、`[]`、`executions[-_BASH_EVIDENCE_MAX_ENTRIES:]`，提前返回结束该路径。

源码依据：[_harvest_bash_executions 实现](../backend/packages/harness/deerflow/subagents/executor.py#L548-L660)


### 5.26.9 辅助函数与局部契约

- **`_run_isolated_subagent_loop`**（`loop, started_event`）：执行 `asyncio.set_event_loop(loop)`；执行 `loop.call_soon(started_event.set)`；在 `try` 分支中，执行 `loop.run_forever()`；在 `finally 收尾` 分支中，执行 `started_event.clear()`。

  源码依据：[_run_isolated_subagent_loop 实现](../backend/packages/harness/deerflow/subagents/executor.py#L669-L681)


### 5.26.10 _shutdown_isolated_subagent_loop

参数：`only_if_pending=False`。

以 `_isolated_subagent_loop` 计算 `loop`。关键判定为 `only_if_pending and not _isolated_subagent_loop_shutdown_pending`、`loop is None`、`only_if_pending and thread is not None and thread.is_alive()`。进入 `_isolated_subagent_loop_shutdown_lock`、`_isolated_subagent_loop_lock` 边界，退出由对应 context manager 处理。

执行 `thread.join(timeout=1)`；执行 `loop.close()`。分支返回 `None`，提前返回结束该路径。

源码依据：[_shutdown_isolated_subagent_loop 实现](../backend/packages/harness/deerflow/subagents/executor.py#L682-L733)


### 5.26.11 _get_isolated_subagent_loop

无显式业务参数。

以 `_isolated_subagent_loop_thread is not None and _isolated_subagent_loop_thread.is_alive()` 计算 `thread_is_alive`；以 `loop` 计算 `_isolated_subagent_loop`。关键判定为 `_isolated_subagent_loop_shutdown_pending`、`_isolated_subagent_loop_shutdown_pending`、`not loop_is_usable`。

进入 `_isolated_subagent_loop_lock` 边界，退出由对应 context manager 处理。执行 `thread.start()`；执行 `thread.join(timeout=1)`。显式抛出路径涉及 `RuntimeError`。分支返回 `_isolated_subagent_loop`，提前返回结束该路径。

源码依据：[_get_isolated_subagent_loop 实现](../backend/packages/harness/deerflow/subagents/executor.py#L737-L789)


### 5.26.12 辅助函数与局部契约

- **`_run_on_isolated_loop_if_live`**（`callback`）：以 `_isolated_subagent_loop` 计算 `loop`；在 `条件 loop is None or loop.is_closed() or not loop.is_running()` 分支中，执行 `callback()`；在 `条件 loop is None or loop.is_closed() or not loop.is_running()` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 `loop.call_soon_threadsafe(callback)`；在 `捕获 RuntimeError` 分支中，执行 `callback()`。

  源码依据：[_run_on_isolated_loop_if_live 实现](../backend/packages/harness/deerflow/subagents/executor.py#L790-L810)


- **`_submit_to_isolated_loop_in_context`**（`context, coro_factory`）：建立局部回调 `def _submit() -> Future[SubagentResult]`，行为在独立条目展开；返回 `context.run(_submit)`。

  源码依据：[_submit_to_isolated_loop_in_context 实现](../backend/packages/harness/deerflow/subagents/executor.py#L811-L844)


- **`_submit_to_isolated_loop_in_context._submit`**（无显式业务参数）：父函数内定义的回调。以 `_get_isolated_subagent_loop()` 计算 `loop`；以 `coro_factory()` 计算 `coroutine`；在 `try` 分支中，返回 `asyncio.run_coroutine_threadsafe(coroutine, loop)`；在 `捕获 BaseException` 分支中，执行 `coroutine.close()`；在 `捕获 BaseException` 分支中，向外继续抛出当前异常。

  源码依据：[_submit 实现](../backend/packages/harness/deerflow/subagents/executor.py#L829-L841)


- **`run_on_isolated_subagent_loop`**（`coro`）：返回 `asyncio.run_coroutine_threadsafe(coro, _get_isolated_subagent_loop())`。

  源码依据：[run_on_isolated_subagent_loop 实现](../backend/packages/harness/deerflow/subagents/executor.py#L845-L856)


- **`_copy_isolated_subagent_context`**（无显式业务参数）：以 `copy_context()` 计算 `context`。关键判定为 `inherited_config is None or "callbacks" not in inherited_config`、`isinstance(callbacks, BaseCallbackManager)`、`isinstance(callbacks, (list, tuple))`，相应动作只在对应分支执行。以 `isolated_callbacks` 计算 `isolated_config["callbacks"]`。执行 `isolated_config.pop("callbacks", None)`。分支返回 `context`，提前返回结束该路径。

  源码依据：[_copy_isolated_subagent_context 实现](../backend/packages/harness/deerflow/subagents/executor.py#L857-L894)


- **`_filter_tools`**（`all_tools, allowed, disallowed`）：以 `all_tools` 计算 `filtered`；在 `条件 allowed is not None` 分支中，以 `set(allowed)` 计算 `allowed_set`；在 `条件 allowed is not None` 分支中，以 `[t for t in filtered if t.name in allowed_set]` 计算 `filtered`；在 `条件 disallowed is not None` 分支中，以 `set(disallowed)` 计算 `disallowed_set`；在 `条件 disallowed is not None` 分支中，以 `[t for t in filtered if t.name not in disallowed_set]` 计算 `filtered`；返回 `filtered`。

  源码依据：[_filter_tools 实现](../backend/packages/harness/deerflow/subagents/executor.py#L895-L924)


### 5.26.13 SubagentExecutor.__init__

**参数摘要**

```text
    config,
    tools,
    app_config=None,
    parent_model=None,
    sandbox_state=None,
    thread_data=None,
    uploaded_files=None,
    thread_id=None,
    trace_id=None,
    user_id=None,
    user_role=None,
    oauth_provider=None,
    oauth_id=None,
    run_id=None,
    channel_user_id=None,
    is_internal=False,
    authz_attributes=None,
    deerflow_trace_id=None,
    knowledge_scope=None,
    extensions=None,
    execution_capacity=None,
    acceptance_criteria=None,
    loop_detection_recorder=None,
    tool_promotion_recorder=None,
    tool_progress_recorder=None,
    approval_runtime=None,
    context_snapshot=None,
    thread_incarnation=_THREAD_INCARNATION_UNSET,
    input_gate=None
```

关键判定为 `config.model != "inherit" or parent_model is not None or app_config is not None`。

以 `config` 计算 `self.config`；以 `app_config` 计算 `self.app_config`；其他写入目标为 `self._resolved_app_config`、`self.parent_model`、`self.model_name: str | None`、`self.model_name`、`self.thinking_enabled`、`self.reasoning_effort`、`self.sandbox_state`、`self.thread_data`、`self.uploaded_files`、`self.context_snapshot`、`self.input_gate`、`self.thread_id`、`self.thread_incarnation`、`self.trace_id`、`self.user_id`、`self.user_role`、`self.oauth_provider`、`self.oauth_id`、`self.run_id`、`self.channel_user_id`、`self.is_internal`、`self.authz_attributes`、`self.deerflow_trace_id`、`self.knowledge_scope`、`self.extensions`、`self.execution_capacity`、`self.acceptance_criteria`、`self.loop_detection_recorder`、`self.tool_promotion_recorder`、`self.tool_progress_recorder`、`self.approval_runtime`、`self._base_tools`、`self.tools`、`self._available_skill_names: set[str]`、`self._stop_reason_middlewares: list[Any]`、`self._return_direct_tools: set[str]`、`self._skill_authorization: Any | None`、`self._recursion_limit: int | None`、`self.assembly_descriptor: Any | None`、`self._assembled_system_prompt`、`self._assembled_skills: list[Any]`、`self._policy_discovery_tools: tuple[BaseTool, ...]`。

源码依据：[__init__ 实现](../backend/packages/harness/deerflow/subagents/executor.py#L931-L1124)


### 5.26.14 辅助函数与局部契约

- **`SubagentExecutor._get_resolved_app_config`**（无显式业务参数）：在 `条件 self._resolved_app_config is None` 分支中，以 `get_app_config()` 计算 `self._resolved_app_config`；返回 `self._resolved_app_config`。

  源码依据：[_get_resolved_app_config 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1125-L1130)


### 5.26.15 SubagentExecutor._create_agent

参数：`tools=None, deferred_setup=None, extensions=None`。

以 `self._get_resolved_app_config()` 计算 `app_config`；以 `create_agent( model=model, tools=bound_tools, middleware=middlewares, system_prompt=None, state_schema=ThreadState, checkpointer=False, )` 计算 `agent`。

关键判定为 `self.model_name is None`、`model_config is None`、`deferred_setup is not None and deferred_setup.deferred_names`。

以 `resolve_subagent_model_name(self.config, self.parent_model, app_config=app_config)` 计算 `self.model_name`；以 `extensions` 计算 `middleware_kwargs["extensions"]`；其他写入目标为 `middleware_kwargs["authorization_provider"]`、`middleware_kwargs["skill_authorization"]`、`middleware_kwargs["mcp_routing_middleware"]`、`self._stop_reason_middlewares`、`self._recursion_limit`、`self._return_direct_tools`。

执行 `middlewares.append(ChildInputContinuationMiddleware(self.input_gate))`。捕获 `ReasoningPolicyError as exc` 的边界见源码，显式抛出路径涉及 `ValueError`。分支返回 `agent`，提前返回结束该路径。

源码依据：[_create_agent 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1131-L1326)


### 5.26.16 辅助函数与局部契约

- **`SubagentExecutor._resolve_recursion_limit`**（`middlewares`）：以 `resolve_recursion_limit(self.config.max_turns, middlewares)` 计算 `recursion_limit`；以 `find_jumping_hooks(middlewares)` 计算 `jumping_hooks`；返回 `recursion_limit`。

  源码依据：[_resolve_recursion_limit 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1327-L1364)


### 5.26.17 SubagentExecutor._describe_assembly

参数：`model_config, effective_reasoning, tools, middlewares, deferred_setup, extensions`。

以 `SimpleNamespace( model=self.model_name, use="unknown", supports_thinking=False, supports_reasoning_effort=False, supports_vision=False, )` 计算 `model_config`。关键判定为 `not getattr(extensions, "has_agent_assembly_observers", False)`、`model_config is None`。以 `descriptor` 计算 `self.assembly_descriptor`。

捕获 `Exception` 的边界见源码，分支返回 `None`，提前返回结束该路径。

源码依据：[_describe_assembly 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1365-L1437)


### 5.26.18 辅助函数与局部契约

- **`SubagentExecutor._consume_guard_stop_reason`**（无显式业务参数）：在 `循环 for mw in self._stop_reason_middlewares` 分支中，以 `mw.consume_stop_reason(self.run_id)` 计算 `reason`；在 `循环 for mw in self._stop_reason_middlewares` → `条件 reason is not None` 分支中，返回 `reason`；返回 `None`。

  源码依据：[_consume_guard_stop_reason 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1438-L1457)


### 5.26.19 SubagentExecutor._load_skills

无显式业务参数。

以 `{"app_config": self.app_config} if self.app_config is not None else {}` 计算 `storage_kwargs`；以 `await asyncio.to_thread(storage.load_skills, enabled_only=True)` 计算 `all_skills`。关键判定为 `self.config.skills is not None and len(self.config.skills) == 0`、`not all_skills`、`self.config.skills is not None`。

捕获 `Exception` 的边界见源码，显式抛出路径涉及 `当前异常`。分支返回 `[]`、`[s for s in all_skills if s.name in allowed]`、`all_skills`，提前返回结束该路径。

源码依据：[_load_skills 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1458-L1514)


### 5.26.20 辅助函数与局部契约

- **`SubagentExecutor._resolve_skill_authorization`**（无显式业务参数）：以 `getattr(self, "_skill_authorization", None)` 计算 `resolved`；在 `条件 resolved is None` 分支中，以 `resolve_skill_authorization(self._skill_authz_context(), self._get_resolved_app_config())` 计算 `resolved`；在 `条件 resolved is None` 分支中，以 `resolved` 计算 `self._skill_authorization`；返回 `resolved`。

  源码依据：[_resolve_skill_authorization 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1515-L1534)


- **`SubagentExecutor._skill_authz_context`**（无显式业务参数）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "user_id": self.user_id, "user_role": self.user_role, "oauth_provider": self.oauth_provider,
     "oauth_id": self.oauth_id, "channel_user_id": self.channel_user_id,
     "is_internal": self.is_internal, "authz_attributes": self.authz_attributes,
     }
  ```


  源码依据：[_skill_authz_context 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1535-L1547)


### 5.26.21 SubagentExecutor._build_initial_state

参数：`task`。

加载获准技能与工具，按 policy/deferred discovery 构造子任务系统提示、上下文快照和任务 HumanMessage；显式继承 sandbox/thread_data，深拷贝 uploaded_files，返回初态及最终工具。

源码依据：[_build_initial_state 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1548-L1716)


### 5.26.22 SubagentExecutor._aexecute

参数：`task, result_holder=None`。

**容量门与状态变化**

输入为任务文本及可选共享 `SubagentResult`。没有 holder 时创建 pending 结果；trace 以显式数据重新绑定，保证切换到隔离事件循环时仍能关联父请求。

1. 选择显式注入的 execution capacity，否则使用进程默认容量。有 input gate 且不允许排队时用 `slot(wait=False)`，其余路径等待容量。
2. 拿到 slot 后在结果状态锁内检查是否已终态，避免排队期间取消的任务再次开始；随后改为 running、记录 started_at，再调用 `_aexecute_admitted`。
3. 容量拒绝转为 FAILED 结果并标记 admission failure。finally 关闭 input gate，最后设置 `execution_done_event`。

源码依据：[容量准入与实际执行完成标记](../backend/packages/harness/deerflow/subagents/executor.py#L1717-L1758)

**边界**

slot 由异步上下文归还；终态字段与 execution_done_event 是两个条件。同步等待者可先记录超时失败，再请求底层取消，因此终态已可见时，底层协程仍可能尚未完成资源归还。

### 5.26.23 SubagentExecutor._aexecute_admitted

参数：`task, result_holder=None`。

**职责与输入继承**

容量已取得后，负责建立并消费实际子图。结果 holder 可被同步等待者与后台查询共同观察。任务拥有独立执行 ID、模型消息与扩展 task store，但可能显式共享父线程文件、sandbox 与宿主服务。

1. 建立子任务 sandbox lease owner，装配扩展 TaskInfo 与生命周期 store；缺 run_id 时不伪造父 run 关联。已有 cancel_event 时直接转 cancelled，不建图。
2. `_build_initial_state` 加载获准技能和工具，生成子任务输入；`_create_agent` 根据真实中间件节点数换算 recursion limit。配置内不手写父 checkpoint 坐标，图框架按子图 namespace 继承；业务 thread_id 经 context 传递。
3. 传递当前可信身份、授权属性、知识范围、审批 runtime 与独立命令 scope。文件归属取父线程，不能从“子图独立”推出“文件系统隔离”。

源码依据：[初态、子图与上下文建立](../backend/packages/harness/deerflow/subagents/executor.py#L1760-L1976)

4. 流式消费子图，聚合 AI 消息、token、工具 receipt 和验收资料。按消息 ID 集合与已处理尾部游标减少重复扫描；流更新可先于子任务最终结果出现。
5. 正常、取消、超时、递归限制或执行异常分别形成结果与 stop reason；终态写入通过结果自身的状态锁协调，避免迟到成功覆盖已经公布的失败。
6. finally 执行 task-stop 通知、命令取消与 sandbox lease 释放；外层 `_aexecute` 此后才归还容量并设置实际完成事件。

源码依据：[流消费、终态及资源收尾](../backend/packages/harness/deerflow/subagents/executor.py#L1977-L2244)

**故障传播**

返回结构化结果让父工具能够回填 ledger 和模型反馈；失败不等于抛到整个父 run，取决于 task 工具如何消费结果。对外部已生效动作没有通用回滚，父子共享文件也没有被 lease 自动串行化。

### 5.26.24 辅助函数与局部契约

- **`SubagentExecutor._aexecute_admitted.terminal_receipts`**（`prefer_citing_turn=False`）：父函数内定义的回调。在 `条件 not getattr(verification_cfg, "receipts_enabled", True)` 分支中，返回 `None`；返回 `_harvest_tool_receipts(final_state, prefer_citing_turn=prefer_citing_turn)`。

  源码依据：[terminal_receipts 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1836-L1840)


- **`SubagentExecutor._aexecute_admitted.current_bash_executions`**（无显式业务参数）：父函数内定义的回调。在 `条件 not self.acceptance_criteria` 分支中，返回 `None`；返回 `_harvest_bash_executions(final_state, summary_cache=bash_summary_cache)`。

  源码依据：[current_bash_executions 实现](../backend/packages/harness/deerflow/subagents/executor.py#L1843-L1851)


- **`SubagentExecutor._aexecute_admitted.cleanup_execution`**（无显式业务参数）：父函数内定义的回调。关键判定为 `execution_context is not None`、`result.status in (SubagentStatus.CANCELLED, SubagentStatus.FAILED, SubagentStatus.TIMED_OUT)`、`task_info is not None and task_store is not None`，相应动作只在对应分支执行。等待 `cancel_sandbox_execution_lease_async(execution_context)` 完成；等待 `release_sandbox_execution_lease_async(execution_context)` 完成。捕获 `Exception` 的边界见源码，捕获不等于已恢复。finally 包含 等待 `release_sandbox_execution_lease_async(execution_context)` 完成；等待 「表达式摘要 1」 完成。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  notify_task_stop( loaded_extensions, task_store, task_info, subagent_task_outcome( cancelled=result.status is
      SubagentStatus.CANCELLED, succeeded=result.status is SubagentStatus.COMPLETED,
       ), timeout=_EXTENSION_TASK_NOTIFY_TIMEOUT_SECONDS, )
  ```


  源码依据：[cleanup_execution 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2205-L2242)


### 5.26.25 SubagentExecutor._execute_in_isolated_loop

参数：`task, result_holder=None`。

**同步调用与异步执行的桥接**

同步 API 把 `_aexecute` 提交到持久隔离事件循环，复制可传递的父 Context，用 `future.result(timeout=...)` 阻塞当前调用线程。复用长期 loop 使共享异步客户端不必绑定每次创建又关闭的临时 loop。

发生 `FuturesTimeoutError` 时，先尝试把共享结果置 FAILED，再设置 cancel_event 并取消 future，随后重新抛错。`execute` 外层记录异常并返回失败 holder。这个顺序使迟到的清理观察者不把已公布的同步超时结果改成另一终态。

源码依据：[同步超时与取消顺序](../backend/packages/harness/deerflow/subagents/executor.py#L2245-L2285)

**关键边界**

`future.cancel()` 请求取消，不等于底层 finally 已结束。调用者返回超时结果时，容量 slot、通知或 sandbox 操作仍可能在收尾；`execution_done_event` 才标记外层执行者完成。

### 5.26.26 辅助函数与局部契约

- **`SubagentExecutor.execute`**（`task, result_holder=None`）：以 `result_holder` 计算 `result`；在 `条件 result is None` 分支中，以 `SubagentResult( task_id=str(uuid.uuid4())[:8], trace_id=self.trace_id, status=SubagentStatus.PENDING, )` 计算 `result`；在 `try` 分支中，返回 `self._execute_in_isolated_loop(task, result)`；在 `捕获 Exception as e` 分支中，执行 `result.try_set_terminal(SubagentStatus.FAILED, error=str(e))`；在 `捕获 Exception as e` 分支中，返回 `result`。

  源码依据：[execute 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2287-L2316)


### 5.26.27 SubagentExecutor.execute_async

参数：`task, task_id=None`。

创建后台 result/future、超时包装与取消回调，注册后立即返回可查询任务；超时/终态与底层 execution_done 分开，supervisor 完成后才可安全清理注册。

源码依据：[execute_async 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2317-L2501)


### 5.26.28 SubagentExecutor.execute_async.run_with_timeout

无显式业务参数。

父函数内定义的回调。以 `asyncio.create_task( self._aexecute(task, result), name=f"subagent-{self.config.name}-execution", )` 计算 `execution_task`。关键判定为 `result.cancel_event.is_set()`、`execution_task in done`、`not execution_task.done()`。执行 `execution_started.set()`；执行 `result.cancel_event.set()`。

执行 `execution_task.cancel()`；执行 `execution_task.cancel()`。捕获 `asyncio.CancelledError`、`(asyncio.CancelledError, Exception)`、`Exception as exc` 的边界见源码，分支返回 `result`、`execution_task.result()`，提前返回结束该路径。finally 包含 执行 `_mark_background_execution_done(execution_id, result)`。

源码依据：[run_with_timeout 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2364-L2443)


### 5.26.29 辅助函数与局部契约

- **`SubagentExecutor.execute_async.publish_pre_start_cancel`**（无显式业务参数）：父函数内定义的回调。在 `条件 execution_started.is_set()` 分支中，返回 None，提前结束当前路径；执行 「表达式摘要 1」；执行 `_mark_background_execution_done(execution_id, result)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  result.try_set_terminal( SubagentStatus.CANCELLED, error="Cancelled before the execution started",
     tool_receipts=result.snapshot_tool_receipts(), )
  ```


  源码依据：[publish_pre_start_cancel 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2459-L2481)


- **`SubagentExecutor.execute_async.forget_future`**（`completed_future`）：父函数内定义的回调。在 `条件 completed_future.cancelled()` 分支中，执行 `_run_on_isolated_loop_if_live(publish_pre_start_cancel)`；在 `同层否则分支` 分支中，执行 `completed_future.exception()`；在 `资源/同步边界 _background_tasks_lock` 分支中，执行 `_background_futures.pop(execution_id, None)`。

  源码依据：[forget_future 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2482-L2497)


- **`request_cancel_background_task`**（`execution_id`）：在 `资源/同步边界 _background_tasks_lock` 分支中，以 `_background_tasks.get(execution_id)` 计算 `result`；在 `资源/同步边界 _background_tasks_lock` 分支中，以 `_background_futures.get(execution_id) if result is not None else None` 计算 `future`；在 `条件 result is not None` 分支中，执行 `result.cancel_event.set()`；在 `条件 result is not None` → `条件 future is not None` 分支中，执行 `future.cancel()`。

  源码依据：[request_cancel_background_task 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2505-L2527)


- **`get_background_task_result`**（`execution_id`）：在 `资源/同步边界 _background_tasks_lock` 分支中，返回 `_background_tasks.get(execution_id)`。

  源码依据：[get_background_task_result 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2528-L2540)


- **`list_background_tasks`**（无显式业务参数）：在 `资源/同步边界 _background_tasks_lock` 分支中，返回 `list(_background_tasks.values())`。

  源码依据：[list_background_tasks 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2541-L2550)


- **`_remove_background_task_locked`**（`execution_id`）：执行 `_background_tasks.pop(execution_id, None)`；执行 `_background_futures.pop(execution_id, None)`；执行 `_background_cleanup_requested.discard(execution_id)`。

  源码依据：[_remove_background_task_locked 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2551-L2556)


- **`_mark_background_execution_done`**（`execution_id, result`）：执行 `result.execution_done_event.set()`；在 `资源/同步边界 _background_tasks_lock` → `条件 execution_id in _background_cleanup_requested and _background_tasks.get(execution_id) is result` 分支中，执行 `_remove_background_task_locked(execution_id)`。

  源码依据：[_mark_background_execution_done 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2557-L2566)


- **`_supervise_background_execution`**（`execution_id, result, task`）：执行 `_background_execution_tasks.add(task)`；建立局部回调 `def settled(completed: asyncio.Task[SubagentResult]) -> None`，行为在独立条目展开；执行 `task.add_done_callback(settled)`。

  源码依据：[_supervise_background_execution 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2567-L2590)


- **`_supervise_background_execution.settled`**（`completed`）：父函数内定义的回调。执行 `_background_execution_tasks.discard(completed)`；在 `条件 not completed.cancelled()` → `try` 分支中，执行 `completed.result()`；执行 `_mark_background_execution_done(execution_id, result)`。

  源码依据：[settled 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2575-L2587)


- **`cleanup_background_task`**（`execution_id`）：以 `_background_tasks.get(execution_id)` 计算 `result`。关键判定为 `result is None`、`terminal and result.execution_done_event.is_set()`、`terminal`，相应动作只在对应分支执行。进入 `_background_tasks_lock` 边界，退出由对应 context manager 处理。执行 `_background_cleanup_requested.discard(execution_id)`；执行 `_background_cleanup_requested.add(execution_id)`。分支返回 `None`，提前返回结束该路径。

  源码依据：[cleanup_background_task 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2591-L2629)


- **`force_cleanup_background_task`**（`execution_id`）：在 `资源/同步边界 _background_tasks_lock` 分支中，执行 `_remove_background_task_locked(execution_id)`。

  源码依据：[force_cleanup_background_task 实现](../backend/packages/harness/deerflow/subagents/executor.py#L2630-L2648)


## 5.27 子任务运行资源容器

源码位置：`backend/packages/harness/deerflow/subagents/runtime.py`。

显式运行时把配置解析的 capacity 和 durable batch worker 交给调用者管理；进入 async context 对应 start，退出对应 stop。它为直接 SDK 提供隔离运行资源，不自动变成跨 Gateway 全局容量。

### 5.27.1 SubagentRuntime.__init__

**参数摘要**

```text
    config=None,
    max_total_per_run=DEFAULT_MAX_TOTAL_SUBAGENTS_PER_RUN,
    batch_submitter=None,
    batch_repository=None,
    batch_config=None,
    app_config=None
```

关键判定为 `not MIN_TOTAL_SUBAGENTS_PER_RUN <= max_total_per_run <= MAX_TOTAL_SUBAGENTS_PER_RUN`、`batch_submitter is not None and batch_repository is not None`、`batch_repository is not None and not bool(getattr(batch_config, "enabled", False))`。

以 `(config or SubagentRuntimeConfig()).model_copy(deep=True)` 计算 `self.config`；以 `max_total_per_run` 计算 `self.max_total_per_run`；其他写入目标为 `self.app_config`、`self.execution_capacity`、`self.batch_config`、`self._external_batch_submitter`、`self._owned_batch_service`、`self._batch_started`、`self._lifecycle_lock`。

显式抛出路径涉及 `ValueError`。

源码依据：[__init__ 实现](../backend/packages/harness/deerflow/subagents/runtime.py#L37-L78)


### 5.27.2 辅助函数与局部契约

- **`SubagentRuntime.from_app_config`**（`app_config, batch_repository=None`）：以 `getattr(app_config, "subagent_runtime", None)` 计算 `runtime_config`。关键判定为 `not isinstance(runtime_config, SubagentRuntimeConfig)`、`batch_repository is not None`、`not isinstance(configured_batches, SubagentBatchesConfig)`，相应动作只在对应分支执行。分支返回 「表达式摘要 1」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  cls( runtime_config, max_total_per_run=max_total_per_run, batch_repository=batch_repository,
     batch_config=batch_config, app_config=app_config, )
  ```


  源码依据：[from_app_config 实现](../backend/packages/harness/deerflow/subagents/runtime.py#L80-L111)


- **`SubagentRuntime.batch_submitter`**（无显式业务参数）：在 `条件 self._external_batch_submitter is not None` 分支中，返回 `self._external_batch_submitter`；在 `条件 self._batch_started` 分支中，返回 `self._owned_batch_service`；返回 `None`。

  源码依据：[batch_submitter 实现](../backend/packages/harness/deerflow/subagents/runtime.py#L113-L119)


- **`SubagentRuntime.start`**（无显式业务参数）：在 `条件 self._owned_batch_service is None` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._lifecycle_lock` → `条件 self._batch_started` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._lifecycle_lock` 分支中，等待 `self._owned_batch_service.start()` 完成；在 `资源/同步边界 self._lifecycle_lock` 分支中，以 `True` 计算 `self._batch_started`。

  源码依据：[start 实现](../backend/packages/harness/deerflow/subagents/runtime.py#L120-L130)


- **`SubagentRuntime.stop`**（无显式业务参数）：以 `self._owned_batch_service` 计算 `service`。关键判定为 `service is None`、`not self._batch_started`、`cancellation is None`，相应动作只在对应分支执行。进入 `self._lifecycle_lock` 边界，退出由对应 context manager 处理。以 `False` 计算 `self._batch_started`。以 `asyncio.create_task(service.stop(), name="subagent-runtime-batch-stop")` 计算 `stop_task`；等待 `asyncio.wait({stop_task})` 完成。捕获 `asyncio.CancelledError as exc`、`(asyncio.CancelledError, Exception) as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `cancellation`。分支返回 `None`，提前返回结束该路径。

  源码依据：[stop 实现](../backend/packages/harness/deerflow/subagents/runtime.py#L131-L168)


- **`SubagentRuntime.__aenter__`**（无显式业务参数）：等待 `self.start()` 完成；返回 `self`。

  源码依据：[__aenter__ 实现](../backend/packages/harness/deerflow/subagents/runtime.py#L169-L172)


- **`SubagentRuntime.__aexit__`**（`exc_type, exc, tb`）：等待 `self.stop()` 完成。

  源码依据：[__aexit__ 实现](../backend/packages/harness/deerflow/subagents/runtime.py#L173-L175)


## 5.28 子任务容量准入

源码位置：`backend/packages/harness/deerflow/subagents/capacity.py`。

绑定当前 event loop 的 FIFO 进程内容量限制器。asyncio.Lock 保护运行计数和 Future 等待者，模块级 threading.Lock 只保护配置/controller 安装；活跃期间拒绝迁移到另一 loop，完全空闲才可重建。取消清除与已交付槽位共同决定释放义务。

### 5.28.1 辅助函数与局部契约

- **`SubagentExecutionCapacity.__init__`**（`config`）：以 `config` 计算 `self._config`；以 `asyncio.Lock()` 计算 `self._lock`；以 `0` 计算 `self._running`；以 `deque()` 计算 `self._waiters: deque[asyncio.Future[None]]`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L40-L45)


- **`SubagentExecutionCapacity.snapshot`**（无显式业务参数）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  SubagentCapacitySnapshot( max_running=self._config.max_running, running=self._running,
     max_queued=self._config.max_queued, queued=len(self._waiters),
     admission_policy=self._config.admission_policy, )
  ```


  源码依据：[snapshot 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L46-L61)


- **`SubagentExecutionCapacity._acquire`**（`wait=True`）：锁内有空位则占用，否则按 wait 决定排队或抛容量错误；等待者取消时移除或归还已交付槽位，防止容量泄漏。

  源码依据：[_acquire 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L62-L86)


- **`SubagentExecutionCapacity._remove_abandoned_waiter`**（`waiter`）：在 `资源/同步边界 self._lock` → `try` 分支中，执行 `self._waiters.remove(waiter)`；在 `资源/同步边界 self._lock` → `捕获 ValueError` → `条件 waiter.done() and not waiter.cancelled()` 分支中，执行 `self._release_locked()`。

  源码依据：[_remove_abandoned_waiter 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L87-L96)


- **`SubagentExecutionCapacity._release_locked`**（无显式业务参数）：在锁内优先将槽位交给仍有效的等待者，已取消者跳过；无等待者才减少 running，避免释放和新获取之间错误扩容。

  源码依据：[_release_locked 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L97-L108)


- **`SubagentExecutionCapacity._release`**（无显式业务参数）：在 `资源/同步边界 self._lock` 分支中，执行 `self._release_locked()`。

  源码依据：[_release 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L109-L112)


- **`SubagentExecutionCapacity._release_cancellation_safe`**（无显式业务参数）：等待 `self._run_cancellation_safe(self._release())` 完成。

  源码依据：[_release_cancellation_safe 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L113-L116)


- **`SubagentExecutionCapacity._run_cancellation_safe`**（`operation`）：以 `asyncio.create_task(operation)` 计算 `cleanup_task`；以 `None` 计算 `cancellation: asyncio.CancelledError | None`；在 `循环 while not cleanup_task.done()` → `try` 分支中，等待 `asyncio.shield(cleanup_task)` 完成；在 `循环 while not cleanup_task.done()` → `捕获 asyncio.CancelledError as exc` → `条件 cancellation is None` 分支中，以 `exc` 计算 `cancellation`；在 `条件 cancellation is not None` → `try` 分支中，执行 `cleanup_task.result()`；在 `条件 cancellation is not None` → `捕获 Exception as exc` 分支中，抛出 `cancellation from exc`；在 `条件 cancellation is not None` 分支中，抛出 `cancellation`；执行 `cleanup_task.result()`。

  源码依据：[_run_cancellation_safe 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L118-L141)


- **`SubagentExecutionCapacity.slot`**（`wait=True`）：异步 context 在进入时取得槽位，退出 finally 以取消安全方式归还；调用者无需在每条异常路径手动减计数。

  源码依据：[slot 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L143-L150)


- **`configure_subagent_execution_capacity`**（`config`）：使用外层变量 `global _config, _controller, _controller_loop`；以 `config.model_copy(deep=True)` 计算 `candidate`；在 `资源/同步边界 _state_lock` → `条件 _config == candidate` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 _state_lock` → `条件 _controller is not None` 分支中，以 `_controller.snapshot()` 计算 `snapshot`；在 `资源/同步边界 _state_lock` → `条件 _controller is not None` → `条件 snapshot.running or snapshot.queued` 分支中，抛出 `RuntimeError("Cannot reconfigure subagent capacity while executions are active")`；在 `资源/同步边界 _state_lock` 分支中，以 `candidate` 计算 `_config`；在 `资源/同步边界 _state_lock` 分支中，以 `None` 计算 `_controller`；在 `资源/同步边界 _state_lock` 分支中，以 `None` 计算 `_controller_loop`。

  源码依据：[configure_subagent_execution_capacity 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L157-L175)


- **`get_subagent_execution_capacity`**（无显式业务参数）：以 `asyncio.get_running_loop()` 计算 `loop`；以 `SubagentExecutionCapacity(_config)` 计算 `_controller`。关键判定为 `_controller is None`、`_controller_loop is not loop`、`snapshot.running or snapshot.queued`，相应动作只在对应分支执行。进入 `_state_lock` 边界，退出由对应 context manager 处理。显式抛出路径涉及 `RuntimeError`。分支返回 `_controller`，提前返回结束该路径。

  源码依据：[get_subagent_execution_capacity 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L176-L196)


- **`configured_subagent_max_running`**（无显式业务参数）：在 `资源/同步边界 _state_lock` 分支中，返回 `_config.max_running`。

  源码依据：[configured_subagent_max_running 实现](../backend/packages/harness/deerflow/subagents/capacity.py#L197-L201)


## 5.29 进程内嵌入客户端

源码位置：`backend/packages/harness/deerflow/client.py`。

同步嵌入客户端既可运行 stream/chat，也提供本地线程、模型、技能、记忆、上传及产物接口。其主执行链共用 assembly；返回值是 Python 对象，不经 HTTP/SSE。辅助管理函数有自身文件副作用，本文仅分析、未调用。

### 5.29.1 辅助函数与局部契约

- **`_stream_with_sandbox_lease_cleanup`**（`items, context`）：在 `try` 分支中，交付 `items` 后暂停生成器，待消费方继续；在 `finally 收尾` → `try` 分支中，执行 `release_sandbox_execution_lease(context)`。

  源码依据：[_stream_with_sandbox_lease_cleanup 实现](../backend/packages/harness/deerflow/client.py#L106-L118)


- **`_run_async_from_sync`**（`coro`）：在 `try` 分支中，以 `asyncio.get_running_loop()` 计算 `loop`；在 `捕获 RuntimeError` 分支中，以 `None` 计算 `loop`；在 `条件 loop is not None and loop.is_running()` → `资源/同步边界 concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor` 分支中，返回 `executor.submit(asyncio.run, coro).result()`；返回 `asyncio.run(coro)`。

  源码依据：[_run_async_from_sync 实现](../backend/packages/harness/deerflow/client.py#L119-L131)


- **`_AIMessageAccumulator.__init__`**（无显式业务参数）：以 `{}` 计算 `self._chunks: dict[str, list[str]]`；以 `""` 计算 `self.last_id`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/client.py#L160-L164)


- **`_AIMessageAccumulator.observe`**（`event`）：在 `条件 event.type != "messages-tuple" or event.data.get("type") != "ai"` 分支中，返回 None，提前结束当前路径；在 `条件 delta := event.data.get("content", "")` 分支中，以 `event.data.get("id") or ""` 计算 `msg_id`；在 `条件 delta := event.data.get("content", "")` 分支中，执行 `self._chunks.setdefault(msg_id, []).append(delta)`；在 `条件 delta := event.data.get("content", "")` 分支中，以 `msg_id` 计算 `self.last_id`。

  源码依据：[observe 实现](../backend/packages/harness/deerflow/client.py#L165-L172)


- **`_AIMessageAccumulator.answer`**（无显式业务参数）：返回 `"".join(self._chunks.get(self.last_id, ()))`。

  源码依据：[answer 实现](../backend/packages/harness/deerflow/client.py#L173-L176)


### 5.29.2 DeerFlowClient.__init__

**参数摘要**

```text
    config_path=None,
    checkpointer=None,
    model_name=None,
    thinking_enabled=True,
    subagent_enabled=False,
    plan_mode=False,
    agent_name=None,
    available_skills=None,
    middlewares=None,
    environment=None
```

以 `getattr(self._app_config, "subagent_runtime", None)` 计算 `runtime_config`。关键判定为 `config_path is not None`、`not isinstance(runtime_config, SubagentRuntimeConfig)`、`agent_name is not None and not AGENT_NAME_PATTERN.fullmatch(agent_name)`。

以 `get_app_config()` 计算 `self._app_config`；以 `runtime_config.max_running` 计算 `self._subagent_execution_capacity`；其他写入目标为 `self._checkpoint_channel_mode`、`self._checkpoint_snapshot_frequency`、`self._checkpointer`、`self._model_name`、`self._thinking_enabled`、`self._subagent_enabled`、`self._plan_mode`、`self._agent_name`、`self._available_skills`、`self._middlewares`、`self._environment`、`self._thread_incarnations: dict[str, str]`、`self._agent`、`self._agent_config_key: tuple | None`、`self._effective_model_name: str | None`、`self._loaded_agent_config_key: tuple[str, str] | None`、`self._loaded_agent_config`。

显式抛出路径涉及 `ValueError`。

源码依据：[__init__ 实现](../backend/packages/harness/deerflow/client.py#L211-L281)


### 5.29.3 辅助函数与局部契约

- **`DeerFlowClient.reset_agent`**（无显式业务参数）：以 `None` 计算 `self._agent`；以 `None` 计算 `self._agent_config_key`；以 `None` 计算 `self._effective_model_name`；以 `None` 计算 `self._loaded_agent_config_key`；以 `None` 计算 `self._loaded_agent_config`。

  源码依据：[reset_agent 实现](../backend/packages/harness/deerflow/client.py#L282-L298)


- **`DeerFlowClient._atomic_write_json`**（`path, data`）：执行 `atomic_write_extensions_config(path, data)`。

  源码依据：[_atomic_write_json 实现](../backend/packages/harness/deerflow/client.py#L300-L303)


- **`DeerFlowClient._write_skill_enabled_state`**（`config_path, name, enabled`）：以 `read_raw_extensions_config(config_path)` 计算 `config_data`；执行 `set_raw_skill_enabled(config_data, name, enabled)`；执行 `validate_raw_extensions_config(config_data)`；执行 `cls._atomic_write_json(config_path, config_data)`；执行 `reload_extensions_config()`。

  源码依据：[_write_skill_enabled_state 实现](../backend/packages/harness/deerflow/client.py#L305-L316)


- **`DeerFlowClient._get_runnable_config`**（`thread_id, **overrides`）：以 「表达式摘要 1」 计算 `configurable`；返回 `RunnableConfig( configurable=configurable, recursion_limit=overrides.get("recursion_limit", 100), )`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "thread_id": thread_id, "model_name": overrides.get("model_name",
       self._model_name), "thinking_enabled": overrides.get("thinking_enabled",
       self._thinking_enabled), "is_plan_mode": overrides.get("plan_mode",
       self._plan_mode), "subagent_enabled": overrides.get("subagent_enabled",
       self._subagent_enabled), }
  ```


  源码依据：[_get_runnable_config 实现](../backend/packages/harness/deerflow/client.py#L317-L330)


### 5.29.4 DeerFlowClient._ensure_agent

参数：`config, context=None`。

按当前模型/技能/配置签名决定重建或复用图，组装可信本地 context 和运行依赖；配置变化会失效缓存，不把首次创建的工具视图永久复用。

源码依据：[_ensure_agent 实现](../backend/packages/harness/deerflow/client.py#L331-L577)


### 5.29.5 辅助函数与局部契约

- **`DeerFlowClient._get_tools`**（`model_name, subagent_enabled, mcp_plugins=None`）：返回 `get_available_tools(model_name=model_name, subagent_enabled=subagent_enabled, mcp_plugins=mcp_plugins)`。

  源码依据：[_get_tools 实现](../backend/packages/harness/deerflow/client.py#L579-L584)


- **`DeerFlowClient._serialize_tool_calls`**（`tool_calls`）：返回 `[{"name": tc["name"], "args": tc["args"], "id": tc.get("id")} for tc in tool_calls]`。

  源码依据：[_serialize_tool_calls 实现](../backend/packages/harness/deerflow/client.py#L586-L589)


- **`DeerFlowClient._serialize_additional_kwargs`**（`msg`）：以 `getattr(msg, "additional_kwargs", None)` 计算 `additional_kwargs`；在 `条件 isinstance(additional_kwargs, dict) and additional_kwargs` 分支中，返回 `dict(additional_kwargs)`；返回 `None`。

  源码依据：[_serialize_additional_kwargs 实现](../backend/packages/harness/deerflow/client.py#L591-L597)


- **`DeerFlowClient._ai_text_event`**（`msg_id, text, usage, additional_kwargs=None`）：以 `{"type": "ai", "content": text, "id": msg_id}` 计算 `data: dict[str, Any]`；在 `条件 usage` 分支中，以 `usage` 计算 `data["usage_metadata"]`；在 `条件 additional_kwargs` 分支中，以 `additional_kwargs` 计算 `data["additional_kwargs"]`；返回 `StreamEvent(type="messages-tuple", data=data)`。

  源码依据：[_ai_text_event 实现](../backend/packages/harness/deerflow/client.py#L599-L607)


- **`DeerFlowClient._ai_tool_calls_event`**（`msg_id, tool_calls, additional_kwargs=None`）：以 `{ "type": "ai", "content": "", "id": msg_id, "tool_calls": DeerFlowClient._serialize_tool_calls(tool_calls), }` 计算 `data: dict[str, Any]`；在 `条件 additional_kwargs` 分支中，以 `additional_kwargs` 计算 `data["additional_kwargs"]`；返回 `StreamEvent(type="messages-tuple", data=data)`。

  源码依据：[_ai_tool_calls_event 实现](../backend/packages/harness/deerflow/client.py#L609-L620)


- **`DeerFlowClient._tool_message_event`**（`msg`）：以 `{ "type": "tool", "content": DeerFlowClient._extract_text(msg.content), "name": msg.name, "tool_call_id": msg.tool_call_id, "id": msg.id, }` 计算 `data: dict[str, Any]`；在 `条件 (artifact := getattr(msg, "artifact", None)) is not None` 分支中，以 `artifact` 计算 `data["artifact"]`；返回 `StreamEvent(type="messages-tuple", data=data)`。

  源码依据：[_tool_message_event 实现](../backend/packages/harness/deerflow/client.py#L622-L634)


- **`DeerFlowClient._serialize_message`**（`msg`）：以 `{"type": "ai", "content": msg.content, "id": getattr(msg, "id", None)}` 计算 `d: dict[str, Any]`；以 `{"type": "system", "content": msg.content, "id": getattr(msg, "id", None)}` 计算 `d`。关键判定为 `isinstance(msg, AIMessage)`、`msg.tool_calls`、`getattr(msg, "usage_metadata", None)`，相应动作只在对应分支执行。以 `DeerFlowClient._serialize_tool_calls(msg.tool_calls)` 计算 `d["tool_calls"]`；以 `msg.usage_metadata` 计算 `d["usage_metadata"]`；其他写入目标为 `d["additional_kwargs"]`、`d["artifact"]`。分支返回 `d`、`{"type": "unknown", "content": str(msg), "id": getattr(msg, "id", None)}`，提前返回结束该路径。

  源码依据：[_serialize_message 实现](../backend/packages/harness/deerflow/client.py#L636-L671)


### 5.29.6 DeerFlowClient._extract_text

参数：`content`。

以 `len(content) > 1 and all(isinstance(block, str) and len(block) <= 20 and any(ch in block for ch in '{}[]":,') for block in content)` 计算 `chunk_like`。关键判定为 `isinstance(content, str)`、`isinstance(content, list)`、`content and all(isinstance(block, str) for block in content)`。

执行 `pending_str_parts.append(block)`；执行 `pieces.append(text_val)`。分支返回 `content`、`"".join(content) if chunk_like else "\n".join(content)`、`"\n".join(pieces) if pieces else ""`、`str(content)`，提前返回结束该路径。

源码依据：[_extract_text 实现](../backend/packages/harness/deerflow/client.py#L673-L712)


### 5.29.7 辅助函数与局部契约

- **`DeerFlowClient._extract_text.flush_pending_str_parts`**（无显式业务参数）：父函数内定义的回调。在 `条件 pending_str_parts` 分支中，执行 `pieces.append("".join(pending_str_parts))`；在 `条件 pending_str_parts` 分支中，执行 `pending_str_parts.clear()`。

  源码依据：[flush_pending_str_parts 实现](../backend/packages/harness/deerflow/client.py#L691-L695)


- **`DeerFlowClient._get_thread_checkpointer`**（无显式业务参数）：以 `self._checkpointer` 计算 `checkpointer`；在 `条件 checkpointer is None` 分支中，以 `get_checkpointer()` 计算 `checkpointer`；返回 `checkpointer`。

  源码依据：[_get_thread_checkpointer 实现](../backend/packages/harness/deerflow/client.py#L713-L720)


- **`DeerFlowClient.get_goal`**（`thread_id`）：执行 `validate_thread_id(thread_id)`；以 `self._get_thread_checkpointer()` 计算 `checkpointer`；以 `_run_async_from_sync(read_thread_goal(checkpointer, thread_id))` 计算 `goal`；返回 `{"goal": goal}`。

  源码依据：[get_goal 实现](../backend/packages/harness/deerflow/client.py#L721-L727)


- **`DeerFlowClient.set_goal`**（`thread_id, objective, max_continuations=DEFAULT_MAX_GOAL_CONTINUATIONS`）：执行 `validate_thread_id(thread_id)`；以 `self._get_thread_checkpointer()` 计算 `checkpointer`；以 `build_goal_state(objective, max_continuations=max_continuations)` 计算 `goal`；建立局部回调 `async def _set_goal() -> None`，行为在独立条目展开；执行 `_run_async_from_sync(_set_goal())`；返回 `{"goal": goal}`。

  源码依据：[set_goal 实现](../backend/packages/harness/deerflow/client.py#L728-L746)


- **`DeerFlowClient.set_goal._set_goal`**（无显式业务参数）：父函数内定义的回调。在 `资源/同步边界 goal_thread_lock(thread_id)` 分支中，等待 `write_thread_goal(checkpointer, thread_id, goal, create_if_missing=True)` 完成。

  源码依据：[_set_goal 实现](../backend/packages/harness/deerflow/client.py#L740-L743)


- **`DeerFlowClient.clear_goal`**（`thread_id`）：执行 `validate_thread_id(thread_id)`；以 `self._get_thread_checkpointer()` 计算 `checkpointer`；建立局部回调 `async def _clear_goal() -> None`，行为在独立条目展开；在 `try` 分支中，执行 `_run_async_from_sync(_clear_goal())`；在 `捕获 LookupError` 分支中，该默认实现/分支不执行额外动作；返回 `{"goal": None}`。

  源码依据：[clear_goal 实现](../backend/packages/harness/deerflow/client.py#L747-L761)


- **`DeerFlowClient.clear_goal._clear_goal`**（无显式业务参数）：父函数内定义的回调。在 `资源/同步边界 goal_thread_lock(thread_id)` 分支中，等待 `write_thread_goal(checkpointer, thread_id, None)` 完成。

  源码依据：[_clear_goal 实现](../backend/packages/harness/deerflow/client.py#L752-L755)


### 5.29.8 DeerFlowClient.list_threads

参数：`limit=10, sort_by="created_at"`。

以 `self._get_thread_checkpointer()` 计算 `checkpointer`。关键判定为 `sort_by not in ("created_at", "updated_at")`、`latest is None`。

执行 「表达式摘要 1」。显式抛出路径涉及 `ValueError`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
threads.append( { "thread_id": span.thread_id, "created_at": first.checkpoint.get("ts") if first is not None else
    None, "updated_at": latest.checkpoint.get("ts"), "latest_checkpoint_id": span.latest_checkpoint_id,
     "title": latest.checkpoint.get("channel_values", {}).get("title"),
     } )
```


分支返回 `{"thread_list": threads}`，提前返回结束该路径。

源码依据：[list_threads 实现](../backend/packages/harness/deerflow/client.py#L762-L807)


### 5.29.9 辅助函数与局部契约

- **`DeerFlowClient.list_threads._checkpoint`**（`thread_id, checkpoint_id`）：父函数内定义的回调。返回 `checkpointer.get_tuple({"configurable": {"thread_id": thread_id, "checkpoint_ns": "", "checkpoint_id": checkpoint_id}})`。

  源码依据：[_checkpoint 实现](../backend/packages/harness/deerflow/client.py#L787-L789)


### 5.29.10 DeerFlowClient.get_thread

参数：`thread_id`。

以 `self._get_thread_checkpointer()` 计算 `checkpointer`。关键判定为 `self._agent is None`、`raw_checkpoint_id`、`"messages" in values`。

以 `list(getattr(raw_tuple, "pending_writes", ()) or ())` 计算 `pending_writes_by_checkpoint[raw_checkpoint_id]`；以 `[self._serialize_message(message) if hasattr(message, "content") else message for message in values["messages"]]` 计算 `values["messages"]`。

执行 「表达式摘要 1」。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
checkpoints.append( { "checkpoint_id": configurable.get("checkpoint_id"),
     "parent_checkpoint_id": parent_configurable.get("checkpoint_id"),
     "ts": snapshot.created_at, "metadata": snapshot.metadata, "values": values,
     "pending_writes": [{"task_id": write[0], "channel": write[1],
         "value": write[2]} for write in pending_writes], } )
```


显式抛出路径涉及 `RuntimeError`。分支返回 `{"thread_id": thread_id, "checkpoints": checkpoints}`，提前返回结束该路径。

源码依据：[get_thread 实现](../backend/packages/harness/deerflow/client.py#L808-L858)


### 5.29.11 DeerFlowClient.stream

参数：`message, thread_id=None, **kwargs`。

同步生成器入口，将一轮交给 _stream_turn；按 client 的目标/续跑协议推进，事件是进程内对象而非 HTTP 帧。

源码依据：[stream 实现](../backend/packages/harness/deerflow/client.py#L859-L912)


### 5.29.12 DeerFlowClient._stream_turn

参数：`message, thread_id=None, **kwargs`。

绑定 checkpointer 与上下文后消费 agent.stream 的多种 mode；按消息 ID 合并 token/工具增量并避免重发元数据，保留 guard 后追加文本；finally 关闭流及 sandbox holder。

源码依据：[_stream_turn 实现](../backend/packages/harness/deerflow/client.py#L913-L1313)


### 5.29.13 辅助函数与局部契约

- **`DeerFlowClient._stream_turn._account_usage`**（`msg_id, usage, is_chunk=False`）：父函数内定义的回调。以 `{key: usage.get(key, 0) or 0 for key in cumulative_usage}` 计算 `delta`；以 `{key: max(value - streamed.get(key, 0), 0) for key, value in delta.items()}` 计算 `delta`。关键判定为 `not usage`、`msg_id and msg_id in counted_usage_ids`、`msg_id`，相应动作只在对应分支执行。以 `streamed.get(key, 0) + value` 计算 `streamed[key]`；对 `cumulative_usage[key]` 执行 `+= value` 更新。执行 `counted_usage_ids.add(msg_id)`。分支返回 `None`、`delta`，提前返回结束该路径。

  源码依据：[_account_usage 实现](../backend/packages/harness/deerflow/client.py#L1098-L1126)


- **`DeerFlowClient._stream_turn._unsent_additional_kwargs`**（`msg_id, additional_kwargs`）：父函数内定义的回调。在 `条件 not additional_kwargs` 分支中，返回 `None`；在 `条件 not msg_id` 分支中，返回 `additional_kwargs`；以 `sent_additional_kwargs_by_id.setdefault(msg_id, {})` 计算 `sent`；以 `{key: value for key, value in additional_kwargs.items() if sent.get(key) != value}` 计算 `delta`；在 `条件 not delta` 分支中，返回 `None`；执行 `sent.update(delta)`；返回 `delta`。

  源码依据：[_unsent_additional_kwargs 实现](../backend/packages/harness/deerflow/client.py#L1127-L1140)


- **`DeerFlowClient.chat`**（`message, thread_id=None, **kwargs`）：完整消费 stream，使用 accumulator 归并回答并返回字符串；它等待本轮图结束，但不表示后台记忆提取结束。

  源码依据：[chat 实现](../backend/packages/harness/deerflow/client.py#L1314-L1341)


- **`DeerFlowClient.list_models`**（无显式业务参数）：以 `getattr(getattr(self._app_config, "token_usage", None), "enabled", False)` 计算 `token_usage_enabled`。关键判定为 `not isinstance(token_usage_enabled, bool)`，相应动作只在对应分支执行。分支返回 「表达式摘要 1」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "models": [ { "name": model.name, "model": getattr(model, "model",
           None), "display_name": getattr(model, "display_name", None),
         "description": getattr(model, "description", None), "supports_thinking": getattr(model,
           "supports_thinking", False), "supports_reasoning_effort": getattr(model,
           "supports_reasoning_effort", False), "reasoning": reasoning_capabilities_payload(resolve_reasoning_contract(model)),
         } for model in self._app_config.models ], "token_usage": {"enabled": token_usage_enabled},
     }
  ```


  源码依据：[list_models 实现](../backend/packages/harness/deerflow/client.py#L1342-L1368)


- **`DeerFlowClient.list_skills`**（`enabled_only=False`）：以 `get_or_new_user_skill_storage(get_effective_user_id(), app_config=self._app_config)` 计算 `storage`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "skills": [ { "name": s.name, "description": s.description, "license": s.license,
         "category": s.category, "enabled": s.enabled, } for s in storage.load_skills(enabled_only=enabled_only)
      ] }
  ```


  源码依据：[list_skills 实现](../backend/packages/harness/deerflow/client.py#L1369-L1392)


- **`DeerFlowClient._memory_scope_kwargs`**（`manager`）：在 `条件 self._agent_name is None` 分支中，返回 `{}`；在 `条件 getattr(manager, "supports_agent_scoped_management", False) is not True` 分支中，抛出 `NotImplementedError(f"agent-scoped memory management not supported by memory backend {type(manager).__name__}")`；返回 `{"agent_name": self._agent_name}`。

  源码依据：[_memory_scope_kwargs 实现](../backend/packages/harness/deerflow/client.py#L1393-L1412)


- **`DeerFlowClient.get_memory`**（无显式业务参数）：以 `get_memory_manager()` 计算 `manager`；返回 `manager.get_memory(user_id=get_effective_user_id(), **self._memory_scope_kwargs(manager))`。

  源码依据：[get_memory 实现](../backend/packages/harness/deerflow/client.py#L1413-L1423)


- **`DeerFlowClient.export_memory`**（无显式业务参数）：以 `get_memory_manager()` 计算 `manager`；返回 `manager.get_memory(user_id=get_effective_user_id(), **self._memory_scope_kwargs(manager))`。

  源码依据：[export_memory 实现](../backend/packages/harness/deerflow/client.py#L1424-L1430)


- **`DeerFlowClient.import_memory`**（`memory_data`）：以 `get_memory_manager()` 计算 `manager`；返回 `manager.import_memory(memory_data, user_id=get_effective_user_id(), **self._memory_scope_kwargs(manager))`。

  源码依据：[import_memory 实现](../backend/packages/harness/deerflow/client.py#L1431-L1437)


- **`DeerFlowClient.get_model`**（`name`）：以 `self._app_config.get_model_config(name)` 计算 `model`；在 `条件 model is None` 分支中，返回 `None`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "name": model.name, "model": getattr(model, "model", None), "display_name": getattr(model,
       "display_name", None), "description": getattr(model, "description",
       None), "supports_thinking": getattr(model, "supports_thinking",
       False), "supports_reasoning_effort": getattr(model, "supports_reasoning_effort",
       False), "reasoning": reasoning_capabilities_payload(resolve_reasoning_contract(model)),
     }
  ```


  源码依据：[get_model 实现](../backend/packages/harness/deerflow/client.py#L1438-L1464)


- **`DeerFlowClient.get_mcp_config`**（无显式业务参数）：以 `get_extensions_config()` 计算 `config`；返回 `{"mcp_servers": {name: server.model_dump() for name, server in config.mcp_servers.items()}}`。

  源码依据：[get_mcp_config 实现](../backend/packages/harness/deerflow/client.py#L1465-L1474)


### 5.29.14 DeerFlowClient.update_mcp_config

参数：`mcp_servers`。

以 `ExtensionsConfig.resolve_config_path()` 计算 `config_path`。关键判定为 `config_path is None`、`committed_failure is not None`、`committed_failure is not None`。进入 `extensions_config_write_lock, extensions_config_file_lock(config_path)` 边界，退出由对应 context manager 处理。

以 `copy.deepcopy(mcp_servers)` 计算 `config_data["mcpServers"]`；以 `plan_mcp_lifecycle(previous_raw, config_data)` 计算 `config_data[LIFECYCLE_KEY]`。捕获 `Exception as exc` 的边界见源码，显式抛出路径涉及 `FileNotFoundError`、`RuntimeError`。

分支返回 `{"mcp_servers": {name: server.model_dump() for name, server in reloaded.mcp_servers.items()}}`，提前返回结束该路径。finally 包含 执行 `self.reset_agent()`；以 `committed_failure` 计算 `_failed_pending, failure`。

源码依据：[update_mcp_config 实现](../backend/packages/harness/deerflow/client.py#L1475-L1548)


### 5.29.15 辅助函数与局部契约

- **`DeerFlowClient.get_skill`**（`name`）：以 `get_or_new_user_skill_storage(get_effective_user_id(), app_config=self._app_config)` 计算 `storage`；以 `next((s for s in storage.load_skills(enabled_only=False) if s.name == name), None)` 计算 `skill`；在 `条件 skill is None` 分支中，返回 `None`；返回 `{ "name": skill.name, "description": skill.description, "license": skill.license, "category": skill.category, "enabled": skill.enabled, }`。

  源码依据：[get_skill 实现](../backend/packages/harness/deerflow/client.py#L1549-L1569)


### 5.29.16 DeerFlowClient.update_skill

参数：`name, enabled`。

以 `get_or_new_user_skill_storage(get_effective_user_id(), app_config=self._app_config)` 计算 `storage`。关键判定为 `skill is None`、`skill.category == SkillCategory.PUBLIC`、`config_path is None`。

进入 `managed_global_state_write(storage, name), skill_projection_mutation(storage, "public", remove_names=removal_names)`、`extensions_config_write_lock, extensions_config_file_lock(config_path)`、`managed_global_state_write(storage, name), extensions_config_write_lock, extensions_config_file_lock(config_path)` 边界，退出由对应 context manager 处理。

以 `storage.load_skills(enabled_only=False)` 计算 `skills`；以 `next((s for s in storage.load_skills(enabled_only=False) if s.name == name), None)` 计算 `updated`。捕获 `Exception as exc` 的边界见源码，显式抛出路径涉及 `ValueError`、`FileNotFoundError`、`RuntimeError`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
{ "name": updated.name, "description": updated.description, "license": updated.license,
   "category": updated.category, "enabled": updated.enabled, }
```


源码依据：[update_skill 实现](../backend/packages/harness/deerflow/client.py#L1570-L1658)


### 5.29.17 辅助函数与局部契约

- **`DeerFlowClient.install_skill`**（`skill_path`）：返回 `get_or_new_user_skill_storage(get_effective_user_id(), app_config=self._app_config).install_skill_from_archive(skill_path)`。

  源码依据：[install_skill 实现](../backend/packages/harness/deerflow/client.py#L1659-L1677)


- **`DeerFlowClient.reload_memory`**（无显式业务参数）：以 `get_memory_manager()` 计算 `manager`；以 `get_effective_user_id()` 计算 `user_id`；以 `self._memory_scope_kwargs(manager)` 计算 `scope_kwargs`；在 `try` 分支中，返回 `manager.reload_memory(user_id=user_id, **scope_kwargs)`；在 `捕获 NotImplementedError` 分支中，执行 `pass # no reload concept; fall back to current memory below`；在 `try` 分支中，返回 `manager.get_memory(user_id=user_id, **scope_kwargs)`；在 `捕获 NotImplementedError` 分支中，抛出 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  NotImplementedError(f"reload_memory not supported by memory backend {type(manager).__name__}: implements neither reload_memory nor get_memory")
  from None
  ```


  源码依据：[reload_memory 实现](../backend/packages/harness/deerflow/client.py#L1678-L1704)


- **`DeerFlowClient.clear_memory`**（无显式业务参数）：以 `get_memory_manager()` 计算 `manager`；返回 `manager.clear_memory(user_id=get_effective_user_id(), **self._memory_scope_kwargs(manager))`。

  源码依据：[clear_memory 实现](../backend/packages/harness/deerflow/client.py#L1705-L1711)


- **`DeerFlowClient.create_memory_fact`**（`content, category="context", confidence=0.5`）：以 `get_memory_manager()` 计算 `manager`；以 「表达式摘要 1」 计算 `memory_data, fact_id`；在 `条件 fact_id is None` 分支中，抛出 `ValueError("Fact was not stored because the configured memory.max_facts capacity policy evicted it")`；返回 `memory_data`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  manager.create_fact(content=content, category=category, confidence=confidence,
     user_id=get_effective_user_id(), **self._memory_scope_kwargs(manager))
  ```


  源码依据：[create_memory_fact 实现](../backend/packages/harness/deerflow/client.py#L1712-L1721)


- **`DeerFlowClient.delete_memory_fact`**（`fact_id`）：以 `get_memory_manager()` 计算 `manager`；返回 `manager.delete_fact(fact_id, user_id=get_effective_user_id(), **self._memory_scope_kwargs(manager))`。

  源码依据：[delete_memory_fact 实现](../backend/packages/harness/deerflow/client.py#L1722-L1728)


- **`DeerFlowClient.update_memory_fact`**（`fact_id, content=None, category=None, confidence=None`）：以 `get_memory_manager()` 计算 `manager`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  manager.update_fact( fact_id=fact_id, content=content, category=category,
     confidence=confidence, user_id=get_effective_user_id(), **self._memory_scope_kwargs(manager),
     )
  ```


  源码依据：[update_memory_fact 实现](../backend/packages/harness/deerflow/client.py#L1729-L1748)


- **`DeerFlowClient.get_memory_config`**（无显式业务参数）：以 `get_memory_config()` 计算 `config`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "enabled": config.enabled, "mode": config.mode, "injection_enabled": config.injection_enabled,
     "shutdown_flush_timeout_seconds": config.shutdown_flush_timeout_seconds,
     "manager_class": config.manager_class, "backend_config": config.backend_config,
     }
  ```


  源码依据：[get_memory_config 实现](../backend/packages/harness/deerflow/client.py#L1749-L1766)


- **`DeerFlowClient.get_memory_status`**（无显式业务参数）：返回 `{ "config": self.get_memory_config(), "data": self.get_memory(), }`。

  源码依据：[get_memory_status 实现](../backend/packages/harness/deerflow/client.py#L1767-L1781)


### 5.29.18 DeerFlowClient.upload_files

参数：`thread_id, files`。

以 `[]` 计算 `resolved_files`。关键判定为 `not p.exists()`、`not p.is_file()`、`not has_convertible_file and p.suffix.lower() in CONVERTIBLE_EXTENSIONS`。进入 `os.scandir(uploads_dir) as entries`、`tempfile.TemporaryDirectory() as md_dir` 边界，退出由对应 context manager 处理。

以 `src_path.name` 计算 `info["original_filename"]`；以 `md_path.name` 计算 `info["markdown_file"]`；其他写入目标为 `info["markdown_path"]`、`info["markdown_virtual_path"]`、`info["markdown_artifact_url"]`。执行 `resolved_files.append((p, dest_name, claim_seed))`；执行 `skipped_files.append(dest_name)`。

捕获 `RuntimeError`、`UnsafeUploadPathError`、`Exception` 的边界见源码，显式抛出路径涉及 `FileNotFoundError`、`ValueError`。分支返回 `{ "success": not skipped_files, "files": uploaded_files, "message": message, "skipped_files": skipped_files, }`，提前返回结束该路径。

finally 包含 执行 `conversion_pool.shutdown(wait=True)`；执行 `conversion_sources.cleanup()`。

源码依据：[upload_files 实现](../backend/packages/harness/deerflow/client.py#L1782-L1961)


### 5.29.19 辅助函数与局部契约

- **`DeerFlowClient.upload_files._convert_in_thread`**（`path, output_path=None`）：父函数内定义的回调。返回 `asyncio.run(convert_file_to_markdown(path, output_path=output_path))`。

  源码依据：[_convert_in_thread 实现](../backend/packages/harness/deerflow/client.py#L1846-L1848)


- **`DeerFlowClient.list_uploads`**（`thread_id`）：执行 `validate_thread_id(thread_id)`；以 `get_uploads_dir(thread_id)` 计算 `uploads_dir`；以 `list_files_in_dir(uploads_dir)` 计算 `result`；返回 `enrich_file_listing(result, thread_id)`。

  源码依据：[list_uploads 实现](../backend/packages/harness/deerflow/client.py#L1962-L1976)


- **`DeerFlowClient.delete_upload`**（`thread_id, filename`）：执行 `validate_thread_id(thread_id)`；以 `get_uploads_dir(thread_id)` 计算 `uploads_dir`；返回 `delete_file_safe(uploads_dir, filename)`。

  源码依据：[delete_upload 实现](../backend/packages/harness/deerflow/client.py#L1977-L2000)


- **`DeerFlowClient.get_artifact`**（`thread_id, path`）：执行 `validate_thread_id(thread_id)`；在 `try` 分支中，以 `get_paths().resolve_virtual_path(thread_id, path, user_id=get_effective_user_id())` 计算 `actual`；在 `捕获 ValueError as exc` → `条件 "traversal" in str(exc)` 分支中，抛出 `PathTraversalError("Path traversal detected") from exc`；在 `捕获 ValueError as exc` 分支中，向外继续抛出当前异常；在 `条件 not actual.exists()` 分支中，抛出 `FileNotFoundError(f"Artifact not found: {path}")`；在 `条件 not actual.is_file()` 分支中，抛出 `ValueError(f"Path is not a file: {path}")`；以 `mimetypes.guess_type(actual)` 计算 `mime_type, _`；返回 `actual.read_bytes(), mime_type or "application/octet-stream"`。

  源码依据：[get_artifact 实现](../backend/packages/harness/deerflow/client.py#L2001-L2031)


## 5.30 运行准入与持久终态

源码位置：`backend/packages/harness/deerflow/runtime/runs/manager.py`。

运行注册与控制面协调器。局部 keyed locks 保护当前对象，持久 RunStore 的原子准入、lease/fence 和终态 ACK 决定跨 worker 正确性。终态、finalizing、持久确认与 registry 淘汰不能混为一个布尔值；取消不等于已收尾。

### 5.30.1 辅助函数与局部契约

- **`_generate_worker_id`**（无显式业务参数）：返回 `f"{socket.gethostname()}:{uuid.uuid4().hex}"`。

  源码依据：[_generate_worker_id 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L66-L70)


- **`_resolve_record_user_id`**（`user_id`）：在 `条件 user_id is not None` 分支中，返回 `user_id`；以 `get_current_user()` 计算 `user`；返回 `str(user.id) if user is not None else None`。

  源码依据：[_resolve_record_user_id 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L71-L85)


- **`_cursor_part`**（`value`）：在 `条件 value is None` 分支中，返回 `None`；以 `value.strip()` 计算 `stripped`；返回 `stripped or None`。

  源码依据：[_cursor_part 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L86-L93)


### 5.30.2 _is_unique_violation

参数：`exc`。

以 `[exc]` 计算 `pending: list[BaseException]`。关键判定为 `id(current) in seen`、`getattr(current, "pgcode", None) == _UNIQUE_PGCODE`、`getattr(current, "sqlcode", None) == _UNIQUE_PGCODE`。执行 `seen.add(id(current))`；执行 `pending.append(inner)`。分支返回 `True`、`False`，提前返回结束该路径。

源码依据：[_is_unique_violation 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L94-L148)


### 5.30.3 辅助函数与局部契约

- **`_is_retryable_persistence_error`**（`exc`）：以 `[exc]` 计算 `pending: list[BaseException]`。关键判定为 `id(current) in seen`、`any(fragment in message for fragment in _RETRYABLE_SQLITE_MESSAGES)`、`isinstance(current, (sqlite3.OperationalError, sqlite3.DatabaseError))`，相应动作只在对应分支执行。执行 `seen.add(id(current))`；执行 `pending.append(chained)`。分支返回 `True`、`False`，提前返回结束该路径。

  源码依据：[_is_retryable_persistence_error 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L149-L178)


- **`_store_accepts_idempotency_parameter`**（`store, parameter_name`）：以 `store.create_thread_operation_atomic` 计算 `callable_`。关键判定为 `getattr(callable_, "__func__", None) is RunStore.create_thread_operation_atomic`、`parameter is not None and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)`、`not any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())`，相应动作只在对应分支执行。捕获 `(TypeError, ValueError)` 的边界见源码，捕获不等于已恢复。分支返回 `False`、`True`、`bool(implementation_owner and implementation_owner.__dict__.get("supports_idempotency_request_kwargs", False))`，提前返回结束该路径。

  源码依据：[_store_accepts_idempotency_parameter 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L197-L217)


- **`_store_supports_terminal_completion`**（`store`）：以 `store.update_run_completion` 计算 `callable_`。关键判定为 `getattr(callable_, "__func__", None) is RunStore.update_run_completion`、「表达式摘要 1」、`any(parameter is not None and parameter.kind is inspect.Parameter.POSITIONAL_ONLY for parameter in required)`，相应动作只在对应分支执行。捕获 `(TypeError, ValueError)` 的边界见源码，捕获不等于已恢复。分支返回 `False`、`True`、`bool(implementation_owner and implementation_owner.__dict__.get("supports_terminal_completion_kwargs", False))`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  all(parameter is not None and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD,
       inspect.Parameter.KEYWORD_ONLY) for parameter in required)
  ```


  源码依据：[_store_supports_terminal_completion 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L218-L240)


- **`_approval_request_ids`**（`metadata`）：以 `metadata.get("hitl_approval")` 计算 `payload`；以 `{}` 计算 `request_ids: dict[str, str]`。关键判定为 `not isinstance(payload, Mapping) or not isinstance(payload.get("actions"), list)`、`not isinstance(action, Mapping)`、`not isinstance(call_id, str) or not isinstance(request_id, str) or call_id in request_ids`，相应动作只在对应分支执行。以 `request_id` 计算 `request_ids[call_id]`。分支返回 `None`、`request_ids`，提前返回结束该路径。

  源码依据：[_approval_request_ids 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L251-L266)


- **`_approval_request_id`**（`metadata`）：以 `metadata.get("hitl_approval")` 计算 `payload`；在 `条件 not isinstance(payload, Mapping)` 分支中，返回 `None`；以 `payload.get("request_id")` 计算 `request_id`；返回 `request_id if isinstance(request_id, str) else None`。

  源码依据：[_approval_request_id 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L267-L274)


### 5.30.4 RunManager.__init__

**参数摘要**

```text
    store=None,
    persistence_retry_policy=None,
    worker_id=None,
    run_ownership_config=None,
    event_store=None,
    recovery_terminal_publisher=None,
    on_orphans_recovered=None
```

关键判定为 `self._recovery_terminal_publisher is None and event_store is not None and store is not None`。

以 `{}` 计算 `self._runs: dict[str, RunRecord]`；以 `{}` 计算 `self._runs_by_thread: dict[str, dict[str, None]]`；其他写入目标为 `self._lock`、`self._admission_locks`、`self._snapshot_locks`、`self._pending_admissions: dict[str, dict[str, int]]`、`self._pending_operation_releases: dict[str, dict[str, str | None]]`、`self._store`、`self._persistence_retry_policy`、`self._worker_id`、`self._run_ownership_config`、`self._event_store`、`self._recovery_terminal_publisher`、`self._on_orphans_recovered`、`self._heartbeat_task: asyncio.Task | None`、`self._heartbeat_stop: asyncio.Event | None`、`self._orphan_recovery_task: asyncio.Task[None] | None`、`self._terminal_attempts: dict[str, set[asyncio.Future[bool]]]`、`self._lease_checks: dict[str, asyncio.Task[None]]`、`self._lease_operations: dict[str, asyncio.Task]`、`self._lease_deadlines: dict[str, asyncio.TimerHandle]`、`self._tail_evictions: set[asyncio.Task]`。

源码依据：[__init__ 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L394-L438)


### 5.30.5 辅助函数与局部契约

- **`RunManager._admission_scope`**（`thread_id, run_id`）：以 `self._pending_admissions.setdefault(run_id, {})` 计算 `threads`；以 `threads.get(thread_id, 0) + 1` 计算 `threads[thread_id]`；在 `try` → `资源/同步边界 self._admission_locks.hold(thread_id)` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾；在 `finally 收尾` 分支中，对 `threads[thread_id]` 执行 `-= 1` 更新；在 `finally 收尾` → `条件 not threads[thread_id]` 分支中，删除 `threads[thread_id]`；在 `finally 收尾` → `条件 not threads` 分支中，执行 `self._pending_admissions.pop(run_id, None)`。

  源码依据：[_admission_scope 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L440-L454)


- **`RunManager._run_scope`**（`run_id`）：以 `set(self._pending_admissions.get(run_id, ()))` 计算 `threads`；以 `self._runs.get(run_id)` 计算 `record`；在 `条件 record is not None` 分支中，执行 `threads.add(record.thread_id)`；在 `资源/同步边界 AsyncExitStack() as stack` → `循环 for thread_id in sorted(threads)` 分支中，等待 `stack.enter_async_context(self._admission_locks.hold(thread_id))` 完成；在 `资源/同步边界 AsyncExitStack() as stack` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾。

  源码依据：[_run_scope 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L456-L465)


- **`RunManager._run_registry_lock`**（`run_id`）：在 `资源/同步边界 self._run_scope(run_id), self._lock` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾。

  源码依据：[_run_registry_lock 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L467-L470)


- **`RunManager._thread_registry_lock`**（`thread_id`）：在 `资源/同步边界 self._admission_locks.hold(thread_id), self._lock` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾。

  源码依据：[_thread_registry_lock 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L472-L475)


- **`RunManager._index_run_locked`**（`record`）：以 `None` 计算 `self._runs_by_thread.setdefault(record.thread_id, {})[record.run_id]`。

  源码依据：[_index_run_locked 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L476-L479)


- **`RunManager._unindex_run_locked`**（`run_id, thread_id`）：执行 `self._clear_lease_deadline(run_id)`；以 `self._runs_by_thread.get(thread_id)` 计算 `bucket`；在 `条件 bucket is not None` 分支中，执行 `bucket.pop(run_id, None)`；在 `条件 bucket is not None` → `条件 not bucket` 分支中，执行 `self._runs_by_thread.pop(thread_id, None)`。

  源码依据：[_unindex_run_locked 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L480-L488)


- **`RunManager._thread_records_locked`**（`thread_id`）：以 `self._runs_by_thread.get(thread_id)` 计算 `run_ids`；在 `条件 not run_ids` 分支中，返回 `[]`；返回 `[record for run_id in run_ids if (record := self._runs.get(run_id)) is not None]`。

  源码依据：[_thread_records_locked 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L489-L506)


- **`RunManager._store_put_payload`**（`record, error=None, stop_reason=None`）：以 「表达式摘要 1」 计算 `payload`。关键判定为 `record.evidence_origin != "unknown"`、`record.evidence_agent_id is not None`、`record.idempotency_request is not None`，相应动作只在对应分支执行。以 `record.evidence_origin` 计算 `payload["evidence_origin"]`；以 `record.evidence_agent_id` 计算 `payload["evidence_agent_id"]`；其他写入目标为 `payload["idempotency_request"]`、`payload["user_id"]`、`payload["stop_reason"]`。分支返回 `payload`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "thread_id": record.thread_id, "assistant_id": record.assistant_id,
     "status": record.status.value, "operation_kind": record.operation_kind.value,
     "multitask_strategy": record.multitask_strategy, "metadata": record.metadata or {},
     "kwargs": record.kwargs or {}, "error": error if error is not None else record.error,
     "created_at": record.created_at, "model_name": record.model_name,
     "owner_worker_id": record.owner_worker_id, "lease_expires_at": record.lease_expires_at,
     "idempotency_key": record.idempotency_key, "goal_verdict": record.goal_verdict,
     }
  ```


  源码依据：[_store_put_payload 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L508-L536)


- **`RunManager._terminal_row_matches_payload`**（`row, payload`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  all( row.get(key) == payload.get(key) for key in ( "thread_id", "user_id",
       "owner_worker_id", "status", "error", "stop_reason", "goal_verdict",
       ) )
  ```


  源码依据：[_terminal_row_matches_payload 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L538-L554)


- **`RunManager._call_store_with_retry`**（`operation_name, run_id, operation`）：以 `self._persistence_retry_policy` 计算 `policy`。关键判定为 `attempt >= policy.max_attempts or not retryable`、`delay > 0`，相应动作只在对应分支执行。等待 `asyncio.sleep(delay)` 完成。捕获 `Exception as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `当前异常`。分支返回 `await operation()`，提前返回结束该路径。

  源码依据：[_call_store_with_retry 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L555-L584)


- **`RunManager._persist_snapshot_to_store`**（`run_id, payload, on_persisted=None`）：在 `条件 self._store is None` 分支中，返回 `True`；在 `try` 分支中，以 `await self._call_store_with_retry( "put", run_id, lambda: self._store.put(run_id, **payload), )` 计算 `admitted`；在 `try` → `条件 on_persisted is not None` 分支中，执行 `on_persisted(admitted)`；在 `try` 分支中，返回 `True`；在 `捕获 Exception` 分支中，返回 `False`。

  源码依据：[_persist_snapshot_to_store 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L585-L607)


- **`RunManager._persist_snapshot_with_reconciliation`**（`run_id, payload, on_persisted=None`）：在 `循环 while True` → `条件 not await self._persist_snapshot_to_store(run_id, payload, on_persisted=on_persisted)` 分支中，返回 `False`；在 `循环 while True` → `资源/同步边界 self._lock` 分支中，以 `self._runs.get(run_id)` 计算 `record`；在 `循环 while True` → `资源/同步边界 self._lock` → `条件 record is None or record.ownership_lost or self._store is None` 分支中，返回 `True`；在 `循环 while True` → `资源/同步边界 self._lock` 分支中，以 `self._store_put_payload(record)` 计算 `latest_payload`；在 `循环 while True` → `资源/同步边界 self._lock` → `条件 latest_payload == payload` 分支中，返回 `True`；在 `循环 while True` → `资源/同步边界 self._lock` 分支中，以 `latest_payload` 计算 `payload`。

  源码依据：[_persist_snapshot_with_reconciliation 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L608-L627)


- **`RunManager._persist_new_run_to_store`**（`record`）：在 `条件 self._store is None` 分支中，返回 None，提前结束当前路径；以 `await self._call_store_with_retry( "put", record.run_id, lambda: self._store.put(record.run_id, **self._store_put_payload(record)), )` 计算 `admitted`；执行 `self._bind_admitted_producer(record, admitted)`。

  源码依据：[_persist_new_run_to_store 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L628-L645)


- **`RunManager._bind_admitted_producer`**（`record, row`）：在 `条件 self._store is None or getattr(self._store, "supports_event_producer_fencing", False) is not True` 分支中，返回 None，提前结束当前路径；在 「表达式摘要 1」 分支中，抛出 `RunStartupError("Run producer admission identity could not be bound")`；以 `row.get("run_incarnation")` 计算 `record.run_incarnation`；以 `row.get("producer_epoch")` 计算 `record.producer_epoch`；以 `row.get("evidence_retention_revision", 0)` 计算 `record.evidence_retention_revision`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 row is None or row.get("run_id") != record.run_id or row.get("owner_worker_id") != self._worker_id or row.get("thread_id")
  != record.thread_id or row.get("user_id") != record.user_id
  ```


  源码依据：[_bind_admitted_producer 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L646-L656)


- **`RunManager._persist_to_store`**（`record, error=None`）：返回 `await self._persist_snapshot_to_store( record.run_id, self._store_put_payload(record, error=error), )`。

  源码依据：[_persist_to_store 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L657-L663)


- **`RunManager._terminal_outcome`**（`record, status=None, error=None, stop_reason=None, goal_verdict=None`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  TerminalCommitProof( status or record.status, error if error is not None else record.error,
     stop_reason if stop_reason is not None else record.stop_reason,
     copy.deepcopy(goal_verdict if goal_verdict is not None else record.goal_verdict),
     )
  ```


  源码依据：[_terminal_outcome 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L665-L672)


- **`RunManager._has_terminal_ack`**（`record, outcome=None`）：返回 `not record.ownership_lost and (outcome or self._terminal_outcome(record)) == record.terminal_commit_proof`。

  源码依据：[_has_terminal_ack 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L673-L675)


- **`RunManager._has_final_terminal_ack`**（`record`）：返回 `not record.rollback_finalization_pending and self._has_terminal_ack(record)`。

  源码依据：[_has_final_terminal_ack 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L676-L678)


- **`RunManager._active_paused_worker_status`**（`record`）：以 `record.paused_worker_terminal_proof` 计算 `proof`；在 「表达式摘要 1」 分支中，返回 `None`；返回 `proof.status.value`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 proof is None or proof.status != RunStatus.waiting_for_input or record.status != RunStatus.success or record.task
  is None or record.task.done()
  ```


  源码依据：[_active_paused_worker_status 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L680-L685)


- **`RunManager.terminal_event_status`**（`record`）：在 「表达式摘要 1」 分支中，返回 `None`；返回 `self._active_paused_worker_status(record) or record.status.value`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 self._runs.get(record.run_id) is not record or record.store_only or record.owner_worker_id != self._worker_id
  or record.status in (RunStatus.pending, RunStatus.running) or not self._has_final_terminal_ack(record)
  ```


  源码依据：[terminal_event_status 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L686-L691)


- **`RunManager.can_release_terminal_barrier`**（`record`）：返回 `not self.heartbeat_enabled or self._store is None or record.ownership_lost or self._has_final_terminal_ack(record)`。

  源码依据：[can_release_terminal_barrier 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L692-L695)


- **`RunManager._begin_terminal_attempt`**（`record`）：以 `asyncio.get_running_loop().create_future()` 计算 `future`；执行 `self._terminal_attempts.setdefault(record.run_id, set()).add(future)`；以 `True` 计算 `record.terminal_commit_pending`；返回 `future`。

  源码依据：[_begin_terminal_attempt 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L696-L701)


- **`RunManager._end_terminal_attempt`**（`record, future, committed`）：执行 `future.set_result(committed)`；以 `self._terminal_attempts[record.run_id]` 计算 `attempts`；执行 `attempts.remove(future)`；在 `条件 not attempts` 分支中，执行 `self._terminal_attempts.pop(record.run_id)`。

  源码依据：[_end_terminal_attempt 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L702-L708)


- **`RunManager._acknowledge_terminal`**（`record, outcome, staged_revision`）：在 `条件 self._runs.get(record.run_id) is not record or record.ownership_lost` 分支中，返回 None，提前结束当前路径；以 `record.terminal_commit_proof` 计算 `previous`；在 「表达式摘要 1」 分支中，返回 None，提前结束当前路径；在 `条件 record.terminal_status_persisted` 分支中，执行 `self._clear_lease_deadline(record.run_id)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 self._store is not None and previous is not None and previous.status != RunStatus.interrupted and staged_revision
  < record.terminal_status_revision
  ```


  源码依据：[_acknowledge_terminal 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L709-L733)


- **`RunManager._terminal_operation`**（`record, outcome, operation, cas=False`）：以 `self._begin_terminal_attempt(record)` 计算 `future`；以 `record.terminal_status_revision` 计算 `staged_revision`；以 `False` 计算 `committed`；在 `try` 分支中，以 `await operation()` 计算 `result`；在 `try` 分支中，以 `result.finalized is True if cas else result is True` 计算 `committed`；在 `try` → `条件 committed` 分支中，执行 `self._acknowledge_terminal(record, outcome, staged_revision)`；在 `try` 分支中，返回 `result`；在 `finally 收尾` 分支中，执行 `self._end_terminal_attempt(record, future, committed)`。

  源码依据：[_terminal_operation 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L734-L746)


- **`RunManager._atomic_replacement`**（`candidates, action, operation`）：以 `[(r, r.terminal_commit_pending, self._begin_terminal_attempt(r), r.terminal_status_revision) for r in candidates]` 计算 `attempts`；以 `await operation()` 计算 `result`。关键判定为 「表达式摘要 1」、`record.ownership_lost`、`record.terminal_commit_proof is not None and record.terminal_commit_proof.status != RunStatus.interrupted`，相应动作只在对应分支执行。执行 `record.abort_event.set()`。执行 `record.task.cancel()`。分支返回 `result`，提前返回结束该路径。finally 包含 执行 `for record, was_pending, future, revision in attempts:`；以 `claimed.get(record.run_id)` 计算 `row`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  row is None or row.get("status") != "interrupted" or row.get("owner_worker_id") != self._worker_id or row.get("thread_id")
  != record.thread_id
  ```


  源码依据：[_atomic_replacement 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L747-L782)


- **`RunManager._fence_unconfirmed_terminal`**（`record`）：以 `self._parse_lease_deadline(record.lease_expires_at)` 计算 `deadline`；以 `max(0.0, (deadline - datetime.now(UTC)).total_seconds()) if deadline else 0.0` 计算 `remaining`；在 `条件 await self._release_confirmed_terminal_barrier(record, timeout=remaining)` 分支中，返回 None，提前结束当前路径；等待 `self._mark_ownership_lost(record, reason="This worker's terminal write was rejected or could not be confirmed.", require_active=False)` 完成。

  源码依据：[_fence_unconfirmed_terminal 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L783-L789)


### 5.30.6 RunManager._persist_status

参数：`record, status, error=None, stop_reason=None`。

在 run 作用域中处理终态尝试、store 支持与授权匹配，只有成功持久确认才 ACK；异常/不匹配时保持 barrier，不能让内存终态直接释放下一次准入。

源码依据：[_persist_status 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L790-L895)


### 5.30.7 辅助函数与局部契约

- **`RunManager._persist_status.operation`**（无显式业务参数）：父函数内定义的回调。返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await self._call_store_with_retry( "update_status", record.run_id,
     lambda: self._store.update_status( record.run_id, status.value,
       error=outcome.error, stop_reason=outcome.stop_reason, owner_worker_id=self._worker_id,
       **({"goal_verdict": copy.deepcopy(outcome.goal_verdict)} if outcome.goal_verdict is not None else {}),
       ), )
  ```


  源码依据：[operation 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L810-L823)


### 5.30.8 RunManager._record_from_store

参数：`row`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
RunRecord( run_id=row["run_id"], thread_id=row["thread_id"], assistant_id=row.get("assistant_id"),
   status=RunStatus(row.get("status") or RunStatus.pending.value),
   on_disconnect=DisconnectMode(row.get("on_disconnect") or DisconnectMode.cancel.value),
   operation_kind=ThreadOperationKind(row.get("operation_kind") or ThreadOperationKind.run.value),
   multitask_strategy=row.get("multitask_strategy") or "reject", metadata=row.get("metadata") or {},
   kwargs=row.get("kwargs") or {}, created_at=row.get("created_at") or "",
   updated_at=row.get("updated_at") or "", user_id=row.get("user_id"),
   error=row.get("error"), model_name=row.get("model_name"), store_only=True,
   total_input_tokens=row.get("total_input_tokens") or 0, total_output_tokens=row.get("total_output_tokens") or
  0, total_tokens=row.get("total_tokens") or 0, llm_call_count=row.get("llm_call_count") or 0,
   lead_agent_tokens=row.get("lead_agent_tokens") or 0, subagent_tokens=row.get("subagent_tokens") or 0,
   middleware_tokens=row.get("middleware_tokens") or 0, token_usage_by_model=row.get("token_usage_by_model") or
  {}, message_count=row.get("message_count") or 0, last_ai_message=row.get("last_ai_message"),
   first_human_message=row.get("first_human_message"), owner_worker_id=row.get("owner_worker_id"),
   run_incarnation=row.get("run_incarnation"), producer_epoch=row.get("producer_epoch"),
   evidence_retention_revision=row.get("evidence_retention_revision",
     0), evidence_origin=row.get("evidence_origin") or "unknown",
   evidence_agent_id=row.get("evidence_agent_id"), lease_expires_at=row.get("lease_expires_at"),
   stop_reason=row.get("stop_reason"), goal_verdict=row.get("goal_verdict"),
   idempotency_key=row.get("idempotency_key"), idempotency_request=row.get("idempotency_request"),
   )
```


源码依据：[_record_from_store 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L897-L946)


### 5.30.9 RunManager.update_run_completion

参数：`run_id, **kwargs`。

以 `None` 计算 `row_recovery_payload: dict[str, Any] | None`。

关键判定为 `record is not None and record.ownership_lost`、`self.heartbeat_enabled and self._store is not None`、「表达式摘要 1」。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
record is None or record.store_only or not self._has_final_terminal_ack(record) or (kwargs.get("status") != record.status.value
  and not stale_paused_completion)
```


进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。以 `record.status.value` 计算 `kwargs["status"]`。等待 `self._mark_ownership_lost( record, reason="A peer terminalized the run before completion data was persisted.", require_active=False, )` 完成。捕获 `Exception` 的边界见源码，分支返回 `None`，提前返回结束该路径。

源码依据：[update_run_completion 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L947-L1050)


### 5.30.10 辅助函数与局部契约

- **`RunManager.update_run_progress`**（`run_id, **kwargs`）：以 `True` 计算 `should_persist`。关键判定为 `record is not None`、`record is not None and should_persist`、`hasattr(record, key) and value is not None`，相应动作只在对应分支执行。进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。等待 `self._store.update_run_progress(run_id, **kwargs)` 完成。捕获 `Exception` 的边界见源码，捕获不等于已恢复。

  源码依据：[update_run_progress 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1051-L1068)


- **`RunManager.update_finalizing_progress`**（`run_id, **kwargs`）：以 `False` 计算 `should_persist`。关键判定为 `record is not None and not record.ownership_lost`、`should_persist`、`hasattr(record, key) and value is not None`，相应动作只在对应分支执行。进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。等待 `self._store.update_run_progress(run_id, **kwargs)` 完成。捕获 `Exception` 的边界见源码，捕获不等于已恢复。

  源码依据：[update_finalizing_progress 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1069-L1088)


- **`RunManager._validate_evidence_origin`**（`origin`）：在 `条件 origin not in EVIDENCE_ORIGINS` 分支中，抛出 `ValueError("invalid evidence origin")`。

  源码依据：[_validate_evidence_origin 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1090-L1093)


- **`RunManager.seal_completed_evidence`**（`run_id, receipt`）：在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `self._runs.get(run_id)` 计算 `record`；在 `资源/同步边界 self._run_registry_lock(run_id)` → `条件 record is None or record.ownership_lost` 分支中，返回 `False`；在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `record.owner_worker_id` 计算 `owner_worker_id`；以 `getattr(self._store, "seal_evidence", None)` 计算 `seal`；在 `条件 seal is None or owner_worker_id is None` 分支中，返回 `False`；返回 `await seal(run_id, owner_worker_id=owner_worker_id, receipt=receipt)`。

  源码依据：[seal_completed_evidence 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1094-L1105)


- **`RunManager.mark_evidence_partial`**（`run_id, error`）：在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `self._runs.get(run_id)` 计算 `record`；在 `资源/同步边界 self._run_registry_lock(run_id)` → `条件 record is None or record.ownership_lost` 分支中，返回 `False`；在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `record.owner_worker_id` 计算 `owner_worker_id`；以 `getattr(self._store, "mark_evidence_partial", None)` 计算 `mark_partial`；在 `条件 mark_partial is None or owner_worker_id is None` 分支中，返回 `False`；返回 `await mark_partial(run_id, owner_worker_id=owner_worker_id, error=error)`。

  源码依据：[mark_evidence_partial 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1106-L1116)


### 5.30.11 RunManager.create

**参数摘要**

```text
    thread_id,
    assistant_id=None,
    on_disconnect=DisconnectMode.cancel,
    metadata=None,
    kwargs=None,
    multitask_strategy="reject",
    user_id=None,
    evidence_origin="unknown",
    evidence_agent_id=None
```

以 `str(uuid.uuid4())` 计算 `run_id`；以 「表达式摘要 1」 计算 `record`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
RunRecord( run_id=run_id, thread_id=thread_id, assistant_id=assistant_id,
   status=RunStatus.pending, on_disconnect=on_disconnect, multitask_strategy=multitask_strategy,
   metadata={key: value for key, value in (metadata or {}).items() if key != EVIDENCE_KEY},
   kwargs=kwargs or {}, user_id=user_id, created_at=now, updated_at=now,
   owner_worker_id=self._worker_id, lease_expires_at=lease_expires_at,
   evidence_origin=evidence_origin, evidence_agent_id=evidence_agent_id,
   )
```


进入 `self._admission_scope(thread_id, run_id)`、`self._lock` 边界，退出由对应 context manager 处理。以 `record` 计算 `self._runs[run_id]`。等待 `self._persist_new_run_to_store(record)` 完成。捕获 `Exception` 的边界见源码，显式抛出路径涉及 `当前异常`。分支返回 `record`，提前返回结束该路径。

源码依据：[create 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1117-L1174)


### 5.30.12 RunManager.get

参数：`run_id, user_id=None, raise_on_store_error=False`。

以 `self._runs.get(run_id)` 计算 `record`；以 `self._runs.get(run_id)` 计算 `record`。关键判定为 `record is not None and not record.evicted`、`record is not None and record.evicted`、`self._store is None`。进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。捕获 `Exception` 的边界见源码，显式抛出路径涉及 `当前异常`。

分支返回 `record`、`None`、`self._record_from_store(row)`，提前返回结束该路径。

源码依据：[get 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1175-L1223)


### 5.30.13 辅助函数与局部契约

- **`RunManager.aget`**（`run_id, user_id=None, raise_on_store_error=False`）：返回 `await self.get( run_id, user_id=user_id, raise_on_store_error=raise_on_store_error, )`。

  源码依据：[aget 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1224-L1240)


### 5.30.14 RunManager.list_by_thread

参数：`thread_id, user_id=None, limit=100, before_created_at=None, before_run_id=None`。

以 `_cursor_part(before_created_at)` 计算 `before_created_at`。关键判定为 `(before_created_at is None) != (before_run_id is None)`、`before_created_at is not None`、`self._store is None`。进入 `self._thread_registry_lock(thread_id)` 边界，退出由对应 context manager 处理。

以 `before_created_at` 计算 `store_kwargs["before_created_at"]`；以 `before_run_id` 计算 `store_kwargs["before_run_id"]`；其他写入目标为 `records_by_id[run_id]`。捕获 `ValueError`、`Exception` 的边界见源码，显式抛出路径涉及 `ValueError`。分支返回 `_page(memory_records)`、`_page(list(records_by_id.values()))`，提前返回结束该路径。

源码依据：[list_by_thread 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1241-L1326)


### 5.30.15 辅助函数与局部契约

- **`RunManager.list_by_thread._page`**（`records`）：父函数内定义的回调。返回 `sorted(records, key=lambda record: run_sort_key(record.created_at, record.run_id), reverse=True)[:limit]`。

  源码依据：[_page 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1276-L1278)


- **`RunManager.list_successful_regenerate_sources`**（`thread_id, user_id=AUTO`）：以 `resolve_user_id(user_id, method_name="RunManager.list_successful_regenerate_sources")` 计算 `resolved_user_id`；以 `set(await self._store.list_successful_regenerate_sources(thread_id, user_id=resolved_user_id)) if self._store is not None else set()` 计算 `sources`。关键判定为 `not isinstance(source, str) or not source`、`record.status == RunStatus.success`，相应动作只在对应分支执行。进入 `self._thread_registry_lock(thread_id)` 边界，退出由对应 context manager 处理。执行 `sources.discard(source)`；执行 `sources.add(source)`。分支返回 `sources`，提前返回结束该路径。

  源码依据：[list_successful_regenerate_sources 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1327-L1358)


- **`RunManager._record_status_value`**（`record`）：以 `record.status` 计算 `status`；返回 `status.value if isinstance(status, RunStatus) else str(status)`。

  源码依据：[_record_status_value 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1360-L1363)


- **`RunManager._compute_edit_replay_visibility`**（`records`）：以 `{}` 计算 `latest_attempt_by_source: dict[str, tuple[str, str]]`。关键判定为 `metadata.get("replay_kind") != "edit"`、`not isinstance(source, str) or not source`、`status in {RunStatus.error.value, RunStatus.timeout.value, RunStatus.interrupted.value}`，相应动作只在对应分支执行。以 `(record.run_id, status)` 计算 `latest_attempt_by_source[source]`。执行 `failed_attempts.add(record.run_id)`；执行 `hidden_sources.add(source)`。分支返回 `EditReplayVisibility( hidden_source_run_ids=hidden_sources, hidden_attempt_run_ids=failed_attempts, )`，提前返回结束该路径。

  源码依据：[_compute_edit_replay_visibility 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1365-L1388)


- **`RunManager.list_edit_replay_visibility`**（`thread_id, user_id=AUTO`）：以 `resolve_user_id(user_id, method_name="RunManager.list_edit_replay_visibility")` 计算 `resolved_user_id`。关键判定为 `self._store is not None`，相应动作只在对应分支执行。进入 `self._thread_registry_lock(thread_id)` 边界，退出由对应 context manager 处理。以 `record` 计算 `records_by_id[record.run_id]`；以 `record` 计算 `records_by_id[record.run_id]`。捕获 `Exception` 的边界见源码，捕获不等于已恢复。分支返回 `self._compute_edit_replay_visibility(list(records_by_id.values()))`，提前返回结束该路径。

  源码依据：[list_edit_replay_visibility 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1389-L1419)


### 5.30.16 RunManager.try_start

参数：`run_id`。

以 `self._runs.get(run_id)` 计算 `record`。关键判定为 `record is None`、`record.abort_event.is_set() or record.status != RunStatus.pending`、`self._store is not None`。进入 `self._run_registry_lock(run_id)`、`record.start_lock` 边界，退出由对应 context manager 处理。执行 `record.abort_event.set()`。

以 `await self._call_store_with_retry( "start_run", run_id, lambda: self._store.start_run(run_id), )` 计算 `updated`；等待 `self._persist_status( record, restore_status, error=restore_error, stop_reason=restore_stop_reason, )` 完成。捕获 `Exception as exc` 的边界见源码，显式抛出路径涉及 `RunStartupError`。

分支返回 `RunStartOutcome.cancelled`、`RunStartOutcome.started`，提前返回结束该路径。

源码依据：[try_start 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1420-L1469)


### 5.30.17 辅助函数与局部契约

- **`RunManager.fail_start_if_pending`**（`run_id, error`）：以 `self._runs.get(run_id)` 计算 `record`。关键判定为 `record is None or record.status != RunStatus.pending`、`self._runs.get(run_id) is record`，相应动作只在对应分支执行。进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。执行 `record.abort_event.set()`。分支返回 `False`、`True`，提前返回结束该路径。

  源码依据：[fail_start_if_pending 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1470-L1491)


### 5.30.18 RunManager.get_many_by_thread

参数：`thread_id, run_ids, user_id=AUTO`。

以 `resolve_user_id(user_id, method_name="RunManager.get_many_by_thread")` 计算 `resolved_user_id`；以 「表达式摘要 1」 计算 `records_by_id`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
{ record.run_id: record for record in self._thread_records_locked(thread_id) if record.operation_kind == ThreadOperationKind.run
  and not record.evicted and record.run_id in run_ids and (resolved_user_id is None or record.user_id == resolved_user_id)
  }
```


关键判定为 `not run_ids`、`self._store is None`、`not remaining`。进入 `self._thread_registry_lock(thread_id)` 边界，退出由对应 context manager 处理。以 `self._record_from_store(row)` 计算 `records_by_id[run_id]`。捕获 `Exception` 的边界见源码，分支返回 `{}`、`records_by_id`，提前返回结束该路径。

源码依据：[get_many_by_thread 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1492-L1536)


### 5.30.19 RunManager.set_status

参数：`run_id, status, error=None, stop_reason=None, goal_verdict=None, persist=True`。

以 `self._runs.get(run_id)` 计算 `record`。

关键判定为 `record is None`、`record.ownership_lost`、「表达式摘要 1」。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
( self.heartbeat_enabled and self._store is not None and proof is not None and proof.status != RunStatus.interrupted
  and self._terminal_outcome(record, status, error=error, stop_reason=stop_reason,
     goal_verdict=goal_verdict) != proof )
```


进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。等待 `self._mark_ownership_lost( record, reason="Successful completion could not be confirmed in the durable run store.", require_active=False, )` 完成。分支返回 `None`，提前返回结束该路径。

源码依据：[set_status 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1537-L1595)


### 5.30.20 辅助函数与局部契约

- **`RunManager.persist_current_status`**（`run_id`）：以 `self._runs.get(run_id)` 计算 `record`；以 `await self._persist_status(record, status, error=error, stop_reason=stop_reason)` 计算 `persisted`。关键判定为 `record is None`、`status not in (RunStatus.pending, RunStatus.running)`、`not persisted and self.heartbeat_enabled and status == RunStatus.success and not record.ownership_lost`，相应动作只在对应分支执行。进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。等待 `self._mark_ownership_lost( record, reason="Successful completion could not be confirmed in the durable run store.", require_active=False, )` 完成。分支返回 `False`、`persisted`，提前返回结束该路径。

  源码依据：[persist_current_status 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1596-L1617)


### 5.30.21 RunManager.mark_approval_answered

参数：`thread_id, expected_request_ids=None`。

以 `await self.list_by_thread(thread_id, limit=5)` 计算 `records`。关键判定为 `record is None`、`expected_request_ids is not None and _approval_request_ids(record.metadata) != expected_request_ids`、`expected_request_ids is not None and expected_request_id is None`。进入 `self._lock` 边界，退出由对应 context manager 处理。

执行 `metadata.pop("hitl_approval", None)`。等待 `restore_local_record()` 完成；等待 `restore_local_record()` 完成。捕获 `Exception as exc`、`BaseException` 的边界见源码，显式抛出路径涉及 `RunStartupError`、`当前异常`。

分支返回 `None`、「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
ApprovalAnswerHandle( run_id=record.run_id, original_metadata=previous_metadata,
   answered_metadata=copy.deepcopy(metadata), answered_updated_at=answered_updated_at,
   record=record, previous_updated_at=previous_updated_at, previous_terminal_proof=previous_terminal_proof,
   previous_terminal_revision=previous_terminal_revision, previous_terminal_persisted=previous_terminal_persisted,
   previous_terminal_commit_pending=previous_terminal_commit_pending,
   previous_paused_worker_proof=previous_paused_worker_proof, answered_revision=staged_revision,
   )
```


源码依据：[mark_approval_answered 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1618-L1728)


### 5.30.22 辅助函数与局部契约

- **`RunManager.mark_approval_answered.restore_local_record`**（无显式业务参数）：父函数内定义的回调。以 `previous_status` 计算 `record.status`。关键判定为 `record.store_only`、「表达式摘要 1」，相应动作只在对应分支执行。进入 `self._lock` 边界，退出由对应 context manager 处理。分支返回 `None`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  self._runs.get(record.run_id) is record and record.metadata is metadata and record.status == RunStatus.success
  and record.terminal_status_revision == staged_revision
  ```


  源码依据：[restore_local_record 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1663-L1676)


### 5.30.23 RunManager.restore_approval_answer

参数：`answer`。

以 `answer.record` 计算 `record`。关键判定为 `not record.store_only`、`current is not None and current is not record`、「表达式摘要 1」。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
current is record and (record.status != RunStatus.success or record.metadata != answer.answered_metadata or record.terminal_status_revision
  != answer.answered_revision)
```


进入 `self._lock` 边界，退出由对应 context manager 处理。捕获 `Exception as exc` 的边界见源码，显式抛出路径涉及 `RunStartupError`。分支返回 `False`、`True`，提前返回结束该路径。

源码依据：[restore_approval_answer 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1729-L1770)


### 5.30.24 辅助函数与局部契约

- **`RunManager.persist_metadata`**（`run_id`）：在 `资源/同步边界 self._lock` 分支中，以 `self._runs.get(run_id)` 计算 `record`；在 `资源/同步边界 self._lock` → `条件 record is None or record.ownership_lost` 分支中，返回 `False`；在 `资源/同步边界 self._lock` → `条件 self._store is None` 分支中，返回 `True`；在 `资源/同步边界 self._lock` 分支中，以 `self._store_put_payload(record)` 计算 `payload`；返回 `await self._persist_snapshot_with_reconciliation(run_id, payload)`。

  源码依据：[persist_metadata 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1771-L1781)


### 5.30.25 RunManager.set_status_if_not_cancelled

参数：`run_id, status, error=None, stop_reason=None, goal_verdict=None, persist=True`。

以 `self._runs.get(run_id)` 计算 `record`。关键判定为 `not persist or not self.heartbeat_enabled or self._store is None`、`record is None or record.store_only or record.ownership_lost`、`self._has_terminal_ack(record, outcome)`。进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。

执行 `record.abort_event.set()`。等待 `self.set_status( run_id, status, error=error, stop_reason=stop_reason, goal_verdict=goal_verdict, persist=persist, )` 完成；等待 `self._fence_unconfirmed_terminal(record)` 完成。捕获 `asyncio.CancelledError`、`Exception as exc` 的边界见源码，显式抛出路径涉及 `当前异常`。

分支返回 `None`、`result.cancel_action`，提前返回结束该路径。

源码依据：[set_status_if_not_cancelled 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1782-L1851)


### 5.30.26 辅助函数与局部契约

- **`RunManager.set_finalizing`**（`run_id, finalizing`）：在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `self._runs.get(run_id)` 计算 `record`；在 `资源/同步边界 self._run_registry_lock(run_id)` → `条件 record is None` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `finalizing` 计算 `record.finalizing`；在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `_now_iso()` 计算 `record.updated_at`。

  源码依据：[set_finalizing 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1852-L1861)


- **`RunManager.wait_for_prior_finalizing`**（`thread_id, run_id, poll_interval=0.01, abort_event=None`）：以 `False` 计算 `found_current`。关键判定为 `record.run_id == run_id`、`record.finalizing`、`not found_current or not prior_finalizing`，相应动作只在对应分支执行。进入 `self._thread_registry_lock(thread_id)` 边界，退出由对应 context manager 处理。等待 `asyncio.sleep(poll_interval)` 完成；等待 `asyncio.wait_for(abort_event.wait(), timeout=poll_interval)` 完成。捕获 `TimeoutError` 的边界见源码，捕获不等于已恢复。分支返回 `None`，提前返回结束该路径。

  源码依据：[wait_for_prior_finalizing 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1862-L1893)


- **`RunManager.has_later_run`**（`thread_id, run_id`）：在 `资源/同步边界 self._thread_registry_lock(thread_id)` 分支中，以 `False` 计算 `seen_current`；在 `资源/同步边界 self._thread_registry_lock(thread_id)` → `循环 for record in self._thread_records_locked(thread_id)` → `条件 record.run_id == run_id` 分支中，以 `True` 计算 `seen_current`；在 `资源/同步边界 self._thread_registry_lock(thread_id)` → `循环 for record in self._thread_records_locked(thread_id)` → `条件 record.run_id == run_id` 分支中，进入下一轮循环；在 `资源/同步边界 self._thread_registry_lock(thread_id)` → `循环 for record in self._thread_records_locked(thread_id)` → `条件 seen_current` 分支中，返回 `True`；返回 `False`。

  源码依据：[has_later_run 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1894-L1905)


- **`RunManager.has_later_started_run`**（`thread_id, run_id`）：在 `资源/同步边界 self._thread_registry_lock(thread_id)` 分支中，以 `False` 计算 `seen_current`；在 `资源/同步边界 self._thread_registry_lock(thread_id)` → `循环 for record in self._thread_records_locked(thread_id)` → `条件 record.run_id == run_id` 分支中，以 `True` 计算 `seen_current`；在 `资源/同步边界 self._thread_registry_lock(thread_id)` → `循环 for record in self._thread_records_locked(thread_id)` → `条件 record.run_id == run_id` 分支中，进入下一轮循环；在 `资源/同步边界 self._thread_registry_lock(thread_id)` → `循环 for record in self._thread_records_locked(thread_id)` → `条件 seen_current and (record.status != RunStatus.pending or record.finalizing)` 分支中，返回 `True`；返回 `False`。

  源码依据：[has_later_started_run 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1906-L1917)


- **`RunManager._persist_model_name`**（`run_id, model_name`）：在 `条件 self._store is None` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，等待 `self._call_store_with_retry( "update_model_name", run_id, lambda: self._store.update_model_name(run_id, model_name), )` 完成。

  源码依据：[_persist_model_name 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1918-L1930)


- **`RunManager.record_assembly_evidence`**（`run_id, evidence`）：以 `self._runs.get(run_id)` 计算 `record`。关键判定为 `record is None or evidence.get("run_id") != run_id or evidence.get("thread_id") != record.thread_id`、`record.ownership_lost`、`EVIDENCE_KEY in record.metadata`，相应动作只在对应分支执行。进入 `self._run_scope(run_id), self._snapshot_locks.hold(run_id)`、`self._lock` 边界，退出由对应 context manager 处理。执行 `record.metadata.pop(EVIDENCE_KEY, None)`。显式抛出路径涉及 `ValueError`、`RuntimeError`。分支返回 `None`，提前返回结束该路径。

  源码依据：[record_assembly_evidence 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1931-L1954)


- **`RunManager._merge_owned_metadata`**（`record, metadata`）：在 `try` 分支中，以 「表达式摘要 1」 计算 `merged`；在 `捕获 NotImplementedError` 分支中，返回 `False`；在 `捕获 Exception` 分支中，返回 `False`；在 `条件 merged is not True` 分支中，等待 `self._mark_ownership_lost(record, reason="The durable store rejected this worker's metadata write.")` 完成；在 `条件 merged is not True` 分支中，返回 `False`；返回 `not record.ownership_lost`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await self._call_store_with_retry( "merge_owned_metadata", record.run_id,
     lambda: self._store.merge_owned_metadata(record.run_id, copy.deepcopy(metadata),
       owner_worker_id=self._worker_id), )
  ```


  源码依据：[_merge_owned_metadata 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1955-L1976)


- **`RunManager.update_model_name`**（`run_id, model_name`）：在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `self._runs.get(run_id)` 计算 `record`；在 `资源/同步边界 self._run_registry_lock(run_id)` → `条件 record is None` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `model_name` 计算 `record.model_name`；在 `资源/同步边界 self._run_registry_lock(run_id)` 分支中，以 `_now_iso()` 计算 `record.updated_at`；等待 `self._persist_model_name(run_id, model_name)` 完成。

  源码依据：[update_model_name 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1977-L1988)


### 5.30.27 RunManager._request_durable_cancel

参数：`run_id, action`。

以 `await self._call_store_with_retry( "request_cancel", run_id, lambda: self._store.request_cancel(run_id, action=action), )` 计算 `winning_action`。关键判定为 `self._store is None`、`winning_action is not None`、`fresh is None`。捕获 `NotImplementedError`、`Exception` 的边界见源码，

分支返回 `CancelOutcome.unknown, None`、`CancelOutcome.lease_valid_elsewhere, None`、`CancelOutcome.requested, winning_action`、`CancelOutcome.not_cancellable, None`，提前返回结束该路径。

源码依据：[_request_durable_cancel 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L1989-L2043)


### 5.30.28 辅助函数与局部契约

- **`RunManager._request_remote_cancel`**（`run_id, action`）：以 `await self._request_durable_cancel( run_id, action=action, )` 计算 `outcome, _`；返回 `outcome`。

  源码依据：[_request_remote_cancel 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2044-L2056)


- **`RunManager._signal_local_cancel`**（`run_id, action`）：以 `self._runs.get(run_id)` 计算 `record`。关键判定为 「表达式摘要 1」、`task_active and record.status == RunStatus.running`，相应动作只在对应分支执行。进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。执行 `record.abort_event.set()`。执行 `record.task.cancel()`。分支返回 `None`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  record is None or (record.status not in (RunStatus.pending, RunStatus.running) and not self._awaits_terminal_commit(record))
  or record.abort_event.is_set()
  ```


  源码依据：[_signal_local_cancel 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2057-L2077)


- **`RunManager.request_fenced_cancel`**（`run_id, execution_fence, user_id`）：在 `条件 not self.heartbeat_enabled or self._store is None or "execution_fence" not in inspect.signature(self._store.request_cancel).parameters` 分支中，抛出 `RunIdempotencyUnsupported("The configured RunStore does not support fenced cancellation")`；以 `await self.get(run_id, user_id=user_id)` 计算 `record`；在 `条件 record is None` 分支中，抛出 `ConflictError("Run unavailable")`；等待 `self._store.request_cancel(run_id, action="interrupt", execution_fence=execution_fence, user_id=user_id)` 完成。

  源码依据：[request_fenced_cancel 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2078-L2088)


### 5.30.29 RunManager.cancel

参数：`run_id, action="interrupt"`。

先区分本地/远端 owner，持久化取消意图并 fence，通知 worker；interrupt 保留状态，rollback 还要等待恢复与收尾，取消请求返回不等于任意外部动作已撤销。

源码依据：[cancel 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2089-L2290)


### 5.30.30 辅助函数与局部契约

- **`RunManager._compute_lease_expires_at`**（无显式业务参数）：在 `条件 self._run_ownership_config is None` 分支中，返回 `None`；在 `条件 not self._run_ownership_config.heartbeat_enabled` 分支中，返回 `None`；以 `self._run_ownership_config.lease_seconds` 计算 `lease_seconds`；返回 `(datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()`。

  源码依据：[_compute_lease_expires_at 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2291-L2305)


### 5.30.31 RunManager.create_or_reject

**参数摘要**

```text
    thread_id,
    assistant_id=None,
    on_disconnect=DisconnectMode.cancel,
    metadata=None,
    kwargs=None,
    multitask_strategy="reject",
    model_name=None,
    user_id=None,
    idempotency_key=None,
    evidence_origin="unknown",
    evidence_agent_id=None,
    idempotency_request=None,
    run_id=None,
    execution_fence=None
```

建立局部回调 `async def admit() -> RunRecord`，行为在独立条目展开；在 `try` 分支中，返回 `await admit()`；在 `捕获 ConflictError` → `条件 idempotency_key is None` 分支中，向外继续抛出当前异常；在 `捕获 ConflictError` → `资源/同步边界 self._admission_locks.hold(thread_id)` → `条件 not self._pending_operation_releases.get(thread_id)` 分支中，向外继续抛出当前异常；在 `捕获 ConflictError` → `资源/同步边界 self._admission_locks.hold(thread_id)` 分支中，等待 `self._release_completed_thread_operations(thread_id)` 完成；在 `捕获 ConflictError` 分支中，返回 `await admit()`。

源码依据：[create_or_reject 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2306-L2358)


### 5.30.32 辅助函数与局部契约

- **`RunManager.create_or_reject.admit`**（无显式业务参数）：父函数内定义的回调。返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await self._admit_thread_operation( thread_id, assistant_id, operation_kind=ThreadOperationKind.run,
     on_disconnect=on_disconnect, metadata=metadata, kwargs=kwargs,
     multitask_strategy=multitask_strategy, model_name=model_name, user_id=user_id,
     idempotency_key=idempotency_key, evidence_origin=evidence_origin,
     evidence_agent_id=evidence_agent_id, idempotency_request=idempotency_request,
     run_id=run_id, execution_fence=execution_fence, )
  ```


  源码依据：[admit 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2326-L2344)


### 5.30.33 RunManager._close_cancelled_admission

参数：`record`。

以 「表达式摘要 1」 计算 `stored`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
await self._call_store_with_retry( "verify cancelled admission", record.run_id,
   lambda: self._store.get(record.run_id, user_id=record.user_id),
   )
```


关键判定为 `self._store is None or (self.heartbeat_enabled and record.ownership_lost)`、`stored is not None and stored.get("status") in active_statuses`、`self.heartbeat_enabled`。进入 `self._run_registry_lock(record.run_id)` 边界，退出由对应 context manager 处理。执行 `self._runs.pop(record.run_id, None)`。

等待 `self.cancel(record.run_id)` 完成；等待 `self._persist_status(record, RunStatus.interrupted)` 完成。显式抛出路径涉及 `RuntimeError`。分支返回 `None`，提前返回结束该路径。

源码依据：[_close_cancelled_admission 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2359-L2412)


### 5.30.34 RunManager._admit_thread_operation

**参数摘要**

```text
    thread_id,
    assistant_id=None,
    operation_kind,
    on_disconnect=DisconnectMode.cancel,
    metadata=None,
    kwargs=None,
    multitask_strategy="reject",
    model_name=None,
    user_id=None,
    idempotency_key=None,
    evidence_origin="unknown",
    evidence_agent_id=None,
    idempotency_request=None,
    run_id=None,
    execution_fence=None
```

**职责与不变量**

统一为运行、检查点写入、分支、删除等线程操作取得准入，返回新 `RunRecord` 或同一幂等请求已有的记录。关键不变量是：同线程本地检查、持久准入、注册必须由同一 admission gate 串联；跨进程冲突由 RunStore 的原子操作解决。

1. 校验证据来源和 `reject/interrupt/rollback` 策略，生成 run ID 与时间，先解析用户身份再比较幂等归属。execution fence 要求持久 store 显式支持，且只能用于 reject。
2. 建立 `pending` 记录，去掉客户可伪造的装配证据字段。进入线程 gate 后，已有相同 key 的本地记录必须匹配 user 和 thread，才能标记重用并返回。
3. 检查本地活跃记录时，还考虑 `finalizing` 和 scheduled goal 清理。reject 直接拒绝；interrupt/rollback 也不能打断任意 checkpoint 写操作，不能越过仍在清理的 scheduled goal。

源码依据：[输入契约与本地准入](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2413-L2543)

4. 到真正持久准入时才计算 lease 截止时间，避免等待 gate 已消耗整段租约。根据 store 的参数能力传入 key、请求快照和 fence，使用 `create_thread_operation_atomic` 等实际路径处理跨 worker 竞争。
5. 唯一约束冲突转为运行冲突；幂等冲突读取持久行并再次核对归属。远端或已清理的已有行作为 `store_only` handle 返回，不注册成本地执行者，否则过期本地状态会挡住后续运行并遮蔽真实持久记录。
6. 需要替换旧运行时按策略请求中止并协调其收尾；最终登记新记录与线程索引。调用者之后才附上 worker task，这里返回 pending 不代表模型已启动。

源码依据：[持久准入及重复请求处理](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2544-L2704)

**失败与适用边界**

不支持的 store 契约会显式失败，不能静默声称幂等或 fencing 生效。进程内锁只能保护同一 RunManager；省略持久 store 的组合没有相同的跨进程保证。幂等 key 的输入等价性还由 Gateway 的 `start_run` 比较，不能只凭 key 相同证明请求相同。

### 5.30.35 辅助函数与局部契约

- **`RunManager._admit_thread_operation.reuse_idempotent_run`**（`conflict`）：父函数内定义的回调。以 `self._record_from_store(conflict.existing)` 计算 `existing`；在 `条件 existing.thread_id != thread_id or existing.user_id != user_id` 分支中，抛出 `RunIdempotencyKeyMismatch("Run idempotency key resolved to a different thread or user") from conflict`；以 `True` 计算 `existing.idempotency_reused`；返回 `existing`。

  源码依据：[reuse_idempotent_run 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2500-L2515)


- **`RunManager._release_completed_thread_operations`**（`thread_id`）：以 `self._pending_operation_releases.get(thread_id)` 计算 `pending`。关键判定为 `not pending or self._store is None`、`removed is not None`、`not pending`，相应动作只在对应分支执行。进入 `self._lock` 边界，退出由对应 context manager 处理。执行 `pending.pop(run_id, None)`；执行 `self._pending_operation_releases.pop(thread_id, None)`。等待 `self._call_store_with_retry( "release thread operation", run_id, lambda: self._store.delete_thread_operation(run_id, user_id=user_id), )` 完成。捕获 `Exception as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `ConflictError`。分支返回 `None`，提前返回结束该路径。

  源码依据：[_release_completed_thread_operations 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2705-L2734)


### 5.30.36 RunManager.reserve_thread_operation

参数：`thread_id, kind, user_id=None`。

以 `await self._admit_thread_operation( thread_id, operation_kind=kind, multitask_strategy="reject", user_id=user_id, )` 计算 `record`。关键判定为 `kind == ThreadOperationKind.run`、`reservation_task is None`、`self._runs.get(record.run_id) is record`。

进入 `self._run_registry_lock(record.run_id)`、`self._admission_locks.hold(thread_id)` 边界，退出由对应 context manager 处理。以 `record.user_id` 计算 `self._pending_operation_releases.setdefault(thread_id, {})[record.run_id]`。交付 `record` 后暂停生成器，待消费方继续；等待 `self._release_completed_thread_operations(thread_id)` 完成。

捕获 `asyncio.CancelledError`、`Exception` 的边界见源码，显式抛出路径涉及 `ValueError`、`RuntimeError`、`asyncio.CancelledError`、`ConflictError`、`当前异常`。finally 包含 以 `record.user_id` 计算 `self._pending_operation_releases.setdefault(thread_id, {})[record.run_id]`；执行 `async with self._admission_locks.hold(thread_id):`。

源码依据：[reserve_thread_operation 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2736-L2797)


### 5.30.37 RunManager.reconcile_orphaned_inflight_runs

参数：`error, before=None, stop_reason=None`。

筛选符合 owner/lease 条件的孤儿活动记录，通过存储原子更新收敛为 error，恢复可观察终态；不会自动调用旧图重放工具。

源码依据：[reconcile_orphaned_inflight_runs 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2798-L2909)


### 5.30.38 辅助函数与局部契约

- **`RunManager.has_inflight`**（`thread_id`）：在 `资源/同步边界 self._thread_registry_lock(thread_id)` 分支中，返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  any(r.operation_kind == ThreadOperationKind.run and (r.status in (RunStatus.pending,
         RunStatus.running) or r.finalizing) for r in self._thread_records_locked(thread_id))
  ```


  源码依据：[has_inflight 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2910-L2914)


### 5.30.39 RunManager.discard_thread_runs

参数：`thread_id`。

以 `list(self._runs_by_thread.get(thread_id, {}))` 计算 `run_ids`；对 `dropped` 执行 `+= 1` 更新。关键判定为 `record is None or record.thread_id != thread_id`、`record.operation_kind != ThreadOperationKind.run`、`task is not None and not task.done()`。进入 `self._thread_registry_lock(thread_id)` 边界，退出由对应 context manager 处理。

执行 `deferred.append(task)`；执行 `self._runs.pop(run_id, None)`。分支返回 `dropped`，提前返回结束该路径。

源码依据：[discard_thread_runs 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2915-L2967)


### 5.30.40 辅助函数与局部契约

- **`RunManager._evict_after_task`**（`thread_id, completed`）：在 `try` 分支中，以 `asyncio.get_running_loop().create_task(self._evict_completed_tail(thread_id))` 计算 `task`；在 `捕获 RuntimeError` 分支中，返回 None，提前结束当前路径；执行 `self._tail_evictions.add(task)`；执行 `task.add_done_callback(self._tail_evictions.discard)`。

  源码依据：[_evict_after_task 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2968-L2989)


- **`RunManager._evict_completed_tail`**（`thread_id`）：关键判定为 `record.thread_id != thread_id or not record.evicted`、`record.operation_kind != ThreadOperationKind.run`、`record.task is not None and not record.task.done()`，相应动作只在对应分支执行。进入 `self._thread_registry_lock(thread_id)` 边界，退出由对应 context manager 处理。执行 `self._runs.pop(run_id, None)`。

  源码依据：[_evict_completed_tail 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L2990-L3002)


- **`RunManager.cleanup`**（`run_id, delay=300`）：以 `self._runs.get(run_id)` 计算 `record`。关键判定为 `self._store is None`、`delay > 0`、`task is not None and task is not asyncio.current_task() and not task.done()`，相应动作只在对应分支执行。进入 `self._run_registry_lock(run_id)` 边界，退出由对应 context manager 处理。等待 `asyncio.sleep(delay)` 完成；等待 `asyncio.shield(task)` 完成。捕获 `asyncio.CancelledError`、`Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `当前异常`。分支返回 `None`，提前返回结束该路径。

  源码依据：[cleanup 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3003-L3039)


- **`RunManager.worker_id`**（无显式业务参数）：返回 `self._worker_id`。

  源码依据：[worker_id 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3041-L3044)


- **`RunManager.input_store`**（无显式业务参数）：返回 `self._store if getattr(self._store, "supports_run_inputs", False) is True else None`。

  源码依据：[input_store 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3046-L3049)


- **`RunManager.owns_active_admission`**（`record`）：以 `await self._store.get(record.run_id, user_id=record.user_id)` 计算 `row`。关键判定为 `self._runs.get(record.run_id) is not record or record.ownership_lost`、`self._store is None`、「表达式摘要 1」，相应动作只在对应分支执行。进入 `self._run_registry_lock(record.run_id)` 边界，退出由对应 context manager 处理。等待 `self._mark_ownership_lost(record, reason="Scheduled goal cleanup lost its active run lease.", require_active=False)` 完成。分支返回 `False`、`True`、`not record.ownership_lost`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  row is None or row.get("thread_id") != record.thread_id or row.get("user_id") != record.user_id or row.get("status")
  not in ("pending", "running") or row.get("owner_worker_id") != record.owner_worker_id
  ```


  源码依据：[owns_active_admission 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3050-L3066)


- **`RunManager.scheduled_goal_source`**（`thread_id, user_id, created_at, objective`）：以 `await self._store.list_by_thread_created_at(thread_id, user_id=user_id, created_at=created_at)` 计算 `rows`。关键判定为 `self._store is None`、「表达式摘要 1」、「表达式摘要 2」，相应动作只在对应分支执行。执行 `matches.append(row)`。显式抛出路径涉及 `RuntimeError`。分支返回 `None`、`self._record_from_store(matches[0]) if matches else None`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  row.get("thread_id") != thread_id or row.get("user_id") != user_id or canonical_run_created_at(row.get("created_at",
       "")) != canonical_run_created_at(created_at)
  ```


  **表达式摘要 2**（非完整源码；换行仅为阅读排版）：

  ```text
  ( row.get("status") not in {"success", "error", "timeout", "interrupted"} or row.get("operation_kind",
       "run") != "run" or not isinstance(metadata.get("scheduled_task_id"),
       str) or not metadata["scheduled_task_id"] or not isinstance(metadata.get("scheduled_task_run_id"),
       str) or not metadata["scheduled_task_run_id"] or not isinstance(candidate,
       str) )
  ```


  源码依据：[scheduled_goal_source 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3067-L3095)


- **`RunManager.heartbeat_enabled`**（无显式业务参数）：在 `条件 self._run_ownership_config is None` 分支中，返回 `False`；返回 `self._run_ownership_config.heartbeat_enabled`。

  源码依据：[heartbeat_enabled 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3097-L3102)


- **`RunManager.grace_seconds`**（无显式业务参数）：返回 `self._run_ownership_config.grace_seconds if self._run_ownership_config else 10`。

  源码依据：[grace_seconds 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3104-L3113)


- **`RunManager._parse_lease_deadline`**（`lease_expires_at`）：在 `条件 lease_expires_at is None` 分支中，返回 `None`；在 `try` 分支中，以 `datetime.fromisoformat(lease_expires_at)` 计算 `deadline`；在 `捕获 (TypeError, ValueError)` 分支中，返回 `None`；在 `条件 deadline.tzinfo is None` 分支中，以 `deadline.replace(tzinfo=UTC)` 计算 `deadline`；返回 `deadline`。

  源码依据：[_parse_lease_deadline 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3115-L3130)


- **`RunManager._mark_ownership_lost`**（`record, reason, require_active=True`）：返回 `self._fence_local_record(record, reason=reason, require_active=require_active)`。

  源码依据：[_mark_ownership_lost 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3131-L3145)


- **`RunManager._fence_local_record`**（`record, reason, require_active=False`）：以 `None` 计算 `task_to_cancel: asyncio.Task | None`。关键判定为 `current is not record`、`require_active`、`record.status not in (RunStatus.pending, RunStatus.running)`，相应动作只在对应分支执行。执行 `record.abort_event.set()`。执行 `task_to_cancel.cancel()`。分支返回 `False`、`True`，提前返回结束该路径。

  源码依据：[_fence_local_record 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3146-L3179)


- **`RunManager.start_heartbeat`**（无显式业务参数）：在 `条件 not self.heartbeat_enabled` 分支中，返回 None，提前结束当前路径；在 `条件 self._heartbeat_task is not None and not self._heartbeat_task.done()` 分支中，返回 None，提前结束当前路径；以 `asyncio.Event()` 计算 `self._heartbeat_stop`；以 `asyncio.create_task(self._heartbeat_loop())` 计算 `task`；执行 `task.set_name("deerflow-run-lease-heartbeat")`；以 `task` 计算 `self._heartbeat_task`。

  源码依据：[start_heartbeat 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3180-L3194)


- **`RunManager.stop_heartbeat`**（`timeout=5.0`）：以 `asyncio.get_running_loop().time() + max(0.0, timeout)` 计算 `deadline`。关键判定为 `self._heartbeat_stop is not None`、`tasks`、`unfinished`，相应动作只在对应分支执行。以 `None` 计算 `self._heartbeat_task`；以 `None` 计算 `self._heartbeat_stop`。执行 `self._heartbeat_stop.set()`。等待 `asyncio.wait(tasks, timeout=max(0.0, deadline - asyncio.get_running_loop().time()))` 完成；执行 `task.cancel()`。

  源码依据：[stop_heartbeat 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3195-L3225)


- **`RunManager.stop_heartbeat.owned_tasks`**（无显式业务参数）：父函数内定义的回调。以 `set(self._lease_checks.values()) | set(self._lease_operations.values())` 计算 `tasks`；在 `条件 self._heartbeat_task is not None` 分支中，执行 `tasks.add(self._heartbeat_task)`；返回 `{task for task in tasks if not task.done()}`。

  源码依据：[owned_tasks 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3201-L3206)


### 5.30.41 RunManager._heartbeat_loop

无显式业务参数。

以 `self._run_ownership_config.lease_seconds` 计算 `lease_seconds`。关键判定为 `self._run_ownership_config is None or self._heartbeat_stop is None`、`cycle % 3 == 0`。等待 `asyncio.wait_for(stop.wait(), timeout=interval)` 完成；等待 `self._renew_leases(wait=False)` 完成。捕获 `TimeoutError`、`Exception` 的边界见源码，

分支返回 `None`，提前返回结束该路径。

源码依据：[_heartbeat_loop 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3226-L3266)


### 5.30.42 辅助函数与局部契约

- **`RunManager._awaits_terminal_commit`**（`record`）：返回 `record.scheduled_goal_cleanup_pending or record.terminal_commit_pending or record.rollback_finalization_pending`。

  源码依据：[_awaits_terminal_commit 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3268-L3271)


- **`RunManager._release_confirmed_terminal_barrier`**（`record, timeout`）：以 `asyncio.get_running_loop().time() + max(0.0, timeout)` 计算 `deadline`。关键判定为 `self._has_final_terminal_ack(record)`、`not attempts or remaining <= 0`、`not done`，相应动作只在对应分支执行。分支返回 `True`、`False`，提前返回结束该路径。

  源码依据：[_release_confirmed_terminal_barrier 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3272-L3290)


- **`RunManager._clear_lease_deadline`**（`run_id`）：以 `self._lease_deadlines.pop(run_id, None)` 计算 `handle`；在 `条件 handle is not None` 分支中，执行 `handle.cancel()`。

  源码依据：[_clear_lease_deadline 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3291-L3295)


- **`RunManager._arm_lease_deadline`**（`record`）：执行 `self._clear_lease_deadline(record.run_id)`；在 `条件 not self.heartbeat_enabled or self._store is None or record.ownership_lost or self._has_final_terminal_ack(record)` 分支中，返回 None，提前结束当前路径；以 `self._parse_lease_deadline(record.lease_expires_at)` 计算 `expiry`；以 `max(0.0, (expiry - datetime.now(UTC)).total_seconds()) if expiry else 0.0` 计算 `remaining`；建立局部回调 `def expired() -> None`，行为在独立条目展开；以 `asyncio.get_running_loop().call_later(remaining, expired)` 计算 `self._lease_deadlines[record.run_id]`。

  源码依据：[_arm_lease_deadline 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3296-L3316)


- **`RunManager._arm_lease_deadline.expired`**（无显式业务参数）：父函数内定义的回调。执行 `self._lease_deadlines.pop(record.run_id, None)`；在 `条件 self._runs.get(record.run_id) is not record or record.ownership_lost or self._has_final_terminal_ack(record)` 分支中，返回 None，提前结束当前路径；以 `self._parse_lease_deadline(record.lease_expires_at)` 计算 `current`；在 `条件 current is not None and current > datetime.now(UTC)` 分支中，执行 `self._arm_lease_deadline(record)`；在 `条件 current is not None and current > datetime.now(UTC)` 分支中，返回 None，提前结束当前路径；执行 `self._fence_local_record(record, reason="The last confirmed lease expired without a final terminal acknowledgment.")`。

  源码依据：[expired 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3303-L3314)


- **`RunManager._supervise_lease_task`**（`registry, run_id, task`）：以 `task` 计算 `registry[run_id]`；建立局部回调 `def settled(completed: asyncio.Task) -> None`，行为在独立条目展开；执行 `task.add_done_callback(settled)`。

  源码依据：[_supervise_lease_task 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3317-L3329)


- **`RunManager._supervise_lease_task.settled`**（`completed`）：父函数内定义的回调。在 `条件 registry.get(run_id) is completed` 分支中，执行 `registry.pop(run_id, None)`；在 `条件 not completed.cancelled()` 分支中，以 `completed.exception()` 计算 `exc`。

  源码依据：[settled 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3320-L3327)


- **`RunManager._renew_leases`**（`wait=True`）：以 「表达式摘要 1」 计算 `records`。关键判定为 `self._store is None or self._run_ownership_config is None`、`task is None or task.done()`、`wait and checks`，相应动作只在对应分支执行。进入 `self._lock` 边界，退出由对应 context manager 处理。执行 `checks.append(task)`。等待 `asyncio.wait(checks)` 完成。分支返回 `None`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  [ record for record in self._runs.values() if not record.ownership_lost and record.owner_worker_id == self._worker_id
    and (record.status in (RunStatus.pending, RunStatus.running) or self._awaits_terminal_commit(record)) and (self._awaits_terminal_commit(record)
      or record.task is None or not record.task.done()) ]
  ```


  源码依据：[_renew_leases 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3330-L3359)


- **`RunManager._rollback_retry_authorized`**（`record, initial_allow_interrupted`）：以 `record.terminal_commit_proof` 计算 `proof`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ( not initial_allow_interrupted and record.rollback_finalization_pending and proof is not None and proof.status
    == RunStatus.interrupted and record.owner_worker_id == self._worker_id and self._runs.get(record.run_id) is
    record and not record.ownership_lost and not self._has_final_terminal_ack(record) )
  ```


  源码依据：[_rollback_retry_authorized 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3360-L3373)


- **`RunManager._await_rollback_retry_authority`**（`record, initial_allow_interrupted, deadline`）：以 `asyncio.get_running_loop()` 计算 `loop`。关键判定为 `self._rollback_retry_authorized(record, initial_allow_interrupted=initial_allow_interrupted)`、`not attempts or remaining <= 0`，相应动作只在对应分支执行。等待 `asyncio.wait(attempts, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)` 完成。分支返回 `True`、`loop.time() < deadline and self._rollback_retry_authorized(record, initial_allow_interrupted=initial_allow_interrupted)`，提前返回结束该路径。

  源码依据：[_await_rollback_retry_authority 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3374-L3386)


- **`RunManager._renew_one_lease`**（`record`）：执行 `self._arm_lease_deadline(record)`；在 `资源/同步边界 self._snapshot_locks.hold(record.run_id)` 分支中，以 `await self._renew_one_lease_locked(record)` 计算 `action`；在 `条件 action is not None` 分支中，等待 `self._signal_local_cancel(record.run_id, action=action)` 完成。

  源码依据：[_renew_one_lease 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3387-L3398)


### 5.30.43 RunManager._renew_one_lease_locked

参数：`record`。

在当前 run 锁内续约并核对 fence/取消/回滚重试条件；lost 或过期则隔离本地 record 并取消执行，避免迟到 worker 覆盖接管结果。

源码依据：[_renew_one_lease_locked 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3399-L3494)


### 5.30.44 辅助函数与局部契约

- **`RunManager._renew_one_lease_locked.fence`**（`reason`）：父函数内定义的回调。在 `条件 not await self._release_confirmed_terminal_barrier(record, timeout=max(0.0, deadline - loop.time()))` 分支中，等待 `self._mark_ownership_lost(record, reason=reason, require_active=False)` 完成。

  源码依据：[fence 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3410-L3413)


- **`RunManager._reconcile_orphans_periodic`**（无显式业务参数）：以 `await self.reconcile_orphaned_inflight_runs( error=LEASE_ORPHAN_RECOVERY_ERROR, stop_reason=ORPHAN_RECOVERY_STOP_REASON, )` 计算 `recovered`；在 `条件 recovered` → `条件 self._on_orphans_recovered is not None` → `try` 分支中，等待 `self._on_orphans_recovered(recovered)` 完成。

  源码依据：[_reconcile_orphans_periodic 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3495-L3523)


- **`RunManager._schedule_orphan_reconciliation`**（无显式业务参数）：以 `self._orphan_recovery_task` 计算 `task`；在 `条件 task is not None and not task.done()` 分支中，返回 None，提前结束当前路径；以 `asyncio.create_task(self._reconcile_orphans_periodic())` 计算 `task`；执行 `task.set_name("deerflow-periodic-orphan-recovery")`；以 `task` 计算 `self._orphan_recovery_task`；执行 `task.add_done_callback(self._orphan_reconciliation_done)`。

  源码依据：[_schedule_orphan_reconciliation 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3524-L3534)


- **`RunManager._orphan_reconciliation_done`**（`task`）：在 `条件 self._orphan_recovery_task is task` 分支中，以 `None` 计算 `self._orphan_recovery_task`；在 `条件 task.cancelled()` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 `task.result()`。

  源码依据：[_orphan_reconciliation_done 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3535-L3545)


- **`RunManager._drain_orphan_recovery_task`**（`timeout`）：以 `self._orphan_recovery_task` 计算 `task`；在 `条件 task is None or task.done()` 分支中，返回 None，提前结束当前路径；以 `await asyncio.wait((task,), timeout=max(0.0, timeout))` 计算 `_, pending`；在 `条件 pending` 分支中，执行 `task.cancel()`；在 `条件 pending` 分支中，等待 `asyncio.sleep(0) # Deliver cancellation without joining I/O.` 完成；在 `条件 pending` → `条件 task.done()` 分支中，执行 `self._orphan_reconciliation_done(task)`。

  源码依据：[_drain_orphan_recovery_task 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3546-L3563)


### 5.30.45 RunManager.shutdown

参数：`timeout=5.0`。

停止接受/推动新工作，取消并排空仍持有的执行与后台 lease/reconcile 任务，清理 registry；外层仍需等它结束后再关闭 saver/store。

源码依据：[shutdown 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L3564-L3688)


## 5.31 运行工作协程与收尾

源码位置：`backend/packages/harness/deerflow/runtime/runs/worker.py`。

单 run 的执行主机。创建可信 runtime context、绑定图与 checkpoint、消费流、处理 HITL/目标续跑/运行追加输入，最后持久化终态、关闭 producer/lease 和断开强引用。内部回调捕获本次 run 对象，所以它们也属于资源生命周期的一部分。

### 5.31.1 辅助函数与局部契约

- **`_declared_bridge_method`**（`bridge, name`）：在 `条件 inspect.getattr_static(bridge, name, _MISSING_BRIDGE_METHOD) is _MISSING_BRIDGE_METHOD` 分支中，返回 `None`；以 `getattr(bridge, name, None)` 计算 `method`；返回 `method if callable(method) else None`。

  源码依据：[_declared_bridge_method 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L126-L133)


- **`_log_cancelled_stream_close_failure`**（`exc, run_id, abort_requested`）：以 `exc.__cause__` 计算 `close_failure`；在 `条件 not isinstance(close_failure, Exception)` 分支中，返回 None，提前结束当前路径；以 `logger.warning if abort_requested else logger.debug` 计算 `log`；以 `"Could not close aborted agent stream for run %s" if abort_requested else "Could not close agent stream for run %s"` 计算 `message`；执行 `log( message, run_id, exc_info=(type(close_failure), close_failure, close_failure.__traceback__), )`。

  源码依据：[_log_cancelled_stream_close_failure 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L134-L151)


- **`_create_contextless_task`**（`coro`）：返回 `asyncio.create_task(coro, context=Context())`。

  源码依据：[_create_contextless_task 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L167-L171)


### 5.31.2 _schedule_terminal_cycle_collection

无显式业务参数。

以 `asyncio.get_running_loop()` 计算 `loop`。关键判定为 `loop in _terminal_cycle_collection_scheduled_loops`。进入 `_terminal_cycle_collection_guard` 边界，退出由对应 context manager 处理。执行 `_terminal_cycle_collection_scheduled_loops.add(loop)`。分支返回 `None`，提前返回结束该路径。

源码依据：[_schedule_terminal_cycle_collection 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L172-L222)


### 5.31.3 辅助函数与局部契约

- **`_schedule_terminal_cycle_collection._collect`**（无显式业务参数）：父函数内定义的回调。以 `time.monotonic()` 计算 `now`。关键判定为 `now - _terminal_cycle_collection_last_at < _TERMINAL_CYCLE_COLLECTION_INTERVAL_SECONDS`、`duration >= _TERMINAL_CYCLE_COLLECTION_INFO_THRESHOLD_SECONDS`，相应动作只在对应分支执行。进入 `_terminal_cycle_collection_guard` 边界，退出由对应 context manager 处理。执行 `_terminal_cycle_collection_scheduled_loops.discard(loop)`。分支返回 `None`，提前返回结束该路径。finally 包含 执行 `with _terminal_cycle_collection_guard:`；执行 `_terminal_cycle_collection_scheduled_loops.discard(loop)`。

  源码依据：[_collect 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L182-L213)


- **`_schedule_terminal_cycle_collection._start_collection`**（无显式业务参数）：父函数内定义的回调。执行 `_create_contextless_task(_collect())`。

  源码依据：[_start_collection 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L214-L219)


- **`_remove_callback`**（`config, handler`）：以 `config.get("callbacks")` 计算 `callbacks`；在 `条件 isinstance(callbacks, list)` 分支中，以 `[callback for callback in callbacks if callback is not handler]` 计算 `callbacks[:]`；在 `条件 isinstance(callbacks, list)` 分支中，返回 None，提前结束当前路径；以 `getattr(callbacks, "remove_handler", None)` 计算 `remove_handler`；在 `条件 callable(remove_handler)` → `try` 分支中，执行 `remove_handler(handler)`。

  源码依据：[_remove_callback 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L223-L235)


### 5.31.4 _release_run_scoped_references

参数：`configs, runtime_context, journal`。

显式断开 run 对 graph、runtime、config、journal 等大对象的强引用，安排后续回收；不把 GC 当容器/连接关闭替代品。

源码依据：[_release_run_scoped_references 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L236-L287)


### 5.31.5 辅助函数与局部契约

- **`_checkpoint_thread_lock`**（`thread_id`）：返回 `_checkpoint_locks.hold(thread_id)`。

  源码依据：[_checkpoint_thread_lock 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L288-L292)


- **`_project_background_tasks`**（`task_rows`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  [ { "task_id": row["id"], "task_name": neutralize_untrusted_tags(str(row["task_name"])),
       "status": row["status"], "updated_at": row["updated_at"], } for row in task_rows ]
  ```


  源码依据：[_project_background_tasks 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L297-L309)


### 5.31.6 _persist_delivery_receipt

参数：`event_store, thread_id, run_id, content`。

以 `len(_DELIVERY_RECEIPT_RETRY_DELAYS_SECONDS) + 1` 计算 `attempts`。关键判定为 `attempt == attempts - 1`。进入 `publisher_scope("run.delivery")` 边界，退出由对应 context manager 处理。

等待 `event_store.put_if_absent( thread_id=thread_id, run_id=run_id, event_type="run.delivery", category="outputs", content=content, )` 完成；等待 `asyncio.sleep(delay)` 完成。捕获 `Exception` 的边界见源码，分支返回 `True`、`False`、`False # pragma: no cover - loop always returns`，提前返回结束该路径。

源码依据：[_persist_delivery_receipt 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L310-L358)


### 5.31.7 辅助函数与局部契约

- **`_recursion_capped_message`**（`recursion_limit, delivery_content`）：以 `f" (recursion_limit={recursion_limit})" if isinstance(recursion_limit, int) and not isinstance(recursion_limit, bool) else ""` 计算 `limit`。关键判定为 `unpresented`，相应动作只在对应分支执行。以 `", ".join(unpresented[:_RECURSION_CAPPED_LISTED_OUTPUTS])` 计算 `listed`。分支返回 `f"{message} Send a message to continue."`，提前返回结束该路径。

  源码依据：[_recursion_capped_message 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L365-L382)


- **`_empty_delivery_content`**（无显式业务参数）：返回 `{"presented": 0, "paths": [], "by_tool": {}}`。

  源码依据：[_empty_delivery_content 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L383-L386)


- **`_presented_path_covers_output`**（`presented_path, produced_path`）：以 `presented_path.rstrip("/")` 计算 `presented_path`；返回 `bool(presented_path) and (produced_path == presented_path or produced_path.startswith(f"{presented_path}/"))`。

  源码依据：[_presented_path_covers_output 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L387-L391)


- **`_delivery_content_with_outputs`**（`content, produced_paths, pre_run_snapshot=None`）：将交付工具呈现路径与本次产出匹配，至少一个匹配即 satisfied；有产出无匹配标未开始/不匹配，前置 snapshot 截断且无确认产出时标跳过。不是每个文件内容的业务验收。

  源码依据：[_delivery_content_with_outputs 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L392-L421)


- **`_delivery_error`**（`content`）：在 `条件 not content.get("produced_paths") or content.get("satisfied") is True` 分支中，返回 `None`；返回 `_DELIVERY_INCOMPLETE_ERROR`。

  源码依据：[_delivery_error 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L422-L428)


- **`_workspace_excluded_dir_names`**（`app_config`）：以 `app_config.tool_output.storage_subdir if app_config is not None else TOOL_RESULTS_DIRNAME` 计算 `storage_subdir`；返回 `frozenset({storage_subdir})`。

  源码依据：[_workspace_excluded_dir_names 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L429-L446)


- **`_produced_output_paths`**（`before, thread_id, user_id, extra_excluded_dir_names=None`）：在 `条件 before is None` 分支中，返回 `[]`；在 `try` 分支中，以 `await capture_workspace_snapshot(thread_id, user_id=user_id, include_text=False, extra_excluded_dir_names=extra_excluded_dir_names)` 计算 `after`；在 `try` 分支中，返回 `get_changed_output_paths(before, after)`；在 `捕获 Exception` 分支中，返回 `[]`。

  源码依据：[_produced_output_paths 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L447-L465)


- **`_PartialAssistantResponseAccumulator.observe`**（`chunk`）：以 `chunk` 计算 `message, _metadata`。关键判定为 `not isinstance(chunk, tuple) or len(chunk) != 2`、`not isinstance(message, (AIMessage, AIMessageChunk))`、`isinstance(additional_kwargs, Mapping) and additional_kwargs.get("hide_from_ui") is True`，相应动作只在对应分支执行。以 `self.text_by_id.get(message_id, "") + text` 计算 `self.text_by_id[message_id]`。分支返回 `None`，提前返回结束该路径。

  源码依据：[observe 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L476-L492)


- **`_PartialAssistantResponseAccumulator.messages`**（无显式业务参数）：返回 `[AIMessage(id=message_id, content=text) for message_id, text in self.text_by_id.items() if text.strip()]`。

  源码依据：[messages 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L493-L496)


### 5.31.8 _LargeFileToolChunkBatcher.push

参数：`chunk`。

以 `chunk` 计算 `message, metadata`；以 `[]` 计算 `outputs: list[Any]`。关键判定为 `not isinstance(chunk, tuple) or len(chunk) != 2`、`not isinstance(message_id, str) or not message_id or not isinstance(tool_call_chunks, list) or len(tool_call_chunks) != 1`、`not isinstance(tool_chunk, dict)`。

以 `tool_name` 计算 `self.tool_names[identity]`；以 `identity` 计算 `self.pending_identity`；其他写入目标为 `self.pending_message`、`self.pending_count`。执行 `self.tool_names.pop(identity, None)`；执行 `outputs.extend(self.flush())`。

执行 `if any(candidate.startswith(tool_name) for candidate in _LARGE_FILE_TOOL_NAMES):`；执行 `outputs.extend(self.flush())`。分支返回 `[*self.flush(), chunk]`、`outputs`，提前返回结束该路径。

源码依据：[push 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L513-L592)


### 5.31.9 辅助函数与局部契约

- **`_LargeFileToolChunkBatcher.flush`**（无显式业务参数）：在 `条件 self.pending_message is None` 分支中，返回 `[]`；以 `(self.pending_message, self.pending_metadata)` 计算 `chunk`；以 `None` 计算 `self.pending_identity`；以 `None` 计算 `self.pending_message`；以 `{}` 计算 `self.pending_metadata`；以 `0` 计算 `self.pending_count`；返回 `[chunk]`。

  源码依据：[flush 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L593-L602)


- **`_LargeFileToolChunkBatcher.finish`**（无显式业务参数）：以 `self.flush()` 计算 `chunks`；执行 `self.tool_names.clear()`；返回 `chunks`。

  源码依据：[finish 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L603-L618)


### 5.31.10 _build_runtime_context

**参数摘要**

```text
    thread_id,
    run_id,
    caller_context,
    app_config=None,
    task_store=None,
    extensions=None,
    conversation_reader=None,
    agent_runs=None,
    thread_incarnation=_THREAD_INCARNATION_UNSET,
    scheduler_capability=None
```

以 `{"thread_id": thread_id, "run_id": run_id}` 计算 `runtime_ctx: dict[str, Any]`。关键判定为 `thread_incarnation is not _THREAD_INCARNATION_UNSET`、`isinstance(caller_context, dict)`、`key in _SERVER_OWNED_RUNTIME_CONTEXT_KEYS`。

以 `thread_incarnation` 计算 `runtime_ctx[THREAD_INCARNATION_CONTEXT_KEY]`；以 `app_config` 计算 `runtime_ctx["app_config"]`；其他写入目标为 `runtime_ctx[AGENT_RUNS_CONTEXT_KEY]`、`runtime_ctx[CONVERSATION_READER_CONTEXT_KEY]`、`runtime_ctx[SCHEDULER_CAPABILITY_CONTEXT_KEY]`、`runtime_ctx[EXTENSION_TASK_STORE_KEY]`、`runtime_ctx[EXTENSION_SNAPSHOT_CONTEXT_KEY]`。

执行 `runtime_ctx.pop(EXTENSION_SNAPSHOT_CONTEXT_KEY, None)`。分支返回 `runtime_ctx`，提前返回结束该路径。

源码依据：[_build_runtime_context 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L648-L706)


### 5.31.11 辅助函数与局部契约

- **`_pin_admission_project_context`**（`config, runtime_context`）：以 `config.get("context")` 计算 `caller_context`；在 `条件 isinstance(caller_context, dict) and PROJECT_CONTEXT_KEY in caller_context` 分支中，以 `caller_context[PROJECT_CONTEXT_KEY]` 计算 `runtime_context[PROJECT_CONTEXT_KEY]`。

  源码依据：[_pin_admission_project_context 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L707-L723)


- **`_install_runtime_context`**（`config, runtime_context`）：以 `config.get("configurable")` 计算 `configurable`。关键判定为 `isinstance(configurable, dict)`、`isinstance(existing_context, dict)`、`key in runtime_context`，相应动作只在对应分支执行。以 `runtime_context[key]` 计算 `existing_context[key]`；以 `runtime_context["app_config"]` 计算 `existing_context["app_config"]`；其他写入目标为 `config["context"]`。执行 `configurable.pop(CONVERSATION_READER_CONTEXT_KEY, None)`；执行 `configurable.pop(SCHEDULER_CAPABILITY_CONTEXT_KEY, None)`。分支返回 `None`，提前返回结束该路径。

  源码依据：[_install_runtime_context 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L760-L787)


- **`_compute_agent_factory_supports_app_config`**（`agent_factory`）：在 `try` 分支中，返回 `"app_config" in inspect.signature(agent_factory).parameters`；在 `捕获 (TypeError, ValueError)` 分支中，返回 `False`。

  源码依据：[_compute_agent_factory_supports_app_config 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L788-L794)


- **`_cached_agent_factory_supports_app_config`**（`agent_factory`）：返回 `_compute_agent_factory_supports_app_config(agent_factory)`。

  源码依据：[_cached_agent_factory_supports_app_config 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L796-L799)


- **`_agent_factory_supports_app_config`**（`agent_factory`）：在 `try` 分支中，返回 `_cached_agent_factory_supports_app_config(agent_factory)`；在 `捕获 TypeError` 分支中，返回 `_compute_agent_factory_supports_app_config(agent_factory)`。

  源码依据：[_agent_factory_supports_app_config 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L800-L807)


- **`_agent_graph`**（`agent_result`）：在 `捕获 Exception` 分支中，返回 `agent_result`；返回 `unwrap_agent_graph(agent_result)`。

  源码依据：[_agent_graph 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L808-L818)


- **`_assembled_model_name`**（`agent_result`）：在 `捕获 Exception` 分支中，返回 `None`；在 `条件 isinstance(agent_result, LeadAgentAssembly)` 分支中，返回 `agent_result.effective_model`；返回 `None`。

  源码依据：[_assembled_model_name 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L819-L829)


- **`_SubagentEventBuffer.__init__`**（`event_store, thread_id, run_id`）：以 `event_store` 计算 `self._event_store`；以 `thread_id` 计算 `self._thread_id`；以 `run_id` 计算 `self._run_id`；以 `[]` 计算 `self._pending: list[dict[str, Any]]`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L854-L859)


- **`_SubagentEventBuffer.add`**（`chunk`）：在 `条件 self._event_store is None` 分支中，返回 None，提前结束当前路径；以 `subagent_run_event(chunk)` 计算 `record`；在 `条件 record is None` 分支中，返回 None，提前结束当前路径；执行 `self._pending.append({"thread_id": self._thread_id, "run_id": self._run_id, **record})`；在 `条件 record["event_type"] == "subagent.end" or len(self._pending) >= self.FLUSH_THRESHOLD` 分支中，等待 `self.flush()` 完成。

  源码依据：[add 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L860-L876)


### 5.31.12 _SubagentEventBuffer.flush

无显式业务参数。

以 `self._pending` 计算 `batch`。关键判定为 `self._event_store is None or not self._pending`、`host_task is None or not host_task.cancelling()`、`cancellation is None`。以 `[]` 计算 `self._pending`；以 `batch + self._pending` 计算 `self._pending`；其他写入目标为 `self._pending`。

以 `asyncio.create_task(self._event_store.put_batch(batch))` 计算 `write_task`；等待 `asyncio.shield(write_task)` 完成。捕获 `asyncio.CancelledError as exc`、`Exception as exc` 的边界见源码，显式抛出路径涉及 `当前异常`、`cancellation`。分支返回 `None`，提前返回结束该路径。

源码依据：[flush 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L877-L918)


### 5.31.13 辅助函数与局部契约

- **`_bind_trace_id`**（`config, runtime_ctx`）：以 `ensure_trace_id()` 计算 `trace_id`；以 `trace_id` 计算 `runtime_ctx[DEERFLOW_TRACE_METADATA_KEY]`；以 `config.get("metadata")` 计算 `incoming_metadata`；以 `dict(incoming_metadata) if isinstance(incoming_metadata, dict) else {}` 计算 `merged_metadata`；以 `trace_id` 计算 `merged_metadata[DEERFLOW_TRACE_METADATA_KEY]`；以 `merged_metadata` 计算 `config["metadata"]`；返回 `trace_id`。

  源码依据：[_bind_trace_id 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L919-L944)


- **`_defer_finalization_interrupt`**（`deferred, interrupt`）：在 `条件 isinstance(interrupt, asyncio.CancelledError)` 分支中，以 `asyncio.current_task()` 计算 `task`；在 `条件 isinstance(interrupt, asyncio.CancelledError)` → `条件 task is not None` → `循环 while task.cancelling()` 分支中，执行 `task.uncancel()`；返回 `deferred if deferred is not None else interrupt`。

  源码依据：[_defer_finalization_interrupt 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L945-L957)


- **`_await_task_stop_after_host_cancellation`**（`task, deferred`）：在 `循环 while True` → `try` 分支中，等待 `asyncio.shield(task)` 完成；在 `循环 while True` → `try` 分支中，返回 `deferred`；在 `循环 while True` → `捕获 asyncio.CancelledError as exc` 分支中，以 `asyncio.current_task()` 计算 `host`；在 `循环 while True` → `捕获 asyncio.CancelledError as exc` → `条件 host is None or not host.cancelling()` 分支中，向外继续抛出当前异常；在 `循环 while True` → `捕获 asyncio.CancelledError as exc` 分支中，以 `_defer_finalization_interrupt(deferred, exc)` 计算 `deferred`。

  源码依据：[_await_task_stop_after_host_cancellation 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L958-L974)


- **`_drain_run_finalization`**（`task, on_host_cancel=None`）：以 `None` 计算 `deferred: BaseException | None`。关键判定为 `host is None or not host.cancelling()`、`deferred is not None`、`on_host_cancel is not None`，相应动作只在对应分支执行。等待 `asyncio.shield(task)` 完成；执行 `if host is None or not host.cancelling():`。捕获 `asyncio.CancelledError as exc`、`BaseException as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `deferred`、`当前异常`。

  源码依据：[_drain_run_finalization 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L975-L1003)


### 5.31.14 run_agent

**参数摘要**

```text
    bridge,
    run_manager,
    record,
    ctx,
    agent_factory,
    graph_input,
    config,
    thread_incarnation=_THREAD_INCARNATION_UNSET,
    stream_modes=None,
    stream_subgraphs=False,
    interrupt_before=None,
    interrupt_after=None,
    knowledge_scope=None
```

**职责、输入与执行所有权**

这是每个新运行的实际 worker。它接收已准入记录、宿主依赖、工厂、graph input 和可信 config；执行期间拥有本次 run 的状态推进、流生产与收尾。调用它不自动产生另一个线程：Python 协程由 Gateway event loop 调度，装配、同步文件与部分依赖另有执行池或资源管理。

1. 先通过 manager 确认可以开始，建立运行级状态、取消控制、checkpoint accessor、日志 journal 和 producer 跟踪。装配图与绑定 checkpointer 是分开的步骤：配置选择的图仍需挂接宿主持久资源。
2. 保存回滚所需的执行前 checkpoint/工作区信息，传入 run、thread incarnation、身份及运行能力。schema/模式不兼容或恢复请求失效在继续执行前失败。
3. `_stream_once` 消费图的 `astream`，区分 root 与子图 namespace，以及 messages、updates、values、custom 等模式；数据序列化后写 bridge，journal 和子任务事件收集器分别积累持久证据。局部 values 帧只是中间状态。

源码依据：[worker 建立与取消闭包](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1005-L1184) · [流消费及暂停识别](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1691-L1816)

4. 图退出后检查人工暂停、目标继续执行和宿主取消等条件。中断产生 `waiting_for_input`，目标续跑会重新消费图；正常无后续动作才进入成功候选路径。执行异常、超时、递归预算和取消具有不同 stop/error 语义。
5. worker 不在取得一条最终 AI 消息后直接释放资源，而是进入独立 `finalize` 协程；已注册 worker 仍等待这个协程的终态和清理 await。宿主取消可能被延后到清理后再传播。

源码依据：[终态与资源收尾](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2021-L2481)

**失败语义**

取消的 interrupt 路径保留已有图状态，rollback 路径尝试恢复执行前图状态并处理运行产物边界；两者都不能撤销任意已完成网络或远端命令动作。worker 失去 lease 后停止以当前 owner 身份写持久终态，将收敛责任留给有权执行者。细节由本文件其余局部函数逐一给出。

### 5.31.15 辅助函数与局部契约

- **`run_agent._ensure_stream_producer_lease`**（无显式业务参数）：父函数内定义的回调。使用外层变量 `nonlocal stream_producer_lease_started`；在 `条件 stream_producer_lease_started` 分支中，返回 None，提前结束当前路径；以 `_declared_bridge_method( bridge, "start_producer_lease", )` 计算 `start_producer_lease`；在 `条件 start_producer_lease is None` 分支中，返回 None，提前结束当前路径；以 `await start_producer_lease(run_id)` 计算 `tracked`；在 `条件 not tracked` 分支中，返回 None，提前结束当前路径；以 `True` 计算 `stream_producer_lease_started`。

  源码依据：[_ensure_stream_producer_lease 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1107-L1123)


### 5.31.16 run_agent._finish_cancellation

参数：`action, restore_checkpoint=True`。

父函数内定义的回调。等待执行停止并取消 sandbox 命令 scope；按 interrupt/rollback 决定保留还是恢复图状态，再提交 interrupted/失败收敛，外部任意副作用没有通用补偿。

源码依据：[_finish_cancellation 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1124-L1184)


### 5.31.17 辅助函数与局部契约

- **`run_agent.record_deferred_result`**（`message`）：父函数内定义的回调。在 `条件 journal is not None and not record.ownership_lost` 分支中，执行 `journal.record_deferred_task_result(message)`。

  源码依据：[record_deferred_result 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1416-L1419)


- **`run_agent.record_steered_response`**（`message`）：父函数内定义的回调。在 `条件 journal is not None and not record.ownership_lost` 分支中，执行 `journal.record_steered_response(message)`。

  源码依据：[record_steered_response 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1420-L1423)


- **`run_agent._continuation_runnable_config`**（无显式业务参数）：父函数内定义的回调。以 `dict(config)` 计算 `continuation_config`；以 `dict(continuation_config.get("configurable", {}) or {})` 计算 `configurable`；以 `""` 计算 `configurable["checkpoint_ns"]`；执行 `configurable.pop("checkpoint_id", None)`；执行 `configurable.pop("checkpoint_map", None)`；以 `configurable` 计算 `continuation_config["configurable"]`；以 `RunnableConfig(**continuation_config)` 计算 `continuation`；执行 `runnable_configs.append(continuation)`；返回 `continuation`。

  源码依据：[_continuation_runnable_config 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1463-L1473)


- **`run_agent._get_goal_evaluator_model`**（无显式业务参数）：父函数内定义的回调。使用外层变量 `nonlocal goal_evaluator_model`；在 `条件 goal_evaluator_model is None` 分支中，以 `create_goal_evaluator_model( model_name=record.model_name, app_config=ctx.app_config, )` 计算 `goal_evaluator_model`；返回 `goal_evaluator_model`。

  源码依据：[_get_goal_evaluator_model 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1628-L1638)


- **`run_agent.observe_stream_pause`**（`mode, chunk, namespace`）：父函数内定义的回调。在 `条件 namespace or mode not in {"values", "updates"} or not isinstance(chunk, Mapping)` 分支中，返回 None，提前结束当前路径；在 `条件 mode == "values"` 分支中，执行 `streamed_interrupts.clear()`；以 `chunk.get("__interrupt__")` 计算 `interrupts`；在 `条件 not interrupts` 分支中，返回 None，提前结束当前路径；执行 `streamed_interrupts.extend(interrupts if isinstance(interrupts, (list, tuple)) else [interrupts])`。

  源码依据：[observe_stream_pause 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1641-L1650)


- **`run_agent.remember_approval`**（`interrupts`）：父函数内定义的回调。以 `[]` 计算 `payloads`；在 `循环 for pending_interrupt in interrupts` 分支中，以 `pending_interrupt.get("value") if isinstance(pending_interrupt, Mapping) else getattr(pending_interrupt, "value", None)` 计算 `value`；在 `循环 for pending_interrupt in interrupts` → `条件 isinstance(value, dict) and value.get("kind") == "hitl_approval"` 分支中，执行 `payloads.append(value)`；在 `条件 payloads` 分支中，以 `combine_approval_payloads(payloads)` 计算 `record.metadata["hitl_approval"]`。

  源码依据：[remember_approval 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1651-L1661)


- **`run_agent.detect_pending_pause`**（无显式业务参数）：父函数内定义的回调。以 `bool(hitl_config is not None and hitl_config.enabled)` 计算 `hitl_enabled`。关键判定为 `pending_pause or record.abort_event.is_set()`、`streamed_interrupts and not hitl_enabled`、`not hitl_enabled and {"values", "updates"} & requested_modes`，相应动作只在对应分支执行。分支返回 `None`，提前返回结束该路径。

  源码依据：[detect_pending_pause 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1662-L1690)


### 5.31.18 run_agent._stream_once

参数：`input_payload, stream_config`。

父函数内定义的回调。消费 agent.astream 并分辨 root/子图 mode，聚合消息和暂停信号，调用 _publish_stream_item 投递桥；图异常向外层分类，不能用一次 values 帧作为最终成功。

源码依据：[_stream_once 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1691-L1816)


### 5.31.19 辅助函数与局部契约

- **`run_agent.capture_goal_verdict`**（`evaluated_goal, evaluation, stand_down_reason`）：父函数内定义的回调。以 `dict(evaluation)` 计算 `final_goal_verdict`。关键判定为 `scheduled_goal is not None and not _goal_instance_matches(scheduled_goal, evaluated_goal)`、`stand_down_reason`、`stand_down_reason.startswith("thread_changed")`，相应动作只在对应分支执行。以 `bool(evaluation.get("relied_on_assumption", False)) if evaluation["satisfied"] else False` 计算 `final_goal_verdict["relied_on_assumption"]`；以 `int(evaluated_goal.get("continuation_count", 0))` 计算 `final_goal_verdict["continuations"]`；其他写入目标为 `final_goal_verdict["stand_down_reason"]`。执行 `final_goal_verdict.update(satisfied=False, blocker="missing_evidence", relied_on_assumption=False)`。执行 `if stand_down_reason.startswith("thread_changed"):`。分支返回 `None`，提前返回结束该路径。

  源码依据：[capture_goal_verdict 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L1820-L1832)


- **`run_agent.interrupt_completion_hook`**（无显式业务参数）：父函数内定义的回调。使用外层变量 `nonlocal host_interrupted`；以 `True` 计算 `host_interrupted`；在 `条件 completion_hook_task is not None` 分支中，执行 `completion_hook_task.cancel()`。

  源码依据：[interrupt_completion_hook 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2015-L2020)


### 5.31.20 run_agent.finalize

**职责与顺序为何影响正确性**

这是 `run_agent` 的局部异步函数，闭包持有图、accessor、journal、审批 runtime、checkpoint 快照、目标候选和宿主资源。它既提交业务结果，也结束运行资源寿命；多处 best-effort 捕获只是允许继续清理，不代表失败步骤已经补齐。

1. 关闭追加输入控制，确认对应 checkpoint；失败或停止时暂停剩余输入。失去 ownership 后跳过只能由 owner 执行的持久写入。编辑重放失败时按条件恢复执行前 checkpoint，避免损坏待恢复状态。
2. 排出剩余子任务事件，记录工作区变化，flush journal，再持久化交付 receipt。若已有输出产物而 receipt 未能保存，成功候选会转为 error，不能把未关联的交付说成成功。
3. 在持久行仍保持运行屏障期间写入结束进度和时长 checkpoint。scheduled goal 属于本次 occurrence，须在释放真实运行槽之前清理，以免下一个用户回合继承它。

源码依据：[输入、journal 与交付记录](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2021-L2145) · [目标清理及终态仲裁](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2146-L2210)

4. 与晚到取消仲裁后提交真实终态；只在持久确认或 ownership loss 允许时释放 terminal barrier。usage 等附加 completion 写失败可记录后继续。普通目标只有在成功和持久状态均确认后才清除。
5. 等待完成 hook、扩展 task-stop 等观察者；结束证据需要可归属的 terminal ACK 和 receipt，不能封口的证据被标为 partial。之后解除 finalizing。
6. 只有没有失去 ownership、状态已非 pending/running、且持久终态确认的运行才向流发终止错误帧及 END；缺少确认时不发送一个虚假的成功结束。

源码依据：[终态 ACK 与 completion](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2185-L2246) · [证据封口与流结束条件](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2290-L2405)

7. 多层 finally 关闭审批 runtime、journal，结束 producer lease，异步归还 sandbox execution lease；清掉 runnable config 中的宿主引用和闭包大对象。流数据延迟 60 秒清理，本地运行注册保持其宽限期再回收；持久 RunStore 历史另行保留。

源码依据：[资源释放与延迟回收](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2406-L2481)

**取消和残余风险**

发生取消时，部分清理会先排空底层操作，再向外重新抛出取消。因此“停止等待”“终态可见”“资源已全部释放”可以处于不同时间。durable terminal、文件、模型供应商和任意外部动作不属于一个跨资源事务。

### 5.31.21 辅助函数与局部契约

- **`run_agent.finalize.invoke_completion_hook`**（无显式业务参数）：父函数内定义的回调。等待 `ctx.on_run_completed(record)` 完成。

  源码依据：[invoke_completion_hook 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2267-L2269)


- **`run_agent.finalize.confirm_terminal`**（无显式业务参数）：父函数内定义的回调。以 `run_manager.terminal_event_status(record)` 计算 `status`。关键判定为 `status is None`、`not confirmation.confirmed`，相应动作只在对应分支执行。显式抛出路径涉及 `RuntimeError`。

  源码依据：[confirm_terminal 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2333-L2352)


- **`_scheduled_goal_objective`**（`record, scheduled_runtime`）：以 `record.metadata or {}` 计算 `metadata`；以 `metadata.get("scheduled_goal_objective")` 计算 `objective`。关键判定为 `scheduled_runtime is None or scheduled_runtime.get("goal_objective") is None`、「表达式摘要 1」、`not isinstance(objective, str) or objective != scheduled_runtime.get("goal_objective")`，相应动作只在对应分支执行。显式抛出路径涉及 `ValueError`。分支返回 `None`、`objective`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  not record.user_id or scheduled_runtime.get("user_id") != record.user_id or scheduled_runtime.get("task_id") !=
  metadata.get("scheduled_task_id") or scheduled_runtime.get("occurrence_id") != metadata.get("scheduled_task_run_id")
  ```


  源码依据：[_scheduled_goal_objective 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2510-L2521)


- **`_install_scheduled_goal`**（`checkpointer, accessor, thread_id, goal`）：以 `await _call_checkpointer_method(checkpointer, "aget_tuple", "get_tuple", {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}})` 计算 `checkpoint`。关键判定为 `checkpointer is None`、`checkpoint is not None`、「表达式摘要 1」，相应动作只在对应分支执行。进入 `goal_thread_lock(thread_id), _checkpoint_thread_lock(thread_id)` 边界，退出由对应 context manager 处理。等待 「表达式摘要 2」 完成。显式抛出路径涉及 `RuntimeError`、`GoalWriteConflict`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  _read_checkpoint_goal(checkpoint) is not None or any(_is_visible_message(message) for message in await _materialized_checkpoint_messages(accessor,
       thread_id))
  ```


  **表达式摘要 2**（非完整源码；换行仅为阅读排版）：

  ```text
  write_thread_goal(checkpointer, thread_id, goal, as_node="scheduled_goal",
     create_if_missing=True, expected_checkpoint_id=_checkpoint_id(checkpoint))
  ```


  源码依据：[_install_scheduled_goal 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2522-L2532)


- **`_clear_scheduled_goal`**（`goal, record, run_manager, bridge, checkpointer, owned_admission=False`）：以 「表达式摘要 1」 计算 `reservation`。关键判定为 `owned_admission and not await run_manager.owns_active_admission(record)`、`checkpoint is None or not _goal_instance_matches(goal, _read_checkpoint_goal(checkpoint)) or record.ownership_lost`、`bridge is not None`，相应动作只在对应分支执行。进入 `goal_thread_lock(record.thread_id), reservation` 边界，退出由对应 context manager 处理。等待 `bridge.publish(record.run_id, "values", serialize(values, mode="values"))` 完成。捕获 `(GoalWriteConflict, ConflictError)` 的边界见源码，捕获不等于已恢复。分支返回 `None`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  _checkpoint_thread_lock(record.thread_id) if owned_admission else run_manager.reserve_thread_operation(record.thread_id,
     kind=ThreadOperationKind.checkpoint_write, user_id=record.user_id)
  ```


  源码依据：[_clear_scheduled_goal 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2533-L2549)


- **`clear_recovered_scheduled_goal`**（`record, run_manager, checkpointer`）：以 `record.metadata or {}` 计算 `metadata`。关键判定为 `checkpointer is None or not record.user_id or not isinstance(objective, str)`、`source is None or source.run_id != record.run_id`，相应动作只在对应分支执行。等待 `_clear_scheduled_goal(goal=expected, record=record, run_manager=run_manager, bridge=None, checkpointer=checkpointer)` 完成。捕获 `Exception` 的边界见源码，捕获不等于已恢复。分支返回 `None`，提前返回结束该路径。

  源码依据：[clear_recovered_scheduled_goal 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2550-L2571)


- **`_clear_stale_scheduled_goal`**（`record, run_manager, bridge, checkpointer`）：以 「表达式摘要 1」 计算 `checkpoint`。关键判定为 `checkpointer is None or not record.user_id`、`current is None`、`current.get("created_at") != created_at`，相应动作只在对应分支执行。进入 `goal_thread_lock(record.thread_id), _checkpoint_thread_lock(record.thread_id)` 边界，退出由对应 context manager 处理。等待 `bridge.publish(record.run_id, "values", serialize(values, mode="values"))` 完成。捕获 `(TypeError, ValueError)`、`Exception as exc`、`GoalWriteConflict` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `RuntimeError`。分支返回 `None`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await _call_checkpointer_method(checkpointer, "aget_tuple", "get_tuple",
     {"configurable": {"thread_id": record.thread_id, "checkpoint_ns": ""}})
  ```


  源码依据：[_clear_stale_scheduled_goal 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2572-L2603)


### 5.31.22 _clear_completed_goal

参数：`candidate, record, run_manager, bridge, accessor, checkpointer`。

以 「表达式摘要 1」 计算 `checkpoint_tuple`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
await _call_checkpointer_method( checkpointer, "aget_tuple", "get_tuple",
   {"configurable": {"thread_id": record.thread_id, "checkpoint_ns": ""}},
   )
```


关键判定为 `checkpoint_tuple is None`、`_read_checkpoint_goal(checkpoint_tuple) != candidate.goal`、`visible_conversation_signature(messages) != candidate.conversation_signature`。

进入 「表达式摘要 1」 边界，退出由对应 context manager 处理。等待 `bridge.publish(record.run_id, "values", serialize(values, mode="values"))` 完成。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
( goal_thread_lock(record.thread_id), run_manager.reserve_thread_operation(record.thread_id,
     kind=ThreadOperationKind.checkpoint_write, user_id=record.user_id),
   )
```


捕获 `GoalWriteConflict`、`ConflictError` 的边界见源码，分支返回 `None`，提前返回结束该路径。

源码依据：[_clear_completed_goal 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2604-L2666)


### 5.31.23 辅助函数与局部契约

- **`_checkpoint_id`**（`checkpoint_tuple`）：以 `getattr(checkpoint_tuple, "config", {}) or {}` 计算 `config`；以 `config.get("configurable", {}) if isinstance(config, dict) else {}` 计算 `configurable`；以 `configurable.get("checkpoint_id") if isinstance(configurable, dict) else None` 计算 `checkpoint_id`；在 `条件 isinstance(checkpoint_id, str)` 分支中，返回 `checkpoint_id`；以 `getattr(checkpoint_tuple, "checkpoint", {}) or {}` 计算 `checkpoint`；在 `条件 isinstance(checkpoint, dict) and isinstance(checkpoint.get("id"), str)` 分支中，返回 `checkpoint["id"]`；返回 `None`。

  源码依据：[_checkpoint_id 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2667-L2678)


- **`_goal_instance_matches`**（`left, right`）：在 `条件 not left or not right` 分支中，返回 `False`；以 `left.get("status") == right.get("status") == "active"` 计算 `same_status`；以 `left.get("objective") == right.get("objective")` 计算 `same_objective`；以 `left.get("created_at") == right.get("created_at")` 计算 `same_created_at`；返回 `same_status and same_objective and same_created_at`。

  源码依据：[_goal_instance_matches 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2679-L2687)


- **`_materialized_checkpoint_messages`**（`accessor, thread_id`）：以 `await accessor.aget({"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}})` 计算 `snapshot`；以 `getattr(snapshot, "values", None) or {}` 计算 `values`；以 `values.get("messages") if isinstance(values, dict) else None` 计算 `messages`；返回 `list(messages) if isinstance(messages, list) else []`。

  源码依据：[_materialized_checkpoint_messages 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2688-L2700)


- **`_read_checkpoint_goal`**（`checkpoint_tuple`）：以 `getattr(checkpoint_tuple, "checkpoint", {}) or {}` 计算 `checkpoint`；以 `checkpoint.get("channel_values", {}) if isinstance(checkpoint, dict) else {}` 计算 `channel_values`；以 `channel_values.get("goal") if isinstance(channel_values, dict) else None` 计算 `raw_goal`；返回 `copy.deepcopy(raw_goal) if isinstance(raw_goal, dict) else None`。

  源码依据：[_read_checkpoint_goal 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2701-L2707)


- **`_has_durable_goal_turn_receipt`**（`checkpoint_tuple, messages`）：在 `条件 _checkpoint_id(checkpoint_tuple) is None` 分支中，返回 `False`；在 `条件 getattr(checkpoint_tuple, "pending_writes", None)` 分支中，返回 `False`；以 `[]` 计算 `visible_messages`；在 `循环 for message in messages` → `条件 _is_visible_message(message) and message_to_text(message).strip()` 分支中，执行 `visible_messages.append(message)`；在 `条件 not visible_messages` 分支中，返回 `False`；返回 `_message_type(visible_messages[-1]) == "ai"`。

  源码依据：[_has_durable_goal_turn_receipt 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2708-L2727)


- **`_ends_on_human_input_request`**（`messages`）：在 `循环 for message in reversed(messages)` → `条件 _message_type(message) != "tool"` 分支中，返回 `False`；在 `循环 for message in reversed(messages)` 分支中，以 `message.get("artifact") if isinstance(message, dict) else getattr(message, "artifact", None)` 计算 `artifact`；在 `循环 for message in reversed(messages)` 分支中，以 `artifact.get("human_input") if isinstance(artifact, Mapping) else None` 计算 `human_input`；在 `循环 for message in reversed(messages)` → `条件 isinstance(human_input, Mapping) and human_input.get("kind") == "human_input_request"` 分支中，返回 `True`；返回 `False`。

  源码依据：[_ends_on_human_input_request 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2728-L2745)


- **`_stand_down_reason`**（`goal, evaluation, no_progress_count`）：在 `条件 evaluation["satisfied"]` 分支中，返回 `None`；在 `条件 evaluation["blocker"] != "goal_not_met_yet"` 分支中，返回 `f"blocked:{evaluation['blocker']}"`；在 `条件 int(goal.get("continuation_count", 0)) >= int(goal.get("max_continuations", DEFAULT_MAX_GOAL_CONTINUATIONS))` 分支中，返回 `"max_continuations_reached"`；在 `条件 no_progress_count >= int(goal.get("max_no_progress_continuations", DEFAULT_MAX_NO_PROGRESS_CONTINUATIONS))` 分支中，返回 `"no_progress_detected"`；返回 `None`。

  源码依据：[_stand_down_reason 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2746-L2759)


### 5.31.24 _persist_goal_evaluation

**参数摘要**

```text
    bridge,
    checkpointer,
    thread_id,
    run_id,
    goal,
    evaluation,
    no_progress_count,
    continuation_count=None,
    stand_down_reason=None,
    evidence_signature="",
    operation_progress=None
```

goal_thread_lock 内重读目标和 checkpoint，按新鲜次数递增并以 expected_checkpoint_id 写 goal，随后 publish values；写前冲突返回 None。锁只覆盖当前 loop，ID 校验不是跨存储原子 CAS。

源码依据：[_persist_goal_evaluation 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2760-L2819)


### 5.31.25 辅助函数与局部契约

- **`_reread_goal_and_checkpoint`**（`checkpointer, thread_id`）：以 `await read_thread_goal(checkpointer, thread_id)` 计算 `goal`；以 `await _call_checkpointer_method( checkpointer, "aget_tuple", "get_tuple", {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}, )` 计算 `checkpoint_tuple`；返回 `goal, checkpoint_tuple`。

  源码依据：[_reread_goal_and_checkpoint 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2820-L2831)


### 5.31.26 _prepare_goal_continuation_input

**参数摘要**

```text
    bridge,
    accessor,
    checkpointer,
    thread_id,
    run_id,
    model_name,
    app_config,
    evaluator_model_factory=None,
    abort_event=None,
    user_id=None,
    deerflow_trace_id=None,
    task_store=None,
    extensions=None,
    run_stop_reason=None,
    interaction_policy=None,
    expected_goal=None,
    verdict_callback=None,
    usage_callback=None
```

只为仍 active 的同一 goal、无 abort 且有 durable 回合凭据评估；格式错误最多重试一次。评估后重读 goal/checkpoint/消息签名，冲突停下；满足则返回完成候选，未满足按 blocker/续跑次数/无进展/hard cap 停下或先脱敏、记次数、再复核后返回隐藏 HumanMessage。

源码依据：[_prepare_goal_continuation_input 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2832-L3104)


### 5.31.27 辅助函数与局部契约

- **`_prepare_goal_continuation_input._persist`**（`goal, evaluation, no_progress_count, stand_down_reason=None, continuation_count=None`）：父函数内定义的回调。以 「表达式摘要 1」 计算 `updated`；在 `条件 updated is not None and verdict_callback is not None` 分支中，执行 `verdict_callback(updated, evaluation, stand_down_reason)`；返回 `updated`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await _persist_goal_evaluation( bridge=bridge, checkpointer=checkpointer,
     thread_id=thread_id, run_id=run_id, goal=goal, evaluation=evaluation,
     no_progress_count=no_progress_count, continuation_count=continuation_count,
     stand_down_reason=stand_down_reason, evidence_signature=evidence_signature,
     operation_progress=operation_progress, )
  ```


  源码依据：[_persist 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2875-L2900)


- **`_is_edit_replay_run`**（`record`）：以 `record.metadata or {}` 计算 `metadata`；返回 `metadata.get("replay_kind") == "edit"`。

  源码依据：[_is_edit_replay_run 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3105-L3109)


- **`_ensure_finalizing_before_edit_failure`**（`run_manager, record`）：在 `条件 _is_edit_replay_run(record) and not record.finalizing` 分支中，等待 `run_manager.set_finalizing(record.run_id, True)` 完成。

  源码依据：[_ensure_finalizing_before_edit_failure 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3110-L3114)


- **`_publish_restored_checkpoint_values`**（`bridge, run_id, accessor, thread_id`）：在 `条件 accessor is None` 分支中，返回 None，提前结束当前路径；以 `await accessor.aget({"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}})` 计算 `snapshot`；以 `getattr(snapshot, "values", None)` 计算 `values`；在 `条件 isinstance(values, dict)` 分支中，等待 `bridge.publish(run_id, "values", serialize(values, mode="values"))` 完成。

  源码依据：[_publish_restored_checkpoint_values 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3115-L3129)


- **`_capture_rollback_point`**（`accessor, checkpointer, read_config`）：以 `await accessor.aget(read_config)` 计算 `snapshot`。关键判定为 `not configurable.get("checkpoint_id")`，相应动作只在对应分支执行。分支返回 `None`、「表达式摘要 1」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  RollbackPoint( config={ "configurable": { "thread_id": configurable.get("thread_id"),
         "checkpoint_ns": configurable.get("checkpoint_ns") or "", "checkpoint_id": configurable.get("checkpoint_id"),
         } }, state_values=state_values, messages=tuple(messages or ()),
     metadata=dict(getattr(snapshot, "metadata", None) or {}), pending_writes=tuple(getattr(checkpoint_tuple,
         "pending_writes", ()) or ()), )
  ```


  源码依据：[_capture_rollback_point 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3147-L3180)


- **`_complete_state_replacement_values`**（`mutation_graph, selected_values, current_values, run_id, operation`）：以 `graph_writable_channels(mutation_graph)` 计算 `writable_fields`；以 `{}` 计算 `replacement_values: dict[str, Any]`。关键判定为 `writable_fields is None or reducer_fields is None`、`field_name in selected_values`、`field_name in current_values`，相应动作只在对应分支执行。以 `Overwrite(replacement) if field_name in reducer_fields else replacement` 计算 `replacement_values[field_name]`。显式抛出路径涉及 `RuntimeError`。分支返回 `replacement_values`，提前返回结束该路径。

  源码依据：[_complete_state_replacement_values 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3181-L3210)


- **`_is_resume_command_input`**（`graph_input`）：返回 `isinstance(graph_input, Command) and graph_input.resume is not None`。

  源码依据：[_is_resume_command_input 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3211-L3215)


- **`_hitl_action_calls`**（`actions`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  {action["tool_call_id"]: action["tool_name"] for action in actions if isinstance(action.get("tool_call_id"),
       str) and action["tool_call_id"] and isinstance(action.get("tool_name"),
       str)}
  ```


  源码依据：[_hitl_action_calls 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3216-L3225)


### 5.31.28 _pending_hitl_resume_actions

参数：`checkpointer, checkpoint_config, graph_input`。

以 `approval_responses(graph_input.resume)` 计算 `responses`；以 `[]` 计算 `matched: list[Mapping[str, Any]]`。关键判定为 `checkpointer is None or not _is_resume_command_input(graph_input) or not isinstance(graph_input.resume, Mapping)`、`not isinstance(responses, list)`、`not response_ids`。

执行 `matched.extend(action for action in actions if isinstance(action, Mapping))`。分支返回 `[]`、`matched`，提前返回结束该路径。

源码依据：[_pending_hitl_resume_actions 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3226-L3272)


### 5.31.29 辅助函数与局部契约

- **`_is_all_reject_hitl_resume`**（`graph_input`）：在 `条件 not isinstance(graph_input, Command) or not isinstance(graph_input.resume, dict)` 分支中，返回 `False`；以 `approval_responses(graph_input.resume)` 计算 `responses`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ( isinstance(responses, list) and bool(responses) and all(isinstance(response,
         dict) and response.get("option_id") == "reject" and isinstance(response.get("tool_call_id"),
         str) and bool(response["tool_call_id"]) for response in responses) )
  ```


  源码依据：[_is_all_reject_hitl_resume 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3273-L3286)


### 5.31.30 _linearize_delta_checkpoint_resume

参数：`accessor, checkpointer, config, thread_id, run_id, resume_command=False`。

以 `config.get("configurable")` 计算 `configurable`。关键判定为 `checkpointer is None or accessor.mode != "delta"`、`not isinstance(configurable, dict)`、`not isinstance(checkpoint_id, str) or not checkpoint_id`。

以 `checkpoint_agent_binding_metadata(getattr(snapshot, "metadata", None))` 计算 `head_config["metadata"]`；以 `head_values.get("goal")` 计算 `selected_values["goal"]`；其他写入目标为 `selected_values["goal_outcome"]`。执行 `configurable.pop("checkpoint_id", None)`；执行 `configurable.pop("checkpoint_map", None)`。

等待 `mutation_accessor.aupdate(head_config, replacement_values, as_node="checkpoint_resume")` 完成。显式抛出路径涉及 `RuntimeError`、`CheckpointResumeRejected`。分支返回 `None`、`list(messages)`，提前返回结束该路径。

源码依据：[_linearize_delta_checkpoint_resume 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3291-L3403)


### 5.31.31 _rollback_to_pre_run_checkpoint

参数：`accessor, checkpointer, thread_id, run_id, rollback_point, snapshot_capture_failed`。

用运行前 checkpoint 与有效 state schema 恢复图 channel，清理相应状态/后续 head；不是对主机文件、网络调用或容器副作用的回滚。

源码依据：[_rollback_to_pre_run_checkpoint 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3404-L3515)


### 5.31.32 辅助函数与局部契约

- **`_new_checkpoint_marker`**（无显式业务参数）：以 `empty_checkpoint()` 计算 `marker`；返回 `{"id": marker["id"], "ts": marker["ts"]}`。

  源码依据：[_new_checkpoint_marker 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3516-L3520)


### 5.31.33 _bump_channel_version

参数：`checkpointer, current_version`。

以 `getattr(checkpointer, "get_next_version", None)` 计算 `get_next_version`；以 `None` 计算 `next_version`。关键判定为 `callable(get_next_version)`、`next_version is not None and next_version != current_version`、`isinstance(current_version, bool)`。捕获 `Exception`、`ValueError` 的边界见源码，

分支返回 `next_version`、`int(current_version) + 1`、`current_version + 1`、`current_version + 1.0` 等值，提前返回结束该路径。

源码依据：[_bump_channel_version 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3521-L3560)


### 5.31.34 辅助函数与局部契约

- **`_checkpoint_identity`**（`ckpt_tuple, checkpoint`）：以 `getattr(ckpt_tuple, "config", {}) or {}` 计算 `tuple_config`；以 `tuple_config.get("configurable", {}) if isinstance(tuple_config, dict) else {}` 计算 `tuple_configurable`；在 `条件 isinstance(tuple_configurable, dict)` 分支中，以 `tuple_configurable.get("checkpoint_id")` 计算 `checkpoint_id`；在 `条件 isinstance(tuple_configurable, dict)` → `条件 isinstance(checkpoint_id, str) and checkpoint_id` 分支中，返回 `checkpoint_id`；以 `checkpoint.get("id")` 计算 `checkpoint_id`；返回 `checkpoint_id if isinstance(checkpoint_id, str) and checkpoint_id else None`。

  源码依据：[_checkpoint_identity 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3561-L3571)


- **`_checkpoint_namespace`**（`ckpt_tuple`）：以 `getattr(ckpt_tuple, "config", {}) or {}` 计算 `tuple_config`；以 `tuple_config.get("configurable", {}) if isinstance(tuple_config, dict) else {}` 计算 `tuple_configurable`；以 `tuple_configurable.get("checkpoint_ns", "") if isinstance(tuple_configurable, dict) else ""` 计算 `checkpoint_ns`；返回 `checkpoint_ns if isinstance(checkpoint_ns, str) else ""`。

  源码依据：[_checkpoint_namespace 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3572-L3578)


- **`_graph_input_messages`**（`graph_input`）：在 `条件 not isinstance(graph_input, dict)` 分支中，返回 `[]`；以 `graph_input.get("messages")` 计算 `messages`；在 `条件 isinstance(messages, list)` 分支中，返回 `messages`；在 `条件 isinstance(messages, tuple)` 分支中，返回 `list(messages)`；返回 `[]`。

  源码依据：[_graph_input_messages 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3579-L3589)


- **`_title_generation_state`**（`channel_values, graph_input`）：以 `dict(channel_values)` 计算 `state`；以 `state.get("messages")` 计算 `messages`；在 `条件 not messages` 分支中，以 `_graph_input_messages(graph_input)` 计算 `fallback_messages`；在 `条件 not messages` → `条件 fallback_messages` 分支中，以 `fallback_messages` 计算 `state["messages"]`；返回 `state`。

  源码依据：[_title_generation_state 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3590-L3599)


- **`valid_duration_entry`**（`run_id, duration_seconds`）：返回 `isinstance(run_id, str) and bool(run_id) and isinstance(duration_seconds, int) and not isinstance(duration_seconds, bool)`。

  源码依据：[valid_duration_entry 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3600-L3604)


- **`valid_run_message_id_entry`**（`message_id, run_id`）：返回 `isinstance(message_id, str) and bool(message_id) and isinstance(run_id, str) and bool(run_id)`。

  源码依据：[valid_run_message_id_entry 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3608-L3612)


### 5.31.35 persist_run_history_metadata

参数：`checkpointer, thread_id, durations=None, message_run_ids=None`。

以 「表达式摘要 1」 计算 `duration_updates`。关键判定为 `not duration_updates and not message_run_id_updates`、`ckpt_tuple is None`、`not changed_durations and not changed_message_run_ids`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
{run_id: max(0, duration_seconds) for run_id, duration_seconds in (durations or {}).items() if valid_duration_entry(run_id,
     duration_seconds)}
```


进入 `_checkpoint_thread_lock(thread_id)` 边界，退出由对应 context manager 处理。以 `"update"` 计算 `metadata["source"]`；以 `(prev_step + 1) if isinstance(prev_step, int) else 1` 计算 `metadata["step"]`；其他写入目标为 `metadata["run_durations"]`、`metadata[RUN_MESSAGE_IDS_METADATA_KEY]`、`metadata["writes"]`。

执行 `run_durations.update(changed_durations)`；执行 `run_message_ids.update(changed_message_run_ids)`。等待 `_call_checkpointer_method( checkpointer, "aput", "put", write_config, checkpoint, metadata, {}, )` 完成。分支返回 `False`、`True`，提前返回结束该路径。

源码依据：[persist_run_history_metadata 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3613-L3699)


### 5.31.36 辅助函数与局部契约

- **`persist_run_durations`**（`checkpointer, thread_id, durations`）：返回 `await persist_run_history_metadata( checkpointer=checkpointer, thread_id=thread_id, durations=durations, )`。

  源码依据：[persist_run_durations 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3700-L3713)


- **`_persist_run_duration`**（`checkpointer, thread_id, run_id, duration_seconds`）：等待 `persist_run_durations( checkpointer=checkpointer, thread_id=thread_id, durations={run_id: duration_seconds}, )` 完成。

  源码依据：[_persist_run_duration 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3714-L3728)


### 5.31.37 _ensure_interrupted_title

参数：`checkpointer, thread_id, app_config, graph_input=None`。

以 `TitleMiddleware(app_config=app_config) if app_config is not None else TitleMiddleware()` 计算 `middleware`；以 `channel_values.get("title")` 计算 `existing_title`；以 `result.get("title") if isinstance(result, dict) else None` 计算 `title`。关键判定为 `existing_title`、`not title`、`base_identity is None`。

以 `title` 计算 `channel_values["title"]`；以 `next_title_version` 计算 `channel_versions["title"]`；其他写入目标为 `checkpoint["channel_versions"]`、`metadata["source"]`、`metadata["step"]`、`metadata["writes"]`。执行 `checkpoint.update({"id": marker["id"], "ts": marker["ts"], "channel_values": channel_values})`。

等待 `_call_checkpointer_method( checkpointer, "aput", "put", write_config, checkpoint, metadata, {"title": next_title_version}, )` 完成。分支返回 `existing_title`、`None`、`title`，提前返回结束该路径。

源码依据：[_ensure_interrupted_title 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3729-L3813)


### 5.31.38 辅助函数与局部契约

- **`_lg_mode_to_sse_event`**（`mode`）：返回 `mode`。

  源码依据：[_lg_mode_to_sse_event 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3814-L3825)


- **`_error_fallback_message_from_metadata`**（`metadata, content`）：以 `metadata.get("error_detail")` 计算 `detail`；在 `条件 isinstance(detail, str) and detail.strip()` 分支中，返回 `detail.strip()`；以 `metadata.get("error_reason")` 计算 `reason`；在 `条件 isinstance(reason, str) and reason.strip()` 分支中，返回 `reason.strip()`；在 `条件 isinstance(content, str) and content.strip()` 分支中，返回 `content.strip()[:2000]`；返回 `"LLM provider failed after retries"`。

  源码依据：[_error_fallback_message_from_metadata 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3826-L3837)


- **`_message_id`**（`obj`）：以 `getattr(obj, "id", None)` 计算 `msg_id`；在 `条件 isinstance(msg_id, str) and msg_id` 分支中，返回 `msg_id`；在 `条件 isinstance(obj, dict)` 分支中，以 `obj.get("id")` 计算 `raw`；在 `条件 isinstance(obj, dict)` → `条件 isinstance(raw, str) and raw` 分支中，返回 `raw`；返回 `None`。

  源码依据：[_message_id 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3838-L3849)


- **`_try_extract_from_message`**（`obj, pre_existing_ids=None`）：以 `_message_id(obj)` 计算 `msg_id`。关键判定为 `pre_existing_ids`、`msg_id is not None and msg_id in pre_existing_ids`、`isinstance(additional_kwargs, dict) and additional_kwargs.get("deerflow_error_fallback")`，相应动作只在对应分支执行。分支返回 `None`、`_error_fallback_message_from_metadata(additional_kwargs, getattr(obj, "content", None))`、`_error_fallback_message_from_metadata(nested_kwargs, obj.get("content"))`，提前返回结束该路径。

  源码依据：[_try_extract_from_message 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3850-L3875)


### 5.31.39 _extract_llm_error_fallback_message

参数：`value, pre_existing_ids=None`。

在 `条件 isinstance(value, dict)` 分支中，以 `value.get("messages")` 计算 `messages`；在 `条件 isinstance(value, dict)` → `条件 isinstance(messages, (list, tuple))` → `循环 for msg in messages` 分支中，以 `_try_extract_from_message(msg, pre_existing_ids)` 计算 `result`；在 `条件 isinstance(value, dict)` → `条件 isinstance(messages, (list, tuple))` → `循环 for msg in messages` → `条件 result is not None` 分支中，返回 `result`；在 `条件 isinstance(value, dict)` → `条件 isinstance(messages, (list, tuple))` 分支中，返回 `None`；以 `set()` 计算 `seen: set[int]`；建立局部回调 `def walk(obj: Any) -> str | None`，行为在独立条目展开；返回 `walk(value)`。

源码依据：[_extract_llm_error_fallback_message 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3876-L3934)


### 5.31.40 辅助函数与局部契约

- **`_extract_llm_error_fallback_message.walk`**（`obj`）：父函数内定义的回调。以 `id(obj)` 计算 `oid`；在 `条件 oid in seen` 分支中，返回 `None`；执行 `seen.add(oid)`；以 `_try_extract_from_message(obj, pre_existing_ids)` 计算 `result`；在 `条件 result is not None` 分支中，返回 `result`；在 `条件 isinstance(obj, dict)` → `循环 for item in obj.values()` 分支中，以 `walk(item)` 计算 `result`；在 `条件 isinstance(obj, dict)` → `循环 for item in obj.values()` → `条件 result is not None` 分支中，返回 `result`；在 `条件 isinstance(obj, dict)` 分支中，返回 `None`；在 `条件 isinstance(obj, (list, tuple, set))` → `循环 for item in obj` 分支中，以 `walk(item)` 计算 `result`；在 `条件 isinstance(obj, (list, tuple, set))` → `循环 for item in obj` → `条件 result is not None` 分支中，返回 `result`；返回 `None`。

  源码依据：[walk 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3908-L3931)


- **`_collect_pre_existing_message_ids`**（`values`）：在 `条件 not isinstance(values, dict)` 分支中，返回 `set()`；以 `values.get("messages")` 计算 `messages`；在 `条件 not isinstance(messages, (list, tuple))` 分支中，返回 `set()`；返回 `{message_id for message in messages if (message_id := _message_id(message)) is not None}`。

  源码依据：[_collect_pre_existing_message_ids 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3935-L3944)


- **`_unpack_stream_item`**（`item, lg_modes, stream_subgraphs`）：以 `item` 计算 `ns, mode, chunk`。关键判定为 `stream_subgraphs`、`isinstance(item, tuple) and len(item) == 3`、`isinstance(item, tuple) and len(item) == 2`，相应动作只在对应分支执行。分支返回 `str(mode), chunk, namespace`、`str(mode), chunk, ()`、`None, None, ()`、`lg_modes[0] if lg_modes else None, item, ()`，提前返回结束该路径。

  源码依据：[_unpack_stream_item 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3945-L3977)


- **`_compose_sse_event`**（`sse_event, namespace`）：在 `条件 not namespace` 分支中，返回 `sse_event`；返回 `"|".join((sse_event, *namespace))`。

  源码依据：[_compose_sse_event 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3978-L3992)


- **`_NO_FEED_WRITES`**（无显式业务参数）：返回 `0`。

  源码依据：[_NO_FEED_WRITES 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3996-L4000)


- **`_MessageSeqStamper.__init__`**（`event_store, thread_id, feed_generation=None`）：以 `event_store` 计算 `self._store`；以 `thread_id` 计算 `self._thread_id`；以 `feed_generation if feed_generation is not None else _NO_FEED_WRITES` 计算 `self._feed_generation`；以 `get_current_user()` 计算 `user`；以 `str(user.id) if user is not None else None` 计算 `self._user_id: str | None`；以 `{}` 计算 `self._seqs: dict[str, int]`；以 `{}` 计算 `self._missing: dict[str, int]`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L4031-L4048)


- **`_MessageSeqStamper.stamp`**（`payload`）：以 `payload.get("messages")` 计算 `messages`。关键判定为 `self._store is None or not isinstance(payload, Mapping)`、`not isinstance(messages, list) or not messages`、`unresolved`，相应动作只在对应分支执行。执行 `self._seqs.update(found)`；执行 `self._missing.update(dict.fromkeys(unresolved - found.keys(), generation))`。捕获 `Exception` 的边界见源码，捕获不等于已恢复。分支返回 `payload`、`{**payload, "messages": stamped}`，提前返回结束该路径。

  源码依据：[stamp 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L4049-L4076)


- **`_build_seq_stamper`**（`event_store, thread_id, journal`）：返回 `_MessageSeqStamper( event_store, thread_id, feed_generation=(lambda: journal.feed_generation) if journal is not None else None, )`。

  源码依据：[_build_seq_stamper 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L4077-L4091)


- **`_publish_stream_item`**（`bridge, run_id, mode, chunk, namespace, file_tool_chunk_batcher, subagent_events, seq_stamper=None`）：解包 mode 与 namespace，转换消息/状态并附加必要元数据，序列化后 publish；子图帧保留命名空间，避免污染根线程视图。

  源码依据：[_publish_stream_item 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L4092-L4130)


## 5.32 流桥抽象契约

源码位置：`backend/packages/harness/deerflow/runtime/stream_bridge/base.py`。

抽象层定义事件、gap、heartbeat、END、producer lease 的契约及默认实现。事件传输结束和业务成功不同；跨进程能力由具体实现声明。

### 5.32.1 辅助函数与局部契约

- **`StreamBridge.__init__`**（`heartbeat_interval=DEFAULT_HEARTBEAT_INTERVAL_SECONDS`）：以 `self._validate_heartbeat_interval(heartbeat_interval)` 计算 `self._heartbeat_interval`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L72-L74)


- **`StreamBridge.heartbeat_interval`**（无显式业务参数）：返回 `self._heartbeat_interval`。

  源码依据：[heartbeat_interval 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L76-L79)


- **`StreamBridge._resolve_heartbeat_interval`**（`heartbeat_interval`）：在 `条件 heartbeat_interval is None` 分支中，返回 `self._heartbeat_interval`；返回 `self._validate_heartbeat_interval(heartbeat_interval)`。

  源码依据：[_resolve_heartbeat_interval 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L80-L84)


- **`StreamBridge._validate_heartbeat_interval`**（`heartbeat_interval`）：在 「表达式摘要 1」 分支中，抛出 `ValueError(f"heartbeat_interval must be a positive finite number no greater than {MAX_HEARTBEAT_INTERVAL_SECONDS:g} seconds")`；返回 `float(heartbeat_interval)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 isinstance(heartbeat_interval, bool) or not isinstance(heartbeat_interval,
     (int, float)) or not math.isfinite(heartbeat_interval) or heartbeat_interval <= 0 or heartbeat_interval > MAX_HEARTBEAT_INTERVAL_SECONDS
  ```


  源码依据：[_validate_heartbeat_interval 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L86-L90)


- **`StreamBridge.publish`**（`run_id, event, data`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[publish 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L92-L94)


- **`StreamBridge.publish_end`**（`run_id`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[publish_end 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L96-L98)


- **`StreamBridge.subscribe`**（`run_id, last_event_id=None, heartbeat_interval=None`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[subscribe 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L100-L115)


- **`StreamBridge.cleanup`**（`run_id, delay=0`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[cleanup 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L117-L123)


- **`StreamBridge.ping`**（无显式业务参数）：返回 `True`。

  源码依据：[ping 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L124-L135)


- **`StreamBridge.start_producer_lease`**（`run_id`）：返回 `False`。

  源码依据：[start_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L136-L139)


- **`StreamBridge.finish_producer_lease`**（`run_id`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[finish_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L140-L142)


- **`StreamBridge.abort_producer_lease`**（`run_id`）：等待 `self.finish_producer_lease(run_id)` 完成。

  源码依据：[abort_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L143-L146)


- **`StreamBridge.transfer_producer_lease`**（`run_id, owner_task`）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[transfer_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L147-L153)


- **`StreamBridge.producer_lease_state`**（`run_id`）：返回 `StreamProducerLeaseState.untracked`。

  源码依据：[producer_lease_state 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L154-L157)


- **`StreamBridge.cleanup_expired_producer`**（`run_id`）：返回 `False`。

  源码依据：[cleanup_expired_producer 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L158-L161)


- **`StreamBridge.close`**（无显式业务参数）：此默认钩子没有额外执行体，具体能力取决于实现类。

  源码依据：[close 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/base.py#L162-L164)


## 5.33 内存事件流桥

源码位置：`backend/packages/harness/deerflow/runtime/stream_bridge/memory.py`。

进程内有界事件日志，每个订阅者独立游标；Condition 用于等待新数据而非生产者容量背压。淘汰与 retired generation 必须显式给读者 gap/END，详见 4.1.8。

### 5.33.1 辅助函数与局部契约

- **`MemoryStreamBridge.__init__`**（`queue_maxsize=256, heartbeat_interval=DEFAULT_HEARTBEAT_INTERVAL_SECONDS`）：执行 `super().__init__(heartbeat_interval=heartbeat_interval)`；以 `max(1, queue_maxsize)` 计算 `self._maxsize`；以 `{}` 计算 `self._streams: dict[str, _RunStream]`；以 `{}` 计算 `self._counters: dict[str, int]`。

  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L37-L44)


- **`MemoryStreamBridge._get_or_create_stream`**（`run_id`）：在 `条件 run_id not in self._streams` 分支中，以 `_RunStream()` 计算 `self._streams[run_id]`；在 `条件 run_id not in self._streams` 分支中，以 `0` 计算 `self._counters[run_id]`；返回 `self._streams[run_id]`。

  源码依据：[_get_or_create_stream 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L45-L50)


- **`MemoryStreamBridge._next_id`**（`run_id`）：以 `self._counters.get(run_id, 0) + 1` 计算 `self._counters[run_id]`；以 `int(time.time() * 1000)` 计算 `ts`；以 `self._counters[run_id] - 1` 计算 `seq`；返回 `f"{ts}-{seq}"`。

  源码依据：[_next_id 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L51-L56)


- **`MemoryStreamBridge._parse_event_seq`**（`event_id`）：在 `条件 match is None` 分支中，返回 `None`；在 `try` 分支中，返回 `int(match.group(1))`；在 `捕获 ValueError` 分支中，返回 `None`。

  源码依据：[_parse_event_seq 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L58-L73)


- **`MemoryStreamBridge._make_gap`**（`stream, requested_event_id`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  StreamGap( requested_event_id=requested_event_id, earliest_available_event_id=stream.events[0].id if stream.events
    else None, latest_available_event_id=stream.events[-1].id if stream.events else None,
     )
  ```


  源码依据：[_make_gap 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L75-L81)


- **`MemoryStreamBridge._resolve_start_offset`**（`stream, last_event_id`）：在 `条件 stream.retired` 分支中，返回 `StreamGap(last_event_id, None, None)`；在 `条件 last_event_id is None` 分支中，返回 `stream.start_offset`；在 `条件 last_event_id == stream.last_evicted_event_id` 分支中，返回 `stream.start_offset`；以 `self._parse_event_seq(last_event_id)` 计算 `seq`；在 `条件 seq is not None` → `条件 seq < stream.start_offset` 分支中，返回 `self._make_gap(stream, last_event_id)`；在 `条件 seq is not None` 分支中，以 `seq - stream.start_offset` 计算 `local_index`；在 `条件 seq is not None` → `条件 0 <= local_index < len(stream.events) and stream.events[local_index].id == last_event_id` 分支中，返回 `stream.start_offset + local_index + 1`；返回 `stream.start_offset`。

  源码依据：[_resolve_start_offset 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L82-L112)


- **`MemoryStreamBridge.stream_exists`**（`run_id`）：返回 `run_id in self._streams`。

  源码依据：[stream_exists 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L113-L118)


- **`MemoryStreamBridge.publish`**（`run_id, event, data`）：深拷贝 payload，按 run 生成序号，Condition 内追加并裁剪旧事件、唤醒订阅者；不等待消费确认，因此慢读者会遇到 gap。

  源码依据：[publish 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L119-L131)


- **`MemoryStreamBridge.publish_end`**（`run_id`）：以 `self._get_or_create_stream(run_id)` 计算 `stream`；在 `资源/同步边界 stream.condition` 分支中，以 `True` 计算 `stream.ended`；在 `资源/同步边界 stream.condition` 分支中，执行 `stream.condition.notify_all()`。

  源码依据：[publish_end 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L132-L137)


### 5.33.2 MemoryStreamBridge.subscribe

参数：`run_id, last_event_id=None, heartbeat_interval=None`。

每订阅者解析 Last-Event-ID 并维护绝对 offset；有事件交付拷贝，无事件等待 Condition/心跳，落后或 retired 发 gap，ended 且已读尽发 END。

源码依据：[subscribe 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L138-L200)


### 5.33.3 辅助函数与局部契约

- **`MemoryStreamBridge.cleanup`**（`run_id, delay=0`）：可延迟执行；将旧 stream generation 标 retired，清事件与 registry 并 notify_all，使已抓住旧对象的读者退出/重载，而不是永久等待。

  源码依据：[cleanup 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L201-L218)


- **`MemoryStreamBridge.close`**（无显式业务参数）：在 `循环 for run_id in tuple(self._streams)` 分支中，等待 `self.cleanup(run_id)` 完成。

  源码依据：[close 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L219-L222)


## 5.34 Redis 事件流桥

源码位置：`backend/packages/harness/deerflow/runtime/stream_bridge/redis.py`。

Redis Streams 版本将事件、边界读取、生产者 token/续约和清理 fencing 组合。顶层 Lua 是事务边界，XREAD 订阅没有消费 ack；临时错误重试有上限，详见 4.1.8。

### 5.34.1 辅助函数与局部契约

- **`RedisStreamBridge.__init__`**（「表达式摘要 1」）：关键判定为 `stream_ttl_seconds is not None and stream_ttl_seconds > 0`，相应动作只在对应分支执行。以 `redis_url` 计算 `self._redis_url`；以 `max(1, queue_maxsize)` 计算 `self._maxsize`；其他写入目标为 `self._legacy_end_scan_enabled`、`self._key_prefix`、`self._stream_ttl_seconds`、`self._redis`、`self._owns_client`、`self.producer_lease_recovery_enabled`、`self._producer_leases: dict[str, _ProducerLease]`、`self._producer_lease_locks`、`self._closing`、`self._producer_setups`、`self._producer_setups_drained`。执行 `self._producer_setups_drained.set()`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  redis_url, queue_maxsize=256, key_prefix="deerflow:stream_bridge",
   heartbeat_interval=DEFAULT_HEARTBEAT_INTERVAL_SECONDS, max_connections=None,
   stream_ttl_seconds=86400, producer_lease_recovery_enabled=False,
   legacy_end_scan_enabled=True, client=None
  ```


  源码依据：[__init__ 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L181-L215)


- **`RedisStreamBridge._stream_key`**（`run_id`）：返回 `f"{self._key_prefix}:{run_id}"`。

  源码依据：[_stream_key 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L216-L218)


- **`RedisStreamBridge._producer_tracking_key`**（`run_id`）：返回 `f"{self._stream_key(run_id)}:producer-tracked"`。

  源码依据：[_producer_tracking_key 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L219-L221)


- **`RedisStreamBridge._producer_active_key`**（`run_id`）：返回 `f"{self._stream_key(run_id)}:producer-active"`。

  源码依据：[_producer_active_key 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L222-L224)


- **`RedisStreamBridge._producer_tracking_ttl_seconds`**（无显式业务参数）：在 `条件 self._stream_ttl_seconds is None` 分支中，返回 `0`；返回 `max( self._stream_ttl_seconds, _PRODUCER_TRACKING_MIN_TTL_SECONDS, )`。

  源码依据：[_producer_tracking_ttl_seconds 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L225-L232)


- **`RedisStreamBridge._delete_if_token`**（`key, token`）：返回 `bool( await self._redis.eval( _COMPARE_DELETE_SCRIPT, 1, key, token, ) )`。

  源码依据：[_delete_if_token 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L233-L242)


### 5.34.2 RedisStreamBridge._rollback_producer_lease

参数：`run_id, token, timeout=_PRODUCER_LEASE_RELEASE_TIMEOUT_SECONDS`。

以 `( self._producer_tracking_key(run_id), self._producer_active_key(run_id), )` 计算 `keys`。关键判定为 `remaining_seconds <= 0`。进入 `asyncio.timeout(remaining_seconds)` 边界，退出由对应 context manager 处理。

等待 `self._delete_if_token(key, token)` 完成；等待 `asyncio.sleep( min( _PRODUCER_LEASE_RELEASE_RETRY_SECONDS, max(0.0, deadline - loop.time()), ) )` 完成。捕获 `RedisError as exc`、`TimeoutError as exc` 的边界见源码，分支返回 `all(value is None or self._decode(value) != token for value in remaining)`、`False`，提前返回结束该路径。

源码依据：[_rollback_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L243-L284)


### 5.34.3 辅助函数与局部契约

- **`RedisStreamBridge._cancel_lost_producer`**（`run_id, lease`）：以 `True` 计算 `lease.lost`；在 `条件 lease.owner_task.done()` 分支中，返回 None，提前结束当前路径；执行 `lease.owner_task.cancel(f"Redis stream producer lease expired for run {run_id}")`。

  源码依据：[_cancel_lost_producer 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L286-L295)


- **`RedisStreamBridge._watch_producer_owner`**（`run_id, lease, owner_task`）：建立局部回调 `def owner_done(done_task: asyncio.Task[Any]) -> None`，行为在独立条目展开；执行 `owner_task.add_done_callback(owner_done)`。

  源码依据：[_watch_producer_owner 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L296-L323)


- **`RedisStreamBridge._watch_producer_owner.owner_done`**（`done_task`）：父函数内定义的回调。以 `self._producer_leases.get(run_id)` 计算 `current`；在 `条件 current is not lease or current.owner_task is not done_task` 分支中，返回 None，提前结束当前路径；以 「表达式摘要 1」 计算 `cleanup`；执行 `cleanup.add_done_callback( lambda task: self._log_owner_cleanup_result( run_id, task, ) )`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  asyncio.create_task( self._stop_producer_lease( run_id, discard_tracking=False,
       expected_owner_task=done_task, ), name=f"redis-stream-producer-owner-cleanup:{run_id}",
     context=Context(), )
  ```


  源码依据：[owner_done 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L302-L321)


- **`RedisStreamBridge._log_owner_cleanup_result`**（`run_id, task`）：在 `条件 task.cancelled()` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 `task.result()`。

  源码依据：[_log_owner_cleanup_result 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L325-L339)


- **`RedisStreamBridge._ensure_producer_can_publish`**（`run_id`）：以 `self._producer_leases.get(run_id)` 计算 `lease`；在 `条件 lease is None` 分支中，返回 None，提前结束当前路径；在 `条件 lease.lost` 分支中，抛出 `RuntimeError(f"Redis stream producer lease was lost for run {run_id}")`；在 `条件 lease.stopped` 分支中，抛出 `RuntimeError(f"Redis stream producer lease was already stopped for run {run_id}")`。

  源码依据：[_ensure_producer_can_publish 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L340-L348)


- **`RedisStreamBridge._renew_producer_lease`**（`run_id, lease`）：以 `asyncio.get_running_loop()` 计算 `loop`。关键判定为 `remaining <= 0`、`remaining <= 0`、`not await self._renew_owned_producer_lease(run_id, lease)`，相应动作只在对应分支执行。进入 `asyncio.timeout(remaining)` 边界，退出由对应 context manager 处理。等待 `asyncio.sleep(min(_PRODUCER_LEASE_RENEW_INTERVAL_SECONDS, remaining))` 完成。捕获 `asyncio.CancelledError`、`(RedisError, TimeoutError)` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `当前异常`。分支返回 `None`，提前返回结束该路径。

  源码依据：[_renew_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L349-L379)


- **`RedisStreamBridge.start_producer_lease`**（`run_id`）：以 `asyncio.current_task()` 计算 `owner_task`；在 `条件 owner_task is None` 分支中，抛出 `RuntimeError("Stream producer lease requires an owning asyncio task")`；在 `条件 self._closing` 分支中，抛出 `RuntimeError("Stream bridge is closing")`；对 `self._producer_setups` 执行 `+= 1` 更新；执行 `self._producer_setups_drained.clear()`；在 `try` 分支中，返回 `await self._start_producer_lease(run_id, owner_task)`；在 `finally 收尾` 分支中，对 `self._producer_setups` 执行 `-= 1` 更新；在 `finally 收尾` → `条件 self._producer_setups == 0` 分支中，执行 `self._producer_setups_drained.set()`。

  源码依据：[start_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L380-L395)


### 5.34.4 RedisStreamBridge._start_producer_lease

参数：`run_id, owner_task`。

以 `self._producer_leases.get(run_id)` 计算 `existing`。关键判定为 `self._closing`、`existing is not None`、`existing.owner_task is owner_task and not existing.stopped`。进入 `self._producer_lease_locks.hold(run_id)`、`asyncio.timeout(_PRODUCER_LEASE_START_TIMEOUT_SECONDS)` 边界，退出由对应 context manager 处理。

以 `lease` 计算 `self._producer_leases[run_id]`。等待 `await_drained( self._rollback_producer_lease(run_id, token), )` 完成；等待 `await_drained( self._rollback_producer_lease(run_id, token), )` 完成。捕获 `asyncio.CancelledError`、`(RedisError, TimeoutError) as exc`、`BaseException` 的边界见源码，显式抛出路径涉及 `RuntimeError`、`当前异常`。

分支返回 `True`、`False`，提前返回结束该路径。

源码依据：[_start_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L396-L458)


### 5.34.5 辅助函数与局部契约

- **`RedisStreamBridge._renew_owned_producer_lease`**（`run_id, lease`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  bool( await self._redis.eval( _COMPARE_RENEW_SCRIPT, 2, self._producer_active_key(run_id),
       self._producer_tracking_key(run_id), lease.token, _PRODUCER_LEASE_SECONDS,
       self._producer_tracking_ttl_seconds(), ) )
  ```


  源码依据：[_renew_owned_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L459-L475)


- **`RedisStreamBridge._xadd_owned`**（`run_id, lease, fields, maxlen`）：把 producer token、active/tracking keys、事件和保留策略一起交给 Lua 检查追加；丢权或已经 END 时不能继续写，避免检查与追加之间的竞态。

  源码依据：[_xadd_owned 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L476-L507)


- **`RedisStreamBridge._xadd_without_active_producer`**（`run_id, fields, maxlen`）：以 「表达式摘要 1」 计算 `result`。关键判定为 `result == _XADD_BLOCKED`、`result == _XADD_ALREADY_ENDED`、`fields.get("kind") == _KIND_END`，相应动作只在对应分支执行。显式抛出路径涉及 `RuntimeError`。分支返回 `None`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await self._redis.eval( _XADD_IF_NO_ACTIVE_PRODUCER_SCRIPT, 2, self._producer_active_key(run_id),
     self._stream_key(run_id), maxlen, self._stream_ttl_seconds or 0,
     fields.get("kind", _KIND_EVENT), fields.get("event", ""), fields.get("data",
       ""), int(self._legacy_end_scan_enabled), )
  ```


  源码依据：[_xadd_without_active_producer 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L508-L540)


### 5.34.6 RedisStreamBridge._stop_producer_lease

参数：`run_id, discard_tracking, expected_owner_task=None, closing=False`。

以 `self._producer_leases.get(run_id)` 计算 `lease`。关键判定为 `lease is not None and expected_owner_task is not None and lease.owner_task is not expected_owner_task`、`lease is None`、`renewal_task is not None`。

进入 `self._producer_lease_locks.hold(run_id)`、`asyncio.timeout( max( 0.0, release_deadline - asyncio.get_running_loop().time(), ) )` 边界，退出由对应 context manager 处理。执行 `self._producer_leases.pop(run_id, None)`；执行 `self._producer_leases.pop(run_id, None)`。

执行 `renewal_task.cancel()`；等待 `asyncio.gather(renewal_task, return_exceptions=True)` 完成。捕获 `(RedisError, TimeoutError)` 的边界见源码，显式抛出路径涉及 `RuntimeError`。分支返回 `None`，提前返回结束该路径。finally 包含 以 `True` 计算 `lease.stopped`；以 `await self._rollback_producer_lease( run_id, lease.token, )` 计算 `discarded`。

源码依据：[_stop_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L541-L610)


### 5.34.7 辅助函数与局部契约

- **`RedisStreamBridge.finish_producer_lease`**（`run_id`）：等待 `self._stop_producer_lease( run_id, discard_tracking=False, )` 完成。

  源码依据：[finish_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L611-L616)


- **`RedisStreamBridge.abort_producer_lease`**（`run_id`）：以 `self._producer_leases.get(run_id)` 计算 `lease`；在 `条件 lease is not None` 分支中，以 `True` 计算 `lease.discard_tracking_requested`；等待 `self._stop_producer_lease( run_id, discard_tracking=True, )` 完成。

  源码依据：[abort_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L617-L625)


- **`RedisStreamBridge.transfer_producer_lease`**（`run_id, owner_task`）：以 `self._producer_leases.get(run_id)` 计算 `lease`；在 `条件 lease is not None and not lease.stopped` 分支中，以 `owner_task` 计算 `lease.owner_task`；在 `条件 lease is not None and not lease.stopped` 分支中，执行 `self._watch_producer_owner(run_id, lease, owner_task)`。

  源码依据：[transfer_producer_lease 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L626-L635)


- **`RedisStreamBridge.producer_lease_state`**（`run_id`）：以 `await self._redis.mget( self._producer_tracking_key(run_id), self._producer_active_key(run_id), )` 计算 `tracked, active`；在 `条件 tracked is None` 分支中，返回 `StreamProducerLeaseState.untracked`；在 `条件 active is not None and self._decode(active) == self._decode(tracked)` 分支中，返回 `StreamProducerLeaseState.active`；返回 `StreamProducerLeaseState.expired`。

  源码依据：[producer_lease_state 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L636-L646)


- **`RedisStreamBridge.cleanup_expired_producer`**（`run_id`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  bool( await self._redis.eval( _DELETE_EXPIRED_TRACKING_SCRIPT, 2,
       self._producer_tracking_key(run_id), self._producer_active_key(run_id),
       ) )
  ```


  源码依据：[cleanup_expired_producer 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L647-L656)


- **`RedisStreamBridge._xadd_retained`**（`key, fields, maxlen`）：在 `条件 self._stream_ttl_seconds is None` 分支中，等待 `self._redis.xadd( key, fields, maxlen=maxlen, approximate=False, )` 完成；在 `条件 self._stream_ttl_seconds is None` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 self._redis.pipeline(transaction=True) as pipe` 分支中，执行 `pipe.xadd( key, fields, maxlen=maxlen, approximate=False, )`；在 `资源/同步边界 self._redis.pipeline(transaction=True) as pipe` 分支中，执行 `pipe.expire(key, self._stream_ttl_seconds)`；在 `资源/同步边界 self._redis.pipeline(transaction=True) as pipe` 分支中，等待 `pipe.execute()` 完成。

  源码依据：[_xadd_retained 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L657-L676)


- **`RedisStreamBridge._decode`**（`value`）：在 `条件 isinstance(value, bytes)` 分支中，返回 `value.decode("utf-8")`；返回 `str(value)`。

  源码依据：[_decode 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L678-L682)


- **`RedisStreamBridge._normalise_fields`**（`fields`）：返回 `{cls._decode(key): cls._decode(value) for key, value in fields.items()}`。

  源码依据：[_normalise_fields 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L684-L686)


- **`RedisStreamBridge._encode_data`**（`data`）：返回 `json.dumps(data, default=str, ensure_ascii=False, separators=(",", ":"))`。

  源码依据：[_encode_data 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L688-L690)


- **`RedisStreamBridge._decode_data`**（`raw`）：在 `条件 raw is None` 分支中，返回 `None`；在 `try` 分支中，返回 `json.loads(raw)`；在 `捕获 json.JSONDecodeError` 分支中，返回 `raw`。

  源码依据：[_decode_data 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L692-L700)


- **`RedisStreamBridge._entry_from_redis`**（`event_id, fields`）：以 `self._normalise_fields(fields)` 计算 `payload`；以 `payload.get("kind", _KIND_EVENT)` 计算 `kind`；在 `条件 kind == _KIND_END` 分支中，返回 `END_SENTINEL`；返回 `StreamEvent( id=event_id, event=payload.get("event", "message"), data=self._decode_data(payload.get("data")), )`。

  源码依据：[_entry_from_redis 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L701-L711)


- **`RedisStreamBridge._is_end_entry`**（`fields`）：返回 `cls._normalise_fields(fields).get("kind") == _KIND_END`。

  源码依据：[_is_end_entry 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L713-L715)


- **`RedisStreamBridge._parse_stream_id`**（`event_id`）：在 `条件 _REDIS_STREAM_ID_RE.fullmatch(event_id) is None` 分支中，返回 `None`；以 `event_id.partition("-")` 计算 `milliseconds, separator, sequence`；在 `try` 分支中，以 `int(milliseconds), int(sequence) if separator else 0` 计算 `parts`；在 `捕获 ValueError` 分支中，返回 `None`；返回 `parts if max(parts) <= _MAX_STREAM_ID_COMPONENT else None`。

  源码依据：[_parse_stream_id 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L717-L727)


- **`RedisStreamBridge._stream_id_lt`**（`left, right`）：以 `cls._parse_stream_id(left)` 计算 `left_parts`；以 `cls._parse_stream_id(right)` 计算 `right_parts`；返回 `left_parts is not None and right_parts is not None and left_parts < right_parts`。

  源码依据：[_stream_id_lt 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L729-L733)


- **`RedisStreamBridge._response_tail_id`**（`response`）：在 `循环 for _stream_name, entries in reversed(response)` → `条件 entries` 分支中，返回 `cls._decode(entries[-1][0])`；返回 `None`。

  源码依据：[_response_tail_id 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L735-L740)


- **`RedisStreamBridge.publish`**（`run_id, event, data`）：执行 `self._ensure_producer_can_publish(run_id)`；以 `self._producer_leases.get(run_id)` 计算 `lease`；以 `{ "kind": _KIND_EVENT, "event": event, "data": self._encode_data(data), }` 计算 `fields`；在 `条件 lease is not None` 分支中，等待 `self._xadd_owned( run_id, lease, fields, maxlen=self._maxsize, )` 完成；在 `条件 lease is not None` 分支中，返回 None，提前结束当前路径；等待 `self._xadd_without_active_producer( run_id, fields, maxlen=self._maxsize, )` 完成。

  源码依据：[publish 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L741-L762)


- **`RedisStreamBridge.publish_end`**（`run_id`）：执行 `self._ensure_producer_can_publish(run_id)`；以 `self._producer_leases.get(run_id)` 计算 `lease`；以 `{"kind": _KIND_END}` 计算 `fields`；在 `条件 lease is not None` 分支中，等待 `self._xadd_owned( run_id, lease, fields, maxlen=self._maxsize + 1, )` 完成；在 `条件 lease is not None` 分支中，返回 None，提前结束当前路径；等待 `self._xadd_without_active_producer( run_id, fields, maxlen=self._maxsize + 1, )` 完成。

  源码依据：[publish_end 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L763-L781)


- **`RedisStreamBridge.stream_exists`**（`run_id`）：返回 `bool(await self._redis.exists(self._stream_key(run_id)))`。

  源码依据：[stream_exists 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L782-L785)


- **`RedisStreamBridge.ping`**（无显式业务参数）：在 `try` 分支中，返回 `bool(await self._redis.ping())`；在 `捕获 RedisError` 分支中，返回 `False`。

  源码依据：[ping 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L786-L799)


- **`RedisStreamBridge._resolve_start_stream_id`**（`key, last_event_id`）：在 `条件 last_event_id is None` 分支中，返回 `"0-0"`；在 `条件 self._parse_stream_id(last_event_id) is not None` 分支中，返回 `last_event_id`；以 `await self._redis.xrevrange(key, count=1)` 计算 `entries`；在 `条件 not entries` 分支中，返回 `"0-0"`；以 `entries[0]` 计算 `event_id, fields`；以 `self._normalise_fields(fields)` 计算 `payload`；在 `条件 payload.get("kind") == _KIND_END` 分支中，返回 `"0-0"`；返回 `self._decode(event_id)`。

  源码依据：[_resolve_start_stream_id 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L800-L813)


- **`RedisStreamBridge._read_retained_snapshot`**（`key, stream_id`）：在 `资源/同步边界 self._redis.pipeline(transaction=True) as pipe` 分支中，执行 `pipe.xrange(key, count=1)`；在 `资源/同步边界 self._redis.pipeline(transaction=True) as pipe` 分支中，执行 `pipe.xrevrange(key, count=1)`；在 `资源/同步边界 self._redis.pipeline(transaction=True) as pipe` 分支中，执行 `pipe.xread({key: stream_id}, count=_XREAD_COUNT)`；在 `资源/同步边界 self._redis.pipeline(transaction=True) as pipe` 分支中，以 `await pipe.execute()` 计算 `earliest, latest, response`；返回 `earliest, latest, response`。

  源码依据：[_read_retained_snapshot 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L814-L833)


### 5.34.8 RedisStreamBridge.subscribe

参数：`run_id, last_event_id=None, heartbeat_interval=None`。

用保留上下界快照和 XREAD 推进独立游标，检测过期/落后/未来 ID 与 END；临时 Redis 错误有上限退避，ResponseError 直接传播，不静默从头重放。

源码依据：[subscribe 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L834-L1011)


### 5.34.9 辅助函数与局部契约

- **`RedisStreamBridge.cleanup`**（`run_id, delay=0`）：以 `self._producer_leases.get(run_id)` 计算 `lease`。关键判定为 `delay > 0`、`token is None`、`current is lease`，相应动作只在对应分支执行。进入 `self._producer_lease_locks.hold(run_id)` 边界，退出由对应 context manager 处理。执行 `self._producer_leases.pop(run_id, None)`。等待 `asyncio.sleep(delay)` 完成；等待 `self.finish_producer_lease(run_id)` 完成。

  源码依据：[cleanup 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L1012-L1039)


- **`RedisStreamBridge.close`**（无显式业务参数）：以 `tuple(self._producer_leases)` 计算 `run_ids`。关键判定为 `run_ids`、`not self._owns_client`、`failures`，相应动作只在对应分支执行。以 `True` 计算 `self._closing`。等待 `self._producer_setups_drained.wait()` 完成；等待 `result` 完成。显式抛出路径涉及 `failures`。分支返回 `None`，提前返回结束该路径。

  源码依据：[close 实现](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L1040-L1069)


## 5.35 同步检查点资源

源码位置：`backend/packages/harness/deerflow/runtime/checkpointer/provider.py`。

同步工厂解析 legacy/统一数据库配置，并区分 singleton、reset 和局部 context 的所有权。delta saver 还包含 cache context，关闭顺序须覆盖两者，详见 4.1.9。

### 5.35.1 辅助函数与局部契约

- **`_ensure_postgres_schema`**（`conn_string, schema`）：执行 `ensure_postgres_schema(conn_string, schema, install_hint=POSTGRES_INSTALL)`。

  源码依据：[_ensure_postgres_schema 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L48-L57)


- **`_resolve_checkpointer_config`**（`app_config`）：以 `app_config.database` 计算 `database`。关键判定为 `app_config.checkpointer is not None`、`database is None or database.backend == "memory"`、`database.backend == "sqlite"`，相应动作只在对应分支执行。显式抛出路径涉及 `ValueError`。分支返回 `app_config.checkpointer`、`CheckpointerConfig(type="memory")`、`CheckpointerConfig(type="sqlite", connection_string=database.checkpointer_sqlite_path)`、`CheckpointerConfig(type="postgres", connection_string=database.postgres_url, postgres_schema=database.postgres_schema)`，提前返回结束该路径。

  源码依据：[_resolve_checkpointer_config 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L58-L81)


- **`_get_checkpointer_config`**（无显式业务参数）：执行 `ensure_config_loaded()`；以 `get_checkpointer_config()` 计算 `legacy_config`；在 `条件 legacy_config is not None` 分支中，返回 `legacy_config`；在 `try` 分支中，以 `get_app_config()` 计算 `app_config`；在 `捕获 FileNotFoundError` 分支中，返回 `CheckpointerConfig(type="memory")`；返回 `_resolve_checkpointer_config(app_config)`。

  源码依据：[_get_checkpointer_config 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L82-L101)


### 5.35.2 _sync_checkpointer_cm

参数：`config`。

以 `resolve_sqlite_conn_str(config.connection_string or "store.db")` 计算 `conn_str`。关键判定为 `config.type == "memory"`、`config.type == "sqlite"`、`config.type == "postgres"`。进入 `SqliteSaver.from_conn_string(conn_str) as saver`、`PostgresSaver.from_conn_string(conn_string) as saver` 边界，退出由对应 context manager 处理。

交付 `InMemorySaver()` 后暂停生成器，待消费方继续；执行 `saver.setup()`。捕获 `ImportError as exc` 的边界见源码，显式抛出路径涉及 `ImportError`、`ValueError`。分支返回 `None`，提前返回结束该路径。

源码依据：[_sync_checkpointer_cm 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L103-L155)


### 5.35.3 辅助函数与局部契约

- **`_wrap_sync_if_delta`**（`saver, app_config`）：以 `getattr(app_config, "database", None)` 计算 `db_config`。关键判定为 `mode != "delta"`、`cache_config.type == "redis"`、`_checkpointer_cache is None or _checkpointer_cache._max_entries != cache_config.max_entries or _checkpointer_cache_prefix != key_prefix`，相应动作只在对应分支执行。显式抛出路径涉及 `ValueError`。分支返回 `saver`、`CachedHistorySaver(saver, _checkpointer_cache, key_prefix=key_prefix)`，提前返回结束该路径。

  源码依据：[_wrap_sync_if_delta 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L163-L195)


### 5.35.4 get_checkpointer

无显式业务参数。

锁外读取配置，锁内安装/复用进程 singleton context；同步资源由 reset 释放，不能把该全局 getter 当作无状态短连接工厂。

源码依据：[get_checkpointer 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L196-L242)


### 5.35.5 辅助函数与局部契约

- **`reset_checkpointer`**（无显式业务参数）：使用外层变量 `global _checkpointer, _checkpointer_ctx, _checkpointer_cache, _checkpointer_cache_prefix`；在 `资源/同步边界 _checkpointer_lock` → `条件 _checkpointer_ctx is not None` → `try` 分支中，执行 `_checkpointer_ctx.__exit__(None, None, None)`；在 `资源/同步边界 _checkpointer_lock` → `条件 _checkpointer_ctx is not None` 分支中，以 `None` 计算 `_checkpointer_ctx`；在 `资源/同步边界 _checkpointer_lock` 分支中，以 `None` 计算 `_checkpointer`；在 `资源/同步边界 _checkpointer_lock` 分支中，以 `None` 计算 `_checkpointer_cache`；在 `资源/同步边界 _checkpointer_lock` 分支中，以 `None` 计算 `_checkpointer_cache_prefix`。

  源码依据：[reset_checkpointer 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L243-L266)


- **`checkpointer_context`**（无显式业务参数）：以 `get_app_config()` 计算 `app_config`；以 `_resolve_checkpointer_config(app_config)` 计算 `config`；在 `资源/同步边界 _sync_checkpointer_cm(config) as saver` 分支中，交付 `_wrap_sync_if_delta(saver, app_config)` 后暂停生成器，待消费方继续。

  源码依据：[checkpointer_context 实现](../backend/packages/harness/deerflow/runtime/checkpointer/provider.py#L268-L287)


## 5.36 异步检查点资源

源码位置：`backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py`。

异步服务工厂不建立全局 singleton，资源寿命由 async with 控制；显式后端错误抛出，未配置才返回 InMemorySaver。Postgres schema、pool 和 delta cache 都在 yield 前后建立/关闭，详见 4.1.9。

### 5.36.1 辅助函数与局部契约

- **`_prepare_sqlite_checkpointer_path`**（`raw`）：以 `resolve_sqlite_conn_str(raw)` 计算 `conn_str`；执行 `ensure_sqlite_parent_dir(conn_str)`；返回 `conn_str`。

  源码依据：[_prepare_sqlite_checkpointer_path 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L40-L45)


- **`_prepare_database_sqlite_checkpointer_path`**（`db_config`）：以 `db_config.checkpointer_sqlite_path` 计算 `conn_str`；执行 `ensure_sqlite_parent_dir(conn_str)`；返回 `conn_str`。

  源码依据：[_prepare_database_sqlite_checkpointer_path 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L46-L51)


- **`_build_postgres_pool`**（`conn_string, schema=""`）：以 「表达式摘要 1」 计算 `kwargs`；以 `dsn_with_search_path(conn_string, schema)` 计算 `dsn`；返回 `AsyncConnectionPool( dsn, kwargs=kwargs, check=AsyncConnectionPool.check_connection, )`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "autocommit": True, "prepare_threshold": 0, "row_factory": dict_row,
     "keepalives": 1, "keepalives_idle": 60, "keepalives_interval": 10,
     "keepalives_count": 6, }
  ```


  源码依据：[_build_postgres_pool 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L52-L79)


- **`_ensure_postgres_schema_with_pool`**（`pool, schema`）：以 `create_schema_sql(schema)` 计算 `statement`；在 `条件 statement is None` 分支中，返回 None，提前结束当前路径；在 `资源/同步边界 drained_async_context(pool.connection()) as conn` 分支中，等待 `conn.execute(statement)` 完成。

  源码依据：[_ensure_postgres_schema_with_pool 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L80-L91)


- **`_ensure_postgres_imports`**（无显式业务参数）：在 `捕获 ImportError as exc` 分支中，抛出 `ImportError(POSTGRES_INSTALL) from exc`；在 `捕获 ImportError as exc` 分支中，抛出 `ImportError(POSTGRES_INSTALL) from exc`；返回 `AsyncPostgresSaver, AsyncConnectionPool`。

  源码依据：[_ensure_postgres_imports 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L92-L111)


### 5.36.2 _async_checkpointer

参数：`config`。

以 `await asyncio.to_thread(_prepare_sqlite_checkpointer_path, config.connection_string or "store.db")` 计算 `conn_str`。关键判定为 `config.type == "memory"`、`config.type == "sqlite"`、`config.type == "postgres"`。

进入 `drained_async_context(AsyncSqliteSaver.from_conn_string(conn_str)) as saver`、`drained_async_context(pool)` 边界，退出由对应 context manager 处理。交付 `InMemorySaver()` 后暂停生成器，待消费方继续；等待 `saver.setup()` 完成。捕获 `ImportError as exc` 的边界见源码，显式抛出路径涉及 `ImportError`、`ValueError`。分支返回 `None`，提前返回结束该路径。

源码依据：[_async_checkpointer 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L113-L153)


### 5.36.3 辅助函数与局部契约

- **`_async_checkpointer_from_database`**（`db_config`）：以 `await asyncio.to_thread(_prepare_database_sqlite_checkpointer_path, db_config)` 计算 `conn_str`。关键判定为 `db_config.backend == "memory"`、`db_config.backend == "sqlite"`、`db_config.backend == "postgres"`，相应动作只在对应分支执行。进入 `drained_async_context(AsyncSqliteSaver.from_conn_string(conn_str)) as saver`、`drained_async_context(pool)` 边界，退出由对应 context manager 处理。交付 `InMemorySaver()` 后暂停生成器，待消费方继续；等待 `saver.setup()` 完成。捕获 `ImportError as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `ImportError`、`ValueError`。分支返回 `None`，提前返回结束该路径。

  源码依据：[_async_checkpointer_from_database 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L155-L190)


- **`_select_inner_checkpointer`**（`app_config`）：以 `getattr(app_config, "database", None)` 计算 `db_config`。关键判定为 `app_config.checkpointer is not None`、`db_config is not None and db_config.backend != "memory"`，相应动作只在对应分支执行。进入 `_async_checkpointer(app_config.checkpointer) as saver`、`_async_checkpointer_from_database(db_config) as saver` 边界，退出由对应 context manager 处理。交付 `saver` 后暂停生成器，待消费方继续；交付 `saver` 后暂停生成器，待消费方继续。分支返回 `None`，提前返回结束该路径。

  源码依据：[_select_inner_checkpointer 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L192-L218)


- **`make_checkpointer`**（`app_config=None`）：以调用者 async context 的寿命选择 raw saver，再按已冻结模式可包 CachedHistorySaver/cache；未配置选 memory，配置错误照常抛出，退出顺序先 cache 后 saver。

  源码依据：[make_checkpointer 实现](../backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py#L220-L258)


## 5.37 检查点状态物化

源码位置：`backend/packages/harness/deerflow/runtime/checkpoint_state.py`。

图状态访问的公共 choke point；mode 标记、兼容检查和图物化在这里统一。状态整体替换使用有效 schema 的单 finish 节点图，避免恢复后意外调度 agent 或丢弃扩展 channel。

### 5.37.1 辅助函数与局部契约

- **`_finish_state_mutation`**（`_state`）：返回 `{}`。

  源码依据：[_finish_state_mutation 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L33-L36)


- **`build_state_mutation_graph`**（`as_node, mode, state_schema=None, snapshot_frequency=None`）：要求非空 as_node，使用线程有效 schema 建单个立即结束节点，供整体替换而不调度 agent；省略扩展 schema 会让未知 channel 写入丢失。

  源码依据：[build_state_mutation_graph 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L37-L69)


- **`graph_state_schema`**（`graph`）：以 `getattr(getattr(graph, "builder", None), "schemas", None)` 计算 `schemas`；在 `条件 not schemas` 分支中，返回 `None`；返回 `next(iter(schemas))`。

  源码依据：[graph_state_schema 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L70-L82)


- **`graph_writable_channels`**（`graph`）：以 `getattr(graph, "channels", None)` 计算 `channels`；在 `条件 not channels` 分支中，返回 `None`；返回 `frozenset(name for name in channels if not name.startswith("__") and not name.startswith("branch:"))`。

  源码依据：[graph_writable_channels 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L83-L95)


- **`graph_reducer_channels`**（`graph`）：以 `getattr(graph, "channels", None)` 计算 `channels`；在 `条件 channels is None` 分支中，返回 `None`；返回 `frozenset(name for name, channel in channels.items() if isinstance(channel, (BinaryOperatorAggregate, DeltaChannel)))`。

  源码依据：[graph_reducer_channels 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L96-L111)


- **`CheckpointStateAccessor.bind`**（`graph, checkpointer, store=None, mode="full", direct_namespace=False`）：以 `checkpointer` 计算 `graph.checkpointer`；在 `条件 store is not None` 分支中，以 `store` 计算 `graph.store`；返回 `cls(graph=graph, checkpointer=checkpointer, mode=mode, direct_namespace=direct_namespace)`。

  源码依据：[bind 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L120-L133)


- **`CheckpointStateAccessor._prepare_config`**（`config`）：以 `{ **config, "configurable": dict(config.get("configurable", {})), "metadata": dict(config.get("metadata", {})), }` 计算 `prepared`；在 `条件 self.direct_namespace and prepared["configurable"].get("checkpoint_ns")` 分支中，以 `self.checkpointer` 计算 `prepared["configurable"][CONFIG_KEY_CHECKPOINTER]`；执行 `inject_checkpoint_mode(prepared, self.mode)`；返回 `prepared`。

  源码依据：[_prepare_config 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L134-L151)


- **`CheckpointStateAccessor.get`**（`config`）：以 `self._prepare_config(config)` 计算 `prepared`；以 `self.graph.get_state(prepared)` 计算 `snapshot`；执行 `raise_if_snapshot_incompatible(snapshot, self.mode)`；返回 `snapshot`。

  源码依据：[get 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L152-L157)


- **`CheckpointStateAccessor.aget`**（`config`）：通过编译图物化并检查模式；delta 读取最新 head 时发现目标被 retention 删除则重试一次，显式历史 checkpoint 不静默迁移。

  源码依据：[aget 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L158-L177)


- **`CheckpointStateAccessor.get_tuple`**（`config`）：返回 `self.checkpointer.get_tuple(self._prepare_config(config))`。

  源码依据：[get_tuple 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L178-L182)


- **`CheckpointStateAccessor.aget_tuple`**（`config`）：返回 `await self.checkpointer.aget_tuple(self._prepare_config(config))`。

  源码依据：[aget_tuple 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L183-L187)


- **`CheckpointStateAccessor.get_metadata`**（`config`）：以 `self.checkpointer.get_tuple(self._prepare_config(config))` 计算 `checkpoint_tuple`；执行 `raise_if_checkpoint_tuple_incompatible(checkpoint_tuple, self.mode)`；返回 `dict(getattr(checkpoint_tuple, "metadata", {}) or {})`。

  源码依据：[get_metadata 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L188-L193)


- **`CheckpointStateAccessor.aget_metadata`**（`config`）：以 `await self.checkpointer.aget_tuple(self._prepare_config(config))` 计算 `checkpoint_tuple`；执行 `raise_if_checkpoint_tuple_incompatible(checkpoint_tuple, self.mode)`；返回 `dict(getattr(checkpoint_tuple, "metadata", {}) or {})`。

  源码依据：[aget_metadata 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L194-L199)


- **`CheckpointStateAccessor.history`**（`config, limit=None`）：以 `self._prepare_config(config)` 计算 `prepared`；在 `条件 limit is not None and limit <= 0` 分支中，返回 `[]`；以 `[]` 计算 `result`；在 `循环 for snapshot in self.graph.get_state_history(prepared, limit=limit)` 分支中，执行 `raise_if_snapshot_incompatible(snapshot, self.mode)`；在 `循环 for snapshot in self.graph.get_state_history(prepared, limit=limit)` 分支中，执行 `result.append(snapshot)`；在 `循环 for snapshot in self.graph.get_state_history(prepared, limit=limit)` → `条件 limit is not None and len(result) >= limit` 分支中，退出当前循环；返回 `result`。

  源码依据：[history 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L200-L211)


- **`CheckpointStateAccessor.ahistory`**（`config, limit=None`）：以 `self._prepare_config(config)` 计算 `prepared`；在 `条件 limit is not None and limit <= 0` 分支中，返回 `[]`；以 `[]` 计算 `result`；在 `循环 async for snapshot in self.graph.aget_state_history(prepared, limit=limit)` 分支中，执行 `raise_if_snapshot_incompatible(snapshot, self.mode)`；在 `循环 async for snapshot in self.graph.aget_state_history(prepared, limit=limit)` 分支中，执行 `result.append(snapshot)`；在 `循环 async for snapshot in self.graph.aget_state_history(prepared, limit=limit)` → `条件 limit is not None and len(result) >= limit` 分支中，退出当前循环；返回 `result`。

  源码依据：[ahistory 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L212-L223)


- **`CheckpointStateAccessor.update`**（`config, values, as_node=None`）：以 `self._prepare_config(config)` 计算 `prepared`；执行 `ensure_checkpoint_mode_compatible(self.checkpointer, prepared, self.mode)`；返回 `self.graph.update_state(prepared, values, as_node=as_node)`。

  源码依据：[update 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L224-L234)


- **`CheckpointStateAccessor.aupdate`**（`config, values, as_node=None`）：以 `self._prepare_config(config)` 计算 `prepared`；等待 `aensure_checkpoint_mode_compatible(self.checkpointer, prepared, self.mode)` 完成；返回 `await self.graph.aupdate_state(prepared, values, as_node=as_node)`。

  源码依据：[aupdate 实现](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L235-L245)


## 5.38 Gateway 启停与公共边界

源码位置：`backend/app/gateway/app.py`。

Gateway 装配 FastAPI、路由、认证、扩展 API 和 lifespan；初始化/关闭宿主依赖是 App 职责。模块级应用声明本身不能证明部署已启动，也不把配置里的扩展导入当作用户可写工具目录。 文件末尾执行 app=create_app() 形成模块级 ASGI 对象；网络服务和 lifespan 的进入仍由服务器驱动。

源码依据：[app.py 实现](../backend/app/gateway/app.py#L1499-L1499)

### 5.38.1 辅助函数与局部契约

- **`_installed_plugin_namespace`**（`request, namespace`）：以 `getattr(getattr(request, "app", None), "state", None)` 计算 `state`；以 `getattr(state, "extensions", None)` 计算 `loaded`；在 `循环 for _, plugin in getattr(loaded, "plugins", ())` → `条件 plugin.namespace == namespace` 分支中，返回 `True`；返回 `False`。

  源码依据：[_installed_plugin_namespace 实现](../backend/app/gateway/app.py#L94-L103)


### 5.38.2 _resolve_extension_plugin_management

参数：`request, namespace, scope="read"`。

在 `条件 not _installed_plugin_namespace(request, namespace)` 分支中，返回 `None`；在 `try` 分支中，以 `resolve_plugin_authorization(request)` 计算 `provider, principal, app_config`；在 `捕获 _PluginAuthorizationUnavailable as unavailable` 分支中，返回 `not unavailable.fail_closed`；在 `条件 provider is None` 分支中，返回 `True`；在 `条件 principal is None` 分支中，返回 `None`；在 `try` 分支中，执行 `enforce_plugin_management( principal=principal, app_config=app_config, namespace=namespace, write=scope == "write", provider=provider, )`；在 `捕获 PluginAuthorizationError` 分支中，返回 `False`；返回 `True`。

源码依据：[_resolve_extension_plugin_management 实现](../backend/app/gateway/app.py#L104-L143)


### 5.38.3 辅助函数与局部契约

- **`_resolve_extension_plugin_management_async`**（`request, namespace, scope="read"`）：在 `条件 not _installed_plugin_namespace(request, namespace)` 分支中，返回 `None`；在 `try` 分支中，以 `await aresolve_plugin_authorization(request)` 计算 `provider, principal, app_config`；在 `捕获 _PluginAuthorizationUnavailable as unavailable` 分支中，返回 `not unavailable.fail_closed`；在 `条件 provider is None` 分支中，返回 `True`；在 `条件 principal is None` 分支中，返回 `None`；在 `try` 分支中，等待 `aenforce_plugin_management( principal=principal, app_config=app_config, namespace=namespace, write=scope == "write", provider=provider, )` 完成；在 `捕获 PluginAuthorizationError` 分支中，返回 `False`；返回 `True`。

  源码依据：[_resolve_extension_plugin_management_async 实现](../backend/app/gateway/app.py#L144-L171)


### 5.38.4 _ensure_admin_user

参数：`app`。

以 `get_local_provider()` 计算 `provider`。关键判定为 `sf is None`、`admin_count == 0`、`row is None`。进入 `sf() as session` 边界，退出由对应 context manager 处理。以 `(await session.execute(stmt)).scalar_one_or_none()` 计算 `row`。捕获 `RuntimeError`、`Exception` 的边界见源码，

分支返回 `None`、`# Should not happen (admin_count > 0 above), but be safe.`，提前返回结束该路径。

源码依据：[_ensure_admin_user 实现](../backend/app/gateway/app.py#L172-L243)


### 5.38.5 辅助函数与局部契约

- **`_iter_store_items`**（`store, namespace, page_size=500`）：以 `0` 计算 `offset`；在 `循环 while True` 分支中，以 `await store.asearch(namespace, limit=page_size, offset=offset)` 计算 `batch`；在 `循环 while True` → `条件 not batch` 分支中，返回 None，提前结束当前路径；在 `循环 while True` → `循环 for item in batch` 分支中，交付 `item` 后暂停生成器，待消费方继续；在 `循环 while True` → `条件 len(batch) < page_size` 分支中，返回 None，提前结束当前路径；在 `循环 while True` 分支中，对 `offset` 执行 `+= page_size` 更新。

  源码依据：[_iter_store_items 实现](../backend/app/gateway/app.py#L244-L263)


- **`_migrate_orphaned_threads`**（`store, admin_user_id`）：以 `0` 计算 `migrated`；对 `migrated` 执行 `+= 1` 更新。关键判定为 `not metadata.get("user_id")`，相应动作只在对应分支执行。以 `admin_user_id` 计算 `metadata["user_id"]`；以 `metadata` 计算 `item.value["metadata"]`。等待 `store.aput(("threads",), item.key, item.value)` 完成。分支返回 `migrated`，提前返回结束该路径。

  源码依据：[_migrate_orphaned_threads 实现](../backend/app/gateway/app.py#L264-L280)


- **`_warm_memory_retrieval`**（`manager`）：在 `try` 分支中，以 `await asyncio.to_thread(manager.warm_retrieval)` 计算 `rebuilt`。

  源码依据：[_warm_memory_retrieval 实现](../backend/app/gateway/app.py#L281-L292)


- **`_shutdown_memory_backend`**（`retrieval_warm_finished`）：以 `None` 计算 `manager`；在 `try` 分支中，以 `await asyncio.to_thread(get_app_config)` 计算 `app_cfg: AppConfig`；在 `try` → `条件 app_cfg.memory.enabled` 分支中，以 `await asyncio.to_thread(get_memory_manager)` 计算 `manager`；在 `try` → `条件 app_cfg.memory.enabled` 分支中，以 `app_cfg.memory.shutdown_flush_timeout_seconds` 计算 `flush_timeout`；在 `try` → `条件 app_cfg.memory.enabled` 分支中，以 `await asyncio.to_thread(manager.shutdown_flush, flush_timeout)` 计算 `completed`；在 `finally 收尾` 分支中，以 `getattr(manager, "close", None)` 计算 `close`；在 `finally 收尾` → `条件 callable(close) and retrieval_warm_finished` → `try` 分支中，等待 `asyncio.to_thread(close)` 完成。

  源码依据：[_shutdown_memory_backend 实现](../backend/app/gateway/app.py#L293-L325)


- **`_run_startup_trash_sweep`**（`app, startup_config`）：在 `try` 分支中，以 `getattr(app.state, "project_document_repo", None)` 计算 `project_document_repo`；在 `try` → `条件 project_document_repo is None` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，以 `getattr(startup_config, "projects", None)` 计算 `projects_config`；在 `try` 分支中，以 `projects_config.trash_retention_days if projects_config is not None else ProjectsConfig().trash_retention_days` 计算 `retention_days`；在 `try` 分支中，以 `await run_trash_retention_sweep(project_document_repo, get_paths(), retention_days=retention_days, user_id=None)` 计算 `sweep_report`。

  源码依据：[_run_startup_trash_sweep 实现](../backend/app/gateway/app.py#L326-L360)


### 5.38.6 _shutdown_startup_trash_sweep

参数：`app`。

以 `getattr(app.state, "startup_trash_sweep_task", None)` 计算 `task`；在 `条件 task is None or task.done()` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，等待 `asyncio.wait_for(asyncio.shield(task), timeout=_SHUTDOWN_HOOK_TIMEOUT_SECONDS)` 完成；在 `捕获 TimeoutError` 分支中，以 `task.cancel()` 计算 `cancelled`；在 `捕获 TimeoutError` → `资源/同步边界 suppress(asyncio.CancelledError)` 分支中，等待 `task` 完成。

源码依据：[_shutdown_startup_trash_sweep 实现](../backend/app/gateway/app.py#L361-L400)


### 5.38.7 辅助函数与局部契约

- **`_shutdown_scheduled_task_service`**（`app`）：以 `getattr(app.state, "scheduled_task_service", None)` 计算 `service`；在 `条件 service is None` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，等待 `asyncio.wait_for(service.stop(), timeout=_SHUTDOWN_HOOK_TIMEOUT_SECONDS)` 完成。

  源码依据：[_shutdown_scheduled_task_service 实现](../backend/app/gateway/app.py#L401-L422)


- **`_shutdown_subagent_batch_service`**（`app`）：以 `getattr(app.state, "subagent_batch_service", None)` 计算 `service`；在 `条件 service is None` 分支中，返回 None，提前结束当前路径；以 `False` 计算 `app.state.subagent_batches_available`；在 `try` 分支中，等待 `asyncio.wait_for(service.stop(), timeout=_SHUTDOWN_HOOK_TIMEOUT_SECONDS)` 完成；在 `finally 收尾` 分支中，执行 `set_subagent_batch_submitter(None)`。

  源码依据：[_shutdown_subagent_batch_service 实现](../backend/app/gateway/app.py#L423-L443)


- **`_load_credentials_cipher`**（`startup_config`）：在 `条件 not credentials_key_consumers(startup_config)` 分支中，返回 `None`；在 `try` 分支中，返回 `await asyncio.to_thread(credentials_key.get_credentials_cipher)`；在 `捕获 Exception` 分支中，返回 `None`。

  源码依据：[_load_credentials_cipher 实现](../backend/app/gateway/app.py#L444-L465)


- **`_scheduled_task_notification_repos`**（`startup_config, credentials_cipher=None`）：以 `getattr(startup_config, "channel_connections", None)` 计算 `connection_config`；在 `条件 connection_config is None or not getattr(connection_config, "enabled", False)` 分支中，返回 `None, None`；以 `get_session_factory()` 计算 `session_factory`；在 `条件 session_factory is None` 分支中，返回 `None, None`；返回 `ChannelConnectionRepository(session_factory, cipher=credentials_cipher), NotificationDeliveryRepository(session_factory)`。

  源码依据：[_scheduled_task_notification_repos 实现](../backend/app/gateway/app.py#L466-L485)


### 5.38.8 _start_scheduled_task_notification_delivery

参数：`app, startup_config, notification_repo, connection_repo`。

以 `getattr(app.state, "scheduled_task_service", None)` 计算 `scheduled_task_service`。关键判定为 `scheduled_task_service is None or notification_repo is None`、`channel_service is None`。等待 `worker.start()` 完成。捕获 `Exception` 的边界见源码，显式抛出路径涉及 `当前异常`。分支返回 `None`，提前返回结束该路径。

源码依据：[_start_scheduled_task_notification_delivery 实现](../backend/app/gateway/app.py#L486-L585)


### 5.38.9 辅助函数与局部契约

- **`_start_scheduled_task_notification_delivery.resolve_run_summary`**（`run_id, user_id`）：父函数内定义的回调。以 `await run_repo.get(run_id, user_id=user_id)` 计算 `row`；返回 `row.get("last_ai_message") if row else None`。

  源码依据：[resolve_run_summary 实现](../backend/app/gateway/app.py#L514-L519)


- **`_start_scheduled_task_notification_delivery.resolve_human_input_pending`**（`delivery`）：父函数内定义的回调。以 `delivery.get("payload") or {}` 计算 `payload`。关键判定为 `not isinstance(thread_id, str) or not thread_id`、`thread_store is not None and owner_user_id`、`thread_meta is None`，相应动作只在对应分支执行。分支返回 `False`、`True`，提前返回结束该路径。finally 包含 执行 `reset_current_user(owner_token)`；以 `extract_unanswered_human_input_request(messages)` 计算 `current_request`。

  源码依据：[resolve_human_input_pending 实现](../backend/app/gateway/app.py#L520-L558)


- **`_runtime_with_mcp_pool_shutdown`**（`app, startup_config`）：在 `try` → `资源/同步边界 langgraph_runtime(app, startup_config)` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾；在 `finally 收尾` → `try` → `try` 分支中，等待 `asyncio.wait_for( get_session_pool().close_all(), timeout=_SHUTDOWN_HOOK_TIMEOUT_SECONDS, )` 完成；在 `finally 收尾` → `finally 收尾` → `try` 分支中，等待 `await_drained(asyncio.to_thread(shutdown_sandbox_provider))` 完成。

  源码依据：[_runtime_with_mcp_pool_shutdown 实现](../backend/app/gateway/app.py#L587-L623)


- **`lifespan`**（`app`）：在 `资源/同步边界 hold_sandbox_shutdown_ownership()` → `资源/同步边界 _gateway_lifespan(app)` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾。

  源码依据：[lifespan 实现](../backend/app/gateway/app.py#L625-L635)


### 5.38.10 _gateway_lifespan

参数：`app`。

进入 Gateway 服务运行时，加载配置/扩展、认证与运行依赖，启动关联宿主服务；退出按拥有关系排空 worker、记忆、通道、MCP 和 sandbox，部分 startup 失败也走清理。

源码依据：[_gateway_lifespan 实现](../backend/app/gateway/app.py#L637-L1095)


### 5.38.11 create_app

无显式业务参数。

构造 FastAPI 并安装 lifespan、认证/路由和扩展 API，定义 principal/证据 reader 回调及健康端点；构造对象不等于已启动 HTTP 服务。

源码依据：[create_app 实现](../backend/app/gateway/app.py#L1096-L1498)


### 5.38.12 辅助函数与局部契约

- **`create_app._resolve_extension_principal`**（`request`）：父函数内定义的回调。以 `getattr(request.state, "user", None)` 计算 `user`。关键判定为 `user is None`，相应动作只在对应分支执行。分支返回 `None`、「表达式摘要 1」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ExtensionPrincipal( user_id=str(user.id), is_admin=is_admin, is_internal=auth_source == AUTH_SOURCE_INTERNAL,
     # The host's only role concept is the single system_role column # (e.g. "admin", "user") — there is no multi-role system to # project, so a set role becomes the one-element tuple rather # than reading a "roles" attribute the user model never had. roles=roles, )
  ```


  源码依据：[_resolve_extension_principal 实现](../backend/app/gateway/app.py#L1205-L1235)


- **`create_app._resolve_extension_run_evidence_reader`**（`request`）：父函数内定义的回调。以 `_resolve_extension_principal(request)` 计算 `principal`；以 `getattr(request.state, "auth", None)` 计算 `auth`；在 `条件 principal is None or not principal.user_id or auth is None or not auth.has_permission("runs", "read")` 分支中，抛出 `PermissionError("run evidence requires an authenticated user with runs:read")`；以 `getattr(app.state, "run_evidence_reader_factory", None)` 计算 `factory`；在 `条件 factory is None` 分支中，返回 `None`；返回 `factory.for_principal(principal)`。

  源码依据：[_resolve_extension_run_evidence_reader 实现](../backend/app/gateway/app.py#L1244-L1254)


- **`create_app._resolve_extension_agent_runs`**（`request`）：父函数内定义的回调。以 `getattr(app.state, "agent_runs_host", None)` 计算 `host`；返回 `host.bind(request) if host is not None else None`。

  源码依据：[_resolve_extension_agent_runs 实现](../backend/app/gateway/app.py#L1259-L1262)


- **`create_app.health_check`**（无显式业务参数）：父函数内定义的回调。返回 `{"status": "healthy", "service": "deer-flow-gateway"}`。

  源码依据：[health_check 实现](../backend/app/gateway/app.py#L1454-L1461)


- **`create_app.readiness_check`**（`request, response`）：父函数内定义的回调。以 `request.app.state` 计算 `state`；以 「表达式摘要 1」 计算 `status_code, payload`；以 `status_code` 计算 `response.status_code`；返回 `payload`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await readiness_payload( getattr(state, READINESS_CHECKPOINTER_CONFIG_ATTR,
       None), stream_bridge=getattr(state, "stream_bridge", None), provisioner_url=getattr(state,
       READINESS_PROVISIONER_URL_ATTR, None), )
  ```


  源码依据：[readiness_check 实现](../backend/app/gateway/app.py#L1463-L1490)


## 5.39 可信运行上下文与启动服务

源码位置：`backend/app/gateway/services.py`。

路由与 Harness 之间的服务层负责不可信输入归一化、内部字段剥离、身份注入、图选择、准入与 worker 调度。checkpoint accessor 的构造也在这里适配 App 配置；SSE 消费者处理 HTTP 生命周期而不推进图循环。

### 5.39.1 辅助函数与局部契约

- **`_declared_bridge_method`**（`bridge, name`）：在 `条件 inspect.getattr_static(bridge, name, _MISSING_BRIDGE_METHOD) is _MISSING_BRIDGE_METHOD` 分支中，返回 `None`；以 `getattr(bridge, name, None)` 计算 `method`；返回 `method if callable(method) else None`。

  源码依据：[_declared_bridge_method 实现](../backend/app/gateway/services.py#L126-L133)


- **`BusyThreadConflict.__init__`**（`detail`）：执行 `super().__init__(status_code=409, detail=detail)`。

  源码依据：[__init__ 实现](../backend/app/gateway/services.py#L137-L140)


- **`reserve_checkpoint_write`**（`request, thread_id, user_id=None`）：以 `get_run_manager(request)` 计算 `run_manager`；在 `资源/同步边界 goal_thread_lock(thread_id)` → `资源/同步边界 run_manager.reserve_thread_operation( thread_id, kind=ThreadOperationKind.checkpoint_write, user_id=user_id, )` 分支中，在 yield 处把控制交给调用者，调用者离开上下文后继续收尾。

  源码依据：[reserve_checkpoint_write 实现](../backend/app/gateway/services.py#L142-L158)


- **`format_sse`**（`event, data, event_id=None`）：以 `json.dumps(data, default=str, ensure_ascii=False)` 计算 `payload`；以 `[f"event: {event}", f"data: {payload}"]` 计算 `parts`；在 `条件 event_id` 分支中，执行 `parts.append(f"id: {event_id}")`；执行 `parts.append("")`；执行 `parts.append("")`；返回 `"\n".join(parts)`。

  源码依据：[format_sse 实现](../backend/app/gateway/services.py#L222-L237)


- **`_run_is_terminal`**（`record`）：返回 `record.status in _TERMINAL_RUN_STATUSES`。

  源码依据：[_run_is_terminal 实现](../backend/app/gateway/services.py#L238-L241)


- **`_consume_task_result`**（`task`）：在 `条件 not task.cancelled()` 分支中，执行 `task.exception()`。

  源码依据：[_consume_task_result 实现](../backend/app/gateway/services.py#L242-L247)


- **`_log_thread_metadata_task_result`**（`task, thread_id`）：在 `条件 task.cancelled()` 分支中，返回 None，提前结束当前路径；在 `try` 分支中，执行 `task.result()`；在 `捕获 asyncio.CancelledError` 分支中，返回 None，提前结束当前路径。

  源码依据：[_log_thread_metadata_task_result 实现](../backend/app/gateway/services.py#L248-L263)


- **`_ensure_thread_metadata`**（`run_ctx, record, owner_user_id, require_existing_thread=False`）：在 `资源/同步边界 goal_thread_lock(record.thread_id)` 分支中，返回 `await _ensure_thread_metadata_locked(run_ctx, record, owner_user_id=owner_user_id, require_existing_thread=require_existing_thread)`。

  源码依据：[_ensure_thread_metadata 实现](../backend/app/gateway/services.py#L264-L275)


### 5.39.2 _ensure_thread_metadata_locked

参数：`run_ctx, record, owner_user_id, require_existing_thread`。

以 `run_ctx.thread_store` 计算 `thread_store`；以 「表达式摘要 1」 计算 `existing`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
await thread_store.create( record.thread_id, assistant_id=record.assistant_id,
   display_name=title if isinstance(title, str) and title.strip() else None,
   metadata=metadata, )
```


关键判定为 `existing is None and owner_user_id`、`unscoped is not None`、`unscoped.get("user_id") != owner_user_id`。等待 `thread_store.update_owner(record.thread_id, owner_user_id, user_id=None)` 完成。显式抛出路径涉及 `LookupError`。分支返回 `existing`，提前返回结束该路径。

源码依据：[_ensure_thread_metadata_locked 实现](../backend/app/gateway/services.py#L276-L320)


### 5.39.3 辅助函数与局部契约

- **`_stream_producer_lease_state`**（`bridge, run_id`）：以 `getattr(bridge, "producer_lease_state", None)` 计算 `producer_lease_state`；在 `条件 producer_lease_state is None` 分支中，返回 `StreamProducerLeaseState.untracked`；在 `try` 分支中，以 `await producer_lease_state(run_id)` 计算 `state`；在 `try` 分支中，返回 `StreamProducerLeaseState(state)`；在 `捕获 (TypeError, ValueError)` 分支中，返回 `StreamProducerLeaseState.untracked`；在 `捕获 Exception` 分支中，返回 `None`。

  源码依据：[_stream_producer_lease_state 实现](../backend/app/gateway/services.py#L321-L341)


- **`_cleanup_expired_stream_producer`**（`bridge, run_id`）：以 `_declared_bridge_method( bridge, "cleanup_expired_producer", )` 计算 `cleanup`；在 `条件 cleanup is None` 分支中，返回 `True`；在 `try` 分支中，返回 `bool(await cleanup(run_id))`；在 `捕获 Exception` 分支中，返回 `False`。

  源码依据：[_cleanup_expired_stream_producer 实现](../backend/app/gateway/services.py#L342-L362)


### 5.39.4 _terminal_record_stream_missing

参数：`bridge, record`。

以 `getattr(bridge, "stream_exists", None)` 计算 `stream_exists`。关键判定为 `not _run_is_terminal(record)`、`not getattr(record, "terminal_status_persisted", True)`、`stream_exists is None`。捕获 `Exception` 的边界见源码，

分支返回 `False`、`True`、`await _cleanup_expired_stream_producer( bridge, record.run_id, )`，提前返回结束该路径。

源码依据：[_terminal_record_stream_missing 实现](../backend/app/gateway/services.py#L363-L408)


### 5.39.5 _orphan_recovery_observed_after_heartbeat

参数：`record, run_mgr, bridge`。

以 `await run_mgr.get(record.run_id, user_id=record.user_id)` 计算 `refreshed`。关键判定为 `refreshed is None or not _run_is_terminal(refreshed)`、`refreshed.stop_reason in ( ORPHAN_RECOVERY_STOP_REASON, "scheduled_task_orphan_recovered", )`、`getattr(bridge, "producer_lease_recovery_enabled", False) is True`。

分支返回 `None`、`_TERMINAL_STREAM_PRODUCER_EXPIRED`、`_TERMINAL_STREAM_RECOVERED`，提前返回结束该路径。

源码依据：[_orphan_recovery_observed_after_heartbeat 实现](../backend/app/gateway/services.py#L409-L462)


### 5.39.6 辅助函数与局部契约

- **`_skips_input_guardrail`**（`additional_kwargs, name`）：在 `条件 name == _SUMMARY_MESSAGE_NAME` 分支中，返回 `True`；返回 `bool(additional_kwargs.get("hide_from_ui")) and read_human_input_response(additional_kwargs) is None`。

  源码依据：[_skips_input_guardrail 实现](../backend/app/gateway/services.py#L463-L475)


- **`_mark_untrusted_framework_markers`**（`additional_kwargs, name`）：在 `条件 not _skips_input_guardrail(additional_kwargs, name)` 分支中，返回 `additional_kwargs`；返回 `{**additional_kwargs, UNTRUSTED_INPUT_KEY: True}`。

  源码依据：[_mark_untrusted_framework_markers 实现](../backend/app/gateway/services.py#L476-L503)


- **`_is_human_message_like`**（`message`）：在 `条件 isinstance(message, BaseMessage)` 分支中，返回 `isinstance(message, HumanMessage)`；在 `条件 isinstance(message, dict)` 分支中，返回 `(message.get("type") or message.get("role")) in {"human", "user"}`；返回 `False`。

  源码依据：[_is_human_message_like 实现](../backend/app/gateway/services.py#L504-L520)


- **`_strip_external_message_metadata`**（`message`）：以 `dict(message.additional_kwargs)` 计算 `additional_kwargs`。关键判定为 `not isinstance(message, BaseMessage)`、`_is_human_message_like(message)`、`additional_kwargs == message.additional_kwargs and clean_artifact is artifact`，相应动作只在对应分支执行。以 `clean_artifact` 计算 `update["artifact"]`。执行 `additional_kwargs.pop(ORIGINAL_USER_CONTENT_KEY, None)`；执行 `additional_kwargs.pop(key, None)`。分支返回 `message`、`message.model_copy(update=update)`，提前返回结束该路径。

  源码依据：[_strip_external_message_metadata 实现](../backend/app/gateway/services.py#L521-L547)


### 5.39.7 _strip_external_metadata_from_message_like

参数：`item`。

以 `item.get("artifact")` 计算 `artifact`；以 `{**item, "artifact": clean_artifact}` 计算 `item`。关键判定为 `isinstance(item, BaseMessage)`、`not isinstance(item, dict)`、`clean_artifact is not artifact`。分支返回 `_strip_external_message_metadata(item)`、`item`、`{**item, "additional_kwargs": additional_kwargs}`，提前返回结束该路径。

源码依据：[_strip_external_metadata_from_message_like 实现](../backend/app/gateway/services.py#L548-L589)


### 5.39.8 辅助函数与局部契约

- **`_strip_external_delegation_verdict`**（`entry`）：在 `条件 isinstance(entry, dict) and _SERVER_OWNED_DELEGATION_VERDICT_KEYS & entry.keys()` 分支中，返回 `{key: value for key, value in entry.items() if key not in _SERVER_OWNED_DELEGATION_VERDICT_KEYS}`；返回 `entry`。

  源码依据：[_strip_external_delegation_verdict 实现](../backend/app/gateway/services.py#L593-L606)


- **`_normalize_input_messages`**（`value, location, trusted_internal=False`）：以 `value if isinstance(value, list) else [value]` 计算 `messages`；以 `[]` 计算 `converted: list[BaseMessage]`。关键判定为 `not trusted_internal`、`isinstance(message, SystemMessage) or (isinstance(message, ChatMessage) and message.role.strip().lower() in {"system", "developer"})`，相应动作只在对应分支执行。执行 `converted.append(message)`。捕获 `(ValueError, TypeError, NotImplementedError, KeyError, AttributeError) as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `HTTPException`。分支返回 `converted`，提前返回结束该路径。

  源码依据：[_normalize_input_messages 实现](../backend/app/gateway/services.py#L607-L645)


- **`_normalize_external_todos`**（`value, location`）：以 `[]` 计算 `normalized: list[dict[str, Any]]`。关键判定为 `value is None`、`not isinstance(value, list)`、`not isinstance(entry, Mapping)`，相应动作只在对应分支执行。执行 `normalized.append({**entry, "content": content, "status": status})`。显式抛出路径涉及 `HTTPException`。分支返回 `None`、`normalized`，提前返回结束该路径。

  源码依据：[_normalize_external_todos 实现](../backend/app/gateway/services.py#L652-L678)


- **`_normalize_external_skill_context`**（`value, location`）：以 `[]` 计算 `normalized: list[dict[str, Any]]`。关键判定为 `value is None`、`not isinstance(value, list)`、`not isinstance(entry, Mapping)`，相应动作只在对应分支执行。执行 `normalized.append({**entry, "name": str(entry.get("name") or "")})`。显式抛出路径涉及 `HTTPException`。分支返回 `None`、`normalized`，提前返回结束该路径。

  源码依据：[_normalize_external_skill_context 实现](../backend/app/gateway/services.py#L679-L704)


- **`_normalize_external_goal`**（`value, location`）：以 `normalize_goal_objective(value["objective"])` 计算 `objective`。关键判定为 `value is None`、`not isinstance(value, Mapping) or not isinstance(value.get("objective"), str)`、`"__overwrite__" in value or value.get("type") == "__overwrite__"`，相应动作只在对应分支执行。捕获 `ValueError as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `HTTPException`。分支返回 `None`、`{**{key: item for key, item in value.items() if key != "last_batch_wakeup_run_id"}, "objective": objective}`，提前返回结束该路径。

  源码依据：[_normalize_external_goal 实现](../backend/app/gateway/services.py#L705-L722)


### 5.39.9 strip_server_owned_state_metadata

参数：`values`。

以 `SERVER_OWNED_STATE_CHANNELS.intersection(values)` 计算 `rejected`；以 `{}` 计算 `stripped: dict[str, Any]`。关键判定为 `rejected`、`channel == "goal"`、`channel == "todos"`。

以 `_strip_external_metadata_from_message_like(_normalize_external_goal(value, location="values.goal"))` 计算 `stripped[channel]`；以 `None if normalized_todos is None else [_strip_external_metadata_from_message_like(item) for item in normalized_todos]` 计算 `stripped[channel]`；其他写入目标为 `stripped[channel]`。

显式抛出路径涉及 `HTTPException`。分支返回 `stripped`，提前返回结束该路径。

源码依据：[strip_server_owned_state_metadata 实现](../backend/app/gateway/services.py#L723-L768)


### 5.39.10 normalize_input

参数：`raw_input, trusted_internal=False`。

兼容消息列表/状态映射/Command 等受支持输入，去除外部伪造的框架标记与服务端拥有元数据，规范 todos/goal/skill 等字段；结构转换不能授予内部能力。

源码依据：[normalize_input 实现](../backend/app/gateway/services.py#L769-L848)


### 5.39.11 辅助函数与局部契约

- **`_assign_missing_run_input_message_ids`**（`graph_input, thread_id, idempotency_key`）：以 `graph_input.get("messages")` 计算 `messages`。关键判定为 `not isinstance(messages, list)`、`not isinstance(message, BaseMessage) or message.id`、`idempotency_key is None`，相应动作只在对应分支执行。执行 `assigned.append(message)`；执行 `used_ids.add(message_id)`。分支返回 `graph_input`、`{**graph_input, "messages": assigned} if changed else graph_input`，提前返回结束该路径。

  源码依据：[_assign_missing_run_input_message_ids 实现](../backend/app/gateway/services.py#L849-L882)


- **`_canonical_run_record_input`**（`raw_input, graph_input`）：在 `条件 not isinstance(graph_input, dict)` 分支中，返回 `raw_input`；以 `dict(raw_input or {})` 计算 `canonical`；以 `graph_input.get("messages")` 计算 `messages`；在 `条件 isinstance(messages, list)` 分支中，以 `[message.model_dump(mode="json") if isinstance(message, BaseMessage) else message for message in messages]` 计算 `canonical["messages"]`；在 `循环 for key in ("goal", "goal_outcome", "todos", "delegations")` → `条件 key in graph_input` 分支中，以 `graph_input[key]` 计算 `canonical[key]`；返回 `canonical`。

  源码依据：[_canonical_run_record_input 实现](../backend/app/gateway/services.py#L883-L904)


- **`_strict_json_identity`**（`value, active=None`）：以 `type(value)` 计算 `value_type`。关键判定为 `value is None`、`value_type is bool`、`value_type is int`，相应动作只在对应分支执行。执行 `active.add(identity)`；执行 `active.remove(identity)`。显式抛出路径涉及 `ValueError`、`TypeError`。分支返回 `["null"]`、`["bool", value]`、`["int", str(value)]`、`["float", value.hex()]` 等值，提前返回结束该路径。finally 包含 执行 `active.remove(identity)`。

  源码依据：[_strict_json_identity 实现](../backend/app/gateway/services.py#L908-L943)


- **`_run_idempotency_request`**（`body`）：以 `getattr(body, "command", None)` 计算 `command`。关键判定为 `not command or command.get("resume") is None`，相应动作只在对应分支执行。捕获 `(RecursionError, TypeError, ValueError) as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `HTTPException`。分支返回 `{"version": RUN_IDEMPOTENCY_REQUEST_VERSION, "kind": "input"}`、`{ "version": RUN_IDEMPOTENCY_REQUEST_VERSION, "kind": "resume", "sha256": hashlib.sha256(encoded).hexdigest(), }`，提前返回结束该路径。

  源码依据：[_run_idempotency_request 实现](../backend/app/gateway/services.py#L944-L960)


- **`_legacy_stored_idempotency_request`**（`kwargs`）：以 `kwargs.get("idempotency_request")` 计算 `identity`；在 `条件 not isinstance(identity, dict) or set(identity) != {"version", "kind", "sha256"}` 分支中，返回 `None`；在 `条件 identity.get("version") != RUN_IDEMPOTENCY_REQUEST_VERSION or identity.get("kind") != "resume"` 分支中，返回 `None`；以 `identity.get("sha256")` 计算 `digest`；在 `条件 not isinstance(digest, str) or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest)` 分支中，返回 `None`；返回 `identity`。

  源码依据：[_legacy_stored_idempotency_request 实现](../backend/app/gateway/services.py#L961-L973)


- **`_find_admitted_run_for_idempotency_key`**（`run_mgr, thread_id, idempotency_key, user_id`）：以 `None` 计算 `before_created_at: str | None`；以 `None` 计算 `before_run_id: str | None`；在 `循环 while True` 分支中，以 `await run_mgr.list_by_thread( thread_id, user_id=user_id, limit=100, before_created_at=before_created_at, before_run_id=before_run_id, )` 计算 `records`；在 `循环 while True` → `循环 for record in records` → `条件 record.idempotency_key == idempotency_key and record.user_id == user_id` 分支中，返回 `record`；在 `循环 while True` → `条件 len(records) < 100` 分支中，返回 `None`；在 `循环 while True` 分支中，以 `records[-1]` 计算 `cursor`；在 `循环 while True` 分支中，以 `cursor.created_at` 计算 `before_created_at`；在 `循环 while True` 分支中，以 `cursor.run_id` 计算 `before_run_id`。

  源码依据：[_find_admitted_run_for_idempotency_key 实现](../backend/app/gateway/services.py#L974-L1001)


- **`_is_retryable_resume_start_failure`**（`record`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  record is not None and record.status == RunStatus.error and isinstance(record.error,
     str) and record.error.startswith("Failed to attach run worker:")
  ```


  源码依据：[_is_retryable_resume_start_failure 实现](../backend/app/gateway/services.py#L1002-L1006)


- **`_discard_failed_resume_admission`**（`request, run_mgr, record`）：等待 `get_run_store(request).delete(record.run_id, user_id=record.user_id)` 完成；等待 `run_mgr.cleanup(record.run_id, delay=0)` 完成。

  源码依据：[_discard_failed_resume_admission 实现](../backend/app/gateway/services.py#L1007-L1012)


- **`strip_internal_context_keys`**（`config`）：在 `循环 for section in ("context", "configurable")` 分支中，以 `config.get(section)` 计算 `value`；在 `循环 for section in ("context", "configurable")` → `条件 isinstance(value, dict)` → `循环 for key in _INTERNAL_ONLY_CONTEXT_KEYS | _SERVER_OWNED_RUNTIME_CONTEXT_KEYS` 分支中，执行 `value.pop(key, None)`。

  源码依据：[strip_internal_context_keys 实现](../backend/app/gateway/services.py#L1123-L1137)


- **`_sanitize_request_client_context`**（`raw`）：以 `sanitize_client_context(raw)` 计算 `client`；返回 `client`。

  源码依据：[_sanitize_request_client_context 实现](../backend/app/gateway/services.py#L1138-L1149)


### 5.39.12 merge_run_context_overrides

参数：`config, context, internal=False`。

以 `config.get("configurable")` 计算 `existing_configurable`。关键判定为 `isinstance(existing_configurable, dict)`、`isinstance(existing_runtime_context, dict) and "locale" in existing_runtime_context`、`locale is not None`。

以 `locale` 计算 `existing_runtime_context["locale"]`；以 `existing_client_context` 计算 `existing_runtime_context["client"]`；其他写入目标为 `runtime_context["locale"]`、`runtime_context[key]`。执行 `existing_configurable.pop("client", None)`；执行 `existing_configurable.pop("locale", None)`。显式抛出路径涉及 `HTTPException`。分支返回 `None`，提前返回结束该路径。

源码依据：[merge_run_context_overrides 实现](../backend/app/gateway/services.py#L1150-L1245)


### 5.39.13 辅助函数与局部契约

- **`resolve_trusted_internal_owner_for_attribution`**（`request, owner_user_id`）：在 `条件 not owner_user_id` 分支中，返回 `None`；以 `getattr(request.state, "user", None)` 计算 `user`；在 `条件 getattr(user, "system_role", None) != INTERNAL_SYSTEM_ROLE` 分支中，返回 `None`；在 `try` 分支中，返回 `await get_local_provider().get_user(owner_user_id)`；在 `捕获 Exception` 分支中，返回 `None`。

  源码依据：[resolve_trusted_internal_owner_for_attribution 实现](../backend/app/gateway/services.py#L1246-L1260)


### 5.39.14 inject_authenticated_user_context

参数：`config, request, internal_owner_user=None, request_context=None`。

以 `config.setdefault("context", {})` 计算 `runtime_context`。关键判定为 `not isinstance(runtime_context, dict)`、`isinstance(configurable, dict)`、`auth_source != AUTH_SOURCE_INTERNAL and getattr(user, "system_role", None) != INTERNAL_SYSTEM_ROLE`。

以 `auth_source == AUTH_SOURCE_INTERNAL` 计算 `runtime_context["is_internal"]`；以 `channel_user_id` 计算 `runtime_context["channel_user_id"]`；其他写入目标为 `runtime_context["user_id"]`、`runtime_context["user_role"]`、`runtime_context["oauth_provider"]`、`runtime_context["oauth_id"]`。

执行 `runtime_context.pop(key, None)`；执行 `configurable.pop(key, None)`。显式抛出路径涉及 `TypeError`。分支返回 `None`，提前返回结束该路径。

源码依据：[inject_authenticated_user_context 实现](../backend/app/gateway/services.py#L1261-L1340)


### 5.39.15 辅助函数与局部契约

- **`resolve_agent_factory`**（`assistant_id`）：返回 `assemble_lead_agent`。

  源码依据：[resolve_agent_factory 实现](../backend/app/gateway/services.py#L1341-L1366)


- **`_resolve_gateway_recursion_limits`**（无显式业务参数）：在 `try` 分支中，以 `get_app_config()` 计算 `app_config`；在 `try` 分支中，以 `app_config.recursion_limit` 计算 `raw`；在 `try` 分支中，以 `app_config.max_recursion_limit` 计算 `max_limit`；在 `try` 分支中，返回 `min(raw, max_limit), max_limit`；在 `捕获 Exception` 分支中，返回 `_DEFAULT_RECURSION_LIMIT, _DEFAULT_MAX_RECURSION_LIMIT`。

  源码依据：[_resolve_gateway_recursion_limits 实现](../backend/app/gateway/services.py#L1371-L1393)


- **`_resolve_max_recursion_limit`**（无显式业务参数）：在 `try` 分支中，返回 `get_app_config().max_recursion_limit`；在 `捕获 Exception` 分支中，返回 `_DEFAULT_MAX_RECURSION_LIMIT`。

  源码依据：[_resolve_max_recursion_limit 实现](../backend/app/gateway/services.py#L1394-L1406)


- **`_resolve_scheduler_recursion_limit`**（无显式业务参数）：在 `try` 分支中，以 `get_app_config().scheduler.recursion_limit` 计算 `raw`；在 `try` 分支中，以 `_resolve_max_recursion_limit()` 计算 `max_limit`；在 `try` 分支中，返回 `min(raw, max_limit)`；在 `捕获 Exception` 分支中，返回 `_DEFAULT_RECURSION_LIMIT`。

  源码依据：[_resolve_scheduler_recursion_limit 实现](../backend/app/gateway/services.py#L1407-L1443)


- **`_clamp_recursion_limit`**（`value, max_limit, default_limit`）：在 `条件 isinstance(value, bool) or not isinstance(value, int) or value <= 0` 分支中，返回 `default_limit`；返回 `min(value, max_limit)`。

  源码依据：[_clamp_recursion_limit 实现](../backend/app/gateway/services.py#L1444-L1457)


- **`custom_agent_name`**（`assistant_id, field="assistant_id"`）：在 `条件 not assistant_id or assistant_id == _DEFAULT_ASSISTANT_ID` 分支中，返回 `None`；以 `assistant_id.strip().lower().replace("_", "-")` 计算 `normalized`；在 `条件 not normalized or not re.fullmatch(r"[a-z0-9-]+", normalized)` 分支中，抛出 「表达式摘要 1」；返回 `normalized`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ValueError(f"Invalid {field} {assistant_id!r:.{_MAX_ECHOED_AGENT_NAME_CHARS}}: must contain only letters, digits, and hyphens after normalization.")
  ```


  源码依据：[custom_agent_name 实现](../backend/app/gateway/services.py#L1461-L1478)


### 5.39.16 build_run_config

参数：`thread_id, request_config, metadata, assistant_id=None`。

从请求配置建立运行参数，合并受控 context、认证身份、模型和递归上限，注入 thread/run 等关联；客户上下文不能覆盖宿主保护字段。

源码依据：[build_run_config 实现](../backend/app/gateway/services.py#L1479-L1608)


### 5.39.17 build_checkpoint_state_mutation_accessor

参数：`request, thread_id, as_node, checkpoint_id=None, checkpoint_ns="", state_schema=None`。

以 `getattr(request.app.state, "checkpoint_channel_mode", "full")` 计算 `mode`；以 `{ "configurable": { "thread_id": thread_id, "checkpoint_ns": checkpoint_ns, } }` 计算 `config: dict[str, Any]`；在 `条件 checkpoint_id is not None` 分支中，以 `checkpoint_id` 计算 `config["configurable"]["checkpoint_id"]`；执行 `inject_checkpoint_mode(config, mode)`；以 `build_state_mutation_graph(as_node, mode, state_schema)` 计算 `graph`；以 「表达式摘要 1」 计算 `accessor`；返回 `accessor, config`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
CheckpointStateAccessor.bind( graph, get_checkpointer(request), store=getattr(request.app.state,
     "store", None), mode=mode, direct_namespace=True, )
```


源码依据：[build_checkpoint_state_mutation_accessor 实现](../backend/app/gateway/services.py#L1609-L1659)


### 5.39.18 辅助函数与局部契约

- **`_accessor_graph_cache_max`**（`app_config`）：返回 `resolve_checkpoint_graph_cache_max( getattr(app_config, "database", None), "accessor_graph_max", _STATE_ACCESSOR_GRAPH_CACHE_MAX, )`。

  源码依据：[_accessor_graph_cache_max 实现](../backend/app/gateway/services.py#L1666-L1673)


- **`_cached_state_accessor_graph`**（`key, agent_factory, app_config`）：在 `资源/同步边界 _state_accessor_graph_cache_lock` 分支中，以 `_state_accessor_graph_cache.get(key)` 计算 `cached`；在 `资源/同步边界 _state_accessor_graph_cache_lock` → `条件 cached is not None and cached[0] is agent_factory and cached[1] is app_config` 分支中，返回 `cached[2]`；返回 `None`。

  源码依据：[_cached_state_accessor_graph 实现](../backend/app/gateway/services.py#L1674-L1681)


- **`_cache_state_accessor_graph`**（`key, agent_factory, app_config, graph`）：在 `资源/同步边界 _state_accessor_graph_cache_lock` → `条件 len(_state_accessor_graph_cache) >= _accessor_graph_cache_max(app_config)` 分支中，执行 `_state_accessor_graph_cache.clear()`；在 `资源/同步边界 _state_accessor_graph_cache_lock` 分支中，以 `(agent_factory, app_config, graph)` 计算 `_state_accessor_graph_cache[key]`。

  源码依据：[_cache_state_accessor_graph 实现](../backend/app/gateway/services.py#L1682-L1688)


- **`_build_state_accessor_graph`**（`agent_factory, config`）：以 `agent_factory(config=config)` 计算 `agent_result`；在 `try` 分支中，返回 `unwrap_agent_graph(agent_result)`；在 `捕获 Exception` 分支中，返回 `agent_result`。

  源码依据：[_build_state_accessor_graph 实现](../backend/app/gateway/services.py#L1689-L1700)


- **`_state_accessor_graph`**（`agent_factory, assistant_id, mode, snapshot_frequency, config`）：以 `(config.get("context") or {}).get("app_config")` 计算 `app_config`；以 `_cached_state_accessor_graph(key, agent_factory, app_config)` 计算 `cached`；以 `_build_state_accessor_graph(agent_factory, config)` 计算 `graph`。关键判定为 `cached is not None`、`cached is not None`，相应动作只在对应分支执行。进入 `_state_accessor_graph_build_locks.hold(key)` 边界，退出由对应 context manager 处理。分支返回 `cached`、`graph`，提前返回结束该路径。

  源码依据：[_state_accessor_graph 实现](../backend/app/gateway/services.py#L1701-L1721)


- **`_RawCheckpointSnapshot.__init__`**（`config, tup`）：以 `getattr(tup, "checkpoint", None) or {}` 计算 `checkpoint`。以 `tup is not None` 计算 `self.checkpoint_exists`；以 `getattr(tup, "config", None) or config` 计算 `self.config`；其他写入目标为 `self.values`、`self.metadata`、`self.parent_config`、`self.created_at`、`self.tasks: tuple`、`self.tasks_known`、`self.next: tuple`、`self.channel_versions`。

  源码依据：[__init__ 实现](../backend/app/gateway/services.py#L1732-L1746)


- **`_RawCheckpointReadAccessor.__init__`**（`checkpointer, mode`）：以 `checkpointer` 计算 `self.checkpointer`；以 `mode` 计算 `self.mode`。

  源码依据：[__init__ 实现](../backend/app/gateway/services.py#L1757-L1760)


- **`_RawCheckpointReadAccessor._gate`**（`tup`）：在 `条件 checkpoint_tuple_uses_delta(tup)` 分支中，抛出 `CheckpointModeMismatchError("Thread requires delta mode; materialize and convert its checkpoints before using full mode.")`。

  源码依据：[_gate 实现](../backend/app/gateway/services.py#L1762-L1765)


- **`_RawCheckpointReadAccessor.aget`**（`config`）：以 `await self.checkpointer.aget_tuple(config)` 计算 `tup`；执行 `self._gate(tup)`；返回 `_RawCheckpointSnapshot(config, tup)`。

  源码依据：[aget 实现](../backend/app/gateway/services.py#L1766-L1770)


- **`_RawCheckpointReadAccessor.ahistory`**（`config, limit=None`）：以 `[]` 计算 `result: list[_RawCheckpointSnapshot]`。关键判定为 `limit is not None and limit <= 0`、`config.get("configurable", {}).get("checkpoint_id")`、`anchor is not None`，相应动作只在对应分支执行。执行 `result.append(_RawCheckpointSnapshot(config, anchor))`；执行 `result.append(_RawCheckpointSnapshot(config, tup))`。分支返回 `[]`、`result`，提前返回结束该路径。

  源码依据：[ahistory 实现](../backend/app/gateway/services.py#L1771-L1800)


### 5.39.19 build_checkpoint_state_accessor

参数：`request, thread_id, assistant_id=None, checkpoint_id=None, checkpoint_ns=""`。

以 `get_run_context(request)` 计算 `ctx`。关键判定为 `checkpoint_id is not None`、`ctx.app_config is not None`、`ctx.checkpoint_channel_mode != "full"`。以 `checkpoint_ns` 计算 `configurable["checkpoint_ns"]`；以 `checkpoint_id` 计算 `configurable["checkpoint_id"]`。捕获 `ValueError`、`Exception` 的边界见源码，显式抛出路径涉及 `当前异常`。

分支返回 `_RawCheckpointReadAccessor(ctx.checkpointer, ctx.checkpoint_channel_mode), config`、`accessor, config`，提前返回结束该路径。

源码依据：[build_checkpoint_state_accessor 实现](../backend/app/gateway/services.py#L1801-L1860)


### 5.39.20 辅助函数与局部契约

- **`abuild_checkpoint_state_accessor`**（`request, thread_id, assistant_id=None, checkpoint_id=None, checkpoint_ns=""`）：以 `{ "thread_id": thread_id, "assistant_id": assistant_id, "checkpoint_id": checkpoint_id, }` 计算 `kwargs: dict[str, Any]`；在 `条件 checkpoint_ns` 分支中，以 `checkpoint_ns` 计算 `kwargs["checkpoint_ns"]`；返回 `await run_assembly(build_checkpoint_state_accessor, request, **kwargs)`。

  源码依据：[abuild_checkpoint_state_accessor 实现](../backend/app/gateway/services.py#L1861-L1890)


- **`resolve_thread_assistant_id`**（`request, thread_id, fail_closed=False`）：在 `try` 分支中，以 `get_thread_store(request)` 计算 `thread_store`；在 `try` 分支中，以 `await thread_store.get(thread_id)` 计算 `record`；在 `捕获 Exception` → `条件 fail_closed` 分支中，向外继续抛出当前异常；在 `捕获 Exception` 分支中，返回 `None`；返回 `record.get("assistant_id") if isinstance(record, dict) else None`。

  源码依据：[resolve_thread_assistant_id 实现](../backend/app/gateway/services.py#L1891-L1915)


- **`build_thread_checkpoint_state_accessor`**（`request, thread_id, checkpoint_id=None, checkpoint_ns="", fail_closed=False`）：以 `await resolve_thread_assistant_id(request, thread_id, fail_closed=fail_closed)` 计算 `assistant_id`；以 `{ "thread_id": thread_id, "assistant_id": assistant_id, "checkpoint_id": checkpoint_id, }` 计算 `kwargs: dict[str, Any]`；在 `条件 checkpoint_ns` 分支中，以 `checkpoint_ns` 计算 `kwargs["checkpoint_ns"]`；返回 `await abuild_checkpoint_state_accessor(request, **kwargs)`。

  源码依据：[build_thread_checkpoint_state_accessor 实现](../backend/app/gateway/services.py#L1916-L1940)


- **`build_thread_checkpoint_state_mutation_accessor`**（`request, thread_id, as_node, checkpoint_id=None, checkpoint_ns=""`）：以 `{ "thread_id": thread_id, "checkpoint_id": checkpoint_id, "fail_closed": True, }` 计算 `read_kwargs: dict[str, Any]`。关键判定为 `checkpoint_ns`、`checkpoint_ns`，相应动作只在对应分支执行。以 `checkpoint_ns` 计算 `read_kwargs["checkpoint_ns"]`；以 `checkpoint_ns` 计算 `mutation_kwargs["checkpoint_ns"]`。分支返回 `build_checkpoint_state_mutation_accessor(request, **mutation_kwargs)`，提前返回结束该路径。

  源码依据：[build_thread_checkpoint_state_mutation_accessor 实现](../backend/app/gateway/services.py#L1941-L1974)


### 5.39.21 apply_checkpoint_to_run_config

参数：`config, body, thread_id, request`。

以 `getattr(body, "checkpoint", None)` 计算 `checkpoint`。关键判定为 `checkpoint`、`not isinstance(checkpoint, Mapping)`、`checkpoint_thread_id is not None and str(checkpoint_thread_id) != thread_id`。

以 `checkpoint_map` 计算 `read_config["configurable"]["checkpoint_map"]`；以 `thread_id` 计算 `configurable["thread_id"]`；其他写入目标为 `configurable["checkpoint_ns"]`、`configurable["checkpoint_id"]`、`configurable["checkpoint_map"]`。捕获 `Exception as exc` 的边界见源码，显式抛出路径涉及 `HTTPException`。分支返回 `None`，提前返回结束该路径。

源码依据：[apply_checkpoint_to_run_config 实现](../backend/app/gateway/services.py#L1975-L2033)


### 5.39.22 ensure_checkpoint_history_seeded

参数：`request, thread_id, assistant_id`。

以 `request.app.state.run_event_store` 计算 `event_store`。关键判定为 `await event_store.list_messages(thread_id, limit=1, user_id=None)`、`await get_checkpointer(request).aget_tuple(checkpoint_config) is None`、`not await checkpoint_messages()`。

进入 `reserve_history_seed(request, thread_id) as reservation` 边界，退出由对应 context manager 处理。等待 `write_history_seed(request, events, reservation=reservation, seed_kind="checkpoint", assistant_id=assistant_id)` 完成。分支返回 `None`，提前返回结束该路径。

源码依据：[ensure_checkpoint_history_seeded 实现](../backend/app/gateway/services.py#L2034-L2096)


### 5.39.23 辅助函数与局部契约

- **`ensure_checkpoint_history_seeded.checkpoint_messages`**（无显式业务参数）：父函数内定义的回调。以 `await abuild_checkpoint_state_accessor(request, thread_id=thread_id, assistant_id=assistant_id)` 计算 `accessor, config`；以 `await accessor.aget(config)` 计算 `snapshot`；以 `getattr(snapshot, "values", None)` 计算 `values`；以 `values.get("messages") if isinstance(values, dict) else None` 计算 `messages`；返回 `messages if isinstance(messages, list) else []`。

  源码依据：[checkpoint_messages 实现](../backend/app/gateway/services.py#L2064-L2072)


- **`_message_identifier`**（`message`）：在 `条件 isinstance(message, BaseMessage)` 分支中，返回 `str(message.id) if message.id else None`；在 `条件 isinstance(message, Mapping)` 分支中，以 `message.get("id")` 计算 `value`；在 `条件 isinstance(message, Mapping)` 分支中，返回 `str(value) if value else None`；返回 `None`。

  源码依据：[_message_identifier 实现](../backend/app/gateway/services.py#L2097-L2105)


- **`_message_additional_kwargs`**（`message`）：在 `条件 isinstance(message, BaseMessage)` 分支中，返回 `message.additional_kwargs`；在 `条件 isinstance(message, Mapping)` 分支中，以 `message.get("additional_kwargs")` 计算 `value`；在 `条件 isinstance(message, Mapping)` 分支中，返回 `value if isinstance(value, Mapping) else {}`；返回 `{}`。

  源码依据：[_message_additional_kwargs 实现](../backend/app/gateway/services.py#L2106-L2114)


- **`_is_scope_source_human_message`**（`message`）：在 `条件 isinstance(message, HumanMessage)` 分支中，返回 `is_genuine_user_message(message)`；在 `条件 not isinstance(message, Mapping)` 分支中，返回 `False`；在 `条件 message.get("type") != "human" and message.get("role") not in {"human", "user"}` 分支中，返回 `False`；返回 `not _skips_input_guardrail(dict(_message_additional_kwargs(message)), message.get("name"))`。

  源码依据：[_is_scope_source_human_message 实现](../backend/app/gateway/services.py#L2115-L2125)


### 5.39.24 _recover_run_knowledge_scope

参数：`request, thread_id, target_message_id, checkpoint_id=None, checkpoint_ns=""`。

以 `{}` 计算 `checkpoint_kwargs: dict[str, Any]`。关键判定为 `checkpoint_id is not None`、`checkpoint_ns`、`not isinstance(messages, list)`。以 `checkpoint_id` 计算 `checkpoint_kwargs["checkpoint_id"]`；以 `checkpoint_ns` 计算 `checkpoint_kwargs["checkpoint_ns"]`。捕获 `Exception as exc` 的边界见源码，

显式抛出路径涉及 `HTTPException`。分支返回 `None`、`additional_kwargs.get(KNOWLEDGE_SCOPE_KEY)`，提前返回结束该路径。

源码依据：[_recover_run_knowledge_scope 实现](../backend/app/gateway/services.py#L2126-L2188)


### 5.39.25 辅助函数与局部契约

- **`_current_human_message`**（`graph_input`）：在 `条件 not isinstance(graph_input, Mapping)` 分支中，返回 `None`；以 `graph_input.get("messages")` 计算 `messages`；在 `条件 not isinstance(messages, list)` 分支中，返回 `None`；返回 `next( (message for message in reversed(messages) if isinstance(message, HumanMessage)), None, )`。

  源码依据：[_current_human_message 实现](../backend/app/gateway/services.py#L2189-L2200)


- **`_load_scope_agent_config`**（`assistant_id, user_id`）：以 `custom_agent_name(assistant_id, field="agent_name")` 计算 `normalized`。关键判定为 `not assistant_id or assistant_id == _DEFAULT_ASSISTANT_ID`、`not isinstance(assistant_id, str)`，相应动作只在对应分支执行。捕获 `ValueError as exc`、`FileNotFoundError as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `HTTPException`。分支返回 `None`、`await asyncio.to_thread( load_agent_config, normalized, user_id=user_id, )`，提前返回结束该路径。

  源码依据：[_load_scope_agent_config 实现](../backend/app/gateway/services.py#L2201-L2234)


- **`_validate_scope_thread_binding`**（`run_ctx, thread_id, assistant_id`）：以 `await run_ctx.thread_store.get(thread_id)` 计算 `existing`；在 `条件 not isinstance(existing, Mapping)` 分支中，返回 None，提前结束当前路径；以 `existing.get("assistant_id")` 计算 `bound`；在 `条件 isinstance(bound, str) and bound and assistant_id and bound != assistant_id` 分支中，抛出 `HTTPException( status_code=409, detail="Thread assistant does not match knowledge_scope assistant", )`。

  源码依据：[_validate_scope_thread_binding 实现](../backend/app/gateway/services.py#L2235-L2255)


- **`_client_timezone_from_context`**（`context`）：以 `context.get("client_timezone") if isinstance(context, Mapping) else None` 计算 `raw`；在 `条件 raw is None` 分支中，返回 `None`；在 `条件 not isinstance(raw, str) or not raw or len(raw) > _MAX_CLIENT_TIMEZONE_CHARS` 分支中，返回 `None`；在 `try` 分支中，返回 `await asyncio.to_thread(validate_timezone, raw)`；在 `捕获 (ValueError, OSError)` 分支中，返回 `None`。

  源码依据：[_client_timezone_from_context 实现](../backend/app/gateway/services.py#L2264-L2283)


- **`_admit_scheduler_metadata`**（`value, trusted`）：以 `dict(value) if isinstance(value, Mapping) else {}` 计算 `result`；在 `条件 not trusted` → `循环 for key in _SCHEDULER_METADATA_KEYS` 分支中，执行 `result.pop(key, None)`；执行 `result.pop("__scheduler_capability", None)`；返回 `result`。

  源码依据：[_admit_scheduler_metadata 实现](../backend/app/gateway/services.py#L2284-L2294)


### 5.39.26 start_run

**参数摘要**

```text
    body,
    thread_id,
    request,
    idempotency_key=None,
    require_existing_thread=False,
    evidence_origin=None,
    scheduled_task_runtime=None
```

**入口责任与返回语义**

HTTP 路由和内部启动器在此共享准入逻辑。输入是已解析的 `RunCreateRequest`、线程 ID 和宿主 request；结果是 `RunRecord`。首次启动通常返回已附 worker 的 pending 记录，幂等重试可返回已有记录；二者都不能直接解释为任务成功。

1. interrupt/rollback 先要求取消权限。带 scheduled runtime authority 的调用必须来自内部认证入口；线程 ID 与 metadata 中不应持久保存的 secret 字段先做校验。
2. 规范 stream mode 和断连策略，取得 bridge、manager、run context。显式模型名必须在配置 allowlist 中。线程访问权在所有入口集中核对，内部 IM 也按可信 owner header 代表连接所有者行事，不借内部角色读取其他用户的线程。
3. 暂时绑定 owner context；规范输入时区分可信内部与普通客户。工厂解析在准入前放到 assembly 执行池，解析失败不会先留下待运行记录。

源码依据：[权限、输入和 owner 边界](../backend/app/gateway/services.py#L2295-L2435)

4. 对 resume 查找真实待答中断并校验响应；有多个 LangGraph interrupt 时建立经过验证的 resume map。普通输入在审批未答时被拒绝。幂等输入保存的是服务端补 ID 之前的规范请求身份，避免每次补 ID 导致相同请求被误判不同。
5. 构建可信运行配置、Agent 和知识范围，并准备线程 metadata、产物生命周期及 worker 闭包。checkpoint/config 内的历史字段不能覆盖当前身份和宿主能力。
6. 在线程协调锁下准备 checkpoint 历史，再检查严格调用者的线程仍存在。先登记流 producer lease，再调用 RunManager 原子准入。若 producer 跟踪临时失败，跨进程桥使用保守恢复，不据此伪造结束事件。

源码依据：[恢复请求与输入身份](../backend/app/gateway/services.py#L2436-L2505) · [运行准入与流生产者移交](../backend/app/gateway/services.py#L2856-L3039)

7. 若返回已有幂等记录，比较 resume 身份或规范输入、assistant 与 conversation references；不一致返回 409。只有“先前 resume 在附 worker 前失败”的同一请求允许清理失败准入并重试，已真正执行的失败不会因此自动重跑。
8. resume 先标记对应暂停已回答，再在无额外 await 的窗口附上 worker。附接失败时关闭未启动协程，取消已建 task、记录启动失败，并尽力恢复原审批回答状态；最后归还未移交的 producer lease，重置临时用户 context。

源码依据：[幂等输入比较](../backend/app/gateway/services.py#L2936-L2978) · [附接失败处理与上下文释放](../backend/app/gateway/services.py#L2979-L3064)

**失败传播**

忙线程转成特定冲突，未知策略返回 501，不支持幂等的 store 返回 503。异常可发生在模型开始之前或 worker 已创建之后，补偿对象分别是 admission、审批状态和 producer lease；不包含任意工具副作用。

### 5.39.27 辅助函数与局部契约

- **`start_run.thread_access_allowed`**（无显式业务参数）：父函数内定义的回调。在 `条件 user is None` → `条件 not require_existing_thread` 分支中，返回 `True`；在 `条件 user is None` 分支中，返回 `await run_ctx.thread_store.get(thread_id) is not None`；以 `await run_ctx.thread_store.check_access( thread_id, str(user.id), require_existing=require_existing_thread, )` 计算 `allowed`；在 `条件 not allowed and owner_user_id and getattr(user, "system_role", None) == INTERNAL_SYSTEM_ROLE` 分支中，以 `await run_ctx.thread_store.check_access( thread_id, owner_user_id, require_existing=require_existing_thread, )` 计算 `allowed`；返回 `allowed`。

  源码依据：[thread_access_allowed 实现](../backend/app/gateway/services.py#L2390-L2410)


- **`start_run.scheduled_reader`**（`*args, **kwargs`）：父函数内定义的回调。以 「表达式摘要 1」 计算 `current_owner`；在 `条件 current_owner is None or "runs:read" not in await resolve_route_permissions(current_owner, is_internal=False)` 分支中，返回 `json.dumps({"status": "unavailable", "messages": [], "next_cursor": None, "has_more": False})`；返回 `await reader(*args, **kwargs)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  get_auth_disabled_user() if str(owner.id) == AUTH_DISABLED_USER_ID and is_auth_disabled() else await get_local_provider().get_user(str(owner.id))
  ```


  源码依据：[scheduled_reader 实现](../backend/app/gateway/services.py#L2657-L2662)


### 5.39.28 start_run.run_after_metadata

参数：`record`。

父函数内定义的回调。以 「表达式摘要 1」 计算 `metadata_task`。关键判定为 `metadata_task in done`、`abort_task not in done`、`require_existing_thread`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
asyncio.create_task( _ensure_thread_metadata( run_ctx, record, owner_user_id=owner_user_id,
     require_existing_thread=require_existing_thread, ) )
```


以 `incarnation` 计算 `incarnation_kwargs["thread_incarnation"]`。执行 `metadata_task.cancel()`；执行 `abort_task.cancel()`。捕获 `asyncio.CancelledError`、`Exception as exc`、`Exception` 的边界见源码，finally 包含 以 `metadata_task.result()` 计算 `metadata_record`；该默认实现/分支不执行额外动作。

源码依据：[run_after_metadata 实现](../backend/app/gateway/services.py#L2683-L2854)


### 5.39.29 辅助函数与局部契约

- **`start_run.run_after_metadata.verify_artifacts`**（无显式业务参数）：父函数内定义的回调。在 `try` 分支中，等待 `await_drained(verify_imports(request, record.user_id, thread_id, artifact_imports, lifetime=artifact_lifetime))` 完成；在 `捕获 AgentRunError` 分支中，以 `"artifact_conflict"` 计算 `record.stop_reason`；在 `捕获 AgentRunError` 分支中，向外继续抛出当前异常。

  源码依据：[verify_artifacts 实现](../backend/app/gateway/services.py#L2820-L2828)


### 5.39.30 start_run.admit_run

无显式业务参数。

父函数内定义的回调。关键判定为 `admitted_run is None and not (command and command.get("resume") is not None)`。等待 `reject_new_run_when_approval_pending(request, thread_id)` 完成。捕获 `RunIdempotencyUnsupported as exc`、`RunIdempotencyKeyMismatch as exc` 的边界见源码，显式抛出路径涉及 `HTTPException`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
await run_mgr.create_or_reject( thread_id, body.assistant_id, on_disconnect=disconnect,
   metadata=run_metadata, # Persist a redacted copy of the config: the run record is # written to runs.kwargs_json
  and echoed by the run API, so a # request-scoped secret (#3861) or client hint must not ride along. # The live
  config built above keeps them for the actual run. kwargs={ "input": run_record_input,
     **({"knowledge_default_request_hash": knowledge_default_request_hash} if accepts_knowledge_default else {}),
     "config": redact_config_secrets(body.config), **({"conversation_references": conversation_references} if
      conversation_references else {}), }, idempotency_request=idempotency_request,
   multitask_strategy=body.multitask_strategy, model_name=model_name,
   user_id=owner_user_id, idempotency_key=idempotency_key, evidence_origin=admitted_origin,
   # Canonical agent keys, not routing aliases or display names. # Bootstrap and unresolved legacy identities
  stay read-only. evidence_agent_id=(None if scope_runtime_config.get("is_bootstrap") else (_DEFAULT_ASSISTANT_ID
      if scope_assistant_id == _DEFAULT_ASSISTANT_ID else getattr(agent_config,
         "name", None))), run_id=admission_run_id, **({"execution_fence": request.state.extension_execution_fence}
    if getattr(request.state, "extension_execution_fence", None) is not None else {}),
   )
```


源码依据：[admit_run 实现](../backend/app/gateway/services.py#L2892-L2933)


### 5.39.31 辅助函数与局部契约

- **`start_run.cleanup_failed_attachment`**（无显式业务参数）：父函数内定义的回调。在 `条件 task is not None` 分支中，执行 `task.cancel()`；在 `条件 task is not None` 分支中，等待 `asyncio.gather(task, return_exceptions=True)` 完成；在 `条件 task is not None` → `条件 record.task is task` 分支中，以 `None` 计算 `record.task`；等待 `run_mgr.fail_start_if_pending( record.run_id, error=attachment_error, )` 完成；在 `条件 answered_approval is not None` 分支中，以 `await run_mgr.restore_approval_answer(answered_approval)` 计算 `restored`；在 `条件 answered_approval is not None` → `条件 not restored` 分支中，抛出 `RuntimeError(f"Failed to restore approval run {answered_approval.run_id} after worker attachment failed")`。

  源码依据：[cleanup_failed_attachment 实现](../backend/app/gateway/services.py#L3022-L3036)


### 5.39.32 launch_scheduled_thread_run

**参数摘要**

```text
    thread_id,
    assistant_id,
    prompt,
    request=None,
    app=None,
    owner_user_id=None,
    metadata=None,
    origin=None,
    title=None
```

以 「表达式摘要 1」 计算 `request`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
SimpleNamespace( app=app, headers=({INTERNAL_OWNER_USER_ID_HEADER_NAME: owner_user_id} if owner_user_id else {}),
   state=SimpleNamespace( user=get_internal_user(), auth_source=AUTH_SOURCE_INTERNAL,
     # Every trigger (schedule or "Run now") is server-started. run_origin=make_origin("schedule"),
     ), cookies={}, )
```


关键判定为 `request is None`、`app is None`、`origin`。进入 `ensure_trace_context()` 边界，退出由对应 context manager 处理。以 `{SCHEDULED_ORIGIN_KEY: origin}` 计算 `message["additional_kwargs"]`；以 `title` 计算 `graph_input["title"]`；其他写入目标为 `scheduled_task_runtime["goal_objective"]`。捕获 `HTTPException as exc` 的边界见源码，

显式抛出路径涉及 `ValueError`、`ConflictError`、`当前异常`。分支返回 `{"run_id": record.run_id, "thread_id": record.thread_id}`，提前返回结束该路径。

源码依据：[launch_scheduled_thread_run 实现](../backend/app/gateway/services.py#L3065-L3168)


### 5.39.33 辅助函数与局部契约

- **`_mcp_task_notification_prompt`**（`event`）：以 `frame_untrusted_text(json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str))` 计算 `payload`；以 「表达式摘要 1」 计算 `instruction`；返回 `f"{instruction}\n\n{payload}"`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ( "A durable background MCP task has an update that requires the user's attention. " "Explain the update clearly and concisely. Do not expose or ask for a remote task ID. "
    "When status is input_required, show the question but explain that this MCP integration " "cannot resume the remote task with user input yet. When tracking_degraded is true, explain "
    "that DeerFlow will continue retrying at a lower frequency." )
  ```


  源码依据：[_mcp_task_notification_prompt 实现](../backend/app/gateway/services.py#L3169-L3181)


### 5.39.34 launch_mcp_task_notification_run

参数：`app, thread_id, assistant_id, owner_user_id, task_id, dispatch_version, dispatch_attempt, event`。

以 「表达式摘要 1」 计算 `request`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
SimpleNamespace( app=app, headers={INTERNAL_OWNER_USER_ID_HEADER_NAME: owner_user_id},
   state=SimpleNamespace(user=get_internal_user(), auth_source=AUTH_SOURCE_INTERNAL,
     run_origin=make_origin("mcp_notification")), cookies={}, )
```


关键判定为 `isinstance(exc.detail, dict) and exc.detail.get("code") == "approval_pending"`、`isinstance(exc, BusyThreadConflict)`、`exc.status_code in {400, 401, 403, 404, 409, 422, 501}`。进入 `ensure_trace_context()` 边界，退出由对应 context manager 处理。捕获 `HTTPException as exc` 的边界见源码，

显式抛出路径涉及 `ConflictError`、`PermanentNotificationError`、`当前异常`。分支返回 `{"run_id": record.run_id, "thread_id": record.thread_id}`，提前返回结束该路径。

源码依据：[launch_mcp_task_notification_run 实现](../backend/app/gateway/services.py#L3182-L3260)


### 5.39.35 sse_consumer

参数：`bridge, record, request, run_mgr, apply_on_disconnect=True, emit_gap_on_missing_stream=False`。

把桥事件转为 SSE，并独立处理 gap、heartbeat、END 和 creator 断连；join/store-only 不借断连取消，普通桥异常也不被误判成用户取消。

源码依据：[sse_consumer 实现](../backend/app/gateway/services.py#L3261-L3383)


### 5.39.36 辅助函数与局部契约

- **`serialize_wait_run_status`**（`record`）：以 `{"status": record.status.value, "error": record.error}` 计算 `result: dict[str, Any]`；在 `条件 record.status == RunStatus.error` 分支中，以 `"GraphRecursionError" if getattr(record, "stop_reason", None) == "recursion_capped" else "RunError"` 计算 `name`；在 `条件 record.status == RunStatus.error` 分支中，以 `{"error": name, "message": record.error or "Run failed."}` 计算 `result["__error__"]`；返回 `result`。

  源码依据：[serialize_wait_run_status 实现](../backend/app/gateway/services.py#L3384-L3395)


### 5.39.37 wait_for_run_completion

参数：`bridge, record, request, run_mgr`。

以 `False` 计算 `completed`。关键判定为 `await _terminal_record_stream_missing(bridge, record)`、`entry is END_SENTINEL`、`isinstance(entry, StreamGap)`。等待 `await_drained(run_mgr.cancel(record.run_id))` 完成。捕获 `asyncio.CancelledError` 的边界见源码，显式抛出路径涉及 `RuntimeError`、`当前异常`。分支返回 `True`、`False`，提前返回结束该路径。

finally 包含 等待 `await_drained(run_mgr.cancel(record.run_id))` 完成。

源码依据：[wait_for_run_completion 实现](../backend/app/gateway/services.py#L3396-L3497)


## 5.40 线程资源读写与分支删除

源码位置：`backend/app/gateway/routers/threads.py`。

线程管理端点与分支/删除/状态/history 操作所在。metadata 列表与 checkpoint 详细状态分离；写操作通过 reservation 和当前 owner，分支复制与 checkpoint 保存不是跨文件/数据库事务。

### 5.40.1 辅助函数与局部契约

- **`_optional_run_event_store`**（`request`）：返回 `getattr(request.app.state, "run_event_store", None)`。

  源码依据：[_optional_run_event_store 实现](../backend/app/gateway/routers/threads.py#L108-L118)


- **`_checkpoint_mode_http_error`**（`exc, thread_id`）：在 `条件 isinstance(exc, CheckpointModeMismatchError)` 分支中，返回 `HTTPException(status_code=409, detail=f"Thread {thread_id}: {exc}")`；返回 `HTTPException(status_code=503, detail=str(exc))`。

  源码依据：[_checkpoint_mode_http_error 实现](../backend/app/gateway/routers/threads.py#L119-L140)


- **`_strip_reserved_metadata`**（`metadata`）：在 `条件 not metadata` 分支中，返回 `metadata or {}`；返回 `{k: v for k, v in metadata.items() if k not in _SERVER_RESERVED_METADATA_KEYS}`。

  源码依据：[_strip_reserved_metadata 实现](../backend/app/gateway/routers/threads.py#L170-L176)


- **`_is_organization_metadata_patch`**（`metadata`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  bool(metadata) and set(metadata) <= {THREAD_PINNED_METADATA_KEY, THREAD_ARCHIVED_METADATA_KEY} and all(isinstance(value,
       bool) for value in metadata.values())
  ```


  源码依据：[_is_organization_metadata_patch 实现](../backend/app/gateway/routers/threads.py#L177-L181)


- **`_message_id`**（`message`）：在 `条件 isinstance(message, dict)` 分支中，以 `message.get("id")` 计算 `raw`；在 `同层否则分支` 分支中，以 `getattr(message, "id", None)` 计算 `raw`；返回 `raw if isinstance(raw, str) and raw else None`。

  源码依据：[_message_id 实现](../backend/app/gateway/routers/threads.py#L182-L189)


- **`_message_type`**（`message`）：在 `条件 isinstance(message, dict)` 分支中，以 `message.get("type")` 计算 `raw`；在 `同层否则分支` 分支中，以 `getattr(message, "type", None)` 计算 `raw`；返回 `raw if isinstance(raw, str) and raw else None`。

  源码依据：[_message_type 实现](../backend/app/gateway/routers/threads.py#L190-L197)


- **`_message_additional_kwargs`**（`message`）：在 `条件 isinstance(message, dict)` 分支中，以 `message.get("additional_kwargs")` 计算 `raw`；在 `同层否则分支` 分支中，以 `getattr(message, "additional_kwargs", None)` 计算 `raw`；返回 `raw if isinstance(raw, dict) else {}`。

  源码依据：[_message_additional_kwargs 实现](../backend/app/gateway/routers/threads.py#L198-L205)


- **`_is_branch_visible_message`**（`message`）：在 `条件 _message_additional_kwargs(message).get("hide_from_ui") is True` 分支中，返回 `False`；返回 `_message_type(message) in {"human", "ai"}`。

  源码依据：[_is_branch_visible_message 实现](../backend/app/gateway/routers/threads.py#L206-L211)


- **`_is_branch_assistant_message`**（`message`）：返回 `_message_type(message) == "ai"`。

  源码依据：[_is_branch_assistant_message 实现](../backend/app/gateway/routers/threads.py#L212-L215)


- **`_checkpoint_messages`**（`snapshot`）：以 `getattr(snapshot, "values", None) or {}` 计算 `values`；以 `values.get("messages") if isinstance(values, dict) else None` 计算 `messages`；返回 `list(messages) if isinstance(messages, list) else []`。

  源码依据：[_checkpoint_messages 实现](../backend/app/gateway/routers/threads.py#L216-L221)


- **`_checkpoint_id`**（`snapshot`）：以 `getattr(snapshot, "config", {}) or {}` 计算 `config`；以 `config.get("configurable", {}).get("checkpoint_id")` 计算 `raw`；返回 `raw if isinstance(raw, str) and raw else None`。

  源码依据：[_checkpoint_id 实现](../backend/app/gateway/routers/threads.py#L222-L227)


- **`_matches_branch_target`**（`messages, target_message_ids`）：在 `条件 not target_message_ids` 分支中，返回 `False`；以 `{_message_id(message): index for index, message in enumerate(messages) if _message_id(message)}` 计算 `index_by_id`；在 `条件 not target_message_ids.issubset(index_by_id.keys())` 分支中，返回 `False`；在 `条件 any(not _is_branch_assistant_message(messages[index_by_id[message_id]]) for message_id in target_message_ids)` 分支中，返回 `False`；以 `max(index_by_id[message_id] for message_id in target_message_ids)` 计算 `target_end_index`；返回 `not any(_is_branch_visible_message(message) for message in messages[target_end_index + 1 :])`。

  源码依据：[_matches_branch_target 实现](../backend/app/gateway/routers/threads.py#L228-L241)


- **`_branch_target_human_message`**（`messages, target_message_ids`）：以 `{_message_id(message): index for index, message in enumerate(messages) if _message_id(message)}` 计算 `index_by_id`；在 `条件 not target_message_ids.issubset(index_by_id.keys())` 分支中，返回 `None`；以 `min(index_by_id[message_id] for message_id in target_message_ids)` 计算 `target_start_index`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  next( (message for message in reversed(messages[:target_start_index]) if _message_type(message) == "human" and
      _is_branch_visible_message(message)), None, )
  ```


  源码依据：[_branch_target_human_message 实现](../backend/app/gateway/routers/threads.py#L242-L252)


- **`_find_branch_checkpoint`**（`accessor, config, target_message_ids`）：以 `await accessor.ahistory(config, limit=_BRANCH_HISTORY_RAW_SCAN_LIMIT)` 计算 `history`。关键判定为 `is_duration_only_checkpoint(snapshot, parent=parent, versions=versions, parent_versions=parent_versions)`、`_matches_branch_target(_checkpoint_messages(snapshot), target_message_ids)`，相应动作只在对应分支执行。捕获 `_CHECKPOINT_MODE_ERRORS as exc`、`Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `_checkpoint_mode_http_error`、`HTTPException`。分支返回 `snapshot`，提前返回结束该路径。

  源码依据：[_find_branch_checkpoint 实现](../backend/app/gateway/routers/threads.py#L253-L277)


- **`_branch_targets_latest_turn`**（`accessor, config, target_message_ids`）：以 `await accessor.ahistory(config, limit=_BRANCH_HISTORY_RAW_SCAN_LIMIT)` 计算 `history`。关键判定为 `is_duration_only_checkpoint(snapshot, parent=parent, versions=versions, parent_versions=parent_versions)`、`not messages`，相应动作只在对应分支执行。捕获 `Exception` 的边界见源码，捕获不等于已恢复。分支返回 `_matches_branch_target(messages, target_message_ids)`、`False`，提前返回结束该路径。

  源码依据：[_branch_targets_latest_turn 实现](../backend/app/gateway/routers/threads.py#L278-L306)


### 5.40.2 _find_branch_replay_base

参数：`accessor, config, snapshot, target_human_id`。

以 `config.get("configurable", {}).get("thread_id", "")` 计算 `thread_id`。关键判定为 `not target_found`。捕获 `CheckpointParentMissingError`、`CheckpointLineageError as exc`、`_CHECKPOINT_MODE_ERRORS as exc`、`Exception as exc` 的边界见源码，显式抛出路径涉及 `HTTPException`、`_checkpoint_mode_http_error`。

分支返回 `await find_checkpoint_before_message( accessor, snapshot, target_human_id, max_depth=_BRANCH_HISTORY_RAW_SCAN_LIMIT, )`、`replay_base`，提前返回结束该路径。

源码依据：[_find_branch_replay_base 实现](../backend/app/gateway/routers/threads.py#L307-L357)


### 5.40.3 辅助函数与局部契约

- **`_ignore_branch_user_data`**（`directory, names`）：以 `set()` 计算 `ignored: set[str]`；以 `Path(directory)` 计算 `base`；在 `循环 for name in names` 分支中，以 `base / name` 计算 `path`；在 `循环 for name in names` → `条件 name.startswith(".upload-") and name.endswith(".part")` 分支中，执行 `ignored.add(name)`；在 `循环 for name in names` → `条件 path.is_symlink()` 分支中，执行 `ignored.add(name)`；返回 `ignored`。

  源码依据：[_ignore_branch_user_data 实现](../backend/app/gateway/routers/threads.py#L358-L369)


- **`_copy_branch_user_data_sync`**（`paths, source_thread_id, target_thread_id, user_id`）：以 `paths.sandbox_user_data_dir(source_thread_id, user_id=user_id)` 计算 `source`。关键判定为 `not source.exists()`、`source_uploads.is_dir() and target_uploads.is_dir()`、`markdown is None`，相应动作只在对应分支执行。分支返回 `"not_found"`、`"current_thread_best_effort"`，提前返回结束该路径。

  源码依据：[_copy_branch_user_data_sync 实现](../backend/app/gateway/routers/threads.py#L370-L390)


- **`_copy_branch_user_data`**（`source_thread_id, target_thread_id`）：以 `get_paths()` 计算 `paths`；以 `get_effective_user_id()` 计算 `user_id`；在 `try` 分支中，返回 `await run_file_io(_copy_branch_user_data_sync, paths, source_thread_id, target_thread_id, user_id=user_id)`；在 `捕获 Exception` 分支中，返回 `"failed"`。

  源码依据：[_copy_branch_user_data 实现](../backend/app/gateway/routers/threads.py#L391-L405)


- **`_next_branch_title_sequence`**（`source_sequence, source_is_branch`）：在 「表达式摘要 1」 分支中，返回 `source_sequence + 1`；返回 `2`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 source_is_branch and isinstance(source_sequence, int) and not isinstance(source_sequence,
     bool) and 2 <= source_sequence < _BRANCH_TITLE_SEQUENCE_MAX
  ```


  源码依据：[_next_branch_title_sequence 实现](../backend/app/gateway/routers/threads.py#L406-L411)


- **`_format_branch_display_name`**（`base, sequence`）：以 `f" ({sequence})"` 计算 `suffix`；以 `base[: _BRANCH_TITLE_MAX_LENGTH - len(suffix)].rstrip()` 计算 `truncated_base`；返回 `f"{truncated_base}{suffix}" if truncated_base else None`。

  源码依据：[_format_branch_display_name 实现](../backend/app/gateway/routers/threads.py#L412-L417)


- **`_default_branch_title`**（`source_title, source_is_branch=False, source_sequence=None, sibling_records=None`）：以 `source_title.strip()` 计算 `display_name`。关键判定为 `not isinstance(source_title, str)`、`source_is_branch`、`not display_name`，相应动作只在对应分支执行。执行 `while display_name.lower().startswith("branch:"):`。分支返回 `None, None`、`display_name, sequence if display_name is not None else None`，提前返回结束该路径。

  源码依据：[_default_branch_title 实现](../backend/app/gateway/routers/threads.py#L418-L452)


- **`_branch_sibling_records`**（`thread_store, parent_thread_id`）：以 `[]` 计算 `siblings: list[dict[str, Any]]`；以 `0` 计算 `offset`；在 `循环 while True` 分支中，以 `await thread_store.search( metadata={"branch_parent_thread_id": parent_thread_id}, limit=_BRANCH_SIBLING_PAGE_SIZE, offset=offset, )` 计算 `page`；在 `循环 while True` 分支中，执行 `siblings.extend(page)`；在 `循环 while True` → `条件 len(page) < _BRANCH_SIBLING_PAGE_SIZE` 分支中，返回 `siblings`；在 `循环 while True` 分支中，对 `offset` 执行 `+= len(page)` 更新。

  源码依据：[_branch_sibling_records 实现](../backend/app/gateway/routers/threads.py#L453-L472)


- **`_MetadataRedactingResponse._redact_legacy_metadata_secret`**（`value`）：返回 `redact_metadata_secrets(value)`。

  源码依据：[_redact_legacy_metadata_secret 实现](../backend/app/gateway/routers/threads.py#L483-L486)


- **`ThreadCreateRequest._assistant_id_names_an_agent`**（`value`）：执行 `custom_agent_name(value)`；返回 `value`。

  源码依据：[_assistant_id_names_an_agent 实现](../backend/app/gateway/routers/threads.py#L523-L529)


- **`ThreadSearchRequest._validate_metadata_filters`**（`v`）：在 `条件 not v` 分支中，返回 `v`；以 `[]` 计算 `bad_entries: list[str]`；在 `循环 for key, value in v.items()` → `条件 not validate_metadata_filter_key(key)` 分支中，执行 `bad_entries.append(f"{key!r} (unsafe key)")`；在 `循环 for key, value in v.items()` → `条件 not validate_metadata_filter_value(value)` 分支中，执行 `bad_entries.append(f"{key!r} (unsupported value type {type(value).__name__})")`；在 `条件 bad_entries` 分支中，抛出 `ValueError(f"Invalid metadata filter entries: {', '.join(bad_entries)}")`；返回 `v`。

  源码依据：[_validate_metadata_filters 实现](../backend/app/gateway/routers/threads.py#L542-L562)


- **`ThreadPatchRequest.validate_archive_flag`**（`value`）：在 `条件 THREAD_ARCHIVED_METADATA_KEY in value and not isinstance(value[THREAD_ARCHIVED_METADATA_KEY], bool)` 分支中，抛出 `ValueError("deerflow_archived must be a boolean")`；返回 `value`。

  源码依据：[validate_archive_flag 实现](../backend/app/gateway/routers/threads.py#L585-L590)


- **`_delete_thread_data`**（`thread_id, paths=None, user_id=None`）：以 `paths or get_paths()` 计算 `path_manager`；在 `try` 分支中，执行 `path_manager.delete_thread_dir(thread_id, user_id=user_id)`；在 `捕获 ValueError as exc` 分支中，抛出 `HTTPException(status_code=422, detail=str(exc)) from exc`；在 `捕获 FileNotFoundError` 分支中，返回 `ThreadDeleteResponse(success=True, message=f"No local data for {thread_id}")`；在 `捕获 Exception as exc` 分支中，抛出 `HTTPException(status_code=500, detail="Failed to delete local thread data.") from exc`；返回 `ThreadDeleteResponse(success=True, message=f"Deleted local thread data for {thread_id}")`。

  源码依据：[_delete_thread_data 实现](../backend/app/gateway/routers/threads.py#L693-L711)


- **`_fetch_raw_pending_writes`**（`checkpointer, config`）：以 `await checkpointer.aget_tuple(config)` 计算 `raw_tuple`；在 `条件 raw_tuple is None` 分支中，返回 `[]`；返回 `list(getattr(raw_tuple, "pending_writes", ()) or ())`。

  源码依据：[_fetch_raw_pending_writes 实现](../backend/app/gateway/routers/threads.py#L712-L725)


- **`_derive_thread_status`**（`snapshot, pending_writes, fallback_status="idle"`）：在 `条件 snapshot is None` 分支中，返回 `"idle"`；在 `循环 for write in pending_writes` → `条件 isinstance(write, (list, tuple)) and len(write) >= 2 and write[1] == "__error__"` 分支中，返回 `"error"`；以 `getattr(snapshot, "tasks", None) or ()` 计算 `tasks`；在 `循环 for task in tasks` → `条件 getattr(task, "error", None) is not None` 分支中，返回 `"error"`；在 `条件 not getattr(snapshot, "tasks_known", True)` 分支中，返回 `fallback_status`；在 `条件 tasks` 分支中，返回 `"interrupted"`；返回 `"idle"`。

  源码依据：[_derive_thread_status 实现](../backend/app/gateway/routers/threads.py#L726-L749)


- **`_ensure_thread_for_goal`**（`thread_id, request`）：以 `get_thread_store(request)` 计算 `thread_store`。关键判定为 `record is None and thread_owner_user_id`、`unscoped_record is not None`、`unscoped_record.get("user_id") != thread_owner_user_id`，相应动作只在对应分支执行。等待 `thread_store.update_owner(thread_id, thread_owner_user_id, user_id=None)` 完成；等待 `thread_store.create(thread_id, metadata={}, **thread_owner_kwargs)` 完成。捕获 `Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `HTTPException`。

  源码依据：[_ensure_thread_for_goal 实现](../backend/app/gateway/routers/threads.py#L750-L784)


- **`delete_thread_data`**（`thread_id, request`）：以 `get_run_manager(request)` 计算 `run_manager`；在 `try` → `资源/同步边界 goal_thread_lock(thread_id)` → `资源/同步边界 run_manager.reserve_thread_operation( thread_id, kind=ThreadOperationKind.delete, user_id=get_effective_user_id(), )` 分支中，返回 `await _delete_thread_data_with_reservation(thread_id, request)`；在 `捕获 ConflictError` 分支中，抛出 `HTTPException( status_code=409, detail="Thread has work in flight. Delete it after the work finishes.", ) from None`。

  源码依据：[delete_thread_data 实现](../backend/app/gateway/routers/threads.py#L787-L809)


- **`_event_delete_owner_kwargs`**（`delete_by_thread, user_id`）：在 `try` 分支中，以 `inspect.signature(delete_by_thread).parameters.values()` 计算 `parameters`；在 `捕获 (TypeError, ValueError)` 分支中，返回 `{}`；在 「表达式摘要 1」 分支中，返回 `{"user_id": user_id}`；返回 `{}`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 any(parameter.kind == inspect.Parameter.VAR_KEYWORD or (parameter.name == "user_id" and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD,
         inspect.Parameter.KEYWORD_ONLY)) for parameter in parameters)
  ```


  源码依据：[_event_delete_owner_kwargs 实现](../backend/app/gateway/routers/threads.py#L810-L827)


### 5.40.4 _delete_thread_data_with_reservation

参数：`thread_id, request`。

在删除 reservation 内清审批、抑制续任务、删除合法 thread 目录与 checkpoint；checkpoint 失败返回500并保留 owner，后续历史/meta 清理有 best-effort 分支，非跨库原子删除。

源码依据：[_delete_thread_data_with_reservation 实现](../backend/app/gateway/routers/threads.py#L828-L975)


### 5.40.5 辅助函数与局部契约

- **`_resolve_existing_thread`**（`thread_store, thread_id, thread_owner_user_id, thread_owner_kwargs`）：以 `await thread_store.get(thread_id, **thread_owner_kwargs)` 计算 `existing_record`；在 `条件 existing_record is None and thread_owner_user_id` 分支中，等待 `thread_store.claim_unowned(thread_id, thread_owner_user_id)` 完成；在 `条件 existing_record is None and thread_owner_user_id` 分支中，以 `await thread_store.get(thread_id, **thread_owner_kwargs)` 计算 `existing_record`；返回 `existing_record`。

  源码依据：[_resolve_existing_thread 实现](../backend/app/gateway/routers/threads.py#L976-L995)


- **`_existing_thread_response`**（`thread_id, record`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ThreadResponse( thread_id=thread_id, status=record.get("status", "idle"),
     created_at=coerce_iso(record.get("created_at", "")), updated_at=coerce_iso(record.get("updated_at",
         "")), metadata=record.get("metadata", {}), )
  ```


  源码依据：[_existing_thread_response 实现](../backend/app/gateway/routers/threads.py#L996-L1005)


- **`create_thread`**（`body, request`）：以 `resolve_thread_id(body.thread_id)` 计算 `thread_id`；在 `资源/同步边界 goal_thread_lock(thread_id)` 分支中，返回 `await _create_thread_locked(body, request, thread_id)`。

  源码依据：[create_thread 实现](../backend/app/gateway/routers/threads.py#L1008-L1019)


### 5.40.6 _create_thread_locked

参数：`body, request, thread_id`。

以 `get_checkpointer(request)` 计算 `checkpointer`。关键判定为 `(origin := admit_origin(getattr(request.state, "run_origin", None))) is not None`、`existing_record is not None`、`existing_record is not None`。以 `origin` 计算 `metadata[DEERFLOW_ORIGIN_KEY]`。

等待 `checkpointer.aput(config, empty_checkpoint(), ckpt_metadata, {})` 完成。捕获 `ProjectNotAssignableError`、`ThreadOwnershipConflictError`、`IntegrityError`、`Exception` 的边界见源码，显式抛出路径涉及 `HTTPException`。

分支返回 `_existing_thread_response(thread_id, existing_record)`、`_existing_thread_response(thread_id, created_record)`，提前返回结束该路径。

源码依据：[_create_thread_locked 实现](../backend/app/gateway/routers/threads.py#L1020-L1113)


### 5.40.7 辅助函数与局部契约

- **`branch_thread`**（`thread_id, body, request`）：在 `try` → `资源/同步边界 goal_thread_lock(thread_id)` → `资源/同步边界 get_run_manager(request).reserve_thread_operation( thread_id, kind=ThreadOperationKind.branch, user_id=get_effective_user_id(), )` 分支中，返回 `await _branch_thread_with_reservation(thread_id, body, request)`；在 `捕获 ConflictError` 分支中，抛出 `HTTPException( status_code=409, detail="Thread has work in flight. Branch it after the work finishes.", ) from None`。

  源码依据：[branch_thread 实现](../backend/app/gateway/routers/threads.py#L1116-L1132)


### 5.40.8 _branch_thread_with_reservation

参数：`thread_id, body, request`。

定位用户回合/检查点，构造分支可见消息和新元数据，复制线程文件并写分支状态；sandbox 保留 fork 标记由 middleware 避免释放父 holder。

源码依据：[_branch_thread_with_reservation 实现](../backend/app/gateway/routers/threads.py#L1133-L1351)


### 5.40.9 辅助函数与局部契约

- **`_branch_thread_with_reservation.branch_values`**（`source_snapshot, replaces=None`）：父函数内定义的回调。以 `dict(source_snapshot.values)` 计算 `source_values`；以 `{}` 计算 `values: dict[str, Any]`。关键判定为 `key in _BRANCH_EXCLUDED_CHANNELS`、`key in branch_reducer_fields`、`replaces is not None`，相应动作只在对应分支执行。以 `Overwrite(list(value) if key == "messages" and isinstance(value, list) else value)` 计算 `values[key]`；以 `value` 计算 `values[key]`；其他写入目标为 `values[key]`、`values["title"]`。分支返回 `values`，提前返回结束该路径。

  源码依据：[branch_values 实现](../backend/app/gateway/routers/threads.py#L1229-L1250)


- **`_branch_thread_with_reservation._write_branch_row`**（`project_id`）：父函数内定义的回调。在 `try` 分支中，返回 「表达式摘要 1」；在 `捕获 ProjectNotAssignableError` 分支中，向外继续抛出当前异常；在 `捕获 Exception` 分支中，抛出 `HTTPException(status_code=500, detail="Failed to create branch") from None`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await thread_store.create( new_thread_id, assistant_id=branch_assistant_id,
     display_name=display_name, metadata=branch_metadata, project_id=project_id,
     **thread_owner_kwargs, )
  ```


  源码依据：[_write_branch_row 实现](../backend/app/gateway/routers/threads.py#L1287-L1302)


### 5.40.10 search_threads

参数：`body, request`。

以 `get_thread_store(request)` 计算 `repo`。捕获 `InvalidMetadataFilterError as exc` 的边界见源码，显式抛出路径涉及 `HTTPException`。

分支返回 `` [ ThreadResponse( thread_id=r["thread_id"], status=r.get("status", "idle"), # ``coerce_iso`` heals legacy unix-second values that # ``MemoryThreadMetaStore`` historically wrote with ``time.time()`「表达式摘要 1」`，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
; # SQL-backed rows already arrive as ISO strings and pass through. created_at=coerce_iso(r.get("created_at",
     "")), updated_at=coerce_iso(r.get("updated_at", "")), metadata=r.get("metadata",
   {}), values={"title": r["display_name"]} if r.get("display_name") else {},
 interrupts={}, unread=(r["thread_id"] in unread_ids) if unread_ids is not None else None,
 ) for r in rows ]
```


源码依据：[search_threads 实现](../backend/app/gateway/routers/threads.py#L1354-L1396)


### 5.40.11 辅助函数与局部契约

- **`_unread_thread_ids`**（`request, thread_ids`）：以 `getattr(request.app.state, "thread_read_repo", None)` 计算 `repo`；在 `条件 repo is None` 分支中，返回 `None`；在 `条件 not thread_ids` 分支中，返回 `set()`；在 `try` 分支中，返回 `await repo.unread_thread_ids(user_id=str(get_effective_user_id()), thread_ids=thread_ids)`；在 `捕获 Exception` 分支中，返回 `None`。

  源码依据：[_unread_thread_ids 实现](../backend/app/gateway/routers/threads.py#L1397-L1414)


- **`mark_thread_read`**（`thread_id, request`）：以 `getattr(request.app.state, "thread_read_repo", None)` 计算 `repo`；在 `条件 repo is None` 分支中，抛出 `HTTPException(status_code=503, detail="Thread read state is not available")`；以 `str(get_effective_user_id())` 计算 `user_id`；等待 `repo.mark_read(user_id=user_id, thread_id=thread_id)` 完成；返回 `ThreadReadResponse(unread=False, read_version=await repo.read_version(user_id=user_id))`。

  源码依据：[mark_thread_read 实现](../backend/app/gateway/routers/threads.py#L1417-L1430)


- **`patch_thread`**（`thread_id, body, request`）：以 `get_thread_store(request)` 计算 `thread_store`。关键判定为 `record is None`，相应动作只在对应分支执行。等待 `thread_store.update_metadata(thread_id, body.metadata, touch=touch)` 完成。捕获 `Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `HTTPException`。分支返回 「表达式摘要 1」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ThreadResponse( thread_id=thread_id, status=record.get("status", "idle"),
     created_at=coerce_iso(record.get("created_at", "")), updated_at=coerce_iso(record.get("updated_at",
         "")), metadata=record.get("metadata", {}), )
  ```


  源码依据：[patch_thread 实现](../backend/app/gateway/routers/threads.py#L1433-L1463)


- **`move_thread`**（`thread_id, body, request`）：以 `get_thread_store(request)` 计算 `thread_store`；以 `await thread_store.set_project(thread_id, body.project_id)` 计算 `moved`；在 `条件 not moved` 分支中，抛出 `HTTPException(status_code=404, detail="Thread or project not found")`；以 `await thread_store.get(thread_id)` 计算 `record`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ThreadResponse( thread_id=thread_id, status=record.get("status", "idle"),
     created_at=coerce_iso(record.get("created_at", "")), updated_at=coerce_iso(record.get("updated_at",
         "")), metadata=record.get("metadata", {}), )
  ```


  源码依据：[move_thread 实现](../backend/app/gateway/routers/threads.py#L1472-L1490)


### 5.40.12 get_thread

参数：`thread_id, request`。

以 `get_thread_store(request)` 计算 `thread_store`。关键判定为 `record is None and not checkpoint_id`、`record is None`。捕获 `_CHECKPOINT_MODE_ERRORS as exc`、`Exception` 的边界见源码，显式抛出路径涉及 `_checkpoint_mode_http_error`、`HTTPException`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
ThreadResponse( thread_id=thread_id, status=status, created_at=coerce_iso(record.get("created_at",
       "")), updated_at=coerce_iso(record.get("updated_at", "")),
   metadata=record.get("metadata", {}), values=serialize_channel_values_for_api(snapshot.values),
   interrupts={task_id: [serialize_lc_object(interrupt) for interrupt in interrupts] for task_id,
     interrupts in (await find_pending_interrupts(request, thread_id,
         accessor=accessor, read_config=config, snapshot=snapshot)).items()},
   )
```


源码依据：[get_thread 实现](../backend/app/gateway/routers/threads.py#L1493-L1544)


### 5.40.13 辅助函数与局部契约

- **`get_thread_goal`**（`thread_id, request`）：以 `get_checkpointer(request)` 计算 `checkpointer`；在 `try` 分支中，以 `await read_thread_goal(checkpointer, thread_id)` 计算 `goal`；在 `捕获 Exception` 分支中，抛出 `HTTPException(status_code=500, detail="Failed to read thread goal") from None`；返回 `ThreadGoalResponse(goal=goal)`。

  源码依据：[get_thread_goal 实现](../backend/app/gateway/routers/threads.py#L1547-L1557)


- **`set_thread_goal`**（`thread_id, body, request`）：以 `get_checkpointer(request)` 计算 `checkpointer`。关键判定为 `batch_repo is not None`，相应动作只在对应分支执行。进入 `reserve_checkpoint_write(request, thread_id, user_id=get_effective_user_id())` 边界，退出由对应 context manager 处理。等待 `reject_new_run_when_approval_pending(request, thread_id, accessor=accessor, read_config=read_config)` 完成；等待 `_ensure_thread_for_goal(thread_id, request)` 完成。捕获 `_CHECKPOINT_MODE_ERRORS as exc`、`ValueError as exc`、`ConflictError`、`HTTPException`、`Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `_checkpoint_mode_http_error`、`HTTPException`、`当前异常`。分支返回 `ThreadGoalResponse(goal=goal)`，提前返回结束该路径。

  源码依据：[set_thread_goal 实现](../backend/app/gateway/routers/threads.py#L1560-L1593)


### 5.40.14 synthesize_thread_goal_from_chat

参数：`thread_id, body, request, app_config=Depends(get_config)`。

以 `await build_thread_checkpoint_state_accessor(request, thread_id=thread_id)` 计算 `accessor, config`。关键判定为 `await get_run_manager(request).has_inflight(thread_id)`、`not source_checkpoint_id`、`extract_unanswered_human_input_request(messages_list) is not None`。

进入 `reserve_checkpoint_write(request, thread_id, user_id=get_effective_user_id())` 边界，退出由对应 context manager 处理。

等待 `reject_new_run_when_approval_pending(request, thread_id, accessor=accessor, read_config=config)` 完成；等待 `asyncio.to_thread(authorize_model_use, user, None, is_internal=_is_internal_caller(request, user), app_config=app_config)` 完成。

捕获 `HTTPException`、`Exception`、`_CHECKPOINT_MODE_ERRORS as exc`、`GoalSynthesisUnclear`、`GoalSynthesisError`、`GoalWriteConflict`、`ConflictError` 的边界见源码，显式抛出路径涉及 `HTTPException`、`当前异常`、`_checkpoint_mode_http_error`。分支返回 `ThreadGoalResponse(goal=goal)`，提前返回结束该路径。

源码依据：[synthesize_thread_goal_from_chat 实现](../backend/app/gateway/routers/threads.py#L1596-L1723)


### 5.40.15 辅助函数与局部契约

- **`clear_thread_goal`**（`thread_id, request`）：以 `get_checkpointer(request)` 计算 `checkpointer`。关键判定为 `batch_repo is not None`，相应动作只在对应分支执行。进入 `reserve_checkpoint_write(request, thread_id, user_id=get_effective_user_id())` 边界，退出由对应 context manager 处理。等待 `reject_new_run_when_approval_pending(request, thread_id, accessor=accessor, read_config=read_config)` 完成；等待 `write_thread_goal(checkpointer, thread_id, None, as_node="goal")` 完成。捕获 `_CHECKPOINT_MODE_ERRORS as exc`、`ConflictError`、`LookupError`、`Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `_checkpoint_mode_http_error`、`HTTPException`。分支返回 `ThreadGoalResponse(goal=None)`，提前返回结束该路径。

  源码依据：[clear_thread_goal 实现](../backend/app/gateway/routers/threads.py#L1726-L1751)


- **`_thread_compact_response`**（`result`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  ThreadCompactResponse( thread_id=result.thread_id, compacted=result.compacted,
     reason=result.reason, removed_message_count=result.removed_message_count,
     preserved_message_count=result.preserved_message_count, summary_updated=result.summary_updated,
     checkpoint_id=result.checkpoint_id, total_tokens=result.total_tokens,
     )
  ```


  源码依据：[_thread_compact_response 实现](../backend/app/gateway/routers/threads.py#L1752-L1764)


### 5.40.16 compact_thread

参数：`thread_id, body, request`。

以 `build_checkpoint_state_mutation_accessor( request, thread_id=thread_id, as_node="manual_compaction", )` 计算 `accessor, read_config`。进入 `reserve_checkpoint_write(request, thread_id, user_id=get_effective_user_id())` 边界，退出由对应 context manager 处理。等待 `reject_new_run_when_approval_pending(request, thread_id)` 完成。

捕获 `_CHECKPOINT_MODE_ERRORS as exc`、`ConflictError`、`ContextCompactionPendingWork as exc`、`ContextCompactionDisabled`、`ContextCompactionFailed`、`LookupError`、`HTTPException`、`Exception` 的边界见源码，显式抛出路径涉及 `_checkpoint_mode_http_error`、`HTTPException`、`当前异常`。分支返回 `_thread_compact_response(result)`，提前返回结束该路径。

源码依据：[compact_thread 实现](../backend/app/gateway/routers/threads.py#L1767-L1815)


### 5.40.17 辅助函数与局部契约

- **`_pending_interrupt_tasks`**（`tasks_raw, pending_interrupts`）：以 `[]` 计算 `tasks: list[dict[str, Any]]`。关键判定为 `task_id not in matched_task_ids`，相应动作只在对应分支执行。执行 `matched_task_ids.add(task_id)`；执行 「表达式摘要 1」。分支返回 `tasks`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  tasks.append( { "id": task_id, "name": getattr(task, "name", ""),
       "interrupts": [serialize_lc_object(interrupt) for interrupt in pending_interrupts.get(task_id,
           [])], } )
  ```


  源码依据：[_pending_interrupt_tasks 实现](../backend/app/gateway/routers/threads.py#L1816-L1842)


### 5.40.18 get_thread_state

参数：`thread_id, request`。

以 `await build_thread_checkpoint_state_accessor(request, thread_id=thread_id)` 计算 `accessor, config`。关键判定为 `not checkpoint_id`、`isinstance(messages, list) and messages`。以 `await stamp_messages_with_seq(_optional_run_event_store(request), thread_id, messages)` 计算 `values["messages"]`。

捕获 `_CHECKPOINT_MODE_ERRORS as exc`、`Exception` 的边界见源码，显式抛出路径涉及 `_checkpoint_mode_http_error`、`HTTPException`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
ThreadStateResponse( values=values, next=list(snapshot.next or ()),
   metadata=metadata, checkpoint={"id": checkpoint_id, "ts": coerce_iso(created_at)},
   checkpoint_id=checkpoint_id, parent_checkpoint_id=parent_checkpoint_id,
   created_at=coerce_iso(created_at), tasks=tasks, )
```


源码依据：[get_thread_state 实现](../backend/app/gateway/routers/threads.py#L1845-L1890)


### 5.40.19 update_thread_state

参数：`thread_id, body, request`。

核对写权限与当前执行冲突，按实际 graph 的可写/reducer channel 处理请求与整体覆盖，调用 mode-matched accessor；不让外部直接写入服务端保护元数据。

源码依据：[update_thread_state 实现](../backend/app/gateway/routers/threads.py#L1893-L2017)


### 5.40.20 辅助函数与局部契约

- **`_checkpoint_run_durations`**（`metadata`）：以 `metadata.get("run_durations") if isinstance(metadata, dict) else None` 计算 `raw_durations`；在 `条件 not isinstance(raw_durations, dict)` 分支中，返回 `{}`；返回 `{run_id: duration_seconds for run_id, duration_seconds in raw_durations.items() if valid_duration_entry(run_id, duration_seconds)}`。

  源码依据：[_checkpoint_run_durations 实现](../backend/app/gateway/routers/threads.py#L2018-L2024)


- **`_checkpoint_run_message_ids`**（`metadata`）：以 `metadata.get(RUN_MESSAGE_IDS_METADATA_KEY) if isinstance(metadata, dict) else None` 计算 `raw_message_run_ids`；在 `条件 not isinstance(raw_message_run_ids, dict)` 分支中，返回 `{}`；返回 `{message_id: run_id for message_id, run_id in raw_message_run_ids.items() if valid_run_message_id_entry(message_id, run_id)}`。

  源码依据：[_checkpoint_run_message_ids 实现](../backend/app/gateway/routers/threads.py#L2025-L2031)


- **`_load_run_durations`**（`run_manager, thread_id, user_id, run_ids`）：以 `await run_manager.list_by_thread( thread_id, user_id=user_id, limit=max(100, len(run_ids)), )` 计算 `runs`。关键判定为 `not run_ids`、`run is not None`，相应动作只在对应分支执行。执行 `runs.append(run)`；执行 `known_run_ids.add(run_id)`。分支返回 `{}`、`{run_id: duration for run_id, duration in computed_durations.items() if run_id in run_ids}`，提前返回结束该路径。

  源码依据：[_load_run_durations 实现](../backend/app/gateway/routers/threads.py#L2032-L2060)


### 5.40.21 _persist_run_history_metadata_background

参数：`request, checkpointer, thread_id, user_id, duration_run_ids, message_run_ids, audited_message_ids`。

以 `dict(message_run_ids)` 计算 `authoritative_message_run_ids`。关键判定为 `audited_message_ids`、`valid_run_message_id_entry(message_id, exact_run_id)`、`authoritative_message_run_ids.get(message_id) != exact_run_id`。进入 `reserve_checkpoint_write(request, thread_id, user_id=user_id)` 边界，退出由对应 context manager 处理。

以 `exact_run_id` 计算 `authoritative_message_run_ids[message_id]`。执行 `authoritative_duration_run_ids.add(exact_run_id)`。等待 「表达式摘要 1」 完成。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
persist_run_history_metadata( checkpointer=checkpointer, thread_id=thread_id,
   durations=authoritative_durations, message_run_ids=authoritative_message_run_ids,
   )
```


捕获 `ConflictError`、`Exception` 的边界见源码，

源码依据：[_persist_run_history_metadata_background 实现](../backend/app/gateway/routers/threads.py#L2061-L2114)


### 5.40.22 get_thread_history

参数：`thread_id, body, request, background_tasks`。

以 `get_checkpointer(request)` 计算 `checkpointer`；以 `[]` 计算 `entries: list[HistoryEntry]`。关键判定为 `title := materialized_values.get("title")`、`thread_data := materialized_values.get("thread_data")`、`is_latest_checkpoint`。

以 `title` 计算 `values["title"]`；以 `thread_data` 计算 `values["thread_data"]`；其他写入目标为 `values["goal"]`、`values[GOAL_OUTCOME_CHANNEL]`、`msg["run_id"]`、`values["messages"]`、`user_meta["step"]`。执行 `legacy_ai_message_ids.add(message_id)`；执行 `turn_run_ids.add(run_id)`。

捕获 `_CHECKPOINT_MODE_ERRORS as exc`、`Exception` 的边界见源码，显式抛出路径涉及 `_checkpoint_mode_http_error`、`当前异常`、`HTTPException`。分支返回 `entries`，提前返回结束该路径。

源码依据：[get_thread_history 实现](../backend/app/gateway/routers/threads.py#L2117-L2348)


## 5.41 运行 HTTP 接口与 SSE

源码位置：`backend/app/gateway/routers/thread_runs.py`。

run HTTP 控制面及 regenerate/edit/feed/artifact 接口。入口封装服务层，不直接调用模型；局部 helper 还负责历史目标选择、可见性、游标和状态映射，避免把隐藏控制消息当用户可编辑回合。 导入时创建 APIRouter 并登记端点，_artifact_archive_slots=asyncio.Semaphore(4) 约束本进程归档并发；这是另一个局部资源边界，不能外推成全实例配额。

源码依据：[thread_runs.py 实现](../backend/app/gateway/routers/thread_runs.py#L86-L145)

### 5.41.1 辅助函数与局部契约

- **`get_tool_approvals`**（`thread_id, request`）：返回 `await list_approvals(thread_id, request)`。

  源码依据：[get_tool_approvals 实现](../backend/app/gateway/routers/thread_runs.py#L92-L95)


- **`submit_tool_approval`**（`thread_id, run_id, request_id, body, request`）：返回 `await answer_approval(thread_id, run_id, request_id, body, request)`。

  源码依据：[submit_tool_approval 实现](../backend/app/gateway/routers/thread_runs.py#L98-L101)


- **`revoke_tool_approval_grants`**（`thread_id, request`）：返回 `await revoke_approval_grants(thread_id, request)`。

  源码依据：[revoke_tool_approval_grants 实现](../backend/app/gateway/routers/thread_runs.py#L104-L107)


- **`create_run_input`**（`thread_id, run_id, body, request`）：返回 `await submit_run_input(thread_id, run_id, body, request)`。

  源码依据：[create_run_input 实现](../backend/app/gateway/routers/thread_runs.py#L110-L113)


- **`get_thread_inputs`**（`thread_id, request`）：返回 `await list_thread_inputs(thread_id, request)`。

  源码依据：[get_thread_inputs 实现](../backend/app/gateway/routers/thread_runs.py#L116-L119)


- **`remove_run_input`**（`thread_id, run_id, input_id, request`）：返回 `await change_run_input(thread_id, run_id, input_id, request)`。

  源码依据：[remove_run_input 实现](../backend/app/gateway/routers/thread_runs.py#L122-L125)


- **`retry_run_input`**（`thread_id, run_id, input_id, request`）：返回 `await change_run_input(thread_id, run_id, input_id, request, retry=True)`。

  源码依据：[retry_run_input 实现](../backend/app/gateway/routers/thread_runs.py#L128-L131)


- **`_scope_http_run_idempotency_key`**（`request, thread_id, key`）：以 `key.strip()` 计算 `key`。关键判定为 `not isinstance(key, str)`、`not key`、`owner_id is None`，相应动作只在对应分支执行。显式抛出路径涉及 `HTTPException`。分支返回 `None`、`"internal-run:" + hashlib.sha256(f"{owner_id}\0{key}".encode()).hexdigest()`、`f"http-run:{digest}"`，提前返回结束该路径。

  源码依据：[_scope_http_run_idempotency_key 实现](../backend/app/gateway/routers/thread_runs.py#L155-L179)


- **`_refresh_store_backed_run`**（`run_mgr, record`）：在 `条件 not getattr(record, "store_only", False)` 分支中，返回 `record`；以 `getattr(run_mgr, "_store", None)` 计算 `store`；以 `getattr(store, "get", None)` 计算 `get`；在 `条件 get is None` 分支中，返回 `record`；在 `try` 分支中，以 `get(record.run_id)` 计算 `row`；在 `try` → `条件 hasattr(row, "__await__")` 分支中，以 `await row` 计算 `row`；在 `捕获 Exception` 分支中，返回 `record`；在 `条件 not isinstance(row, dict)` 分支中，返回 `record`；以 `row.get("status")` 计算 `raw_status`；在 `条件 raw_status` → `捕获 ValueError` 分支中，该默认实现/分支不执行额外动作；返回 `record`。

  源码依据：[_refresh_store_backed_run 实现](../backend/app/gateway/routers/thread_runs.py#L180-L209)


- **`_is_duration_only_checkpoint`**（`checkpoint_tuple, history_index, versions=None, parent_versions=None`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  is_duration_only_checkpoint(checkpoint_tuple, parent=parent_from_history_index(checkpoint_tuple,
       history_index), versions=versions, parent_versions=parent_versions)
  ```


  源码依据：[_is_duration_only_checkpoint 实现](../backend/app/gateway/routers/thread_runs.py#L210-L213)


- **`compute_run_durations`**（`runs`）：以 `{}` 计算 `durations: dict[str, int]`；在 `循环 for r in runs` → `条件 r.created_at and r.updated_at` → `try` 分支中，以 `datetime.fromisoformat(r.created_at.replace("Z", "+00:00"))` 计算 `created`；在 `循环 for r in runs` → `条件 r.created_at and r.updated_at` → `try` 分支中，以 `datetime.fromisoformat(r.updated_at.replace("Z", "+00:00"))` 计算 `updated`；在 `循环 for r in runs` → `条件 r.created_at and r.updated_at` → `try` 分支中，以 `int((updated - created).total_seconds())` 计算 `durations[r.run_id]`；返回 `durations`。

  源码依据：[compute_run_durations 实现](../backend/app/gateway/routers/thread_runs.py#L214-L231)


### 5.41.2 stamp_turn_duration_on_last_ai

参数：`messages, run_durations, thread_id=None`。

以 `set()` 计算 `stamped: set[str]`。关键判定为 `not rid or rid in stamped or rid not in run_durations`、`metadata is None`、`not isinstance(metadata, Mapping)`。以 `additional_kwargs` 计算 `payload["additional_kwargs"]`；以 `run_durations[rid]` 计算 `additional_kwargs["turn_duration"]`。执行 `stamped.add(rid)`。

以 `str(metadata.get("caller", "")).startswith("middleware:")` 计算 `is_middleware`。

源码依据：[stamp_turn_duration_on_last_ai 实现](../backend/app/gateway/routers/thread_runs.py#L232-L298)


### 5.41.3 辅助函数与局部契约

- **`require_cancel_permission_when_action`**（`request, action`）：执行 `require_cancel_permission_if(request, action is not None)`。

  源码依据：[require_cancel_permission_when_action 实现](../backend/app/gateway/routers/thread_runs.py#L394-L407)


- **`_cancel_conflict_detail`**（`run_id, record`）：在 `条件 record.status in (RunStatus.pending, RunStatus.running)` 分支中，返回 `f"Run {run_id} is not active on this worker and cannot be cancelled"`；返回 `f"Run {run_id} is not cancellable (status: {record.status.value})"`。

  源码依据：[_cancel_conflict_detail 实现](../backend/app/gateway/routers/thread_runs.py#L408-L413)


- **`_compute_retry_after`**（`lease_expires_at, grace_seconds`）：在 `条件 lease_expires_at is None` 分支中，返回 `None`；在 `try` 分支中，以 `datetime.fromisoformat(lease_expires_at)` 计算 `dt`；在 `try` → `条件 dt.tzinfo is None` 分支中，以 `dt.replace(tzinfo=UTC)` 计算 `dt`；在 `捕获 (ValueError, TypeError)` 分支中，返回 `None`；以 `(dt - datetime.now(UTC)).total_seconds() + grace_seconds` 计算 `remaining`；返回 `max(1, int(remaining))`。

  源码依据：[_compute_retry_after 实现](../backend/app/gateway/routers/thread_runs.py#L414-L436)


- **`_raise_lease_valid_elsewhere`**（`run_id, run_mgr, # RunManager (avoid import for testability) record: RunRecord`）：以 `await run_mgr.get(run_id)` 计算 `fresh`；在 `条件 fresh is not None` 分支中，以 `fresh` 计算 `record`；以 `_compute_retry_after(record.lease_expires_at, run_mgr.grace_seconds)` 计算 `retry_after`；以 `{}` 计算 `headers: dict[str, str]`；在 `条件 retry_after is not None` 分支中，以 `str(retry_after)` 计算 `headers["Retry-After"]`；抛出 `HTTPException( status_code=409, detail=f"Run {run_id} is active on another worker; retry after lease expiry.", headers=headers, )`。

  源码依据：[_raise_lease_valid_elsewhere 实现](../backend/app/gateway/routers/thread_runs.py#L437-L461)


- **`_record_to_response`**（`record`）：以 `dict(record.kwargs or {})` 计算 `kwargs`。关键判定为 `isinstance(legacy_idempotency_request, dict) and legacy_idempotency_request.get("kind") == "resume"`、`"config" in kwargs`，相应动作只在对应分支执行。以 `None` 计算 `kwargs["input"]`；以 `redact_config_secrets(kwargs["config"])` 计算 `kwargs["config"]`。分支返回 「表达式摘要 1」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  RunResponse( run_id=record.run_id, thread_id=record.thread_id, assistant_id=record.assistant_id,
     status=record.status.value, metadata=redact_metadata_secrets(record.metadata),
     kwargs=kwargs, multitask_strategy=record.multitask_strategy, created_at=record.created_at,
     updated_at=record.updated_at, total_input_tokens=record.total_input_tokens,
     total_output_tokens=record.total_output_tokens, total_tokens=record.total_tokens,
     llm_call_count=record.llm_call_count, lead_agent_tokens=record.lead_agent_tokens,
     subagent_tokens=record.subagent_tokens, middleware_tokens=record.middleware_tokens,
     message_count=record.message_count, stop_reason=record.stop_reason,
     )
  ```


  源码依据：[_record_to_response 实现](../backend/app/gateway/routers/thread_runs.py#L462-L494)


- **`_message_id`**（`message`）：以 `getattr(message, "id", None)` 计算 `value`；在 `条件 value is None and isinstance(message, dict)` 分支中，以 `message.get("id")` 计算 `value`；返回 `str(value) if value else None`。

  源码依据：[_message_id 实现](../backend/app/gateway/routers/thread_runs.py#L495-L501)


- **`_message_type`**（`message`）：以 `getattr(message, "type", None)` 计算 `value`；在 `条件 value is None and isinstance(message, dict)` 分支中，以 `message.get("type") or message.get("role")` 计算 `value`；在 `条件 value == "assistant"` 分支中，返回 `"ai"`；返回 `str(value) if value else None`。

  源码依据：[_message_type 实现](../backend/app/gateway/routers/thread_runs.py#L502-L510)


- **`_message_name`**（`message`）：以 `getattr(message, "name", None)` 计算 `value`；在 `条件 value is None and isinstance(message, dict)` 分支中，以 `message.get("name")` 计算 `value`；返回 `str(value) if value else None`。

  源码依据：[_message_name 实现](../backend/app/gateway/routers/thread_runs.py#L511-L517)


- **`_message_content`**（`message`）：在 `条件 isinstance(message, dict)` 分支中，返回 `message.get("content")`；返回 `getattr(message, "content", None)`。

  源码依据：[_message_content 实现](../backend/app/gateway/routers/thread_runs.py#L518-L523)


- **`_message_text`**（`message`）：返回 `message_to_text(message)`。

  源码依据：[_message_text 实现](../backend/app/gateway/routers/thread_runs.py#L524-L527)


- **`_message_additional_kwargs`**（`message`）：以 `getattr(message, "additional_kwargs", None)` 计算 `value`；在 `条件 value is None and isinstance(message, dict)` 分支中，以 `message.get("additional_kwargs")` 计算 `value`；返回 `dict(value or {}) if isinstance(value, dict) else {}`。

  源码依据：[_message_additional_kwargs 实现](../backend/app/gateway/routers/thread_runs.py#L528-L534)


- **`_message_tool_calls`**（`message`）：以 `getattr(message, "tool_calls", None)` 计算 `value`；在 `条件 value is None and isinstance(message, dict)` 分支中，以 `message.get("tool_calls")` 计算 `value`；在 `条件 value is None` 分支中，以 `_message_additional_kwargs(message).get("tool_calls")` 计算 `value`；返回 `list(value) if isinstance(value, list) else []`。

  源码依据：[_message_tool_calls 实现](../backend/app/gateway/routers/thread_runs.py#L535-L543)


- **`_is_hidden_or_control_message`**（`message`）：以 `_message_type(message)` 计算 `message_type`；以 `_message_additional_kwargs(message)` 计算 `additional_kwargs`；返回 `message_type == "remove" or _message_name(message) == "summary" or additional_kwargs.get("hide_from_ui") is True`。

  源码依据：[_is_hidden_or_control_message 实现](../backend/app/gateway/routers/thread_runs.py#L544-L549)


- **`_is_visible_human_message`**（`message`）：返回 `_message_type(message) == "human" and not _is_hidden_or_control_message(message)`。

  源码依据：[_is_visible_human_message 实现](../backend/app/gateway/routers/thread_runs.py#L550-L553)


- **`_is_regenerate_human_message`**（`message`）：在 `条件 _message_type(message) != "human" or _message_name(message) == "summary"` 分支中，返回 `False`；以 `_message_additional_kwargs(message)` 计算 `additional_kwargs`；在 `条件 additional_kwargs.get("hide_from_ui") is True` 分支中，返回 `read_human_input_response(additional_kwargs) is not None`；返回 `True`。

  源码依据：[_is_regenerate_human_message 实现](../backend/app/gateway/routers/thread_runs.py#L554-L569)


- **`_is_visible_ai_message`**（`message`）：返回 `_message_type(message) == "ai" and not _is_hidden_or_control_message(message)`。

  源码依据：[_is_visible_ai_message 实现](../backend/app/gateway/routers/thread_runs.py#L570-L573)


- **`_checkpoint_messages`**（`snapshot`）：返回 `checkpoint_messages(snapshot)`。

  源码依据：[_checkpoint_messages 实现](../backend/app/gateway/routers/thread_runs.py#L574-L577)


- **`_checkpoint_values`**（`snapshot`）：以 `getattr(snapshot, "values", None)` 计算 `values`；返回 `dict(values) if isinstance(values, dict) else {}`。

  源码依据：[_checkpoint_values 实现](../backend/app/gateway/routers/thread_runs.py#L578-L582)


- **`_checkpoint_configurable`**（`checkpoint_tuple`）：返回 `checkpoint_configurable(checkpoint_tuple)`。

  源码依据：[_checkpoint_configurable 实现](../backend/app/gateway/routers/thread_runs.py#L583-L586)


- **`_checkpoint_response`**（`checkpoint_tuple`）：以 `_checkpoint_configurable(checkpoint_tuple)` 计算 `configurable`；以 `configurable.get("checkpoint_id")` 计算 `checkpoint_id`；在 `条件 not checkpoint_id` 分支中，抛出 `HTTPException(status_code=409, detail="Checkpoint is missing checkpoint_id")`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  { "checkpoint_ns": str(configurable.get("checkpoint_ns") or ""), "checkpoint_id": str(checkpoint_id),
     "checkpoint_map": configurable.get("checkpoint_map"), }
  ```


  源码依据：[_checkpoint_response 实现](../backend/app/gateway/routers/thread_runs.py#L587-L598)


- **`_clean_human_message_for_regenerate`**（`message`）：以 `_message_additional_kwargs(message)` 计算 `additional_kwargs`；以 `{ "type": "human", "content": [{"type": "text", "text": content}], "additional_kwargs": additional_kwargs, }` 计算 `clean_message: dict[str, Any]`。关键判定为 `not is_hidden_human_input_response`、`message_id`、`name`，相应动作只在对应分支执行。以 `message_id` 计算 `clean_message["id"]`；以 `name` 计算 `clean_message["name"]`。执行 `additional_kwargs.pop(ORIGINAL_USER_CONTENT_KEY, None)`；执行 `additional_kwargs.pop("hide_from_ui", None)`。分支返回 `clean_message`，提前返回结束该路径。

  源码依据：[_clean_human_message_for_regenerate 实现](../backend/app/gateway/routers/thread_runs.py#L599-L628)


- **`_clean_human_message_for_edit`**（`message, replacement_id, replacement_text`）：以 `_message_additional_kwargs(message)` 计算 `source_kwargs`；以 `{}` 计算 `additional_kwargs: dict[str, Any]`；在 `循环 for key in ("files", "referenced_message_contexts")` → `条件 key in source_kwargs` 分支中，以 `deepcopy(source_kwargs[key])` 计算 `additional_kwargs[key]`；以 `{ "type": "human", "id": replacement_id, "content": [{"type": "text", "text": replacement_text}], "additional_kwargs": additional_kwargs, }` 计算 `clean_message: dict[str, Any]`；以 `_message_name(message)` 计算 `name`；在 `条件 name` 分支中，以 `name` 计算 `clean_message["name"]`；返回 `clean_message`。

  源码依据：[_clean_human_message_for_edit 实现](../backend/app/gateway/routers/thread_runs.py#L629-L647)


- **`_is_terminal_assistant_text_message`**（`message`）：返回 `_is_visible_ai_message(message) and bool(_message_text(message).strip()) and not _message_tool_calls(message)`。

  源码依据：[_is_terminal_assistant_text_message 实现](../backend/app/gateway/routers/thread_runs.py#L648-L651)


- **`_has_title`**（`values`）：以 `values.get("title")` 计算 `title`；返回 `isinstance(title, str) and bool(title)`。

  源码依据：[_has_title 实现](../backend/app/gateway/routers/thread_runs.py#L652-L656)


- **`_has_active_goal`**（`snapshot`）：返回 `is_active_goal(_checkpoint_values(snapshot).get("goal"))`。

  源码依据：[_has_active_goal 实现](../backend/app/gateway/routers/thread_runs.py#L657-L660)


- **`_latest_editable_turn`**（`messages, human_message_id`）：以 `next((index for index in range(len(messages) - 1, -1, -1) if _is_visible_human_message(messages[index])), None)` 计算 `latest_human_index`。关键判定为 `latest_human_index is None or _message_id(messages[latest_human_index]) != human_message_id`、`_is_visible_human_message(message)`、`_is_visible_ai_message(message)`，相应动作只在对应分支执行。显式抛出路径涉及 `HTTPException`。分支返回 `latest_human_index, source_human, last_ai_index, messages[last_ai_index], source_message_ids`，提前返回结束该路径。

  源码依据：[_latest_editable_turn 实现](../backend/app/gateway/routers/thread_runs.py#L661-L680)


- **`_event_message_id`**（`row`）：以 `row.get("content")` 计算 `content`；在 `条件 isinstance(content, BaseMessage)` 分支中，返回 `_message_id(content)`；在 `条件 isinstance(content, dict)` 分支中，返回 `_message_id(content)`；返回 `None`。

  源码依据：[_event_message_id 实现](../backend/app/gateway/routers/thread_runs.py#L681-L689)


- **`_run_last_ai_matches_message`**（`record, message`）：以 `(record.last_ai_message or "").strip()` 计算 `last_ai_message`；在 `条件 not last_ai_message` 分支中，返回 `False`；以 `_message_text(message)` 计算 `target_text`；在 `条件 not target_text` 分支中，返回 `False`；返回 `any(last_ai_message == text[: len(last_ai_message)] for text in (target_text.strip(), strip_leading_think_blocks(target_text)))`。

  源码依据：[_run_last_ai_matches_message 实现](../backend/app/gateway/routers/thread_runs.py#L690-L700)


### 5.41.4 _find_target_run_id

参数：`thread_id, message_id, target_message, source_human, request`。

以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`；以 `row.get("run_id")` 计算 `run_id`；以 `_message_additional_kwargs(source_human).get("run_id")` 计算 `source_run_id`。

关键判定为 `row.get("event_type") not in {"ai_message", "llm.ai.response"}`、`_event_message_id(row) == message_id`、`isinstance(run_id, str) and run_id`。显式抛出路径涉及 `HTTPException`。分支返回 `run_id`、`source_run_id`、`fallback_record.run_id`，提前返回结束该路径。

源码依据：[_find_target_run_id 实现](../backend/app/gateway/routers/thread_runs.py#L701-L740)


### 5.41.5 _find_base_checkpoint_before_human

参数：`thread_id, human_message_id, request, head_checkpoint=None`。

以 `await build_thread_checkpoint_state_accessor(request, thread_id=thread_id)` 计算 `accessor, base_config`。关键判定为 `head_checkpoint is not None`、`not _is_duration_only_checkpoint(item, history_index, versions=versions, parent_versions=parent_versions)`、`target_found`。执行 `checkpoints.append(item)`。

捕获 `CheckpointParentMissingError`、`CheckpointLineageError as exc`、`Exception as exc` 的边界见源码，显式抛出路径涉及 `HTTPException`。分支返回 `await find_checkpoint_before_message( accessor, head_checkpoint, human_message_id, max_depth=REGENERATE_HISTORY_RAW_SCAN_LIMIT, )`、`previous_checkpoint`，提前返回结束该路径。

源码依据：[_find_base_checkpoint_before_human 实现](../backend/app/gateway/routers/thread_runs.py#L741-L807)


### 5.41.6 辅助函数与局部契约

- **`_run_status_value`**（`record`）：以 `getattr(record, "status", None)` 计算 `status`；在 `条件 isinstance(status, RunStatus)` 分支中，返回 `status.value`；返回 `str(status) if status is not None else None`。

  源码依据：[_run_status_value 实现](../backend/app/gateway/routers/thread_runs.py#L808-L814)


- **`_require_successful_source_run`**（`thread_id, run_id, request`）：以 `get_run_manager(request)` 计算 `run_mgr`；以 `next((candidate for candidate in records if getattr(candidate, "run_id", None) == run_id), None)` 计算 `record`。关键判定为 `record is None`、`record is None`、`isinstance(record_thread_id, str) and record_thread_id and record_thread_id != thread_id`，相应动作只在对应分支执行。等待 `_reject_multi_input_replay(run_id, request, user_id=user_id)` 完成。显式抛出路径涉及 `HTTPException`。分支返回 `record`，提前返回结束该路径。

  源码依据：[_require_successful_source_run 实现](../backend/app/gateway/routers/thread_runs.py#L815-L834)


- **`_reject_multi_input_replay`**（`run_id, request, user_id=None`）：以 `getattr(get_run_manager(request), "input_store", None)` 计算 `store`；在 `条件 getattr(store, "supports_run_inputs", False) is not True` 分支中，返回 None，提前结束当前路径；以 `await store.get(run_id, user_id=user_id)` 计算 `row`；在 「表达式摘要 1」 分支中，抛出 `HTTPException(409, "This Run was adjusted while working; send a new message instead of replaying part of it")`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  条件 row is not None and any(entry.get("mode") == "steer" and entry.get("status") != "cancelled" for entry in (row.get("run_inputs")
      or {}).get("items", []))
  ```


  源码依据：[_reject_multi_input_replay 实现](../backend/app/gateway/routers/thread_runs.py#L835-L843)


- **`_find_interrupted_target_run_id`**（`thread_id, source_human, request`）：以 `_message_additional_kwargs(source_human).get("run_id")` 计算 `source_run_id`。关键判定为 `not isinstance(source_run_id, str) or not source_run_id`、`record is None`、`record is None`，相应动作只在对应分支执行。分支返回 `None`、`source_run_id`，提前返回结束该路径。

  源码依据：[_find_interrupted_target_run_id 实现](../backend/app/gateway/routers/thread_runs.py#L844-L870)


### 5.41.7 _prepare_regenerate_payload

参数：`thread_id, message_id, request`。

从指定可见成功源 run 及用户回合选取前置 checkpoint，清理旧 AI/控制字段并生成受控重生成输入；有多输入/不匹配来源时拒绝而非猜测重放范围。

源码依据：[_prepare_regenerate_payload 实现](../backend/app/gateway/routers/thread_runs.py#L871-L953)


### 5.41.8 _prepare_edit_regenerate_payload

参数：`thread_id, human_message_id, replacement_text, request`。

以 `replacement_text.strip()` 计算 `normalized_text`。关键判定为 `not normalized_text`、`not latest_checkpoint_id`、`_has_active_goal(latest_checkpoint)`。以 `latest_title` 计算 `edit_input["title"]`。捕获 `Exception as exc` 的边界见源码，显式抛出路径涉及 `HTTPException`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
EditRegeneratePrepareResponse( input=edit_input, checkpoint=checkpoint,
   metadata=metadata, target_run_id=target_run_id, replacement_human_message_id=replacement_human_message_id,
   source_message_ids=source_message_ids, )
```


源码依据：[_prepare_edit_regenerate_payload 实现](../backend/app/gateway/routers/thread_runs.py#L954-L1048)


### 5.41.9 辅助函数与局部契约

- **`prepare_regenerate_run`**（`thread_id, body, request`）：返回 `await _prepare_regenerate_payload(thread_id, body.message_id, request)`。

  源码依据：[prepare_regenerate_run 实现](../backend/app/gateway/routers/thread_runs.py#L1051-L1059)


- **`prepare_edit_regenerate_run`**（`thread_id, body, request`）：返回 `await _prepare_edit_regenerate_payload(thread_id, body.human_message_id, body.replacement_text, request)`。

  源码依据：[prepare_edit_regenerate_run 实现](../backend/app/gateway/routers/thread_runs.py#L1062-L1070)


- **`create_run`**（`thread_id, body, request, idempotency_key=None`）：以 `await start_run( body, thread_id, request, idempotency_key=_scope_http_run_idempotency_key(request, thread_id, idempotency_key), )` 计算 `record`；返回 `_record_to_response(record)`。

  源码依据：[create_run 实现](../backend/app/gateway/routers/thread_runs.py#L1073-L1088)


### 5.41.10 stream_run

参数：`thread_id, body, request, idempotency_key=None`。

这是创建运行并订阅结果的 HTTP 入口。它先取得 bridge，再调用共享 `start_run`；发生权限、审批或准入失败时，还没有一个可供客户端消费的正常 SSE 会话。

若重用记录仅属于远端 worker、当前桥不支持跨进程且运行仍活跃，返回服务不可用，不能把“找到了 RunStore 行”误作“能接到本地 token 流”。其余路径用 `sse_consumer` 返回 `text/event-stream`，为幂等重用时已消失的流启用显式 gap。

响应的 `Content-Location` 指向规范 run 资源，供 SDK 提取 run_id。HTTP 流建立只证明准入与订阅开始，END、错误帧、gap 和断连各有独立含义；实际消费与取消条件在 `sse_consumer`。

源码依据：[共享启动、桥边界与 SSE 响应](../backend/app/gateway/routers/thread_runs.py#L1091-L1138)

### 5.41.11 wait_run

参数：`thread_id, body, request, idempotency_key=None`。

以 `get_stream_bridge(request)` 计算 `bridge`。关键判定为 `getattr(record, "store_only", False) and not getattr(bridge, "supports_cross_process", False)`、`record.task is not None or getattr(record, "store_only", False)`、`completed`。捕获 `Exception` 的边界见源码，

分支返回 `serialize_wait_run_status(record)`、`serialize_channel_values_for_api(snapshot.values)`，提前返回结束该路径。

源码依据：[wait_run 实现](../backend/app/gateway/routers/thread_runs.py#L1141-L1203)


### 5.41.12 辅助函数与局部契约

- **`_parse_run_page_created_at`**（`value`）：在 `try` 分支中，以 `normalize_run_created_at_iso(value)` 计算 `normalized`；在 `try` 分支中，执行 `datetime.fromisoformat(normalized)`；在 `捕获 ValueError` 分支中，抛出 `HTTPException(status_code=422, detail="before_created_at must be an ISO-8601 timestamp") from None`；返回 `normalized`。

  源码依据：[_parse_run_page_created_at 实现](../backend/app/gateway/routers/thread_runs.py#L1204-L1212)


- **`_thread_ownership_established`**（`request, thread_id`）：以 `getattr(request.app.state, "thread_store", None)` 计算 `thread_store`；在 `条件 thread_store is None` 分支中，返回 `False`；以 `await thread_store.get(thread_id, user_id=None)` 计算 `meta`；以 `meta.get("user_id") if isinstance(meta, dict) else getattr(meta, "user_id", None)` 计算 `meta_owner`；返回 `meta is not None and bool(meta_owner)`。

  源码依据：[_thread_ownership_established 实现](../backend/app/gateway/routers/thread_runs.py#L1213-L1227)


### 5.41.13 _run_scope_user_id

参数：`request, thread_id`。

以 `getattr(request, "state", None)` 计算 `state`；以 `getattr(state, "user", None)` 计算 `user`；在 `条件 getattr(user, "system_role", None) != INTERNAL_SYSTEM_ROLE` 分支中，返回 `await get_current_user(request)`；在 `条件 await _thread_ownership_established(request, thread_id)` 分支中，返回 `None`；以 `get_trusted_internal_owner_user_id(request)` 计算 `owner`；在 `条件 owner is not None` 分支中，返回 `owner`；返回 `await get_current_user(request)`。

源码依据：[_run_scope_user_id 实现](../backend/app/gateway/routers/thread_runs.py#L1228-L1271)


### 5.41.14 辅助函数与局部契约

- **`_require_run_visible_to_scope`**（`run_id, thread_id, request`）：以 `getattr(request, "state", None)` 计算 `state`；以 `getattr(state, "user", None)` 计算 `user`；在 `条件 getattr(user, "system_role", None) != INTERNAL_SYSTEM_ROLE` 分支中，返回 None，提前结束当前路径；在 `条件 await _thread_ownership_established(request, thread_id)` 分支中，返回 None，提前结束当前路径；以 `get_trusted_internal_owner_user_id(request) or "default"` 计算 `scope`；以 `await get_run_manager(request).get(run_id)` 计算 `record`；在 `条件 record is None` 分支中，返回 None，提前结束当前路径；以 `getattr(record, "user_id", None) or "default"` 计算 `record_owner`；在 `条件 record.thread_id != thread_id or record_owner != scope` 分支中，抛出 `HTTPException(status_code=404, detail=f"Run {run_id} not found")`。

  源码依据：[_require_run_visible_to_scope 实现](../backend/app/gateway/routers/thread_runs.py#L1272-L1300)


- **`list_runs`**（`thread_id, request`）：以 `get_run_manager(request)` 计算 `run_mgr`；以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`；以 `await run_mgr.list_by_thread(thread_id, user_id=user_id)` 计算 `records`；返回 `[_record_to_response(r) for r in records]`。

  源码依据：[list_runs 实现](../backend/app/gateway/routers/thread_runs.py#L1303-L1310)


### 5.41.15 list_runs_page

**参数摘要**

```text
    thread_id,
    request,
    limit=Query(default=50, ge=1, le=200),
    before_created_at=Query(default=None),
    before_run_id=Query(default=None, min_length=1)
```

以 `_parse_run_page_created_at(before_created_at)` 计算 `before_created_at`。关键判定为 `(before_created_at is None) != (before_run_id is None)`、`before_created_at is not None`。显式抛出路径涉及 `HTTPException`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
ThreadRunsPageResponse( data=[_record_to_response(record) for record in page],
   has_more=has_more, next_before_created_at=format_run_cursor_created_at(last.created_at) if last else None,
   next_before_run_id=last.run_id if last else None, )
```


源码依据：[list_runs_page 实现](../backend/app/gateway/routers/thread_runs.py#L1313-L1352)


### 5.41.16 辅助函数与局部契约

- **`get_run`**（`thread_id, run_id, request`）：以 `get_run_manager(request)` 计算 `run_mgr`；以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`；以 `await run_mgr.get(run_id, user_id=user_id)` 计算 `record`；在 `条件 record is None or record.thread_id != thread_id` 分支中，抛出 `HTTPException(status_code=404, detail=f"Run {run_id} not found")`；返回 `_record_to_response(record)`。

  源码依据：[get_run 实现](../backend/app/gateway/routers/thread_runs.py#L1355-L1364)


### 5.41.17 cancel_run

**参数摘要**

```text
    thread_id,
    run_id,
    request,
    wait=Query(default=False, description="Block until run completes after cancel"),
    action=Query(default="interrupt", description="Cancel action")
```

以 `get_run_manager(request)` 计算 `run_mgr`。关键判定为 `record is None or record.thread_id != thread_id`、`outcome in ( CancelOutcome.cancelled, CancelOutcome.requested, CancelOutcome.taken_over, )`、`wait and record.task is not None`。

等待 `_require_run_visible_to_scope(run_id, thread_id, request)` 完成；以 `await run_mgr.cancel(run_id, action=action)` 计算 `outcome`。捕获 `asyncio.CancelledError` 的边界见源码，显式抛出路径涉及 `HTTPException`。分支返回 `Response(status_code=204)`、`Response(status_code=202)`，提前返回结束该路径。

源码依据：[cancel_run 实现](../backend/app/gateway/routers/thread_runs.py#L1367-L1425)


### 5.41.18 辅助函数与局部契约

- **`join_run`**（`thread_id, run_id, request`）：以 `get_run_manager(request)` 计算 `run_mgr`。关键判定为 `record is None or record.thread_id != thread_id`、`record.store_only and not bridge.supports_cross_process`，相应动作只在对应分支执行。等待 `_require_run_visible_to_scope(run_id, thread_id, request)` 完成。显式抛出路径涉及 `HTTPException`。分支返回 「表达式摘要 1」，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  StreamingResponse( # Joins are read-only observation: the creator's cancel-on-disconnect # policy must not fire because an observer closed their connection. sse_consumer(bridge, record, request, run_mgr, apply_on_disconnect=False), media_type="text/event-stream", headers=sse_response_headers(), )
  ```


  源码依据：[join_run 实现](../backend/app/gateway/routers/thread_runs.py#L1428-L1447)


- **`_reject_get_stream_action`**（`action=Query(default=None, include_in_schema=False)`）：在 `条件 action is not None` 分支中，抛出 `` HTTPException( status_code=405, detail="`action` is only supported on POST requests", headers={"Allow": "POST"}, ) ``。

  源码依据：[_reject_get_stream_action 实现](../backend/app/gateway/routers/thread_runs.py#L1448-L1462)


### 5.41.19 _stream_existing_run

参数：`thread_id, run_id, request, action, wait`。

以 `get_run_manager(request)` 计算 `run_mgr`。关键判定为 `record is None or record.thread_id != thread_id`、`record.store_only and action is None and not bridge.supports_cross_process`、`action is not None`。

等待 `_require_run_visible_to_scope(run_id, thread_id, request)` 完成；以 `await run_mgr.cancel(run_id, action=action)` 计算 `outcome`。捕获 `(asyncio.CancelledError, Exception)` 的边界见源码，显式抛出路径涉及 `HTTPException`。

分支返回 `Response(status_code=202)`、`Response(status_code=204)`、`Response(status_code=204 if completed else 202)`、「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
StreamingResponse( # Both methods of this handler are join surfaces: a POST carrying an # action cancels explicitly
  above (already gated by # require_cancel_permission_when_action),
   and an action-less join is # read-only observation — the creator's cancel-on-disconnect policy # must not fire because a joiner closed their connection. sse_consumer(bridge, record, request, run_mgr, apply_on_disconnect=False), media_type="text/event-stream", headers=sse_response_headers(), )
```


源码依据：[_stream_existing_run 实现](../backend/app/gateway/routers/thread_runs.py#L1463-L1535)


### 5.41.20 辅助函数与局部契约

- **`stream_existing_run`**（「表达式摘要 1」）：返回 `await _stream_existing_run(thread_id, run_id, request, action=action, wait=wait)`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  thread_id, run_id, request, action=Query(default=None, description="Cancel action"),
   wait=Query(default=0, description="Block until cancelled (1) or return immediately (0)")
  ```


  源码依据：[stream_existing_run 实现](../backend/app/gateway/routers/thread_runs.py#L1538-L1548)


- **`join_existing_run_stream`**（`thread_id, run_id, request`）：返回 `await _stream_existing_run(thread_id, run_id, request, action=None, wait=0)`。

  源码依据：[join_existing_run_stream 实现](../backend/app/gateway/routers/thread_runs.py#L1556-L1565)


### 5.41.21 list_thread_messages

**参数摘要**

```text
    thread_id,
    request,
    limit=Query(default=50, ge=1, le=200),
    before_seq=Query(default=None, ge=1),
    after_seq=Query(default=None, ge=1)
```

以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`。关键判定为 `msg.get("event_type") == "llm.ai.response"`、`last_ai_per_run`、`i in last_ai_indices`。

以 `i` 计算 `last_ai_per_run[msg["run_id"]]`；以 `( { "feedback_id": fb["feedback_id"], "rating": fb["rating"], "comment": fb.get("comment"), } if fb else None )` 计算 `msg["feedback"]`；其他写入目标为 `msg["feedback"]`。分支返回 `messages`，提前返回结束该路径。

源码依据：[list_thread_messages 实现](../backend/app/gateway/routers/thread_runs.py#L1568-L1637)


### 5.41.22 辅助函数与局部契约

- **`_scan_thread_message_page`**（`thread_id, limit, before_seq, request, user_id`）：返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await read_visible_message_page( event_store=get_run_event_store(request),
     run_manager=get_run_manager(request), thread_id=thread_id, limit=limit,
     before_seq=before_seq, user_id=user_id, batch_size=THREAD_MESSAGE_PAGE_SCAN_BATCH,
     )
  ```


  源码依据：[_scan_thread_message_page 实现](../backend/app/gateway/routers/thread_runs.py#L1638-L1657)


### 5.41.23 _enrich_thread_message_page

参数：`thread_id, rows, request, user_id`。

以 `deepcopy(rows)` 计算 `data`。关键判定为 `not data`、`feedback_run_ids`、`row.get("seq") == last_ai_seq_by_run.get(run_id)`。以 `None` 计算 `row["feedback"]`；以 `{ "feedback_id": feedback["feedback_id"], "rating": feedback["rating"], "comment": feedback.get("comment"), }` 计算 `row["feedback"]`。分支返回 `data`，提前返回结束该路径。

源码依据：[_enrich_thread_message_page 实现](../backend/app/gateway/routers/thread_runs.py#L1658-L1705)


### 5.41.24 辅助函数与局部契约

- **`list_thread_messages_page`**（`thread_id, request, limit=Query(default=50, ge=1, le=200), before_seq=Query(default=None, ge=1)`）：在 `条件 "after_seq" in request.query_params` 分支中，抛出 `HTTPException(status_code=422, detail="after_seq is not supported by this backward-only endpoint")`；以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`；以 `await _scan_thread_message_page( thread_id, limit=limit, before_seq=before_seq, request=request, user_id=user_id, )` 计算 `rows, has_more`；以 `await _enrich_thread_message_page(thread_id, rows, request=request, user_id=user_id)` 计算 `data`；返回 `ThreadMessagesPageResponse( data=data, has_more=has_more, next_before_seq=data[0]["seq"] if has_more else None, )`。

  源码依据：[list_thread_messages_page 实现](../backend/app/gateway/routers/thread_runs.py#L1708-L1733)


- **`list_run_messages`**（`thread_id, run_id, request, limit=Query(default=50, le=200, ge=1), before_seq=Query(default=None, ge=1), after_seq=Query(default=None, ge=1)`）：以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`。关键判定为 `data`、`record`、`durations`，相应动作只在对应分支执行。等待 `_require_run_visible_to_scope(run_id, thread_id, request)` 完成。分支返回 `{"data": data, "has_more": has_more}`，提前返回结束该路径。

  源码依据：[list_run_messages 实现](../backend/app/gateway/routers/thread_runs.py#L1736-L1771)


- **`_archive_response_chunks`**（`result`）：在 `try` → `循环 while chunk := result.file.read(1024 * 1024)` 分支中，交付 `chunk` 后暂停生成器，待消费方继续；在 `finally 收尾` 分支中，执行 `result.file.close()`。

  源码依据：[_archive_response_chunks 实现](../backend/app/gateway/routers/thread_runs.py#L1772-L1779)


- **`_build_archive_without_abandoning_worker`**（`outputs_dir, user_data_dir, presented_paths, extra_reserved_dir_names`）：以 「表达式摘要 1」 计算 `build_task`。关键判定为 `_artifact_archive_slots.locked()`、`not build_task.cancelled()`，相应动作只在对应分支执行。等待 `_artifact_archive_slots.acquire()` 完成；等待 `asyncio.shield(build_task)` 完成。捕获 `asyncio.CancelledError`、`Exception` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `ArtifactArchiveError`、`当前异常`。分支返回 `await asyncio.shield(build_task)`，提前返回结束该路径。finally 包含 执行 `_artifact_archive_slots.release()`。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  asyncio.create_task( asyncio.to_thread( build_artifact_archive, outputs_dir,
       presented_paths, user_data_dir=user_data_dir, extra_reserved_dir_names=extra_reserved_dir_names,
       ) )
  ```


  源码依据：[_build_archive_without_abandoning_worker 实现](../backend/app/gateway/routers/thread_runs.py#L1780-L1818)


- **`_presented_files_from_delivery`**（`events`）：以 `events[0].get("content")` 计算 `content`；以 `[]` 计算 `presented: list[str]`。关键判定为 `len(events) != 1`、`not isinstance(by_tool, dict)`、`not isinstance(paths, list)`，相应动作只在对应分支执行。执行 `presented.extend(paths)`。显式抛出路径涉及 `HTTPException`。分支返回 `presented`，提前返回结束该路径。

  源码依据：[_presented_files_from_delivery 实现](../backend/app/gateway/routers/thread_runs.py#L1819-L1836)


- **`_archive_presented_paths`**（`thread_id, run_id, request`）：以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`；以 `await get_run_store(request).get(run_id, user_id=user_id)` 计算 `run`；在 `条件 run is None or run.get("thread_id") != thread_id or run.get("operation_kind", "run") != "run"` 分支中，抛出 `HTTPException(status_code=404, detail=f"Run {run_id} not found")`；在 `条件 run.get("status") in {RunStatus.pending.value, RunStatus.running.value}` 分支中，抛出 `HTTPException(status_code=409, detail="This run has not finished")`；以 `await get_run_event_store(request).list_events( thread_id, run_id, event_types=["run.delivery"], limit=2, user_id=user_id, )` 计算 `events`；返回 `_presented_files_from_delivery(events)`。

  源码依据：[_archive_presented_paths 实现](../backend/app/gateway/routers/thread_runs.py#L1837-L1854)


- **`get_run_artifact_archive_manifest`**（`thread_id, run_id, request`）：等待 `_require_run_visible_to_scope(run_id, thread_id, request)` 完成；以 `await _archive_presented_paths(thread_id, run_id, request)` 计算 `presented_paths`；返回 `ArtifactArchiveManifestResponse(file_count=len(dict.fromkeys(presented_paths)))`。

  源码依据：[get_run_artifact_archive_manifest 实现](../backend/app/gateway/routers/thread_runs.py#L1860-L1870)


### 5.41.25 create_run_artifact_archive

参数：`thread_id, run_id, request`。

以 `await _archive_presented_paths(thread_id, run_id, request)` 计算 `presented_paths`。进入 `get_run_manager(request).reserve_thread_operation( thread_id, kind=ThreadOperationKind.artifact_archive, user_id=effective_user_id, )` 边界，退出由对应 context manager 处理。等待 `_require_run_visible_to_scope(run_id, thread_id, request)` 完成。

捕获 `ConflictError as exc`、`ArtifactArchiveError as exc` 的边界见源码，显式抛出路径涉及 `HTTPException`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
StreamingResponse( _archive_response_chunks(result), media_type="application/zip",
   headers={ "Content-Disposition": f'attachment; filename="artifacts-{safe_run_id}.zip"',
     "Content-Length": str(result.size), "Cache-Control": "private, no-store",
     "X-Content-Type-Options": "nosniff", }, background=BackgroundTask(result.file.close),
   )
```


源码依据：[create_run_artifact_archive 实现](../backend/app/gateway/routers/thread_runs.py#L1873-L1929)


### 5.41.26 list_run_events

**参数摘要**

```text
    thread_id,
    run_id,
    request,
    event_types=Query(default=None),
    task_id=Query(default=None),
    limit=Query(default=500, ge=1, le=2000),
    after_seq=Query(default=None, ge=1)
```

以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`。关键判定为 `event_types`、`len(types) > MAX_EVENT_TYPE_FILTERS`。等待 `_require_run_visible_to_scope(run_id, thread_id, request)` 完成。显式抛出路径涉及 `HTTPException`。

分支返回 「表达式摘要 1」，提前返回结束该路径。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
[ { **event, "metadata": redact_metadata_secrets(event.get("metadata")),
     } if isinstance(event, dict) and "metadata" in event else event for event in events ]
```


源码依据：[list_run_events 实现](../backend/app/gateway/routers/thread_runs.py#L1932-L1983)


### 5.41.27 辅助函数与局部契约

- **`get_run_workspace_changes`**（`thread_id, run_id, request, include_files=Query(default=True), include_diff=Query(default=True)`）：等待 `_require_run_visible_to_scope(run_id, thread_id, request)` 完成；以 `await _run_scope_user_id(request, thread_id)` 计算 `user_id`；以 `get_run_event_store(request)` 计算 `event_store`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  await get_workspace_changes_response( event_store, thread_id, run_id,
     include_files=include_files, include_diff=include_diff, user_id=user_id,
     )
  ```


  源码依据：[get_run_workspace_changes 实现](../backend/app/gateway/routers/thread_runs.py#L1986-L2006)


- **`thread_token_usage`**（`thread_id, request, include_active=Query(default=False, description="Include running run progress snapshots")`）：以 `get_run_store(request)` 计算 `run_store`；以 `await _run_scope_user_id(request, thread_id)` 计算 `scope_user_id`；在 `条件 include_active` 分支中，以 `await run_store.aggregate_tokens_by_thread(thread_id, include_active=True, user_id=scope_user_id)` 计算 `agg`；在 `同层否则分支` 分支中，以 `await run_store.aggregate_tokens_by_thread(thread_id, user_id=scope_user_id)` 计算 `agg`；以 `await build_context_usage(request, thread_id, run_store, user_id=scope_user_id)` 计算 `context_usage`；返回 `ThreadTokenUsageResponse(thread_id=thread_id, context_usage=context_usage, **agg)`。

  源码依据：[thread_token_usage 实现](../backend/app/gateway/routers/thread_runs.py#L2009-L2023)


## 5.42 中断审批恢复校验

源码位置：`backend/app/gateway/hitl_approval.py`。

HITL 恢复校验对照 checkpoint 的 interrupt、run 等待状态与请求关联，拒绝无对应中断的新 resume。辅助扫描兼容旧 checkpoint 历史；找到历史 interrupt 不等于当前可授权执行。

### 5.42.1 辅助函数与局部契约

- **`_error`**（`status, code, message`）：返回 `HTTPException(status_code=status, detail={"code": code, "message": message})`。

  源码依据：[_error 实现](../backend/app/gateway/hitl_approval.py#L32-L35)


- **`_responses_of`**（`command`）：在 `条件 not isinstance(command, Mapping)` 分支中，返回 `None`；以 `command.get("resume")` 计算 `resume`；在 `条件 not isinstance(resume, Mapping)` 分支中，返回 `None`；以 `resume.get("responses")` 计算 `responses`；返回 `responses if isinstance(responses, list) else None`。

  源码依据：[_responses_of 实现](../backend/app/gateway/hitl_approval.py#L36-L45)


- **`_all_reject`**（`responses`）：返回 `bool(responses) and all(isinstance(r, Mapping) and r.get("option_id") == "reject" for r in responses)`。

  源码依据：[_all_reject 实现](../backend/app/gateway/hitl_approval.py#L46-L49)


- **`has_any_pending_interrupt`**（`accessor, read_config`）：在 `循环 async for checkpoint in recent_checkpoint_history(accessor.checkpointer, read_config)` 分支中，以 `list(getattr(checkpoint, "pending_writes", ()) or ())` 计算 `writes`；在 `循环 async for checkpoint in recent_checkpoint_history(accessor.checkpointer, read_config)` → `条件 any(len(write) >= 2 and write[1] == "__resume__" for write in writes)` 分支中，返回 `False`；在 `循环 async for checkpoint in recent_checkpoint_history(accessor.checkpointer, read_config)` → `条件 any(len(write) >= 2 and write[1] == "__interrupt__" for write in writes)` 分支中，返回 `True`；返回 `False`。

  源码依据：[has_any_pending_interrupt 实现](../backend/app/gateway/hitl_approval.py#L50-L64)


- **`_pending_interrupts_from_checkpoint_history`**（`checkpointer, read_config`）：以 `list(getattr(checkpoint, "pending_writes", ()) or ())` 计算 `writes`；以 `{}` 计算 `grouped: dict[str, list[Any]]`。关键判定为 `any(len(write) >= 2 and write[1] == "__resume__" for write in writes)`、`not interrupt_writes`，相应动作只在对应分支执行。执行 `grouped.setdefault(str(write[0]), []).extend(interrupts)`。分支返回 `{}`、`grouped`，提前返回结束该路径。

  源码依据：[_pending_interrupts_from_checkpoint_history 实现](../backend/app/gateway/hitl_approval.py#L65-L81)


- **`has_waiting_for_input_run`**（`request, thread_id`）：在 `try` 分支中，以 `get_run_store(request)` 计算 `run_store`；在 `捕获 (HTTPException, AttributeError) as exc` → `条件 isinstance(exc, AttributeError) or exc.status_code == 503` 分支中，返回 `False`；在 `捕获 (HTTPException, AttributeError) as exc` 分支中，向外继续抛出当前异常；以 `await run_store.list_by_thread(thread_id, limit=5)` 计算 `runs`；返回 `any(run.get("status") == "waiting_for_input" for run in runs)`。

  源码依据：[has_waiting_for_input_run 实现](../backend/app/gateway/hitl_approval.py#L82-L95)


- **`mark_approval_answered`**（`request, thread_id, expected_request_ids=None`）：以 `get_run_manager(request)` 计算 `run_manager`。关键判定为 `isinstance(exc, HTTPException) and exc.status_code != 503`、`isinstance(store_exc, AttributeError) or store_exc.status_code == 503`，相应动作只在对应分支执行。捕获 `(HTTPException, AttributeError) as exc`、`(HTTPException, AttributeError) as store_exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `当前异常`。分支返回 `None`、`await run_manager.mark_approval_answered(thread_id, expected_request_ids=expected_request_ids)`，提前返回结束该路径。

  源码依据：[mark_approval_answered 实现](../backend/app/gateway/hitl_approval.py#L96-L117)


- **`_interrupt_from_checkpoint_history`**（`accessor, read_config, payload`）：以 `list(getattr(checkpoint, "pending_writes", ()) or ())` 计算 `writes`。关键判定为 `accessor is None`、`getattr(accessor, "checkpointer", None) is None`、`any(len(write) >= 2 and write[1] == "__resume__" for write in writes)`，相应动作只在对应分支执行。分支返回 `None`、`str(write[0]), pending_interrupt`，提前返回结束该路径。

  源码依据：[_interrupt_from_checkpoint_history 实现](../backend/app/gateway/hitl_approval.py#L118-L137)


### 5.42.2 find_pending_interrupts

参数：`request, thread_id, accessor=None, read_config=None, snapshot=None`。

以 `await accessor.aget(read_config)` 计算 `snapshot`；以 「表达式摘要 1」 计算 `grouped`。

**表达式摘要 1**（非完整源码；换行仅为阅读排版）：

```text
await _pending_interrupts_from_checkpoint_history( checkpointer, read_config or {"configurable": {"thread_id":
      thread_id, "checkpoint_ns": ""}}, )
```


关键判定为 `snapshot is None and accessor is not None and read_config is not None`、`snapshot is not None`、`grouped`。捕获 `(HTTPException, AttributeError) as exc` 的边界见源码，显式抛出路径涉及 `当前异常`。

分支返回 `grouped`、`{}`、`{task_id: [pending_interrupt]}`、`{task_id: [Interrupt(value=payload, id=payload.get("request_id", task_id))]}`，提前返回结束该路径。

源码依据：[find_pending_interrupts 实现](../backend/app/gateway/hitl_approval.py#L138-L212)


### 5.42.3 辅助函数与局部契约

- **`_approval_request_ids`**（`payload`）：以 `payload.get("pending_tool_call_ids")` 计算 `expected_ids`；以 `{}` 计算 `expected_request_ids: dict[str, str]`。关键判定为 「表达式摘要 1」、`not isinstance(actions, list)`、`not isinstance(action, Mapping)`，相应动作只在对应分支执行。以 `request_id` 计算 `expected_request_ids[call_id]`。显式抛出路径涉及 `_error`。分支返回 `expected_request_ids`，提前返回结束该路径。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  not isinstance(expected_ids, list) or any(not isinstance(call_id,
       str) for call_id in expected_ids) or len(set(expected_ids)) != len(expected_ids)
  ```


  源码依据：[_approval_request_ids 实现](../backend/app/gateway/hitl_approval.py#L213-L235)


- **`current_approval_request_ids`**（`request, thread_id, accessor, read_config`）：以 `await find_pending_interrupts(request, thread_id, accessor=accessor, read_config=read_config)` 计算 `pending`；以 `hitl_approval_payloads(pending)` 计算 `payloads`；返回 `_approval_request_ids(combine_approval_payloads(payloads)) if payloads else None`。

  源码依据：[current_approval_request_ids 实现](../backend/app/gateway/hitl_approval.py#L236-L248)


### 5.42.4 validate_resume_request

参数：`request, thread_id, accessor, read_config, enabled, body`。

把回复映射到当前待处理 interrupt/request IDs，检查 waiting/run 与全拒绝兼容分支，拒绝无匹配或非法 resume；checkpoint 历史和当前审批状态共同确定能否恢复。

源码依据：[validate_resume_request 实现](../backend/app/gateway/hitl_approval.py#L249-L335)


### 5.42.5 辅助函数与局部契约

- **`reject_new_run_when_approval_pending`**（`request, thread_id, accessor=None, read_config=None`）：以 `await find_pending_interrupts(request, thread_id, accessor=accessor, read_config=read_config)` 计算 `pending`；在 `条件 hitl_approval_payloads(pending)` 分支中，抛出 `_error(409, "approval_pending", "Thread has a pending tool approval; resume or reject it before starting a new run")`。

  源码依据：[reject_new_run_when_approval_pending 实现](../backend/app/gateway/hitl_approval.py#L336-L352)


## 5.43 实时工具审批协调

源码位置：`backend/app/gateway/tool_approvals.py`。

审批控制面显式解析用户/thread/run scope，查询活动审批并更新答案；审批存储与实际图恢复是不同动作。只读查询不应产生取消或授权副作用。

### 5.43.1 辅助函数与局部契约

- **`_scope`**（`request, thread_id`）：以 `await get_current_user(request)` 计算 `owner`；在 `条件 not owner` 分支中，抛出 `HTTPException(401, "Authentication required")`；以 `await get_thread_store(request).get(thread_id, user_id=owner)` 计算 `thread`；在 `条件 thread is None` 分支中，抛出 `HTTPException(404, "Thread not found")`；以 `thread.get("incarnation")` 计算 `incarnation`；在 `条件 not isinstance(incarnation, str)` 分支中，抛出 `HTTPException(409, "Thread approval identity is unavailable")`；返回 `(owner, thread_id, incarnation)`。

  源码依据：[_scope 实现](../backend/app/gateway/tool_approvals.py#L20-L32)


- **`_store`**（`request`）：以 `getattr(request.app.state, "approval_store", None)` 计算 `store`；在 `条件 store is None` 分支中，抛出 `HTTPException(503, "Approval storage is unavailable")`；返回 `store`。

  源码依据：[_store 实现](../backend/app/gateway/tool_approvals.py#L33-L39)


- **`_active`**（`request, row, owner`）：在 `条件 row["expires_at"] is not None and row["expires_at"] <= time.time()` 分支中，返回 `False`；以 `await get_run_manager(request).get(row["run_id"], user_id=owner)` 计算 `run`；返回 「表达式摘要 1」。

  **表达式摘要 1**（非完整源码；换行仅为阅读排版）：

  ```text
  run is not None and run.thread_id == row["thread_id"] and run.status in ("running",
     "pending") and not run.ownership_lost and not run.abort_event.is_set()
  ```


  源码依据：[_active 实现](../backend/app/gateway/tool_approvals.py#L40-L46)


- **`list_approvals`**（`thread_id, request`）：以 `await _scope(request, thread_id)` 计算 `scope`；以 `await _store(request).list(scope=scope, status="pending")` 计算 `rows`；以 `[row["payload"] for row in rows if row["kind"] == "live" and await _active(request, row, scope[0])]` 计算 `requests`；以 `await _store(request).list(scope=scope, grants=True)` 计算 `grants`；返回 `{"requests": requests, "grants": [{"request_id": row["request_id"], "operation": row["payload"]} for row in grants]}`。

  源码依据：[list_approvals 实现](../backend/app/gateway/tool_approvals.py#L47-L54)


### 5.43.2 answer_approval

参数：`thread_id, run_id, request_id, body, request`。

解析用户/thread/run scope，校验归属和活动审批，提交对应 request 的答案；并不凭普通文本直接执行被批准工具。

源码依据：[answer_approval 实现](../backend/app/gateway/tool_approvals.py#L55-L101)


### 5.43.3 辅助函数与局部契约

- **`revoke_approval_grants`**（`thread_id, request`）：以 `await _scope(request, thread_id)` 计算 `scope`；等待 `await_drained(_store(request).revoke(scope=scope))` 完成；返回 `{"revoked": True}`。

  源码依据：[revoke_approval_grants 实现](../backend/app/gateway/tool_approvals.py#L102-L106)


## 5.44 运行中追加输入

源码位置：`backend/app/gateway/run_inputs.py`。

Queue/Steer 的服务层检查归属、当前 run 与输入门控，建立持久输入记录并在可用边界分发。排队、被消费、应用与取消/重试分别记录，不是 enqueue 新 run 的别名。

### 5.44.1 辅助函数与局部契约

- **`RunInputRequest.bounded_context`**（无显式业务参数）：在 `条件 len(json.dumps(self.context, ensure_ascii=True, allow_nan=False).encode()) > 32 * 1024` 分支中，抛出 `ValueError("Input context exceeds 32 KiB")`；返回 `self`。

  源码依据：[bounded_context 实现](../backend/app/gateway/run_inputs.py#L26-L31)


- **`_store`**（`request`）：以 `get_run_manager(request).input_store` 计算 `store`；在 `条件 store is None` 分支中，抛出 `HTTPException(503, "Run inputs are unavailable")`；返回 `store`。

  源码依据：[_store 实现](../backend/app/gateway/run_inputs.py#L32-L38)


- **`submit_run_input`**（`thread_id, run_id, body, request`）：校验当前用户/thread/run 与 queue/steer 模式，生成输入记录并交输入控制面；接受只表示已登记，应用时机在图的安全边界。

  源码依据：[submit_run_input 实现](../backend/app/gateway/run_inputs.py#L39-L65)


- **`list_thread_inputs`**（`thread_id, request`）：以 `await get_current_user(request)` 计算 `user_id`。关键判定为 `user_id is None`、`status in ("pending", "applying") and (row["status"] not in ("pending", "running", "success") or row.get("stop_reason"))`，相应动作只在对应分支执行。执行 `items.append({key: value for key, value in {**entry, "status": status, "target_run_id": row["run_id"]}.items() if key != "context"})`。显式抛出路径涉及 `HTTPException`。分支返回 `{"items": [items[i] for i in sorted(keep)], "active_run_id": await _store(request).active_input_run(thread_id, user_id=user_id)}`，提前返回结束该路径。

  源码依据：[list_thread_inputs 实现](../backend/app/gateway/run_inputs.py#L66-L88)


- **`dispatch_queued_inputs`**（`inputs, agent_runs, thread_id`）：以 `await inputs.store.get(inputs.run_id, user_id=inputs.user_id)` 计算 `row`。关键判定为 `row is None or row["status"] != "success" or row.get("stop_reason")`、`entry["mode"] != "queue" or entry["status"] != "pending"`、`run.status != "success" or run.stop_reason`，相应动作只在对应分支执行。以 `await inputs.read()` 计算 `state`；等待 `inputs.pause()` 完成。捕获 `InputCancelled`、`TimeoutError`、`Exception` 的边界见源码，捕获不等于已恢复。分支返回 `None`，提前返回结束该路径。

  源码依据：[dispatch_queued_inputs 实现](../backend/app/gateway/run_inputs.py#L89-L126)


### 5.44.2 dispatch_one_input

参数：`thread_id, run_id, input_id, request, retry=False`。

以 `await get_current_user(request)` 计算 `user_id`。关键判定为 `user_id is None`、`source is None or source["thread_id"] != thread_id`、`entry.get("run_id")`。以 `{KNOWLEDGE_SCOPE_KEY: copy.deepcopy(scope)}` 计算 `message["additional_kwargs"]`。等待 `inputs.dispatched(entry["id"], run.run_id)` 完成。显式抛出路径涉及 `HTTPException`。

分支返回 `AgentRun(thread_id, run.run_id, run.status, run.assistant_id, run.stop_reason)`，提前返回结束该路径。

源码依据：[dispatch_one_input 实现](../backend/app/gateway/run_inputs.py#L127-L170)


### 5.44.3 辅助函数与局部契约

- **`change_run_input`**（`thread_id, run_id, input_id, request, retry=False`）：以 `await get_current_user(request)` 计算 `user_id`。关键判定为 `user_id is None`、`run is None or run.thread_id != thread_id`、`retry`，相应动作只在对应分支执行。等待 `inputs.cancel(input_id)` 完成。捕获 `InputConflict as exc` 的边界见源码，捕获不等于已恢复。显式抛出路径涉及 `HTTPException`。分支返回 `{"run_id": dispatched.run_id}`、`{"removed": True}`，提前返回结束该路径。

  源码依据：[change_run_input 实现](../backend/app/gateway/run_inputs.py#L171-L194)


## 5.45 工具执行前的审批与参数绑定

源码位置：`backend/packages/harness/deerflow/approvals/middleware.py`。

该文件是审批实际包围工具 handler 的位置。模块顶层定义宿主 bridge context 键；`ToolApprovalMiddleware` 既支持根图持久 interrupt，也支持子图通过宿主 bridge 挂起原调用。它保留内层授权和 guardrail，批准不是绕过执行端检查。以下覆盖全部 17 个自行定义的函数，包含两个同名局部 prepare 回调。

### 5.45.1 上下文、消息来源和生命周期钩子

- **`_context(request)`**：只接受 runtime.context 为 dict，否则返回空 dict。它统一读取宿主上下文，不从消息内容重建用户权限。

  源码依据：[上下文读取](../backend/packages/harness/deerflow/approvals/middleware.py#L29-L31)

- **`user_instructions(messages)`**：仅提取 HumanMessage 的原始用户内容，排除宿主 producer 标记和 untrusted_input；用于给 reviewer/bridge 提供可见指令依据，不能把工具输出伪装成用户授权。

  源码依据：[用户指令筛选](../backend/packages/harness/deerflow/approvals/middleware.py#L34-L39)

- **`denial(request, reason)`**：构造关联原 tool_call_id、status=error 的 ToolMessage，明确工具未执行。task 委派额外写 cancelled 状态契约，使 ledger 不会把拒绝的任务留在进行中；拒绝结果本身不调用 handler。

  源码依据：[拒绝反馈与委派状态](../backend/packages/harness/deerflow/approvals/middleware.py#L42-L57)

- **`ToolApprovalMiddleware.__init__`**：深拷贝 policy，保存 app config、reviewer、supports_resume、model_name，计算包含 reviewer policy version 的策略摘要。**`release_policy_parameters()`** 返回该摘要，供装配描述归属实际策略。

  源码依据：[策略快照与摘要](../backend/packages/harness/deerflow/approvals/middleware.py#L68-L77)

- **`before_model(state, runtime)`**：只有主图且有 bridge 时更新其 user_messages；子 Agent 不用自己的委派文字覆盖父用户指令。**`abefore_model`** 直接复用此同步逻辑，没有独立异步动作。

  源码依据：[父指令传入 bridge](../backend/packages/harness/deerflow/approvals/middleware.py#L79-L86)

- **`after_model(state, runtime)`**：找最后一条 AIMessage；无调用或调用 ID 全部非空且唯一则不改。缺失、空或重复 ID 时清空整组调用，同时同步 provider 表示并附说明，防止无法可靠关联的动作进入审批/执行。**`aafter_model`** 复用该规则。

  源码依据：[调用标识校验与整组阻断](../backend/packages/harness/deerflow/approvals/middleware.py#L88-L103)

### 5.45.2 `_prepare`：生成绑定到具体操作的审批计划

输入是工具调用 request，异步返回 plan，尚不执行工具。计划保存 request_id、tool_call_id、实际 descriptor、深拷贝 args、fingerprint、policy_digest、owner、thread_id、模式和 grant_allowed；request ID 用随机 UUID，fingerprint 根据实际工具、参数和策略计算。

分支有明确顺序：

1. 同轮动作超过 max_actions 或调用 ID 重复先 deny；deny_tools 也先拒绝。always_ask_tools 强制保留 ask 且不允许线程长期授权。
2. ask 模式只有在操作员显式允许无人值守、宿主支持恢复且非子任务等条件下，才可根据交互策略/来源 allow。
3. full 或 allow_tools 允许；未列入审批工具且属于可信常规读/导航工具可允许。关闭 review_unknown_tools 时，未显式选择工具也可允许，这是一项实际部署权限取舍。
4. bridge 有相同 fingerprint 的线程 grant 则 allow，但记录 basis，后续必须再查 grant 是否撤销。
5. auto 模式把 descriptor/args 与筛选后的用户指令交给 reviewer。自定义 reviewer 有 timeout，异常降为 ask；默认 reviewer 经项目封装调用。非法 verdict 不能直接当允许，最终经过 effective_outcome 规范化。

源码依据：[计划字段与分支优先级](../backend/packages/harness/deerflow/approvals/middleware.py#L105-L144) · [自动审查调用与降为等待](../backend/packages/harness/deerflow/approvals/middleware.py#L145-L163)

文字理由只保存有限长度，reviewer 的结果仍是程序约束下的外部判断。auto 得到 allow 不证明操作事实上无风险；full 也只改变审批选择，handler 内层强制保护仍存在。

### 5.45.3 `_payload`：界面卡片的交互契约

从 plan 取 action 身份、工具、args、fingerprint 和原因，加上 args schema、source 与 grant_allowed；返回 version=2、kind=hitl_approval 的 payload。通常给 approve/reject/edit，允许 exact thread grant 时再给 approve_thread；pending_tool_call_ids、agent_id 和 request_id 用于前后端关联。

它生成卡片数据，不证明用户已收到或已同意。完整 payload 大小在 `_decide` 再检查，不能仅以参数本身较小就保证卡片可发送。

源码依据：[卡片字段和选项](../backend/packages/harness/deerflow/approvals/middleware.py#L165-L180)

### 5.45.4 `_validate_response`：编辑参数与响应关联

输入为原 plan 和恢复响应，返回 `(decision, args)`。先要求 responses 是列表，且恰好一个元素匹配当前 request_id，同时 tool_call_id 相同。决策只接受 approve、approve_thread、edit、reject；没有 grant_allowed 时拒绝持久线程授权。

edit 使用用户提供的 args，其他决策仍取 plan 原参数。参数须为 JSON 对象、序列化字节不超 max_payload_bytes，并通过 Draft 2020-12 JSON Schema 校验。如果 schema 存在 properties，还拒绝未列出的额外字段，即使 schema 本身允许 additionalProperties，也不能让用户注入宿主 runtime 字段。

源码依据：[关联、大小和 schema 校验](../backend/packages/harness/deerflow/approvals/middleware.py#L182-L207)

这些是字段和结构限制；业务权限仍由 handler 内层验证。校验抛错在 `_decide` 变成明确拒绝，未经检查的编辑参数不会直接交给执行器。

### 5.45.5 `_decide`：等待、恢复和实际放行

1. 先比较计划策略摘要与当前摘要、owner 与当前用户、thread 与当前线程；再根据当前工具和参数重算 fingerprint。任一改变都返回 denial，不复用旧批准。
2. 使用 thread grant 的计划再查询 grant。deny/allow 有 bridge 时分别记录决策；allow 返回原 request，deny 返回 ToolMessage。
3. ask 生成卡片并检查整体大小。非交互或带受控来源的运行不能现场收集批准，返回拒绝；因此模型无法靠继续等待无限获得某个不存在的用户响应。
4. 子 Agent 必须有可交互父 bridge，调用 wait_for_decision，挂起原调用；根图且 supports_resume 时调用 LangGraph interrupt，由 checkpoint 恢复；不支持恢复则拒绝。
5. 收到响应后校验关联与参数，记录决定。reject 产生未执行反馈；批准或 edit 返回 `request.override` 后的调用，外层 wrapper 才把它交给 handler。

源码依据：[恢复前重新绑定与等待方式](../backend/packages/harness/deerflow/approvals/middleware.py#L209-L240) · [回答消费与参数回填](../backend/packages/harness/deerflow/approvals/middleware.py#L241-L251)

根图恢复与子图 bridge 等待不是同一种寿命：根图可持久暂停；子图等待仍依赖父宿主 runtime。Gateway 的恢复校验还会核对真实 pending interrupt，不能把一个历史批准卡片当作当前通用通行证。

源码依据：[Gateway 恢复入口](../backend/app/gateway/hitl_approval.py#L1-L35)

### 5.45.6 `awrap_tool_call` 与局部异步 `prepare`

`awrap_tool_call` 用 LangGraph `@task(name="prepare_tool_approval")` 包装局部异步 `prepare()`；该回调只 await `_prepare(request)`。准备结果和 `_decide` 串联，使持久执行框架能够关联已准备计划与恢复步骤。

`GraphBubbleUp` 必须重新抛出，让 interrupt 按图控制信号传播；其他审批异常返回服务不可用的 denial。decision 为 ToolMessage 则直接反馈，否则才 await handler(decision)。handler 位于审批 try 块之后，工具执行失败不会被谎报成审批服务不可用。

源码依据：[异步 wrapper 与回调](../backend/packages/harness/deerflow/approvals/middleware.py#L253-L265)

### 5.45.7 `wrap_tool_call` 与局部同步 `prepare`

同步 wrapper 的同名局部 `prepare()` 用 asyncio.run 调 `_prepare`，外层取 task.result，再用 asyncio.run 执行 `_decide`。同样重新抛 GraphBubbleUp，普通审批错误拒绝，只有实际放行的 request 才进入同步 handler。

这是同步入口契约，不能在任意已运行 event loop 的线程上无条件套用。项目异步图路径使用前一小节的 awrap，不能把存在同步 wrapper 当作任何调用环境均可安全同步阻塞的证明。

源码依据：[同步 wrapper 与回调](../backend/packages/harness/deerflow/approvals/middleware.py#L267-L279)

# 6. 跨模块协作全景

下图以一次带工具执行、可委派、可恢复的 Gateway 请求展示控制流与状态流；括号中的 M 编号指向第4章。箭头表示源码支持的调用或数据交付，不表示每次请求必经全部分支。

~~~text
进程启动
 App lifespan → runtime/bridge/engine/checkpointer/repositories → RunManager
             → orphan 收敛 + heartbeat；资源在退出时按拥有关系逆序排空
用户输入
 nginx → Gateway route → normalize_input → 注入认证身份/受控 context
       → owner/HITL/幂等/准入检查 → RunRecord(pending) → worker.try_start
       → assembly [4.1.1] → graph/middleware/schema/prompt/tool registry
       → 绑定 checkpointer/accessor [4.1.3,4.1.9]，恢复当前 thread channel
图执行
 before_agent 正序 [4.1.2]
   ThreadData → sandbox/skill holder [4.1.4] → 其他初始数据
 每轮：
   before_model 正序 → 摘要/上下文投影/预热
   wrap_model_call 外→内 → 模型 → 内→外
   after_model 反序
     ├─ task 超额 [4.1.6] → 同 ID AI 消息替换、移除超额 calls
     ├─ loop warning [4.1.7] → pending（下一请求注入，不写历史）
     ├─ loop hard [4.1.7] → 清 calls、stop_reason=loop_capped
     └─ 保留 calls → 工具 wrapper
          授权/审批/receipt/progress/error handling [4.1.2]
          ├─ 待人工输入 → interrupt/checkpoint → waiting_for_input
          ├─ 普通 sandbox 工具 [4.1.4] → 外部副作用 → ToolMessage/Command
          └─ task [4.1.6] → capacity → 独立子图 → result/ledger/证据
          → reducers [4.1.3] → checkpoint [4.1.9] → 下一轮
 无继续动作 → after_agent 反序
   RunInput 可决定续跑 → Memory 入队 [4.1.5] → sandbox holder 归还 [4.1.4]
流输出（贯穿模型与工具阶段）
 agent.astream → mode/namespace → serialize → bridge [4.1.8]
             → 独立订阅游标 → SSE（event/id/data）
             → END / gap / heartbeat / creator disconnect 分支
运行收尾
 completion + durable terminal ACK → END → producer/lease/journal 清理
                                 → 延迟 bridge/registry 清理
后台记忆
 queue debounce → LLM 提取 → gates/revision → manifest+facts 提交 [4.1.5]
恢复/取消
 resume → 重建可信 context → accessor 物化/模式门禁 → 对应中断节点
 cancel → 请求与 fencing → 停执行/关流/取消命令 → interrupt 或图状态 rollback
~~~

这张图的三个汇聚点决定正确性。第一是 ToolMessage/Command 与消息 ID 的汇聚：模型调用集合必须和反馈配对，截断必须清理所有调用表示。第二是 channel/reducer：并行写到 sandbox、todos、delegations 时必须遵守字段规则，不能靠最后一条流帧覆盖全部状态。第三是持久终态 ACK：图结束后仍需确认运行写入权和完成记录，才可以释放准入屏障并让后续 run 安全开始。

源码依据：[tool_call_metadata.py 实现](../backend/packages/harness/deerflow/agents/middlewares/tool_call_metadata.py#L87-L130) · [thread_state.py 实现](../backend/packages/harness/deerflow/agents/thread_state.py#L258-L298) · [manager.py 实现](../backend/packages/harness/deerflow/runtime/runs/manager.py#L709-L733)

全生命周期覆盖的是多个嵌套寿命：进程拥有连接/服务，thread 拥有持久会话和路径空间，run 拥有执行权与流 producer，工具和子任务拥有具体 holder，后台记忆拥有独立队列与写入。业务回答可以先于记忆落盘出现；run 完成可以先于 warm 容器销毁；SSE gap 可以在 run 仍正常执行时出现。读者必须按对应层判断“完成”，不能选择一个 END 或 cancelled future 代替所有层的结论。

源码依据：[worker.py 实现](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2021-L2481) · [queue.py 实现](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L390-L459) · [aio_sandbox_provider.py 实现](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2807-L2875)

# 7. 设计取舍、重要风险与适用边界

## 7.1 设计怎样发挥作用

### 7.1.1 可嵌入框架与服务控制面的分工

应用工厂把部署配置、当前用户、工具目录与运行策略转成图，参数化工厂则让 Python 宿主提供资源。App 依赖 Harness 的方向让发布包可独立使用；代价是 SDK 调用者必须理解没有被带入的 Gateway owner、幂等和生命周期语义。它是两套入口契约，不是两个完全不同的 Agent 算法。

源码依据：[参数工厂契约](../backend/packages/harness/deerflow/agents/factory.py#L67-L204) · [已有依赖边界断言](../backend/tests/test_harness_boundary.py#L1-L47)

### 7.1.2 reducer、模式门禁与运行屏障

sandbox 相同 ID 的重复写可接受，冲突 ID 抛错；todos 的空写不抹去已有计划；delegation 依标识归并。字段分别定义合并语义，避免把并行更新误作最后一个整状态覆盖。delta 存储减少重复保存的机制，需要匹配 schema、模式标记和状态物化；未经物化的 raw channel_values 不能当完整 ThreadState。

源码依据：[字段合并](../backend/packages/harness/deerflow/agents/thread_state.py#L79-L99) · [状态 schema](../backend/packages/harness/deerflow/agents/thread_state.py#L258-L298) · [状态物化契约](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L1-L15)

terminal ACK、finalizing 与 ownership fence 补上图结束之后的写入窗口。收益是阻止旧 owner 在新执行开始后继续提交相冲突状态；代价是持久依赖故障可延长收尾和准入等待。这些屏障没有把任意网络和文件副作用纳入同一事务。

源码依据：[最终提交确认](../backend/packages/harness/deerflow/runtime/runs/manager.py#L692-L733) · [结束前的持久协调](../backend/packages/harness/deerflow/runtime/runs/worker.py#L2146-L2210)

### 7.1.3 生命周期租约与容器复用

active、warm、destroy 的分离使最后一个 run 结束后仍可保留容器。**基于代码的推断：**后续运行可复用已建立资源，代价是空闲容器、所有权协调和清理状态增多，不能量化推断启动延迟改善。Redis ownership 协议用 token、TTL 和删除占有状态协调跨进程销毁，execution lease 则保护正在使用的具体操作；两者各有对象和寿命。

源码依据：[warm 保留与释放](../backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py#L2807-L2875) · [所有权协议](../backend/packages/harness/deerflow/community/aio_sandbox/ownership/base.py#L1-L188) · [操作 lease](../backend/packages/harness/deerflow/sandbox/lease.py#L60-L88)

### 7.1.4 后台记忆与文件恢复日志

防抖队列合并累积历史，减少每回合立即触发提取；manifest/fact revision 防止旧快照覆盖新修改，journal/backup 把多文件半写收敛到可恢复状态。收益来自具体合并和冲突重算机制；代价是回答出现时记忆可能未保存、文件锁部署语义必须成立、索引通知仍是另一个边界。置信度过滤只约束采纳条件，不证明事实真伪。

源码依据：[队列合并](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L170-L239) · [提取冲突重算](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L1533-L1634) · [日志提交](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/storage.py#L1322-L1474)

### 7.1.5 分层限额和动作审批

子任务有提示引导、单轮硬截断、每 run 总量与实际执行容量等不同限制；loop guard 另以参数哈希及工具频率识别重复，warning 与 hard stop 分级。收益是模型不遵守文字要求时仍有程序控制，代价是高频但有进展的操作也可能触发停止。审批再绑定工具、参数、用户、线程和策略；edit 之后仍走内层授权，不以“卡片点过批准”代替实际执行端检查。

源码依据：[单轮硬截断](../backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py#L104-L183) · [循环控制](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L347-L416) · [审批重新绑定](../backend/packages/harness/deerflow/approvals/middleware.py#L182-L251)

## 7.2 已确认的不一致与局部契约缺陷

**说明过时。**middleware 流程文档里的固定链数量与当前条件装配不能逐项等同；client 注释将 bridge 概述为 asyncio queue，内存实现实际使用 list 与 Condition。这些是文档表述差异，不能扩大成运行事故。当前 LoopDetection.before_agent/abefore_agent 明确 no-op，清理归 after_agent 和有界结构；它是当前实现事实，没有其他证据就不称为缺失功能。

源码依据：[旧链说明](../backend/docs/middleware-execution-flow.md#L1-L32) · [当前装配入口](../backend/packages/harness/deerflow/agents/lead_agent/agent.py#L485-L545) · [client 说明](../backend/packages/harness/deerflow/client.py#L967-L980) · [内存桥实际结构](../backend/packages/harness/deerflow/runtime/stream_bridge/memory.py#L20-L40) · [当前 no-op 钩子](../backend/packages/harness/deerflow/agents/middlewares/loop_detection_middleware.py#L883-L897)

**已确认的局部契约缺陷：同步记忆 API 在活动事件循环内仍会阻塞调用线程。**`MemoryUpdater.update_memory` 检测 running loop 后把同步提取提交到 executor，却立即执行 `future.result()`。工作确实转到池线程，但调用 loop 线程仍同步等待，和注释“不阻塞 caller loop”矛盾。触发前提是从运行中的 loop 线程直接调用这个同步 API；影响为该线程等待直到 future 完成，不是普通 Timer 队列路径必然阻塞主 Agent。它没有用 await 保护这个入口，也不能由“存在异步 API”修正同步函数已经发生的等待。

源码依据：[注释与同步等待](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/updater.py#L2105-L2144)

## 7.3 条件性风险及当前保护边界

### 7.3.1 回答已完成，记忆仍未保存

触发条件是硬退出发生在队列消费前，或提取模型/文件操作持续失败。用户可已收到回答，checkpoint 也可存在，长期记忆仍未更新。当前保护包括防抖合并、watermark、日志恢复和优雅 flush；watermark 不提供持久投递，drain 也不证明每项写成功。范围仅限独立记忆流水线，不能因此断言会话历史全部丢失。

源码依据：[逐项失败后继续](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L314-L366) · [有限等待排空](../backend/packages/harness/deerflow/agents/memory/backends/deermem/deermem/core/queue.py#L390-L459)

### 7.3.2 运行成功但流历史不完整

生产速度持续高于消费速度或事件保留期已过时，客户端重放可收到 gap；这不改变 run 已完成的事实。bridge 的有界保留保护宿主内存/Redis，代价是不能提供无限 token 历史。现有反馈指向 durable state，而不是静默伪装成一条没有输出的成功流。

源码依据：[Redis 游标与 gap](../backend/packages/harness/deerflow/runtime/stream_bridge/redis.py#L834-L1011) · [HTTP gap 契约](../backend/app/gateway/services.py#L3291-L3339)

### 7.3.3 不同资源上限被误认为同一个总预算

将 warm pool replicas 当硬容器上限，或把进程子任务容量当跨实例全局配额时，实际资源数可高于预期。温池在所有容器 active 时可超 soft cap，多个进程容量可以叠加。现有 LRU、局部 capacity 与 scheduler 数据库预算分别限制不同集合；没有统一等价关系。

源码依据：[温池软上限](../backend/packages/harness/deerflow/community/warm_pool_lifecycle.py#L46-L57) · [进程子任务容量](../backend/packages/harness/deerflow/subagents/capacity.py#L176-L196) · [定时任务共享预算](../backend/packages/harness/deerflow/persistence/scheduled_task_runs/sql.py#L560-L632)

### 7.3.4 Local host bash 与扩展代码的宿主权限

显式开放 Local host bash 后，不可信命令在宿主运行。默认关闭、工具入口能力和路径检查降低暴露面，但虚拟路径、只读文件接口和 best-effort shell 检查不提供内核隔离。直接底层 execute_command 不重复所有工具门禁。另一个独立前提是启用不可信 Python extension：它在 Gateway 权限下导入，registry 回退不能撤销自行产生的副作用。二者都须按实际配置解释，本文不确认部署是否启用。

源码依据：[host bash 开关](../backend/packages/harness/deerflow/sandbox/security.py#L35-L46) · [工具入口约束](../backend/packages/harness/deerflow/sandbox/tools.py#L1885-L1993) · [扩展代码执行](../backend/packages/harness/deerflow/extensions/loader.py#L244-L266)

### 7.3.5 超时或取消之后，远端动作结果未知

远端动作已接受/执行但响应未回来时，本地超时、future cancelled 或图 rollback 都不能证明副作用撤销。命令 scope 取消和 lease 排空保护具体 sandbox 操作，execution fence 保护有权写入者；任意网络动作不在通用事务内。重放仍可能重复外部动作。同步子任务超时先公布 FAILED，后续资源释放还有独立完成事件。

源码依据：[子任务同步超时](../backend/packages/harness/deerflow/subagents/executor.py#L2245-L2285) · [实际执行完成事件](../backend/packages/harness/deerflow/subagents/executor.py#L1717-L1758) · [取消与回滚](../backend/packages/harness/deerflow/runtime/runs/worker.py#L3404-L3515)

### 7.3.6 多类存储只能部分删除或分支

删除/分支跨 metadata、checkpoint、事件历史和文件时，其中一项失败可留下部分状态。reservation 防止同线程冲突写；checkpoint 删除失败保留 owner 等规则避免直接丢失访问边界，后续 best-effort 清理不提供跨资源原子提交。不能把“请求失败”理解成前面没有发生动作。

源码依据：[删除顺序与失败处理](../backend/app/gateway/routers/threads.py#L828-L975)

### 7.3.7 父子图独立，但文件仍共享

父子 Agent 并发修改同一 sandbox 路径时，即使消息、任务 ledger 和图 namespace 独立，文件内容也可能互相影响。显式 context 与 lease 维持归属和保活，不序列化任意文件业务写入。风险范围取决于工具和任务实际访问相同路径，不能从启用子任务直接推定发生冲突。

源码依据：[子任务初态与共享资源](../backend/packages/harness/deerflow/subagents/executor.py#L1548-L1716)

### 7.3.8 独立 SDK 不自动继承 Gateway 控制面

Python 宿主若直接建图，却假定有 Gateway 的 owner、运行准入、幂等和跨 worker lease，就可能在同线程并发或不受预期权限约束的环境执行。工厂支持显式 runtime/checkpointer，但不接管全部宿主职责。这个入口差异不构成 Gateway 本身缺少相关保护的证据。

源码依据：[SDK 工厂契约](../backend/packages/harness/deerflow/agents/factory.py#L67-L204) · [Gateway 统一入口](../backend/app/gateway/services.py#L2295-L2435)

### 7.3.9 定时运行已存在而关联记账不完整

真实 run 建立后数据库写失败，服务仍报告 launched，并保留 active 槽防止普通轮询立即重复启动。这是有实现处理的部分成功；持续存储故障仍可能延迟 run_id 投影、计数和最终状态关联。后续协调可处理其覆盖的状态，不能证明所有故障窗口均在进程崩溃后无损恢复。

源码依据：[启动后补记与返回值](../backend/app/scheduler/service.py#L806-L915)

## 7.4 待确认事项与覆盖边界

实际数据库、Redis、sandbox provider、共享卷、身份策略、扩展清单、模型和实例数未知。文件锁在目标共享文件系统是否有效，Redis/容器引擎的实际通信与超时语义，以及外部工具是否幂等，都缺部署和依赖运行证据。本文分析实现选择条件，不把已定义能力当作已启用能力。

LangChain/LangGraph、模型供应商与存储驱动以项目侧契约和锁定版本为边界，没有执行或导入第三方实现。中间件 before/after 次序的项目材料与实际链约束已区分；CheckpointStateAccessor 使用的私有配置键有升级敏感性，源码注释称核对过 1.2.9，而锁定为 1.2.14。这是版本边界，现有材料不足以确认发生兼容缺陷。

源码依据：[项目中间件执行说明](../backend/docs/middleware-execution-flow.md#L1-L100) · [私有键边界](../backend/packages/harness/deerflow/runtime/checkpoint_state.py#L134-L150) · [锁定版本](../backend/uv.lock#L2214-L2216)

本报告逐函数范围为第 5 章列明的 45 个核心文件和 1466 个函数/方法/局部回调。第 2 至 4、6 章还覆盖配置、模型、工具与扩展绑定、调度、IM 和浏览器恢复的关联实现；这些关联文件只展开所需链路。全部 provider、各 IM 协议、全部业务插件、项目空间全部 API、前端所有组件、TUI 和数据库全部迁移不在逐函数覆盖范围。动态模型输出、第三方内部控制流和真实部署表现没有从静态阅读中得到确认。

权限结论按入口成立：Gateway 认证、资源 owner、工具授权、审批、运行 lease 和 sandbox ownership 分别保护不同对象。局部 helper 未见检查不证明系统没有保护；提示文本、历史字段和类型声明也不能代替执行端检查。分析行为并不表示本次实际执行了它。
