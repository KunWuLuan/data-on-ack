# Queue 可靠性改进设计

状态：核心协议已实现，仍处于内部验收阶段；基于对 `7c8250d` 的评审。本文补充
[当前设计](DESIGN_ZH.md)，
[English version](RELIABILITY_PLAN.md) 提供对应协议。

## 1. 决策与交付边界

| 优先级 | 问题 | 决策 |
| --- | --- | --- |
| P1 | acquire 长期占用 actor 调用槽 | 登记/查询均为短 RPC；等待移至客户端；入口分流和有界背压 |
| P1 | 不确定提交被自动 release | 增加持久化 SUBMITTING；仅证明确实未进入提交时可取消预约 |
| P1 | 存储故障后一直 unhealthy | 服务状态机管理 writer 停止、redo、全量恢复；管理 recover 命令作为兜底 |
| P2 | track 失败丢失业务句柄 | 本地保存 ticket→handle；专门的“已提交登记失败”异常和重试 API |
| P2 | 全链路开销无界 | session 索引、合并水位、有限批量监控、完整生命周期基准 |

保留 GCS KV 单 writer、先落盘再批准、从许可恢复 usage、先持久化释放再归还额度、
水位阻止 ticket 复活这些原则。不开辟第二个可并发写 GCS 的控制器，不引入外部数据库。

发布门槛：先完成并验证三个 P1；同时提供最小可用的 P2 句柄恢复接口，避免以普通
提交异常掩盖已执行事实。性能改进可随后交付，但在测得容量边界前不能宣称稳定吞吐。

当前实现已经包含短 RPC、SUBMITTING、进程内句柄恢复、writer 自动恢复协调、
`queue recover`、按 session 墓碑索引、50ms 确认合并、actor 表查询节流和生命周期
基准。仍未提供跨进程句柄找回、session 退休、选择性 actor-ID 查询和卡死线程下的
自动强制重建；后者会进入 `RESTART_REQUIRED`，不会假报恢复成功。

## 2. 短 RPC 准入协议

替换长时间挂起的 acquire：

1. `enqueue(ticket, request)` 幂等登记，返回 WAITING、RESERVED 或明确错误。
2. `inspect(ticket)` 返回当前状态、建议轮询间隔和控制器 incarnation。
3. 客户端按带抖动的退避轮询；每个 ticket 最多一个在途查询。FIFO 按登记顺序维护，
   查询频率不影响优先级。drainer 独立放行，查询不能重复扣费。
4. 客户端见到 RESERVED 后调用 `begin_submit(ticket)`，只有得到持久化成功响应才
   允许调用 Ray 原始提交函数。
5. 重启丢失 WAITING 时返回 NOT_REGISTERED，客户端用原 ticket 和请求重新登记；
   仍不承诺故障前的排队顺序。已释放或低于确认水位必须返回 TERMINAL，不能混同于缺失。

WAITING 不持久化。RESERVED/SUBMITTING/TRACKED 持久化。响应丢失可以查询同一
ticket，不能生成新 ticket 重试同一次协议。请求内容冲突必须拒绝。

短 RPC 消除永久占槽，但本身不能保证洪峰下管理可达。目标实现将管理/恢复、
track/release 和 enqueue/inspect 使用独立调度容量；若采用 Ray concurrency groups，
必须先验证 Ray 2.58 async actor 的执行线程和事件循环语义，不能跨线程复用 asyncio.Lock。
若无法在同一事件循环安全隔离，则由无状态入口 actor 做限流，单独的状态 actor 仍是
唯一 writer。不能仅增加 max_concurrency 就算完成。

内部调度使用有界分级队列：恢复/释放/跟踪优先，轮询低优先；每轮有工作预算，禁止
drainer 持锁无限遍历所有配额键。管理快照不等待写锁，返回最后已提交状态及健康状态。
记录全局、每 Job 的等待条数和请求字节上限，超限返回明确 OVERLOADED；不是隐式挂起。
配置阈值需通过容量测试确定，不能直接把测试数值当生产默认值。

## 3. 许可与提交状态分离

| 状态 | 占用 | 可以自动释放的条件 |
| --- | --- | --- |
| WAITING | 否 | 本地取消并幂等撤销登记 |
| RESERVED | 是 | 原始提交尚不允许发生；cancel 与 begin_submit 串行竞争 |
| SUBMITTING | 是 | 不能仅凭客户端异常或时间释放 |
| TRACKED | 是 | 已关联的执行对象终态或有证据的显式核对 |
| RELEASED | 否 | 水位覆盖后删除墓碑 |

`begin_submit` 是 RESERVED→SUBMITTING 的持久化边界。成功响应前客户端禁止调用
原始提交；响应丢失则查询/重试该转换，但仍不能提交两次。客户端维护每 ticket 的
进程内提交互斥，恢复协议只允许重试登记，不自动重新执行用户函数。

cancel 若先于 begin_submit 提交则释放并终结 ticket，后者必须拒绝；begin_submit
先提交则 cancel 返回 SUBMISSION_UNKNOWN，不得释放。旧状态中的 RESERVED 可能
已经提交，迁移时必须保守视为 SUBMITTING，不能按新协议的安全取消语义解释。
需要新 schema/协议版本并拒绝不兼容客户端，不能直接混跑新旧 wrapper。

客户端异常分类依据**执行阶段和证据**，不能只依据异常类型：

- 在调用原始 Ray 提交前完成的参数验证失败：可取消尚未进入 SUBMITTING 的预约。
- begin_submit 已落盘但尚未进入原始调用：默认保守保留；将来可设计可验证的撤销协议。
- 进入原始调用后任何异常，包括 KeyboardInterrupt、SystemExit、超时、看似序列化
  错误：默认结果未知，不 release、不推进该 ticket 的确认水位。
- 已拿到 handle：执行已提交，进入 tracking 恢复路径；不得用普通“提交失败”掩盖。

未知状态以 `SubmissionOutcomeUnknown(ticket, phase, cause)` 表达。进程可在中断时
保留原 KeyboardInterrupt 语义并附加 ticket，同时将未知项登记到诊断表；不得吞掉中断。
无 handle 的未知执行需要人工/后续关联协议核对，不能保证自动恢复。管理员的显式
reconcile 必须记录依据，默认拒绝无证据释放；超时、task 列表缺失都不是充分证据。

此协议解决提前归还额度，不保证 Ray 提交与 GCS 写入跨系统原子性，也不提供任意
driver 崩溃下 exactly-once 执行。

## 4. tracking 失败与执行句柄找回

客户端拿到 ObjectRef 或 actor handle 后立即保存本地
`ticket → {handle, execution_identity, tracking_state, last_error}`，再调用 track。
对同一执行身份幂等重试；重试期限耗尽后抛出专门的
`SubmissionTrackingError(ticket, handle, retry_tracking)`，明确“已提交，登记未完成”。
接口已经实现为 `SubmissionTrackingError`、`retry_tracking()` 和
`get_submission()`；其找回范围仍仅限当前进程。

调用方可继续 `ray.get(error.handle)` 或使用 actor handle，也可调用
`retry_tracking(ticket)`；该方法只登记原执行，不再调用 `.remote()`。
`get_submission(ticket)` 返回已保存的关联状态。完成持久化 tracking 后才能确认
该 ticket；禁止因 tracking 失败自动生成新 ticket 重提业务。

未登记 handle 的本地表要有容量上限：达到上限时，在下一次 begin_submit 前背压，
不能驱逐未登记记录来腾空间。成功登记后允许清理本地关联，文档需明确查询保留范围。
进程死亡会丢失该本地表；不能把它描述成跨进程找回保证。要支持这一点，还需独立的
持久执行关联协议，属于后续工作。

## 5. 存储故障的服务恢复状态机

```text
READY -> QUIESCING -> RECOVERING -> READY
                    |               ^
                    +-> BACKOFF -----+
                        （重新进入 RECOVERING）
不可安全确认旧 writer 停止 -> RESTART_REQUIRED
状态损坏 / 不支持的 schema -> MANUAL_REQUIRED
```

一旦写入不确定：先关闭准入/修改入口，返回可重试的 SERVICE_RECOVERING；保留健康
查询和认证后的恢复入口。writer 不再消费新 mutation，排队但未执行的写请求明确失败。
已有后台写可能继续执行，**取消 asyncio task 不代表线程停止**。

由单个恢复协调任务执行以下流程，自动触发与管理触发合并为同一次恢复：

1. 通过调度代次阻止旧 continuation 发布内存或响应成功，停止 drainer/monitor 修改。
2. 等待当前 writer I/O 真实结束，并确认没有旧写线程仍可能发出 GCS 请求。
3. 重放唯一 redo 日志，校验记录，在临时结构中重建所有状态。
4. 一次性替换内存状态，创建新 writer，启动监控/调度，再开放请求。
5. GCS 仍不可用时保持关闭，以指数退避和抖动重试，并输出次数、阶段、最近错误。

恢复协调任务不依赖被等待请求占满的 RPC 槽。不能原地清空 `_storage_error` 和
writer.error 后继续，不能只重建 writer 而沿用可能过期的 usage。

若 I/O 无法在有界期限内结束，进入 RESTART_REQUIRED。管理 CLI
`ray-admission queue recover` 验证管理员身份并报告状态；无法安全原地恢复时，
请求 Ray 终止旧 actor 进程并重建。它必须等到旧进程终止且旧 RPC 写入不再可能
影响新 writer，才能宣布恢复成功；仅名称消失、RPC 超时或固定 sleep 都不够。
必须用故障注入验证当前 Ray/GCS 的终止与在途写入顺序。如果不能证明这个前提，
该分支保持关闭并要求运维处理，不能宣称支持分区条件下自动切换。

正常可结束的短暂失败走自动原地恢复；卡死 writer 的强制重建作为受控兜底。
`queue start` 遇到 unhealthy 必须返回诊断和非零退出码，不能仅通过凭据检查就报成功。
`status` 至少增加 recovery_state、incarnation、last_successful_commit、重试次数、
writer_inflight 和恢复耗时。incarnation 用于诊断/客户端重新查询，不是 GCS fencing。

## 6. 索引、确认合并与监控预算

- 在恢复时构建 `released_by_session` 的有序序号索引；watermark 只访问该 session
  被覆盖的记录。旧 UUID 墓碑单独管理；不能每次确认扫描所有墓碑。
- 客户端按时间或数量合并累计确认，每 session 最多一个在途更新。服务端合并并发
  水位为最大值，尽量与其他批次同写；必须持久化后才删除墓碑、响应 durable ack。
- flush 失败保留待发送最大水位；正常退出尝试有界 flush，崩溃只影响回收速度。
  如果没有后续任务，定时 flush 仍会重试，解决“最后一次确认失败永不补发”。
- 活动许可不因确认而释放。会话记录回收仍需要退休协议，不能用 TTL 偷换重试保证。
- 监控按时间/数量预算处理引用；优先验证按被跟踪 actor ID 查询的接口。若仍需读取
  全表，降低频率并明确 O(集群 actor 数) 的成本，不能宣称已变成常量开销。

`benchmarks/lifecycle.py` 已统计完整任务链路的 RPC、存储量、延迟、吞吐和残留记录。
后续长期基准必须继续统计 enqueue/inspect/begin_submit/track/ack/release 的 RPC，
GCS get/list/put/delete 数和字节、吞吐、准入等待与端到端延迟的 p50/p95/p99、
控制器 CPU/内存、事件循环延迟、记录数及存储增长。对比完整旧/新协议，不能只比较
单个 commit。负载包含短任务、长任务、高竞争、大量已退出 session、不可压缩墓碑、
大量不受管 actor 和故障恢复；持续运行以观察每百万次提交后的增长。
报告硬件、Ray 版本、并发数、资源配额、样本数和运行时长，不预设未经测量的吞吐承诺。

当前本地 Ray 2.58 冒烟结果（2 CPU、100 个 0.01 CPU 短任务）：53.37 task/s，
提交 p50/p95/p99 为 16.906/24.325/48.630ms，100 次完成合并为 28 次确认 RPC，
最终许可和等待项均为零。该小样本只验证基准与指标链路，不是容量结论。

## 7. 必须新增的验收测试

| 编号 | 注入/负载 | 必须成立 |
| --- | --- | --- |
| R1 | quota=0，登记 2,000 请求或至少两倍实际调用并发上限 | status、quota set、track/release 在明确测试超时内完成；上调后能排空；无长期占槽 |
| R2 | enqueue/poll 洪峰，同时连续管理操作 | 背压有界，管理与完成通道不饥饿；记录队列长度和延迟 |
| R3 | 原始提交已执行并启动任务，返回句柄前抛异常/KeyboardInterrupt | 保留占用；下一请求不超额放行；可定位未知 ticket |
| R4 | cancel 与 begin_submit 并发、响应丢失后重试 | 只有安全取消或进入 SUBMITTING 一种结果；同 ticket 不重复执行 |
| R5 | journal 或第 K 个记录写成功后抛错，再恢复 GCS | 无需手工构造控制器即可自动恢复；quota/usage 一致；等待者继续协议 |
| R6 | writer 线程被阻塞，触发恢复；随后解除阻塞 | 新 writer 在旧线程结束前不启动；旧 continuation 不发布；管理健康查询可用 |
| R7 | writer 长期卡住、恢复触发并发、状态损坏 | 明确 RESTART_REQUIRED/MANUAL_REQUIRED；无双 writer，不假报成功 |
| R8 | 取得 handle 后 track 失败，包括已落盘但响应丢失 | 异常带原 handle/ticket；同 ticket 重试登记成功；业务只执行一次 |
| R9 | 合并确认期间崩溃、删除中断、旧 acquire 延迟到达 | 活动许可不丢；水位以下不复活；清理可继续 |
| R10 | 大量其他 session 墓碑及非受管 actor | 单 session 确认不全表扫描；完整生命周期成本和长期增长有测量结果 |

R1 的测试超时应作为 CI 预算明确记录，不等同生产 SLO。测试不能通过放大 actor
并发上限、无限等待或仅构造一个新 Python controller 来绕过目标故障。

实施顺序：协议版本/状态定义 → 短 RPC 与调度隔离 → 提交阶段与句柄关联 → writer
恢复协调及管理命令 → 索引/确认合并 → 故障矩阵和长期基准。每一步更新对应中英文
设计和已验证能力列表；全部验收前继续保持内部迭代状态。
