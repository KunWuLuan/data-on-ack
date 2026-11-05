# Ray Admission Queue 设计文档

状态：内部迭代中的实验性实现。本文以 `7c8250d` 的代码为基线，后续仅补充文档，
区分已经实现的行为和后续建议，不代表已达到生产可用标准。
[English design](DESIGN.md) 与本文描述同一套设计。

**评审结论：当前实现不能按“故障后可自动恢复的配额服务”验收。**
可靠性协议、实施情况与验收门槛见 [可靠性改进设计](RELIABILITY_PLAN_ZH.md)。
核心修复已经实现，完整验收矩阵通过前仍保持内部迭代状态。

## 1. 目标与范围

Ray Admission 在 Python task 和 actor 提交前增加协作式准入检查。控制器按
`(quota namespace, Ray Job ID)` 限制已准入工作的声明资源总量。配额不足的请求
在提交到 Ray 之前等待；Ray 仍然负责物理资源调度、放置、执行和执行重试。

安装包独立于 Ray 源码发行版。管理员、driver、worker 使用同一 wheel。目前的
验证基线是 Ray 2.58。由于使用了私有 Python 接口，其他 Ray 版本需要单独验证。

设计目标：

- 统一 admission CLI，通过模块发现加载不同实现；queue 是当前的一种实现。
- 固定控制器定位，按业务 namespace 和 Job 分账，由管理员配置资源上限。
- 明确许可何时占用、何时释放，以及控制器重建后的处理方式。
- 使用 GCS KV 增量记录、有界写批次和可安全压缩的重试记录。
- 存储或完成状态不确定时保留占用，并暴露故障信息。

当前不包含：

- 面向恶意租户的安全隔离，或对直接访问 GCS 的客户端进行约束。
- Ray 调度器内部强制执行，以及 Java、C++、Ray Client 提交拦截。
- 仅扫描 task 表恢复全部状态、跨 Ray 集群恢复或额外数据库依赖。
- 持久化等待顺序、全局公平调度、抢占、自动配置 Job 配额，以及自动修复所有孤儿许可。

## 2. 组件与职责

| 组件 | 文件 | 职责 |
| --- | --- | --- |
| CLI 发现 | `src/ray_admission/cli.py` | 枚举 admission 实现并分发命令 |
| 客户端适配 | `src/ray_admission/queue.py` | 资源提取、ticket、拦截、worker 传播、管理入口 |
| 状态服务 | `src/ray_admission/_controller.py` | 配额、FIFO、许可、跟踪、完成检测、压缩 |
| 持久化 | `src/ray_admission/_storage.py` | GCS 记录、redo 恢复、旧格式迁移、后台写入 |
| 构建 | `build_wheel.py` | 将包内模块和 CLI 入口打入纯 Python wheel |

请求路径：

```text
driver / worker 调用 .remote()
  -> 客户端包装器 -> 短 enqueue/inspect RPC，等待留在客户端
  -> 对应配额键的 FIFO -> 持久化 RESERVED -> 持久化 SUBMITTING
  -> 调用 Ray 原始提交方法 -> 得到 ObjectRef 或 actor handle
  -> controller.track / track_actor -> 持久化 TRACKED
  -> 客户端推进确认水位

控制器完成监控
  -> 检查 ObjectRef ready / GCS actor 状态
  -> 持久化释放或删除 -> 更新 usage -> 放行等待请求
```

CLI 通过 `pkgutil.iter_modules(ray_admission.__path__)` 搜索当前包的直接子模块，
跳过子包、私有名称和 `cli`，注册具有可调用 `register_cli` 的模块。
可工作的实现还需要提供 `DESCRIPTION` 和 `run_cli`。目前会导入被发现模块，
不递归搜索、不隔离模块导入失败，也不是基于 entry point 的第三方插件体系。

现有命令包括 `ray-admission list`、`ray-admission queue ... start`、`status`、
`quota set` 和 `quota delete`。尚无专门的 stop/restart CLI。

## 3. 身份、定位与管理权限

| 身份 | 来源 | 用途 |
| --- | --- | --- |
| 控制器 actor name | 固定 `ray-submission-queue` | 所有客户端使用同一名称查找 |
| 控制器 Ray namespace | 固定 `ray-submission-queue` | actor 查找作用域 |
| GCS KV namespace | 固定 `ray-submission-queue` | 内部存储作用域 |
| 业务 quota namespace | driver 的 `RAY_NAMESPACE`；缺省 `ray-submission-queue` | 配额键第一部分 |
| Job ID | Ray runtime context | 配额键第二部分 |
| Session ID | 每次客户端安装拦截时生成 UUID hex | 重试和压缩作用域 |
| Ticket | `<session>:<正整数序号>` | 一次被拦截的提交 |

启动和查找都不暴露控制器 name/namespace 配置。Kubernetes Downward API 可以将
`metadata.namespace` 注入 driver 的 `RAY_NAMESPACE`，用于业务配额定位，不改变
控制器的位置。改变业务 namespace 需要先禁用再启用本地拦截。新 worker 通过
启动 hook 继承已选择的业务 namespace。

`start(admin_token=...)` 创建或验证 detached 控制器。令牌使用 SHA-256 摘要，
比较采用恒定时间比较函数；已有状态必须使用原有凭据。修改配额要通过管理员验证。
业务侧 `enable_admission()` 只查找已存在的控制器并确认就绪，不创建控制器，
也不会取得管理权限。

这只保护预期的管理 API。准入 RPC 和会话水位信任协作客户端；能绕过包装器、
直接调用内部 RPC 或修改 GCS 的用户仍能破坏策略。因此它不是租户安全边界。
当前没有自动凭据轮换协议。

## 4. 集成与提交过程

1. 管理员、driver 和 worker 环境安装同一 wheel。
2. 管理员启动 detached 控制器。
3. 确定业务 namespace 和实际 Ray Job ID，在需要受限的业务提交发生前设置配额。
4. driver 在 `ray.init(address="auto")` 后调用 `enable_admission()`。
5. 使用正常的 Ray task/actor API 提交，不需要任何按 Enter 的交互步骤。

配额下发属于作业编排流程。未配置的配额键默认无限制，因而“先运行业务、稍后设置
配额”不是默认拒绝的流程。当前代码没有自动建立 Job 与配额配置之间的启动屏障。

拦截点是 `RemoteFunction._remote`、`ActorClass._remote` 和
`ActorHandle._actor_method_call`，覆盖此前已定义的 remote 对象和
`.options().remote()`。控制器自身 RPC 走 bypass，避免递归准入。
资源向量来自生效的 Ray options；actor 默认 CPU 按适配层的生命周期计费逻辑处理。
需要明确计费时建议显式声明 actor 资源，并在升级 Ray 时验证默认语义。

客户端通过短 RPC 轮询等待 RESERVED，再调用 `begin_submit` 和 Ray 原始提交方法，
所以 `.remote()` 仍可能阻塞
调用线程。在 async event loop 中直接调用也会阻塞事件循环，目前没有异步准入 API。

worker hook 保留原有 setup hook，再安装 admission，并复用 hook 标记以避免嵌套
提交时无限追加 hook。会话在首次被拦截的提交时才打开，不在 worker 初始化 hook
内进行会话 RPC。既有 actor 和其他 driver 不会被追溯安装拦截。
driver 调用 `disable_admission()` 只恢复自己的方法，不影响已启动 worker；
不支持一边并发提交、一边启用或禁用拦截。

## 5. 配额模型与 FIFO

对配额键 `q`，已占用资源为 `U`，新请求为 `R`，已配置上限为 `L`。
对 `L` 中每个资源维度，都满足 `U[r] + R[r] <= L[r] + 1e-9` 才可放行。

- 没有配置的配额键不限流；已配置向量中没有列出的资源维度也不限流。
- 显式配置为零会阻止该维度的正资源请求。
- 资源量必须有限、非负，不能使用布尔值；请求中的零资源需求会被移除。
- quota set 替换整个资源向量；quota delete 删除限制。
- 下调配额不抢占或取消已有工作，新请求等待占用下降或管理员调整配额。
- usage 表示已准入的声明需求，不是实测 CPU/GPU 用量，也不只是 RUNNING task。
  已准入但尚在 Ray 内等待调度的工作仍然占用许可。

每个配额键维护独立 FIFO。队头放不下时，该键后面即使有更小的请求也不能越过，
其他配额键仍可处理。没有加权公平或全局顺序保证。drainer 每批选择最多 32 个
队列条目，按预期 usage 检查、持久化预约，再完成对应的准入 future。

父任务持有整个配额并等待同键子任务时可能死锁。当前没有资源借用或父子依赖检测。
需求超过上限的队头可能一直等待，直到配额变化或请求取消。

## 6. 许可生命周期与不变量

| 状态 | 是否占用 | 持久化内容 | 离开条件 |
| --- | --- | --- | --- |
| WAITING | 否 | 无许可记录；客户端轮询，FIFO 在控制器内存中 | 配额足够或取消 |
| RESERVED | 是 | ticket、配额键、资源向量 | begin_submit 或安全取消 |
| SUBMITTING | 是 | 已进入提交边界、结果可能未知的预约 | 跟踪或有证据的核对 |
| TRACKED | 是 | 预约信息及序列化 ObjectRef / actor ID | 检测到完成或显式释放 |
| RELEASED | 否 | 墓碑记录，等待安全压缩 | 被会话确认水位覆盖 |

必须保持的不变量：

1. 返回资源准入成功前，先持久化 RESERVED。
2. 重启后从 RESERVED/SUBMITTING/TRACKED 恢复 usage，不能将占用直接清零。
3. 持久化释放成功后，才减少 usage 并允许替代工作进入。
4. 同 ticket、同配额键、同资源的活动请求复用预约；键或资源冲突时拒绝。
   重复登记和查询保持幂等。
5. 已释放或已确认的 ticket 不可再次申请。
6. 跟踪重试只能重复相同执行对象，不能替换成另一对象，也不能同时跟踪 task 和 actor。
7. 无法确定写入结果时停止后续准入，等待恢复。

task 的全部结果 ObjectRef 都 ready 后释放。任务失败但错误结果已 ready 也算完成，
无需取回结果内容。消耗资源的 generator，以及没有正数固定返回值的 task 会被拒绝，
因为当前机制不能准确跟踪其结束。零资源工作不需要持久化资源预约。

actor 创建后持有声明的生命周期资源，直到 GCS 状态为 `DEAD`。
actor method 会被拦截和计数，但不重复预约资源。复用 named actor 不能双重计费：
客户端处理 `get_if_exists`，控制器发现相同 actor ID 已被跟踪时释放重复预约。
actor 暂时处于重启过程时继续保留占用。

确认水位表示客户端已经完成提交/跟踪协议，不表示任务执行结束。已确认的活动许可
仍然占用配额，不能因为水位推进就释放。

## 7. 提交过程中的故障窗口

| 故障位置 | 当前行为 |
| --- | --- |
| 准入前 | 尚未向 Ray 提交；请求可重试或取消 |
| RESERVED 已写入，准入响应丢失 | 重试相同 ticket 复用原预约 |
| SUBMITTING 后原始提交抛错 | 保留占用，以 `SubmissionOutcomeUnknown` 返回 ticket |
| Ray 已接受，客户端在 track 前退出 | SUBMITTING 保留，无法自动推断执行完成 |
| Ray 已接受，但 track 失败 | `SubmissionTrackingError` 携带原句柄和重试接口 |
| TRACKED 已写入，响应丢失 | 同执行对象可幂等重试 |
| 确认响应丢失 | 后续提交完成时重发累计水位 |
| 释放已写入，响应丢失 | 重复释放安全，不能重新激活 ticket |

SUBMITTING 到 TRACKED 的窗口仍然存在。只扫描运行中的 task，不能可靠地把未跟踪
ticket 与执行对象关联，也不能区分延迟提交和真正失败。因此不会仅凭超时回收。
管理员必须先确认真实执行状态再显式协调释放。目前尚无专门的安全孤儿许可处理 CLI。

## 8. GCS KV 数据模型

以下键都位于固定的内部 KV namespace。记录值使用 JSON；管理员摘要和迁移标记
使用简单编码值。

| 键 | 内容 |
| --- | --- |
| `auth-v2` | 管理员令牌摘要 |
| `pending-v2` | 最多 32 条记录替换/删除的 redo 列表 |
| `records-v2/quota/<encoded-key>` | `key: [namespace, job]`、`resources`、`revision` |
| `records-v2/permit/<ticket>` | `state`、活动记录的 `key/resources`、可选 `refs/actor_id`、`revision` |
| `records-v2/session/<session>` | 连续确认 `watermark`、`revision` |
| `controller-state-v1` | 保留供检查的旧整份快照 |
| `migrated-v1` | 旧快照已完成复制的标记 |

quota 键后缀是 JSON 二元组的 URL-safe base64。task 引用通过
`ray.cloudpickle.dumps(ObjectRef)` 后 base64 编码保存；只保存十六进制 ObjectRef
ID 不足以满足本实现的所有权和 ready 检测行为。恢复的引用只属于原 Ray 集群，
可能在许可清理前继续持有分布式引用，其生命周期开销还需大规模验证。
actor 记录保存 actor ID。usage 从许可推导，不独立持久化。
revision 用于诊断，不提供 compare-and-set 保护。

## 9. 写入协议与并发

控制器修改由一个 `asyncio.Lock` 串行化。后台 writer 具有容量为 32 的请求队列和
单个消费者，同步 GCS 调用在线程中执行，使存储等待不阻塞 actor 事件循环。
每次 commit 最多 32 条变更；较大的释放和压缩集合拆批处理。
这个上限不限制等待请求数量、控制器总内存或全部保留数据。

每个非空批次的步骤：

1. 将完整变更列表写入 `pending-v2`。
2. 顺序执行各记录 put/delete，替换和删除都可重复执行。
3. 删除 `pending-v2`。
4. 更新相应内存状态，再返回成功或完成准入 future。

构造控制器时先重放未完成日志，再加载记录、接受请求。写响应失败不等于写入未发生。
控制器将存储标记为故障，使等待请求失败并拒绝后续修改，不猜测性推进内存状态。
取消也可能与后台写线程并发，因此同样按不确定写入处理。writer 自身也会记住失败。

这里的 GCS KV 没有多键原子事务，也没有独立 fencing token。正确性依赖固定命名
actor 单写，并且在重建前旧进程已经终止。redo 日志是恢复意图，不是分布式锁。
外部并发读取可能看到部分批次；不支持外部修改或多个 writer。
存储恢复连通不会自动清除控制器的故障标记，需要重建控制器。

## 10. 自动重建与恢复顺序

detached 控制器设置 `max_restarts=-1` 和 `max_task_retries=-1`，生命周期独立于
启动它的 driver。非预期 actor 故障可以触发重建；显式禁止重启的终止操作不同，
之后需要管理员重新启动服务。

重建顺序：

1. 如有旧快照，检查版本和凭据，再验证 `auth-v2`。
2. 重放未完成的 `pending-v2`。
3. 必要时完成旧格式迁移。
4. 列出并读取各条记录，恢复 quota、session 和 permit。
5. 对活动许可求和恢复 usage，反序列化 task 引用并恢复 actor ID。
6. 启动完成监控和清理，接受重试或新请求。

控制器重建不会重启它管理的业务 task/actor。运行中的许可继续计费。
task 在控制器停机期间完成，恢复后的引用可以检测 ready；actor 在此期间死亡，
GCS 返回 `DEAD` 后即可回收。查不到 actor 状态不等于死亡，仍然保留预约。
如果终态在监控前已被清理，可能需要人工核对。

WAITING 不持久化。仍然存活的在途 actor RPC 由 Ray 重试，重新进入队列，因此
不能保持原有全局或单键等待顺序。不再重试的客户端不会凭空恢复为等待项。
准入计数重置。恢复需要读取 O(N) 条保留记录，不是常数时间启动。

GCS 状态的可用性取决于集群自己的持久化配置。容忍控制器重启不等于能够容忍整个
集群或全部 GCS 状态丢失，也不支持将保存的 ObjectRef 转移到替代集群。

## 11. 完成监控与可观测性

每轮监控对全部 task 引用去重，在线程中执行非阻塞 `ray.wait`。
actor 每秒最多按 ID 查询 64 个受管 actor，并轮转选择，不再读取整个 actor 表。

正常轮询间隔为 0.1 秒。查询失败记录 `monitor_error`，退避上限为 5 秒，不释放
状态不确定的许可。成功后清除错误。存储故障后停止监控中的状态修改。

`status` 返回配额、推导 usage、等待请求、活动许可，以及：

- `health.storage_error`：持久化是否进入故障状态。
- `health.monitor_error`：最近一次完成查询错误。
- `health.accepting`：是否没有存储故障。
- 存储 batch/write/byte 计数、保留墓碑数和 session 数。

计数是当前进程的诊断数据。`accepting=true` 不保证配额充足、监控健康或端到端延迟。

## 12. 会话确认水位与安全压缩

每个 session 的 ticket 序号单调递增。客户端记录已完成提交协议的序号，只推进连续
水位。如果 2 先于 1 完成，水位不能越过 1。确认是累计值；后续完成会重发之前可能
失败的确认。

控制器先持久化水位，再分批删除其覆盖的 RELEASED 记录。若在两步之间失败，
重建保留水位并继续清理。低于水位的延迟 acquire 即使找不到墓碑也会被拒绝。
低于水位的活动许可仍保留计费，直到完成时才直接删除。

每次客户端安装产生的 session 记录仍会长期存在。客户端退出、序号缺口、旧 UUID
ticket 都可能使墓碑无限保留。不能用 TTL 假设任意延迟重试已经结束。
确认失败后若再无后续提交完成，压缩也可能一直延迟。水位协议信任客户端的确认，
这也是它只能用于协作式准入的原因之一。

## 13. 存储开销与测量结果

从整份快照改为记录级增量后，一次写入的载荷从 O(全部保留状态) 降为
O(本次变更记录大小之和)。K 条变更需要 K+2 次 KV 修改：一次日志 put、K 次记录
操作和一次日志 delete。优化减少序列化和载荷，不保证减少 RPC 次数。
完成检测和客户端确认还会产生额外批次，下面测的是单次存储更新，不是完整提交周期。

内存和恢复成本为 O(保留 quota + permit + session)，另外还有等待请求和客户端
乱序确认集合。当前压缩会扫描保留墓碑，旧格式迁移会在内存中生成全部变更列表。
这些扩展性成本并未因单批 32 条的限制而消失。

本地 Ray 2.58，每个规模进行 100 次单记录更新：

| 记录数 | 每次 put 键值字节 | KV 修改次数 | p50 / p95 毫秒 | 记录加载毫秒 |
| --- | --- | --- | --- | --- |
| 10 | 207.88 | 3 | 0.479 / 0.589 | 1.890 |
| 100 | 207.88 | 3 | 0.443 / 0.495 | 15.151 |
| 1000 | 207.88 | 3 | 0.444 / 0.503 | 151.631 |

字节数不含 RPC framing 和 delete 请求字节。加载时间不含完整 actor 构造和 task
引用恢复。可在隔离本地环境运行 `PYTHONPATH=src python benchmarks/storage.py`。
结果验证该负载下更新载荷不随活动集增长，不代表生产吞吐量或容量承诺。

## 14. 旧快照迁移与运维

先停止旧的命名控制器，再启动新实现。接受业务请求前校验 `controller-state-v1`
的版本和原始凭据，分批复制 quota/permit/released，最后写 `migrated-v1`。
因为复制完成前不接受新修改，中断后可以重放。错误令牌不能初始化 `auth-v2`，
否则会阻止合法的旧状态迁移。

旧快照保留供检查，但迁移标记写入后不再是权威状态。回退到仅读取旧快照的代码会
恢复过期状态，不属于受支持的回滚方式。不要通过删除在线 GCS 键来修复控制器，
这可能丢失已占用许可并导致超额放行。

排障先检查存储健康，再检查监控错误、活动许可状态、FIFO 队头和实际执行状态。
重建必须确保旧 writer 已停止。显式释放疑似孤儿许可之前，应有证据证明对应执行
已经结束或从未存在；目前既没有自动证明，也没有专门的管理命令。

## 15. 验证情况与内部迭代计划

GCS 转换通过了 48 项回归测试。之后补充旧快照/令牌测试，再运行 12 项故障测试
通过。这两个测试集合有重叠，不能宣称为 60 项独立测试。
wheel 已重新构建并安装，验证了安装后 CLI 的 `list` 和 `queue start --help`，
Ruff 和 diff 空白检查通过。

已覆盖活动 task/actor 恢复、控制器停机期间 actor 死亡、重复跟踪和等待、提交响应
丢失、写入拒绝与取消、部分批次重放、水位压缩、有界放行以及监控瞬时故障。
这些是本地测试；网络分区、GCS 故障切换、大集群和长时间内存/引用行为仍需验证。

后续建议，不是现有能力：

1. 建立持久的提交与执行关联协议，提供安全的孤儿许可核对工具，缩小或明确处理
   SUBMITTING 到 TRACKED 的窗口。
2. 定义 session 退休和重试上限协议后再回收水位；允许任意延迟重试时不能仅按时间删除。
3. 优化压缩索引和 actor 查询，增加等待请求上限，长期测量存储及 ObjectRef 引用成本。
4. 对 actor 终止、GCS 中断和重建进行故障注入，专门验证单 writer 假设。
5. 补充 stop/restart/reconcile 管理能力，将非交互式配额下发纳入作业提交流程。
6. 对 Ray 私有 API 增加兼容性门禁和明确的支持版本矩阵。

具体使用命令见 [README_ZH.md](README_ZH.md)。本文不会将上述后续计划描述成
已经实现的保证。
