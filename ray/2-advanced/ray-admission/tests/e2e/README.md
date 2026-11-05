# Queue 端到端验收

本套件通过**安装后的 `ray-admission` CLI 子进程**管理服务，通过业务进程中的
`enable_admission()` 和真实 `.remote()` 提交工作。使用真实本地 Ray、GCS KV、
task 和 actor，不 mock 控制器 RPC 或存储。仅用环境变量 patch 模拟业务 namespace。

## 运行

在安装了 Ray 2.58 的 Python 虚拟环境中，从包根目录执行：

```bash
python build_wheel.py
python -m pip install --no-deps --force-reinstall dist/ray_admission-0.1.0-py3-none-any.whl
python -m unittest discover -s tests/e2e -p 'test_*.py' -v
```

测试使用当前 Python 旁的 `ray-admission` 可执行文件，需先安装 wheel。
测试代码和 worker 必须能导入同一个安装版本；不要以另一份 `PYTHONPATH` 覆盖安装包。
每个用例创建 3 CPU 的独立本地 Ray 集群，CLI 使用其明确的 GCS 地址连接，
不使用 `auto` 连接已有集群，也不会清空用户集群的 KV。需要允许启动本地进程和监听端口。

本目录不加入普通 `tests` 发现路径的递归包，需显式执行上述端到端命令。
较底层的故障注入仍由 `tests/test_controller_faults.py` 负责。

## 场景与验收条件

| 用例 | 操作链路 | 核心断言 |
| --- | --- | --- |
| 启动、发现和权限 | list → 未启动时 enable → CLI start → 重复 start → quota set → 错误令牌操作 → CLI status | 未启动不可由业务创建；定位固定；重复启动复用 actor；错误令牌不能启动验证、修改或删除配额；业务 namespace 正确；后端为 GCS KV |
| remote 与动态配额 | 设置 CPU=0 → 异步发起 remote → CPU=1 → 再降到 0 → 第二个 remote → 删除配额 | 零配额请求尚未执行；上调放行；下调不抢占；第二个请求等待；删除配额后放行；完成后 usage 清零 |
| 运行 task 和等待者自动恢复 | CPU=1 → task 1 持有许可 → task 2 等待 → kill 控制器且允许重启 → 放开 task 1 | quota/usage 恢复；task 2 重试后仍等待；task 1 只进入一次；释放后 task 2 执行；最终无残留占用 |
| 控制器停机期间 actor 死亡 | 创建业务 actor → 禁止控制器自动重启并终止 → 终止业务 actor → CLI start → 创建替代 actor | 从 GCS 恢复配额；识别旧 actor 终态并回收；替代 actor 可获得同一配额；再次释放后 usage 清零 |

task 使用零资源 Gate actor 控制执行完成，因此阻塞/释放断言不依赖固定执行时长。
轮询等待实际 waiting、usage 和执行入口记录，超时输出控制器状态。
CLI 有 45 秒进程超时，Ray 状态读取和结果等待也设置超时。
失败清理尝试放开 Gate、删除测试 Job 配额并关闭控制器和本地集群。

## 测试边界

- 这里验证安装包、CLI 和准入生命周期的组合行为，不是 ACK/Kubernetes 部署测试。
- 自动重启采用 Ray 的 `ray.kill(..., no_restart=False)`；没有模拟 GCS 进程故障、
  网络分区或整个集群丢失。
- 业务 driver 是测试进程，管理 CLI 是独立进程，task/actor 在真实 worker 中执行。
- 部分批次写入、响应丢失、水位压缩和取消竞态见故障注入套件；多 Job、嵌套提交、
  多返回值、资源向量、FIFO 和运行 actor 自动重建见 `tests/test_queue.py`。
- 测试不会修复或掩盖设计中 RESERVED→TRACKED 的孤儿许可窗口，也不据此声称生产可靠性。
