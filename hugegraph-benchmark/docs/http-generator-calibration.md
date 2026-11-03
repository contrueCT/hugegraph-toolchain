# HTTP 驱动独立校准

用户已确认[独立驱动校准 + 真实 HG 逐点检查](http-go-driver-calibration-v2-proposal.md)。此工具使用真实 HTTP 和完整答案检查，环境类型固定为 `http-generator-calibration`。它验证发生器能否承载指定工作，结果不代表图数据库的吞吐量或容量。

```text
冻结的题集和独立答案 --> 预制 identity/gzip 资产
                                  |
                      Node provider：1CPU / 512MiB
                                  |
                    Go driver：4 / 6 / 6 / 4 CPU
                                  |
            原始到达 + 答案 + 进程身份 + 独立资源采集
                                  |
                  离线 JSON / 可筛选 HTML 报告
```

TS CLI 组织任务，Node 组件准备和发送固定字节，独立 Go 组件执行 HTTP，宿主 Python 控制 Docker 并采集原始身份和资源。报告组件重新核对全部原始证据，不执行归档中的程序。正式校准的判据、题目和执行顺序在运行前冻结。

## 公共入口

```bash
hgbench http generator --help
hgbench http generator describe
hgbench schema http-generator-calibration-request
hgbench http generator plan request.json --out /absolute/new-study --dry-run
hgbench http generator plan request.json --out /absolute/new-study
hgbench http generator verify /absolute/new-study
hgbench --root /absolute/task-state task run \
  --plan /absolute/new-study/task.json --request-id generator-001 --detach
hgbench --root /absolute/task-state task status --request-id generator-001
hgbench --root /absolute/task-state task wait --id <task-id> --timeout-ms 30000
hgbench --root /absolute/task-state task cancel --id <task-id>
hgbench http generator report /absolute/new-study --out /absolute/new-report
```

`describe` 输出严格 JSON schema、固定资源、覆盖范围和预算语义。`plan --dry-run` 不写文件或发送请求。正式计划准备预制 gzip，冻结题目请求、完整响应字节、题集和来源摘要，再产生通用 `task` 计划。所有输出均为新目录；原执行失败、中断或 SSH 断开后，使用同一个任务标识查询和收尾，不删除 `output` 重放。

## 固定执行与预算

- 全 11 题，每题分别执行 identity/gzip，22 单元/臂，共 88 单元。
- open 80rps、IF64、GOMAXPROCS4；驱动 3GiB，CPU 依次为 4/6/6/4，形成两个平衡 block。
- provider 1CPU/512MiB，独立 cgroup，仅共享网络命名空间。容器以当前非 root UID/GID 运行，只读源码和运行时；绑定写入目录单独记录。
- 正式窗口每单元预热 30 秒、测量 60 秒，计划测量到达 422,400 次，预热 211,200 次，另有 176 次前后服务检查。固定窗口共 132 分钟，另有准备、握手和收尾耗时。
- 默认整任务 8 小时上限，预留 100 秒清理；证据上限 4GiB、宿主剩余至少 80GiB。预算保护有明确轮询间隔，可能发生一个轮询间隔的超额。
- `budget.mode` 默认 `total`，可显式选择 `per-arm`。后者给每臂独立时间上限，通用任务最大时间相应乘以 4；不会减少题目、替换失败臂或缩短冻结窗口。
- `development-verification` 用于安装、协议和生命周期验证；短窗口始终不具备正式校准资格。正式的 `engineering-calibration` 固定 30/60 秒等规则，不接受隐式降级。

运行前核对 Linux、CPU 数量、同一 Go binary、原独立时钟证明、Docker 镜像及代码资产。每个逻辑 Go 进程都有 ready/finished 两侧宿主观察，核对实际 PID/start、可执行文件、namespace、cgroup 及 nonce 回执。进程退出成功仍需经过数据核验和质量门。

## 核验和结论

报告始终保留 88 个单元。未执行、部分证据或独立身份不可核验的单元保持 unknown；存在错误、拒绝、迟到、资源压力或预热/趋势未达要求时不授予资格。provider 的发送计数和字节数与实际 driver 准入及前后检查独立对账。provider 的资源压力使相关驱动结论保持未合格。

每个题目/编码的四臂 P99分别核对两组 CPU 对照和同 CPU 的跨 block 复现，工程容差为 `max(1ms, 10% × 较小值)`。10 秒窗口 P50/P95 检查预热过渡和测量趋势；低样本保留 unknown。Go 的原资源门保持：CPU 份额超过声明配额的 80%、出现节流、缺失资源或超出 RSS 边界，都不能通过。独立宿主 collector 保留 provider/driver cgroup、宿主 I/O、CPU、内存和压力证据。保守的 workflow 边界区间包含准备与 drain，不进行逐请求因果归因。

`driverHeadroomEngineeringAccepted` 只针对冻结环境和负载的工程资格。`performanceAccepted`、`serverCapacityAccepted` 和 `statisticalCertificationAccepted` 均保持 false。真实 HG 的每个负载点仍须独立核对同 binary/资源/执行策略、全部正确性、完成窗吞吐、延迟、拒绝/错误/迟到、驱动 CPU/RSS/节流和实际 HG 环境。正式 HG 继续逐次验证关闭 memory monitor 手动 GC 和宿主 swap。

全 11 题吞吐曲线、独立 4 对/8 个新 JVM 确认、A/A、已知退化检测、获批三题观测成本、forward/embedded 和 SF100 范围均保持。
