# Headroom002 本地迁移草案

本操作保留原 headroom001 的三个代表题（`scalar-add`、`rows-4096`、`string-1024k`）、10/20/40 rps、两个独立顺序块和 CPU 1/2 配对。共12个独立 JVM/driver 臂、36个题目单元。测量代码使用 A/A005 同一份529文件冻结异步 trace 源，IF4、响应验证、JVM观测和资源 collector 全部启用。CPU配额是配对内唯一的配置变量。

这是有限的 driver/warmup 工程诊断计划。三题、最高40 rps及两个块不能验收完整十一题或更宽负载范围，也不能给出服务端容量上界。旧 headroom001 原件及其阈值保持原样；本操作尚未在 Linux/HugeGraph 执行。

## 输入与公开入口

- 生成器：`.hgbench/http-driver-headroom-002-plan.py`，复用已锁定的 known-delay002 规划帮助代码。
- 执行器：`.hgbench/http-driver-headroom-002-execute.py`，复用已锁定的 known-delay002 归属检查及任务接口；新增的是固定 collector/JVM 顺序，无任意命令或场景钩子。
- 离线测量源：`evidence/phase2-http-aa-cohort-005/source`，锁定529成员；source lock文件 SHA-256为 `2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a`。
- 输入选择：`.hgbench/http-known-delay-002-selection`。JVM堆、服务器配置、镜像与005一致；本操作单独冻结所需的目标/driver/collector存活时间。
- 公开 `http generate/validate/plan` 为12臂生成36条实际离线规划命令；另有12条公开 `task plan` 验证 collector 模板任务。离线 collector 模板含明确的 `needs-live-binding` 类型，不能直接运行。Mac draft只使用本机Python验证该模板；Linux prepare和最终绑定固定使用runtime选择的hostPython。
- 公开 `http environment plan` 只在 Linux prepare 阶段生成12个目标任务。HTTP运行时使用一次前台公开 `task run`；正常测量中不启动周期性 Node 状态查询。

已有可审阅 draft：`.hgbench/http-driver-headroom-002-draft-v2`。它是不可执行的本地计划。首版 `.hgbench/http-driver-headroom-002-draft-v1` 在本机找不到Linux Python时退出，命令回执和失败原件保留。

## 固定顺序及质量边界

```text
前置单位与 source 检查
  -> 新目标 JVM / ready / forward mode
  -> 精确独占 driver / 公开 HTTP plan
  -> 绑定实际 target+driver CID / hostPID / start / boot / cgroup
  -> 公开 collector task / 第一份完整样本
  -> driver CPU起点 / 一次前台 HTTP / CPU终点
  -> collector controlled-stop / 原任务终态 / 独立原样本重验
  -> driver stop+remove / post-mode / target stop+remove
  -> 原始 HTTP 报告字节重建 / JVM原解析+独立重解析
  -> 固定窗口诊断 / 后置单位检查
```

每题预热30s、测量120s，timeout/drain各5s；响应 gzip，maxStartDelay1000ms，IF4。每题测量计划为1200/2400/4800请求。窗口10s、最少50请求；窗口P99仍因样本不足1000保持unknown。保留旧的原始CPU>0.8核或ELU>0.8压力门、全部原始run issues及暖机差异阈值；按配额归一CPU只作补充。CPU2无改善本身不能证明headroom。

collector首样与HTTP起止窗口、同一host boot、实际anchor身份绑定；CPU2臂独立验证期望配额为2核。正常路径先完成collector验证，再移除anchor。JVM只接受完整capture/raw、实际ready PID/boot、冻结环境计划与解析器重算一致。HTTP公开报告必须重建字节相同，且实际HTTP组件库存含 `trace-writer.mjs`。

late/drop/原CPU或ELU压力允许完成同率安全CPU配对，然后保留两块较高率为未执行。错误答案、其他质量问题、源漂移、OOM、空间或时间上限、取消、未知归属等终止后续派发。所有结果始终保留12臂、36单元分母；任务完成不会自动转为测量有效或验收通过。`performanceAccepted=false`、资格false，warmup/headroom验收仍为unknown。

## 完整预算

| 项目 | 固定上限或所需量 |
|---|---:|
| HTTP workflow / task / 前台等待 | 549 / 609 / 644s |
| driver完整所需 / TTL | 855 / 900s |
| target完整所需 / TTL | 1294 / 1500s |
| collector所需 / duration / task | 735 / 800 / 810s |
| collector interval / maxSamples / raw | 1s / 801 / 32MiB |
| JVM原解析与独立重解析 | 各10s执行 + 5s监督清理，共30s |
| 前、后单位检查 | 各共享10s |
| 单臂完整预算（含故障清理） | 2074s |
| 全12臂 + seal计算下限 | 25248s |
| 操作 / 父服务 RuntimeMax / TimeoutStop | 26000 / 26100 / 100s |
| 故障清理 | 全部角色共用90s |
| 活跃空间 / 最坏保留预留 | 16GiB / 14.9921875GiB |
| prepare所需空闲 / 运行空闲下限 | 96 / 80GiB |
| 父控制器 | 1CPU / 512MiB / TasksMax128 |

原25200s操作预算不足以容纳完整迁移，已前瞻调整为26000s；没有缩短原测量。每臂另有target/raw各512MiB、analysis96MiB、命令64MiB、JVM raw8MiB/parsed32MiB、observer proof2MiB上限。公开plan的实际task budget必须匹配上述值，不能缩短工作适配预算。

故障时只查询/取消原collector与HTTP任务，不重派；query/cancel失败仍进入精确owned容器清理。collector先使用共享90s中的至多15s，剩余时间供driver和target；不可在子阶段重置总截止。JVM verifier的detached parser owner在取消时先收TERM并获得最多3s清理，然后精确组KILL/reap，计入各自15s总预算。父systemd必须使用 `KillMode=mixed`、100s停止宽限，给控制器完成90s清理。

## 运行前尚需完成

A/A005及known-delay002必须真实终态、精确owned资源已清理并由协调者审阅；prepare中的单位inactive/not-found只证明当前无live unit，不替代历史成功或全机独占。protected d6只记录不停止。当前005运行中禁止prepare或dispatch本操作。

待协调者批准后，创建全新Linux目录，用同529源码运行 `prepare`，显式传入 `--require-inactive-unit hgbench-http-aa-cohort-005.service` 和 `--require-inactive-unit hgbench-http-known-delay-002.service`。再用公开的固定 `inspect` / `freeze --expected-plan-sha256` 核验新锁。实际 `run` 只能读该freeze；全局一次claim，响应丢失只读原state/status，禁止重派。此本地交付没有执行上述步骤，也没有远端派发材料。

本地专测验证的是实际离线CLI规划和合成协议故障路径，包括真实本机子进程取消/reap；Docker、HugeGraph、procfs/collector采样及性能证据仍待实际执行。
