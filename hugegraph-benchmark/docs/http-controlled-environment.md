# 受控 HugeGraph HTTP 单臂环境

本组件为 M2 提供一次 `forward` 或 `embedded` 的完整生命周期。它使用同一份固定 distribution、JDK 17 和镜像，在新目录初始化空 RocksDB，不加载用户图数据。当前仅接受 `functional`、`calibration` 用途；ready 表示 `1+1` 的完整 HTTP 答案正确，实际模式由独立 mode probe 核验。

```text
离线 settings → 新 attempt / plan + 内容锁 + 独立配置
                         │
                  serve（前台监督）
                         │ Docker create/start，单个精确归属容器
                         ▼
        容器内 watchdog → JDK17 检查 → InitStore → HugeGraphServer
             │               TTL / 日志 / 磁盘看护       │
             └──────────── stop / 预算耗尽 / 服务退出 ───┘
```

无需 shell、在线安装或镜像拉取。镜像必须已存在，使用完整 `sha256:<64 hex>` image ID；包含 settings 指定的 Python 3 路径及 JDK/RocksDB 所需动态库。计划阶段只读取文件并冻结身份，不启动 Docker 或执行 Java。运行主机必须为 Linux、使用非 root UID/GID，并允许该用户调用本机 Docker Unix socket。

## 命令与输入

```bash
python3 components/http/hugegraph-environment.py plan --settings settings.json --out /absolute/new-attempt
python3 /absolute/new-attempt/hugegraph-environment.py serve --attempt /absolute/new-attempt
python3 /absolute/new-attempt/hugegraph-environment.py status --attempt /absolute/new-attempt
python3 /absolute/new-attempt/hugegraph-environment.py stop --attempt /absolute/new-attempt
```

`serve` 前台运行，适合由通用 `hgbench task` 或 systemd 管理。断线后调用同一 attempt 的 `status`；再次 `serve` 会拒绝。`stop` 可以重复调用，只作用于核验通过的精确容器 ID。停止后的数据、退出状态和日志保留；需要新运行时创建新的 plan 目录。输出为 `{schemaVersion,ok,command,data}` JSON，失败为 `{...,ok:false,error}`、退出码 6。`--help` 为文本；计划父目录需已存在。

settings 为严格 JSON 对象，以下示例仅定义功能验证预算，实际性能参数仍须校准：

```json
{
  "schemaVersion": 1,
  "kind": "hugegraph-http-environment-settings",
  "purpose": "functional",
  "distribution": "/absolute/apache-hugegraph-server-1.8.0",
  "javaHome": "/absolute/jdk-17.0.17+10",
  "image": "sha256:REPLACE_WITH_64_LOWERCASE_HEX",
  "python": "/usr/bin/python3",
  "dockerSocket": "/var/run/docker.sock",
  "mode": "forward",
  "budget": {
    "cpus": 2,
    "memoryBytes": 4294967296,
    "pids": 512,
    "wallSeconds": 1800,
    "startupSeconds": 300,
    "shutdownSeconds": 10,
    "maxAllocatedBytes": 2147483648,
    "maxLogicalBytes": 4294967296,
    "maxLogBytes": 67108864,
    "minFreeBytes": 85899345920,
    "tmpfsBytes": 268435456,
    "pollSeconds": 1
  },
  "jvm": {
    "heapMinBytes": 1073741824,
    "heapMaxBytes": 2147483648,
    "maxDirectMemoryBytes": 536870912,
    "activeProcessorCount": 2
  },
  "server": {
    "gremlinPool": 4,
    "bossThreads": 1,
    "workerThreads": 4,
    "maxWorkQueueSize": 1024,
    "httpWorkerThreads": 16,
    "maxRoute": 16,
    "timeoutSeconds": 30,
    "idleTimeoutSeconds": 30,
    "connectionMaxRequests": 10000,
    "maxContentLengthBytes": 65536,
    "resultIterationBatchSize": 64
  }
}
```

上述字段均必需；另允许可选 `jvmObservation`，其余未识别字段拒绝。mode 仅为 `forward|embedded`，purpose 仅为 `functional|calibration`。CPU 为 0.1–64 核，内存 512 MiB–128 GiB，PID 64–4096，TTL 5–86400 秒，启动 1–3600 秒且不超过 TTL，停止宽限 1–30 秒，目录检查间隔 0.1–5 秒。参数关系和完整上下限由 `validate_settings` 检查；heap 最大值、direct memory 和至少 128 MiB 的剩余空间必须放入容器内存上限。内存留量是否足够仍需真实启动/校准。

`jvmObservation` 为 [GC/JIT 组件](http-jvm-observation.md) 中完整 spec 的内联对象，必须显式配置。
`maxRawBytes` 必须等于 `budget.maxLogBytes`：版本检查、初始化、server 输出共享这一个硬上限，
因此实际 `server.log` 能保留的空间还会扣除前两阶段的输出。观测含应用 stdout/stderr；解析输出预算
另计。未提供该字段时沿用既有计划、控制文件和 argv，不生成 JVM 观测回执。

启用时仅 server argv 添加固定同步 GC/JIT `-Xlog` 参数。冻结的 JDK 必须包含普通文件
`bin/java`、`lib/server/libjvm.so`；release 中的运行时版本绑定进计划，实际 `java -version` 还须匹配
该版本及 HotSpot/OpenJDK 64-Bit Server VM。shipped `jvm-module.options` 限制为不含隐藏日志、agent、
嵌套 argfile 或反斜线续行的 ASCII 文件；被拒绝时须先审查资产，不能在运行中放宽或补加参数。

目录路径为绝对路径，不支持逗号、换行或 NUL。完整 distribution/JDK 文件清单记录 SHA-256、大小、模式及内部符号链接；链接必须使用相对目标且不得逃出资产根目录，确保映射到容器路径后仍可解析。计划与资产目录不得互相包含。JDK release 必须声明 17，容器启动还会实际执行 `java -version` 检查。完整资产在 `serve` 和容器 wrapper 启动时复核；修改资产、配置或执行组件后须重新计划。`status/stop` 只读取小型计划与归属数据，资产变化不会妨碍定点停止。

## 配置、隔离与保护边界

- 固定容器路径 `/opt/hugegraph`、`/opt/jdk`、`/control`、`/work`，两个 mode 的生成配置仅改变 `gremlinserver.rest_execution_mode`；其余线程、队列、超时、连接、JVM 设置相同。
- 沿用固定 distribution 的 Gremlin YAML 插件/序列化配置、shipped `scripts/empty-sample.groovy` 和 `bin/jvm-module.options`；显式覆盖声明字段。`InitStore` 接收 REST properties。服务通过官方 `org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap true` 启动，随后接收 Gremlin YAML 和 REST properties；传入固定复制的 `-Djava.security.properties=/control/conf/java-security.properties`。安全属性缺失时拒绝计划，安全管理器安装失败时拒绝启动，没有关闭检查或宽松回退选项。服务 classpath 保留 log4j binding 优先、HugeGraph JAR、其他 lib、ext、plugins 顺序；InitStore 沿用 HugeGraph JAR 优先的独立顺序。
- REST 绑定 `127.0.0.1:18080`，Gremlin 绑定 `127.0.0.1:18182`；单个 server 容器 `network=none`，不发布端口，driver 后续使用 `--network container:<verified-container-id>` 加入同一私有命名空间。
- 使用计划用户的非 root UID/GID、只读根文件系统、`cap-drop ALL`、`no-new-privileges`、固定 CPU/内存/PID、`memory-swap=memory`、不自动重启。Docker 日志驱动关闭；进程输出由 wrapper 按预算写入。
- distribution/JDK/control/scripts 只读挂载；唯一持久可写目录为 `/work`。`/tmp` 为有大小上限的私有 tmpfs，允许执行以支持 RocksDB native library，使用 nosuid/nodev。tmpfs 受容器内存限制。
- wrapper 的 TTL 覆盖运行时身份检查、初始化和服务运行。文件枚举、每个哈希块和阶段边界都检查共同启动截止及取消；全臂另外安装容器内计时信号。健康请求具有绝对截止，到期会 shutdown 实际 socket，逐字节滴流无法通过不断刷新 socket timeout 拖延 ready。返回后再次核对启动/全臂截止和取消，再发布 ready。它在 Java 所在容器内部监督，即使宿主 supervisor 被 SIGKILL 或 SSH 断线仍会执行时间/日志/目录保护。每个 Java 子进程使用独立进程组，停止先 TERM 再 KILL，并等待回收；不向任何其他进程组发信号。
- stdout/stderr 合流，在 JDK 检查、初始化和服务三个阶段共享 `maxLogBytes`，文件只保留上限内的前缀。TERM 宽限和后续最多 5 秒的回收期继续非阻塞读取，避免 shutdown hook 因写满管道而阻塞；观察到 EOF 后才关闭日志。达到保留上限后仍有界排空并计入丢弃字节。每阶段 `*-capture.json` 保存单调时间窗、实际保留 SHA/字节、读取/丢弃计数、EOF、强杀和 I/O 错误；传输完整性与服务正常退出分别报告。全臂计时信号在回收中只记录到期，回收使用原有固定截止。RocksDB 自身日志及其他 `/work` 文件受目录预算约束。
- 目录预算分别检查逻辑字节、实际分配字节、文件数和所在文件系统剩余空间。**bind mount 没有通用 Docker 磁盘硬配额**：这里是采样看护，最多一个采样/停止间隔仍可能增长；实际阶段预算应保留余量。CPU/内存/PID/tmpfs 上限由 Docker 执行。无宿主 I/O 带宽硬限额。
- 目录扫描遇到文件正常原子替换/删除导致的 ENOENT 时，最多重新扫描三次；成功重采样标记 `usage.unstable/resamples`，累计次数写入 `unstableSamples`。持续不稳定、权限错误、链接或特殊文件会失败停止，避免静默低估空间。
- 真正不可中断的内核磁盘 I/O 不能由 Python 检查或计时信号保证即时退出；外层 Docker/systemd 的停止和主机恢复机制仍须保留。这里不声称在内核 I/O 挂死时具有硬墙钟保证。

## 证据、恢复和模式核验

计划目录保存 `plan.json`、`plan.lock.json`、三个冻结 Python 组件及五个配置文件（包含原样复制的 `java-security.properties`）。`plan.controlFiles` 为 `{path,sha256,bytes}` 数组；`plan.sources.distribution/javaHome` 为 `{path,files}`。通用 task 可追踪 plan/lock/controlFiles；大资产完整复核由环境组件负责，避免 CLI 重复扫描。

启用 JVM 观测再冻结 `jvm-observation.py`、`jvm-observation/spec.json`、
`jvm-observation/plan.json`、`jvm-observation/plan.lock.json`，四项都纳入 `controlFiles`。
环境 `plan.jvmObservation` 绑定子计划目录、原字节 SHA 及 JDK java/libjvm SHA 和版本。任何 spec、
组件、子计划、JDK 身份或预算改变都需新计划。server 启动后、ready 发布前，wrapper 读取真实
`/proc/<pid>/cmdline`、`stat`、`exe`、`maps`。argv 必须逐项等于本次启动，PID/start ticks 保持相同且
非 zombie，exe 与冻结 java 为同一 device/inode，实际 libjvm 映射必须包含可执行段且所有映射的
路径、device/inode 均与已打开并校验摘要的冻结 `lib/server/libjvm.so` 相同。映射尚未加载时继续
等待，身份不符或启动截止到期则失败，缺少证明不会发布 ready。

`work/jvm-launch.json` 符合 parser 原有精确契约；`work/jvm-runtime.json` 附原始 cmdline、前后 stat、
maps 的 base64、读到的 exe 路径、实际打开文件的 device/inode/字节/SHA、共同计划及 launch 绑定。
每份 `/proc` 文件分别限制：cmdline 128 KiB、stat 64 KiB、maps 1 MiB；观测期间读取均受共同启动
截止和取消控制。runtime 保存 capture supervisor 和 JVM 的 time namespace、time_for_children、
timens_offsets；每项明确 available/value/reason。capture 时间原域为 Linux CLOCK_MONOTONIC，
phaseMapping 固定 `unverified`，这些信息不自动证明 JVM/driver 阶段映射。

server 停止并关闭日志后，`work/jvm-capture.json` 从实际 `server-capture.json` 与 termination 生成，
绑定 `jvm-launch.json` SHA、完整保留前缀 SHA/字节、读取和丢弃字节、单调时间窗、EOF、capHit、
forcedKill、cleanupComplete 和 I/O error。它不接收手填的 complete 状态；parser 按原始字段判断
完整性。身份检查失败时保留服务失败及已有日志，不为缺失 launch 补写成功回执。所有观测文件、
原始 `server.log` 和冻结配置一起封存；同步日志扰动、GC/JIT 解析和 warmup 判定仍需独立校准。

运行时保存 `dispatch.json`、Docker 直接生成的 `container.id`、`container-before-start.json`、`container-final.json` 和 `supervisor-result.json`。若 create/start 响应中断，status/stop 按 cidfile 或精确随机名称查询，且必须同时匹配 attempt label、plan SHA label、镜像和容器名称。未知或外来容器不会被停止。stop 在查询 Docker 前原子发布绑定本 attempt/plan 的 `stop-request.json`；wrapper 在资产核验、每次 Java 启动之前及监督循环检查它，关闭 create/start 窗口中的竞争。create 成功但尚未 start 的容器保持未启动状态，不会因恢复查询而补启动。

`/work` 中的 `service-started.json` 防止容器手工重启后重复初始化；`service-status.json` 提供目录资源观测；`java-process.json` 提供当前 Java PID 和 Linux start ticks；`ready.json` 提供 endpoint、Java 身份与 `/opt/jdk/bin/jcmd`；`service-result.json` 保存初始化/服务退出原因和 `termination`（TERM/KILL 是否发送、最终退出码、主进程回收、进程组消失及 cleanupComplete）。TTL、OOM、异常退出、没有 ready 或缺少终态证据会作为失败保留。成功终态要求明确 stop、发送 TERM 后在宽限内完成清理、未发送 KILL、Java 退出码为 0/143/−15、无 OOM、wrapper/container exit 0 和完整 receipt。143 是 HotSpot 常见的 SIGTERM 停止码；这些条件证明未强杀的受控停止，不额外宣称每个应用 shutdown hook 都已完成。强杀会保留独立失败状态；JSON `ok` 与退出码使用同一条件。

mode probe 冻结在 `/control/hugegraph-mode-probe.py`，后续以同一 UID 在精确 server 容器执行。读取 `ready.java.pid`、`ready.jcmdExecutable` 作为参数；输出可使用全新 `/work/mode-probe-pre` 或 `/work/mode-probe-post`，纳入同一目录预算。健康检查协商 gzip，限制线上和解压后的响应为 64 KiB，并校验 `status.code=200`、`result.data=[2]`。健康检查不替代 JVM stack 的实际模式核验；测量窗口内不执行模式探针。

当前本地验证覆盖离线冻结、只改 mode 的配置、源变化拒绝、精确归属、重复运行拒绝、argv 和限制核验、故障清理，以及真实本地子进程的 TTL/日志/目录/取消。Docker/候选 HugeGraph 的真实部署由主流程另行验收；本组件与任何成功退出均不代表性能有效。
