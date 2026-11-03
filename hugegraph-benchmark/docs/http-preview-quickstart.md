<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to You
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# HTTP preview 快速上手

已有服务和 profile 的普通运行可直接使用 [一条命令入口](http-run.md)：`hgbench http run profile.json --out /absolute/new-run`，自动复用现有运行与报告。下面保留受控 Docker 比较的高级流程。

本指南对应 `http-preview-20261024.1`，内部 npm 版本仍是 `@hugegraph/hgbench@0.1.0`，尚未发布到 npm。
使用交付目录中的 `hgbench-http-preview-20261024.1.tgz`，其 SHA-256 必须为：

```text
496bb1597f474f045271753f181ac0e6d034adfcbb24894d5d332f81d675823b
```

该包的独立安装、help、版本和依赖锁检查已通过。同版本真实 HugeGraph 正常 quick、总截止和测量中取消，
均已完成终态、部分或完整报告、归属清理及独立重建核验，执行记录保留在本地工作区。
下面命令与封存574成员版本的公开接口对应；性能资格仍待完整校准。

## 1. 安装到独立目录

需要 Node.js **22.14.0 或更高版本**及 npm。以下是 Linux shell 示例，先替换三个绝对路径；目标目录必须不存在，父目录须存在。

```sh
HG_ARCHIVE=/absolute/hgbench-http-preview-20261024.1.tgz
HG_TOOL=/absolute/hgbench-http-preview-20261024.1
HG_WORK=/absolute/my-http-quick-001

sha256sum "$HG_ARCHIVE"
mkdir "$HG_TOOL" "$HG_WORK"
tar -xzf "$HG_ARCHIVE" -C "$HG_TOOL" --strip-components=1
cp "$HG_TOOL/release/dependency-lock.json" "$HG_TOOL/package-lock.json"
(cd "$HG_TOOL" && npm ci --omit=dev --ignore-scripts)
node "$HG_TOOL/dist/cli.js" --version
node "$HG_TOOL/dist/cli.js" http preset plan --help
```

这会按发布锁在工具目录安装两个生产依赖，无需重新构建。已有 npm 缓存时可给 `npm ci` 添加 `--offline`；缺缓存会明确失败。
`--version` 显示内部版本 `0.1.0`，精确构建由上面的包摘要区分。安装后保持工具、依赖和已固定资产不变。

## 2. 准备一次项目配置

实际运行目标是**单机 Linux 受控 Docker 环境**：非 root 用户可访问本机 Docker Unix socket，宿主 `/proc` 可读，使用 cgroup v2；
HugeGraph distribution、JDK 17、Node、宿主 Python，以及含指定 Python/动态库的固定 Docker 镜像均已就绪。
镜像使用完整 image ID，执行器不会自动拉取。当前 HTTP 题集使用新建空 RocksDB 环境，不需要加载 SF100。

交付目录的 `configured-host/` 附带本次正常 quick 使用的 `project.json` 和四份实际引用配置；它们是该机器的已执行实例。
跨主机使用时需重新核实构建、宿主和运行时身份，重新采集环境，不能直接复用旧环境声明或计划/任务身份。

| 项目引用 | 需要的实际材料 |
| --- | --- |
| `sources.build` | HugeGraph 构建提交、distribution 路径及文件清单 |
| `sources.settings` | 目标 JDK、镜像、服务/JVM 参数及 CPU、内存、磁盘和生命周期预算 |
| `sources.environment` | 实际宿主和运行环境资料 |
| `sources.runtime` | 当前工具 `sourceRoot`、Node/Python/Docker 路径、driver、collector 与 supervisor 资源 |

`runtime.sourceRoot` 必须指向此次安装目录的真实绝对路径，Node 也必须与运行 CLI 的实际 Node 一致；旧 quick001 的源码路径不能当作新版本来源。
项目中四个相对引用都相对于 `project.json` 本身。可参考包内 `examples/http-project.json`，具体设置见[受控环境](http-controlled-environment.md)和[comparison 输入](http-comparison-run.md)。
这些示例仍需填入真实资产和预算，当前没有零配置环境安装命令。

```sh
HG_PROJECT=/absolute/current-host/project.json
node "$HG_TOOL/dist/cli.js" schema http-project
node "$HG_TOOL/dist/cli.js" schema http-environment-settings
node "$HG_TOOL/dist/cli.js" http preset show quick
```

先验证同模式可用性时，项目使用 `comparison:"aa"`、`aaMode:"forward"`；`forward-embedded` 项目要求候选实际支持两种 REST 执行模式。
quick 固定为三题、open10、最多四个在途、两配对/四个新 JVM、每题每臂预热3秒/测量15秒；配置入口保留完整答案校验和 trace。

## 3. 预览、冻结、prepare

以下示例把整次执行预算显式设为 **1200000ms（20分钟）**。`total` 共享启动、工作、清理和封存的同一个截止；规划、事前 prepare 和事后报告各有独立预算。
固定预热加测量基线为216秒，另有4秒 settle；启动等变动开销另列。20分钟是本示例的上限，预计通常耗时仍未知。

```sh
node "$HG_TOOL/dist/cli.js" http preset plan --project "$HG_PROJECT" \
  --preset quick --budget-mode total --budget-ms 1200000 \
  --out "$HG_WORK/plan" --dry-run

node "$HG_TOOL/dist/cli.js" http preset plan --project "$HG_PROJECT" \
  --preset quick --budget-mode total --budget-ms 1200000 \
  --out "$HG_WORK/plan"
node "$HG_TOOL/dist/cli.js" http preset verify "$HG_WORK/plan"

node "$HG_TOOL/dist/cli.js" http comparison prepare "$HG_WORK/plan/project/runtime.json" \
  --plan "$HG_WORK/plan/comparison" --out "$HG_WORK/attempt"
node "$HG_TOOL/dist/cli.js" --root "$HG_WORK/tasks" task run \
  --plan "$HG_WORK/attempt/task.json" --request-id my-http-quick-001 --dry-run
```

dry-run 不创建指定的计划输出，也不发送目标请求；prepare 只冻结实际资产和 task，不启动容器。
检查返回 JSON 的预算、样本上界和 `nextActions`。预算不足会明确拒绝；不会自动删题、减配对或降低样本要求。
`stage-sum` 可选，它要求覆盖各阶段保守余量之和，不能把该数当作通常 ETA。

## 4. 运行、查询和取消

确认资源空闲后，向同一新 attempt 只提交一次。保存返回的 `taskId`、request-id 和目录。

```sh
node "$HG_TOOL/dist/cli.js" --root "$HG_WORK/tasks" task run \
  --plan "$HG_WORK/attempt/task.json" --request-id my-http-quick-001 --detach
node "$HG_TOOL/dist/cli.js" --root "$HG_WORK/tasks" task status --request-id my-http-quick-001
node "$HG_TOOL/dist/cli.js" http comparison status "$HG_WORK/attempt"
```

断线后查询上述原 request-id，不再次 run。`ok:true` 只表示命令成功，仍需检查 task 的 `state`、`terminal`、错误及执行证据。
若需要取消，下面的 `HG_TASK_ID` 从原 run/status 返回值复制：

```sh
HG_TASK_ID=task-REPLACE_WITH_ORIGINAL_TASK_ID
node "$HG_TOOL/dist/cli.js" --root "$HG_WORK/tasks" task cancel --id "$HG_TASK_ID"
node "$HG_TOOL/dist/cli.js" --root "$HG_WORK/tasks" task wait --id "$HG_TASK_ID" --timeout-ms 60000
node "$HG_TOOL/dist/cli.js" --root "$HG_WORK/tasks" task status --id "$HG_TASK_ID"
node "$HG_TOOL/dist/cli.js" http comparison status "$HG_WORK/attempt"
```

cancel 是异步请求。原任务 owner 会向 Python 控制器发送 TERM，并按冻结 task 的 **30秒终止宽限**等待其收尾，超时再回收进程组。
`task wait` 最多等待60秒，超时也会退出0；必须继续检查返回的 `terminal` 和 `state`。若仍在运行，继续查询或等待同一任务，**不要同时调用 comparison stop**。
`interrupted:true` 只说明任务 owner 丢失，不能证明 Python 控制器已经退出；先核实原控制器 PID/进程组状态。

**只有原任务已经进入真实终态、原控制器已退出，而归属清理仍缺少证明或有错误时，才单独运行下面的恢复步骤。**
`comparison stop` 会另建一个控制实例并立即操作原 attempt 的 driver/target；它不会等待仍在运行的控制器，紧随 cancel 调用可能与原收尾并发。

```sh
node "$HG_TOOL/dist/cli.js" http comparison stop "$HG_WORK/attempt"
node "$HG_TOOL/dist/cli.js" http comparison status "$HG_WORK/attempt"
```

检查原 owned 容器/PID及清理回执；stop 返回成功也不等于资源已全部消失。保留 partial、原日志和所有已执行/未执行臂。
取消或超时不回滚数据库操作。需复测时创建新计划/attempt，保留旧目录。

## 5. 离线报告及结果范围

终态后在相同冻结工具版本下生成新报告目录：

```sh
node "$HG_TOOL/dist/cli.js" http preset report "$HG_WORK/attempt/execution" \
  --plan "$HG_WORK/plan" --out "$HG_WORK/report"
```

打开返回的 HTML，查看 `preset-report.json`、`comparison/` 中的原始验证/分析及逐臂问题。报告生成成功仍可能是 invalid、unknown 或 partial；
若封存证据不足以重建，保留明确错误和原 partial，不补造缺项。每题每臂150次计划到达只是正确样本的上界，quick 不支持可靠 P99、微小胜负或容量结论。

交付中的 `example/quick001-report.html` 来自旧511源码版本的真实运行：544.146秒、1800个测量请求全正确、零失败/drop/迟到，原证据已独立重建。
该示例展示报告形态，**不替代本574版本的正常、总截止和主动取消验收**。两配对不能完成正式 A/A、驱动余量、预热稳态及观测成本校准，性能接受保持 false。
standard/full 预设目前为 design-only；全11题容量和独立确认的真实验收仍待完成，SF100 原验收目标继续保留。
