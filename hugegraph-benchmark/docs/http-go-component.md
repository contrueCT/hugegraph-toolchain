# Go HTTP执行组件

用户已批准TS CLI + 独立Go组件。兼容内核、独立Linux安装、信号/证据生命周期与复合控制器已实现；真实HG全11题功能验收（本机记录未随包分发）通过，33请求全部正确，66次两侧活进程观察完整，离线核心产物逐字节重建。quick/capacity显式Go project入口已完成本地公共链验证，尚待真实批量校准。阶段一未验收。

首次同资源40rps结果（本机记录未随包分发）显示Go标量CPU更低、rows和大字符串CPU更高；该版本未获得整体驱动资格。parser-only的同资源复测002（本机记录未随包分发）仍保留质量失败。当前进一步的编码器v2（本机记录未随包分发）已经完成兼容、独立Linux安装和离线核验，尚无该新source的真实HG测量；微测收益不能替代HG结果。

```text
hgbench http generate (现有题集) → Node profile v1
               |
hgbench http engine register --binary ... → 新Go profile v2
               |
http validate → http plan → task run/status/cancel
               |
TS workflow → 单个Go runner → trace + run + writer/clock/resource回执
               |
raw manifest v3 → http report → 离线JSON + HTML
```

## 显式构建和注册

需要明确安装的Go 1.27.3，使用标准库，无第三方Go运行依赖。工具不会在执行测试时下载或编译。Linux工具链资产锁位于`release/http-go-toolchain.json`；正式交付二进制还需保留对应Go许可。

```bash
hgbench http engine build --go /absolute/path/to/go --out /new/build-directory
hgbench http engine inspect /new/build-directory/hgbench-http-go
hgbench http generate examples/http-suite-functional.json --out /new/node-profile.json
hgbench http engine register /new/node-profile.json \
  --binary /new/build-directory/hgbench-http-go --out /new/go-profile.json \
  --gomaxprocs 2 --cpu-quota-cores 2 --max-rss-bytes 1073741824
hgbench http validate /new/go-profile.json
hgbench http plan /new/go-profile.json --out /new/attempt
hgbench task run --plan /new/attempt/task.json --request-id unique-go-attempt --detach
hgbench http report /new/attempt/output --out /new/rebuilt-report
```

`build`冻结源码清单/摘要、工具链可执行文件摘要、版本和构建参数，禁止工具链/依赖网络获取，删除自己的编译缓存；在首次调用Go之前创建私有HOME和遥测off标记，失败也保留阶段和错误回执。`register`保留原题目、答案、负载和预算，移除Node专属clockReference，写入新版本配置；旧文件不覆盖。它会调用组件的`version`读取身份，不联系目标服务。

计划必须使用与当前包源码摘要一致的二进制，锁定binary/source SHA、Go版本、平台、GOMAXPROCS及资源声明。运行前与每个组件调用前核验二进制；组件也核验自身身份。CPU/RSS声明需由外部容器/cgroup实际实施；资源证据不可用或不匹配时保持未验证，不自动通过驱动门。

## 版本和测量口径

| 层次                  | Go新版本                                    | 旧Node处理            |
| --------------------- | ------------------------------------------- | --------------------- |
| profile               | schemaVersion=2，executor.engine=go-http-v1 | schemaVersion=1原定义 |
| 组件request/run/trace | schemaVersion=2                             | schemaVersion=1原定义 |
| raw.lock.json         | schemaVersion=3，显式engine与注册清单       | v1/v2按原reader分支   |
| analysis              | schemaVersion=2，Go资源与时钟分支           | schemaVersion=1原输出 |

一个Go进程执行一个逻辑case/load/repeat，包括其预热与测量。准入令牌覆盖HTTP、响应缓冲、解压和完整答案校验；执行goroutine不作为独立重复。

固定HTTP/1.1 POST、keepalive、连接上限、无环境代理/重定向/自动解压。请求体非空且GetBody=nil，覆盖复用连接在写入前/后故障时不重放。TLS默认校验开启。原编码响应收完即捕获endMs，之后计算原body摘要、解压与校验；wire与decoded分别受上限保护。正确吞吐继续只计固定完成窗口，排空完成单列。

open模型以绝对时刻和sequence产生ceil(duration×rate)个到达，保留迟到、并发拒绝及取消后的未来到达。取消在途HTTP并停止准入；队列或磁盘失败返回非零、保留失败/部分证据，不生成有效raw锁。task guardian仍负责进程组硬收尾。

答案兼容编码器匹配现有JS的UTF-16排序、整数形状键枚举、二进制浮点、负零、溢出、孤立代理项及重复键。响应UTF-8严格解码并匹配TextDecoder对一个开头BOM的处理。新引擎最多接受256层JSON嵌套；超界作为invalid-json，不宣称覆盖任意深度JSON。

## 时钟、资源和资格

使用Go进程内单调elapsed，Linux原CLOCK_MONOTONIC在Time.Now前后采样，纳秒以十进制字符串保存，避免JSON安全整数上限。保存boot/PID/start/namespace身份和真实phase边界，校验原始区间与Go elapsed的一致性。Darwin功能检查保留原Linux时钟不可用状态。

进程时钟检查不等于宿主/JVM时间窗关联。当前externalCorrelationAccepted固定false。已实现[宿主时钟与两侧活进程核验](http-go-clock-contract.md)。Docker协议联验通过探针及实际测量进程身份分项；复合控制器已在真实HG功能验收中复核活进程、target身份与观测资源；collector/JVM细粒度相位归因及独立性能校准仍待完成。

资源记录进程CPU、生命周期最大RSS、Go heap/GC/goroutine、cgroup quota和throttling前后值。Node ELU显式null且不适用。当前80% CPU份额和throttling/迟到/RSS只用于记录压力或未验证问题；独立校准仍是必要条件。Go GC百分比固定100，软内存限制off，外部硬限制和失败清理另行核验；GODEBUG必须为空。

用户新增正式测试约束在新的calibration环境中冻结：HG `memory_monitor.threshold=1`，宿主SwapTotal=0且无活动swap区域；在开始、监督轮询与结束采证。轮询仅证明采样时状态。开发功能检查免除此门，旧封存实验不追溯改写。

## 复现开发检查

```bash
node tools/http-go-vectors.mjs --out /new/go-golden.json
cd components/http-go
HGBENCH_GO_GOLDEN=/new/go-golden.json GOTOOLCHAIN=local GOPROXY=off go test ./...
```

Go完整CLI协议检查位于`tests-normative/http-go.test.mjs`，运行时需显式提供`HGBENCH_GO_BINARY`。未提供时只执行schema检查，其余明确skip，不能据此声称Go执行验收。`HGBENCH_GO_KEEP_ROOT`可指定开发证据保留目录；其中测试会故意损坏一份原manifest，完整未损坏副本位于相应根目录的`moved/`。

## 已验证与待验证范围

- core-v1、clock-build-v1和真实HG composite-functional-v1已分别封存；旧binary及源包按各自清单重建，不能换用当前源码解释旧实验。
- 当前source `69421df2`；完整10,022答案向量、90项含子项检查、本机35项公共组件/quick/容量规划链及独立Linux安装9项通过，无skip；新的同binary时钟与跨平台离线核验通过，见codec-fast-v2。
- runtime v2正式比较使用`hostIdentityGate=true`、同binary clockProof和只读挂载；每个实际run/probe独立采证。新版Go project v2要求显式executor，project、runtime、binary和实际CPU/RSS必须匹配，生成profile仅从这一入口继承。
- 复合控制器的Go总截止/主动取消、真实批量cohort/capacity/engineering消费者仍需当前版本的实际验收。单臂已通过的取消不自动覆盖复合路径。
- 新驱动余量、A/A、正控制及必要观测成本校准，随后全11题曲线/独立确认与forward/embedded比较仍待完成。SF100继续保留在阶段一。

已有Node校准失败/unknown保持原结论，Go各项资格依照自己的证据判定；当前没有整体Node/Go性能提升结论。
