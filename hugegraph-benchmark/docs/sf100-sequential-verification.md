# SF100顺序全量内容核验

`tools/ldbc-scan.py`完整读取冻结源分片和目标图，比较全部顶点/边的身份、端点、label、全属性、逐label计数及schema/index。默认先核验顶点内容再导边，最终再次核验全部顶点/边；可显式选择在顶点写入全部确认后导边、只做最终全图核验。该选择须在执行前固定，不能运行中切换。

```text
默认content：顶点写入确认 → 全顶点内容门 → 边写入确认 → 最终顶点/边内容门
可选acknowledged-writes：顶点写入确认 → 边写入确认 → 最终顶点/边内容门
```

写入确认由`ldbc-load-ack.mjs`检查：相同请求/结果/回执、完整日志摘要、退出0、全部insert futures结束、全部解析/插入成功计数等于源记录且失败0。它不授予内容或数据资格。`ldbc-import-sequence.mjs`控制顺序、取消、已分类前缀不重放以及两类内容门；部署者仍须证明独占访问、原输入/目标绑定和外部资源保护。

## 直接运行与任务管理

先看`python3 tools/ldbc-scan.py --help`。它只读数据库和源数据，写入新的证据目录；无重试、抽样或自动续跑。可由现有`hgbench task run`管理进程生命周期，CPU/内存/I/O和空闲磁盘底线由部署监督器控制。以下是参数模板，路径、摘要、租约和原整任务截止必须先冻结：

```sh
python3 tools/ldbc-scan.py --job jobs/000886.json \
  --prepared prepared/prepare-manifest.json --prepared-sha256 PREPARED_SHA256 \
  --quiescence vertices-lease.json --endpoint http://127.0.0.1:8080/gremlin --alias g \
  --scope vertices --deadline-unix ORIGINAL_DEADLINE --out evidence/vertices

python3 tools/ldbc-scan.py --job jobs/004448.json \
  --prepared prepared/prepare-manifest.json --prepared-sha256 PREPARED_SHA256 \
  --quiescence final-lease.json --endpoint http://127.0.0.1:8080/gremlin --alias g \
  --scope full --prior-scan evidence/vertices/scan.json --prior-scan-sha256 VERTEX_PROOF_SHA256 \
  --deadline-unix ORIGINAL_DEADLINE --out evidence/full
```

可选策略仅跑第二次完整扫描，显式传`--vertex-admission acknowledged-writes`，省略`--prior-scan`及其摘要。它仍须先确认全部顶点写入、完成全部边写入并停止所有写者；没有完整最终内容证明就不能测量。

两次扫描须使用相同的绝对截止和所有预算；第二次验证第一份证明、原件摘要、页账本链及计数，不能重置已用页数、字节或时间。默认4096条/页、32MiB响应、整任务60万请求（含schema调用）、合计1GiB页账本；每次输入账本/摘要/元数据合计4MiB。默认SF100方案至少571,488请求，可选方案至少502,482请求，实际可能更多。达到上限保留partial并失败，不缩短范围。

## Go执行组件

`components/ldbc-scan-go`提供相同逻辑编码、64桶/双HMAC、全量范围及证据合约的独立执行组件，Go依赖仅含标准库。它使用既有Node入口验证冻结Loader job和源绑定，随后自行执行完整文件/gzip扫描、HTTP分页、租约/文件身份、绝对截止、预算、取消和报告。Python实现继续作为规范对照，TS入口及任务生命周期保持。

```sh
node components/ldbc-scan-go/build.mjs --help
node components/ldbc-scan-go/build.mjs --go /absolute/pinned-go/bin/go \
  --out NEW-build-directory --goos linux --goarch amd64

NEW-build-directory/hgbench-ldbc-scan-go --identity
NEW-build-directory/hgbench-ldbc-scan-go --help
NEW-build-directory/hgbench-ldbc-scan-go --root /absolute/installed-hgbench \
  --job jobs/000886.json --prepared prepared/prepare-manifest.json \
  --prepared-sha256 PREPARED_SHA256 --quiescence vertices-lease.json \
  --endpoint http://127.0.0.1:8080/gremlin --alias g --scope vertices \
  --deadline-unix ORIGINAL_DEADLINE --out evidence/vertices
```

构建显式使用仓库固定的Go版本，禁止自动下载工具链或依赖；`build.json`保留全部源码、二进制和目标平台身份，临时构建缓存会删除。交叉构建不证明目标平台可运行，实际部署须核对二进制SHA-256、`--identity`及该平台的组件资格。正式两次扫描必须使用同一二进制、依赖和预算；旧执行器的顶点内容证明不能跨组件复用。Go报告额外记录`executor`身份，`componentSha256`绑定实际二进制。

Go协议检查使用305条固定向量及原生RocksDB页样本；完整CLI检查另外经过真实冻结job校验、固定HTTP响应和Python独立证据重读。可在显式安装Go后运行`go test ./...`，或将`HGBENCH_SCAN_GO_BINARY`设为已构建的本平台二进制，再运行`python3 -m unittest discover -s tests-normative -p test_ldbc_scan_go.py`。这些检查不创建图服务，也不授予SF100内容或性能资格。

## 结果与边界

退出0只代表所选scope通过；`vertices`始终`datasetVerified=false`。只有`full`的内容、计数、首尾schema和epoch/租约/输入身份均通过，才产生完整数据证明。错误或未知返回4；输入/契约前置检查也可能以非零退出，不能将任务成功提交当作通过。`performanceAccepted`始终false，图quick/standard、环境及工程校准另行验收。

全部记录参与逐label/64桶的计数和两份有域分隔HMAC-SHA256加法摘要。规范化复用逐条核验器，精确保留大整数、文本和额外属性，SET仅排序并拒绝重复。比较依赖碰撞假设，不能称为逐条成对比较或密码学认证。32字节核验种子在写者退出租约通过后产生，后随原件公开；最终扫描必须使用新种子。它不用于身份认证。

[固定线格式向量](../tests-normative/fixtures/ldbc-scan-fingerprint-v1.json)冻结当前协议的8条接受及35条拒绝输入，包含规范UTF-8字节、三个HMAC域、桶号和非零聚合。它们不在测试运行时重新生成期望值，可用于发现编码或域分隔的无意变化；在仅有8份可交付源文件、没有私有证据的临时目录中，全部22项扫描组件检查也已通过。运行`python3 -m unittest discover -s tests-normative -p test_ldbc_scan.py`；这仍是协议/控制验证，不授予SF100内容或性能资格。

原始SF100资产、已接受旧证明、Loader日志和失败原件须保留。源变化、未知写入效果、缺记录、重复、游标循环、错误图/epoch、schema变化、截断、租约变化/过期、预算或截止耗尽都会拒绝资格。未知写入先使用原逐条工具消除不确定性，禁止盲目重放Loader。

本地功能资格见组件证据（本机记录未随包分发）：22项Python拒错/真实HTTP检查、本地真实RocksDB分页到Python摘要消费，以及独立的写入确认/编排检查。原生脚本检查通过Gremlin实际HTTP解析器生成整数`Long`绑定，并保留直接脚本`Integer`兼容；18项检查覆盖完整分页、错误目标和整数/浮点/字符串/布尔/空值及范围边界。页大小仍限定为1至100,000的整数。这些组件检查不授予HStore/SF100内容或性能资格；正式状态见[阶段一验收边界](phase1-acceptance.md)。
