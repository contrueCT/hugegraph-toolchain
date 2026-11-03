# Known-delay 002：异步 trace 与端到端 P99 正控制

本版只迁移既有固定操作。执行逻辑来自最终known v5/spec-v3，一次前台HTTP task、原ID恢复、精确owned清理和共享截止沿用原实现。没有新运行器、通用hook、统计算法或部署系统。原001/v4/v5材料、A/A005与旧冻结源保持原件。

当前只有本地draft和不可运行的execution草案，未SSH、Linux prepare、Docker或HTTP。005仍记录为running，本操作不能与其重叠；材料的prepareAllowed/runAllowed均false，终态与清理审阅尚未进行。

## 前瞻指标与原实验范围

同一d1239ef88构建、forward，A=`1 + 1`，B=`Thread.sleep(10); 1 + 1`，完整正确答案`[2]`。8个独立pair、4AB/4BA、16个全新JVM，顺序/种子在请求发出前冻结。固定open20rps、IF4、最大开始延迟1000ms，预热10s/200次、测量60s/1200次，timeout与drain各2s；每个物理臂至少1000个正确measurement观测，禁止合并凑数。

唯一推断指标现为`end-to-end-p99`，严格映射到原报告的`correctEndToEndMs.p99`，与005的主计时口径一致。原统计阈值继承：相对5%、绝对5ms，只有两种区间的下界同时超过预算才标detected；not-detected/inconclusive/invalid均保留。单指标族α=.05、8pair、bootstrap10000、minimumTailDraws20，预期尾抽样125。response P50/P99、端到端P50/P95留在诊断中，不进入推断族。

这没有改动005的5%/1ms比较预算。注入10ms不等于真实HTTP P99精确增加10ms，也不证明对1ms变化的检出能力或名义误报率。goodput差≤1rps是低速计数负控制，不是吞吐退化检出、置信等价或数据库容量成绩。本custom算术控制不冒充标准11题成绩。

## 同一测量源与公共预算复算

测量代码严格选用005的529成员异步trace冻结源；原source lock SHA为`2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a`。生成器拒绝其他成员集，逐文件复核，不使用旧同步trace的v6代码运行新成绩。selection的build/settings与comparison runtime取自005已保存的实际操作输入；Node/JDK/image、target2CPU/2GiB、driver1CPU/768MiB保持同层。

本控制沿用原独立观测配置：JVM observation与持续collector关闭，实际目标寿命改为600s、driver为180s；不能把结果称为005完整观测栈的全部工程资格。selection中未使用的cohort TTL/collector字段仅作输入选择审计，以本操作实际argv/spec为准。

本地16份公开`http validate/plan`均来自所选冻结源。额外调用其公开`httpDeadline`验证两种profile均为85000ms；16个真实task plan均为145000ms、TERM grace5000ms，trackedFiles含新的`trace-writer.mjs`。详见最终draft的`public-budget-review.json`，并未凭复制旧预算宣称校验完成。

保留原硬边界：prepare1800s（父1830s）、operation9000s（父9095s），controller1CPU/512MiB/128tasks、work8GiB、持续80GiB空闲；共享cleanup90s、父stop100s/mixed。准备窗口20s，HTTP单次调用最多145s且不越过driver启动后155s绝对线；为180s TTL内的artifacts/inspect/删除保留余量。正常ETA未知，超时/空间不足会停止后续并留完整16臂/8pair分母。

## 本地入口及材料

新增文件为`.hgbench/http-known-delay-002-plan.py`、`http-known-delay-002-execute.py`及专项fixture；主CLI、runner、comparison和005源码没有修改。

最终本地draft为`.hgbench/http-known-delay-002-draft-v3`，execution草案为`.hgbench/http-known-delay-002-execution-draft-v2`，后者`runnable=false`。较早的本地draft保留为迁移迭代记录，不用于远端派发。`http-plan-host`只供规划核验，不能在宿主运行loopback请求。

`.hgbench/http-known-delay-002-materials-v1/`提供source/helper差异、六份小资产的SHA、固定prepare/freeze/execute systemd argv、预算和完整前置清单。它没有部署入口或自动队列；未来由协调者按现有create-only步骤核验成员后安装到全新`/absolute/hgbench/http-known-delay-002`。现有005 source/dependencies只读挂载，不拷贝或安装另一份测量栈。

未来Linux prepare必须显式选择新的selection、005 source、唯一002 ID及`hgbench-http-aa-cohort-005.service` inactive门；完成后用该次原`plan.lock.json` SHA绑定freeze，另存解析后的argv，禁止复用Mac锁或覆盖模板。实际run只读取这份新spec并claim一次；响应丢失只查原任务/容器，不run第二次。

## 实际派发前的证据门

必须先审阅005原parent task `task-f8e2acbd98379459d19ed4611e641726`、原task plan digest、cohort/result、实际派发children的终态与身份；失败终态保留且不改成通过。枚举原回执中的owned CID和PID/startTicks/boot，逐项核对正常收尾、真实absence及最后当前臂，不能只凭task terminal、not-found或cleanup布尔放行。具体绑定见materials的`prerequisites.json`，目前全部pending。

生成器和executor本身严格检查当前005单位inactive，活动单位会拒绝；**完整parent/children终态与owned清理审阅仍由协调者在prepare/freeze/execute前保存真实证据**，本地pending清单不是自动认证器。单位被GC时允许严格not-found/inactive/PID0/空CG；原历史成功需原stage/任务回执。d6继续只读披露，不停止它或其他任务，不宣称独占。

每个臂都要实际新目标、同构建/forward、pre/post mode通过、全答案和计数守恒、原始profile/Node/source绑定、driver/target正常停止并精确移除。随后公开HTTP report逐字节重建通过才归一端到端P99；缺臂/坏臂保留政策分母。所有结果仍不自动设置engineeringAccepted或performanceAccepted。

本地专项10项通过：新主指标/阈值严格绑定、response诊断映射、529源/16公开预算、005活动拒绝、原官方taskID规则、前台单次调用/绝对截止、失联与cancel失败后的归属清理、首故障停止且保留分母。映射/故障样本明确为合成fixture；没有真实HG延迟检出结果。
