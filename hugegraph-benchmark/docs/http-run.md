# 一条命令完成普通 HTTP 运行

准备一份现有格式的 HTTP profile，填写目标 endpoint、题目/负载、资源及响应预算。profile v1 使用 Node 执行器，已注册的 profile v2 使用 Go。目标可以是已部署的服务；该入口不负责启动数据库。

```sh
hgbench http run profile.json --out /absolute/new-run --dry-run
hgbench http run profile.json --out /absolute/new-run
```

第二条命令使用共用输入准备、一个现有task和同一HTTP workflow/analyzer。默认链已实际收敛，见[精简说明](http-default-execution.md)。前台结果直接给出每个单元的计划数、正确数、错误/拒绝/迟到、完成窗吞吐和原质量门，正常结束还会返回原报告的绝对路径：

```text
new-run/
  run.json                  提交键、taskId、任务存储位置和结果路径
  evidence/
    raw.lock.json           原始证据锁
    analysis/report.json    既有独立分析
    analysis/report.html    既有交互报告
```

异步执行适合 agent 和 SSH：

```sh
hgbench --root /absolute/task-state http run profile.json \
  --out /absolute/new-run --request-id run-001 --detach
hgbench --root /absolute/task-state task status --request-id run-001
hgbench --root /absolute/task-state task wait --id <task-id> --timeout-ms 30000
hgbench --root /absolute/task-state task cancel --id <task-id>
```

保存返回的 `taskId` 和 `run.json`。同一个输出目录不能再次派发；中断后先查询原任务。`run.json`在准备前原子保存root、request-id和确定的task ID，提交回执后更新。同一task库维护实际运行状态；`submissionAcknowledged=false`仅表示尚无持久提交回执，先查询原ID，不据此自动重放。

`--dry-run`不写文件或发送HTTP，显示固定窗口、执行总上限和清理预留。可配置：

```sh
hgbench http run profile.json --out /absolute/new-run --budget-ms 600000
hgbench http run profile.json --out /absolute/new-run --budget-mode stage-sum --dry-run
```

默认`total`以一个执行上限覆盖前后检查、预热、测量、排空、封存、报告及预留终止；耗尽即停止后续调度，不缩窗或减题。`stage-sum`额外要求预算覆盖保守阶段界。未给预算时按profile推导。离线输入准备和提交有各自界限，不计入已启动组件的执行预算；固定窗口不作为ETA。

取消、总截止和前检失败会自动保留部分JSON/HTML报告：完整计划分母、可核验的已完成运行、unfinished文件和全部缺项。部分结果的sampleAccounting为partial，不能当作完整有效成绩。损坏记录仍明确拒绝，不补造数据。

完成状态只表示进程正常结束。查看报告中的完整计数、答案、质量问题、资源和未验证项；此入口不授予性能优势、容量或统计认证。正式 HG 测试继续要求关闭内存监测触发的手动 GC 和宿主 swap；接入已有实例时，这些设置由部署方完成，普通 HTTP 结果无法独立证明它们。

启用了 `hostIdentityGate` 的 Go profile 需要实际宿主观察器，请使用现有受控比较任务。`http run` 会在写文件或发送请求前拒绝该配置，并保留它的要求。

需要审阅中间计划时，继续使用 `http plan`、`task run` 和 `http report`。题集、计时、完整校验和报告算法共用，新增入口没有另一套执行或统计引擎。

普通报告提供可单独导出的 SVG 图表；多份已有运行可通过 `http compare` 组合，见[离线报告说明](http-reports.md)。关闭 MemoryMonitor 不保证其它路径不会调用 System.gc；正式测试还应记录实际 JVM GC 行为。
