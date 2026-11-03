# HTTP 总deadline

新计划默认使用 `--budget-mode total`；`--budget-mode stage-sum` 保留各阶段上限求和的规划要求。两种方式均前瞻冻结，不修改旧计划、旧源码或历史结果。

```sh
hgbench http preset plan --project project.json --out quick-5m \
  --budget-ms 300000 --budget-mode total
```

这里的5分钟是运行控制器启动后的整次硬时限。实际source/runtime验证、所有预声明臂、清理、封存与分析共用同一单调时钟截止。离线plan/prepare有各自资源预算，尚未开始运行负载。外层task在该T触发终止，其有限回收宽限仅用于故障处理，不能算作预算内完成。

规划仍展示每个阶段cap及其求和上界。total允许T小于这个保守求和，但必须覆盖所有arm固定的预热、测量、settle时长以及一次stopMs和10s最小终态/partial回执保留。未知启动、探针或I/O耗时可能导致超时。工具不自动缩短预热、测量或删题；总预算不足时保留所有预声明分母、可用原件和partial状态，未测量、未完成、未独立验证的项保持unknown/invalid。

运行在T前预留stopMs和10s收尾空间；正常、取消或超时路径均受原T约束。driver与target清理独立尝试，一方失败不会省掉另一方。当前步骤耗尽共享清理窗口后，不会为后续步骤新开完整预算。桥接验证器的子进程先收TERM并获得至多3s回收时间，这段时间也包含在同一阶段和T中。最终source重验和分析使用剩余时间；不能因为新开封存阶段而延长T。

quick预设在300000ms total下可预览、冻结、重验。这个配置可能产生partial；它没有承诺5分钟内成功。历史quick001约544s的实际耗时来自原冻结配置和环境，不能作为新配置的ETA。`stage-sum`下相同300000ms会因阶段求和不足被拒绝；增加预算是显式的新计划选择。

版本边界：旧request/plan v1、v2及preset-plan/binding v1继续原有阶段求和语义；其输入没有新字段，也不注入默认值。新comparison request v3明确冻结 `budgets.budgetMode`；single-observation使用唯一O臂和描述性分析，paired明确冻结其推断/质量角色。新preset-plan/binding为v2，quick保持原三题、原四臂和两指标推断family。模式、T或展示解释的重封更改需要通过原始请求与绑定重验。

工程验收、统计资格和性能结论继续使用原有独立门。任务退出成功、deadline内完成或部分正确结果均不能替代测量证据有效性。
