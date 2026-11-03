# SF100 组件文件协议与恢复 v1（草案）

本协议连接控制器、转换器、Loader 和官方 driver 适配层。当前已实现文件校验、检查点与恢复决策；
组件调度和真实数据库观测仍待接入。所有 `benchmark exchange/recover` 命令均离线、只读，不自动重试或恢复数据库。

## 命令与边界

```bash
node dist/cli.js schema sf100-request
node dist/cli.js schema sf100-receipt
node dist/cli.js schema sf100-load-observation
node dist/cli.js benchmark exchange request.json --receipt receipt.json
node dist/cli.js benchmark exchange request.json --receipt receipt.json --artifact-root evidence
node dist/cli.js --root work benchmark checkpoint request.json
node dist/cli.js --root work benchmark checkpoint request.json --receipt receipt.json --artifact-root evidence
node dist/cli.js benchmark recover work/sf100-checkpoints/request-001/request.json --receipt receipt.json --artifact-root evidence
node dist/cli.js benchmark recover request.json --observation observation.json --artifact-root evidence
```

`exchange` 默认只校验元数据；显式传 `--artifact-root` 才流式读取、校验回执引用文件的大小和 SHA-256。
`recover` 同样只给建议；它验证本地证明文件后才允许输出复用或跳过建议。超大证据文件的 hash 检查也可能耗时，应纳入执行预算。
`checkpoint` 只写少量请求/回执元数据，封存回执时校验回执引用文件。它不启动组件，也不会下载数据。

退出 0 表示命令成功；结果中的 `executed: false`、`liveDatabaseVerified: false` 明确其离线边界。
检查点缺失为 exit 3/`CHECKPOINT_MISSING`；已保存的请求或回执发生冲突为 exit 5/`CHECKPOINT_CONFLICT`。
协议不符为 exit 2/`SF100_PROTOCOL_INVALID`，JSON schema 不符为 exit 2/`SCHEMA_INVALID`。

## 请求：在组件启动前固定

每次调用有独立 `requestId` 和 `attemptId`。请求整体的 canonical JSON SHA-256 为 `requestDigest`，回执必须匹配它。
`identity` 同时固定 plan、数据集、映射与组件的 digest；输入文件固定原始字节 SHA-256 和十进制字符串 bytes。
`target.targetDigest` 固定图及部署身份，`target.epoch` 标识一次空图创建/恢复状态。重新恢复图后必须换 epoch。

| stage | 单位 | 必需输入名称 | target / repetition |
| --- | --- | --- | --- |
| convert | shard ID、预期输出记录数 | source | null / null |
| load | shard ID、预期记录数 | shard | 目标身份 / null |
| validate | 完整验证任务 | reference、driver-config | 目标身份 / null |
| restore | 一轮恢复 | restore-config | 目标身份 / 从 0 开始的轮次 |
| measure | 一轮正式测量 | driver-config、restoration | 目标身份 / 从 0 开始的轮次 |

各输入路径相对执行器的输入根目录；输出路径相对独立的证据根目录。拒绝绝对路径、路径穿越、重复名称和重复路径。
实际核验文件时也拒绝符号链接。字节预算和记录数使用十进制字符串；运算使用 BigInt。
运行时须验证输入、组件二进制、部署身份、恢复回执与测量轮次的关联；当前离线检查只验证元数据约束，不宣称完成这些在线前置条件。

## 回执：进程状态与实际效果分开记录

`status` 为 completed/failed/cancelled；`effect` 为 none/committed/unknown。失败或取消必须提供错误代码与说明。
进程返回码不够判断写入是否发生；超时或断连后不能自动填 none。

completed 回执要求效果 committed，且必须提供阶段产物：shard / commit-proof / validation / restoration / measurements。
分片完成记录数必须与请求完全一致，所有声明输出的总 bytes 不得超过请求预算。
这里的 completed 表示组件报告完成，性能成绩有效性仍由后续分析器判断。

Loader 的 commit-proof 是一个 `sf100-load-observation`，必须绑定完整请求、图 epoch 和 shard，
并引用核对所依据的证据文件。仅有“HTTP 200”或总记录数相等不足以得到 skip 建议。
离线验证能证明文件身份和内容完整，不能证明观测生产者诚实或证明数据库当前仍保持该状态；正式执行器需验证其来源和当前目标状态。

## 检查点持久化

```text
登记 request.json -> 调度组件 -> 校验产物 -> 封存 receipt.json
        |                 |
        +-- 无回执 -------+----> outcome unknown -> 核对数据库
```

目录为 `<root>/sf100-checkpoints/<requestId>/`。文件先写临时路径并 fsync，再用排他硬链接原子发布，
最后同步目录。两个调用同时写同一请求时只会首次创建一次；相同内容重复调用返回 reused，变化的内容拒绝覆盖。
发布失败的临时文件不作为完成记录；正常错误退出会清理本次临时文件。文件系统需支持本地 POSIX 硬链接和 fsync。
这不替代数据库提交协议，也不保证远程文件系统或硬件故障下的 exactly-once。

控制器只能在首次登记成功后安排一次 dispatch；reused 不授权再次启动。
登记后崩溃，即便组件可能尚未启动，也按结果未知处理。登记本身没有执行数据库写入。
终态回执不可改写；重新核对观察以独立 observation 文件提供。新尝试使用新 requestId/attemptId。

## 分片恢复决策

| 证据状态 | action | 后续执行器职责 |
| --- | --- | --- |
| 转换产物完整、请求一致且文件 hash 通过 | reuse-shard | 复用不可变分片 |
| 转换未完成或没有可核验产物 | retry-new-attempt | 新目录重做，保留旧尝试记录 |
| Loader 无回执、失败、取消，或结果仍未知 | reconcile | 先停定旧写入者，核对该分片全部身份与内容 |
| 完整提交且证据匹配当前 epoch | skip | 跳过这一分片 |
| 全部分片内容确认不存在，旧写入者已停止 | retry-new-attempt | 新请求重新导入 |
| 部分提交、内容不完整，或已完成回执与后续观测冲突 | restore-and-reload | 恢复干净图并更换 epoch，按清单重新导入 |
| validate/restore/measure 中断 | restore-new-attempt | 恢复起始状态，新尝试重跑，不拼接测量数据 |

absence/committed 证明必须覆盖整个分片的 ID 和内容，不能用抽样代替。`writerQuiesced: true` 表示旧请求已停止产生后续写入；
否则“当前不存在”可能在检查后被迟到提交推翻。缺乏停写/核对能力的 Loader 要采用恢复重导路径。
unknown 观测可以用 `writerQuiesced: false` 如实记录无法确认停写，此时仍只能继续核对。
恢复建议只针对传入的请求与证据，调度器仍需检查同一图上其他分片和并发任务的状态。

## 本次验证与下一步

自动测试使用临时目录中的少量字节，完成后删除，不获取图数据、不启动数据库。
覆盖跨 attempt/epoch 回执、未知和部分提交、陈旧证明、活动写入者、分片损坏、路径链接、64 位预算与并发幂等登记。

全分片核对与完整官方 driver 适配见 [最小执行适配器](ldbc-adapter.md)。
仍需在真实 SF100/HStore 环境验证停写、恢复、导入和全部操作；通用组件 dispatch/事件流属于后续工具闭环。
已有 SF100 planning 的 `RUNTIME_NOT_IMPLEMENTED` 仍指统一自动执行尚未实现，最小独立组件可以按文档执行。
