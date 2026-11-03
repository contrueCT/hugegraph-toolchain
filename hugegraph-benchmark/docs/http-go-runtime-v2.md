# Go复合评测运行时 v2

TS CLI继续管理计划与任务。Go profile v2显式固定binary/source SHA、Go版本、平台、CPU/RSS和hostIdentityGate；comparison runtime v2绑定相同binary及独立clockProof。旧Node runtime v1按原定义读取。

```text
Go profile v2 + 配对/单观察计划 + runtime v2
       |
prepare：核源码/binary/宿主clock proof → 冻结控制脚本、SDK和预算
       |
新HG JVM → 实际模式核验 → collector开始 → driver task
       |                                   |
       |                             Go ready marker（等待）
       |                                   |
宿主按容器PID清单查真实Go身份 → 保存观察 → 精确nonce/SHA释放
       |                                   |
       |                        HTTP预热/测量 → trace writer收尾
       |                                   |
       └───────────────────── Go finished marker（等待）
                                           |
                               再次宿主采证/释放 → 下一逻辑run
       |
模式后验 → collector/容器收尾 → 封存raw+观察+clock proof
       |
独立reader逐件重建 → 功能有效性/质量/校准分别判定
```

runtime v2在原runtime字段上增加executor：

```json
{
    "engine": "go-http-v1",
    "binary": {
        "path": "/absolute/registered/hgbench-http-go",
        "sha256": "FROZEN_BINARY_SHA256",
        "sourceSha256": "FROZEN_SOURCE_SHA256",
        "goVersion": "go1.27.3",
        "goos": "linux",
        "goarch": "amd64"
    },
    "clockProof": "/absolute/previously-captured-clock-proof"
}
```

profile注册时显式启用`--host-identity-gate`。该配置必须由活跃宿主控制器释放，单独task执行会在等候上限后失败并保留证据。binary按冻结的同一绝对路径只读挂载；不得遮蔽控制器或系统虚拟目录。driver容器CPU/memory必须与profile声明相等。prepare核完整Go source清单、实际binary及clock proof；宿主重启使原clock proof失效。

每个逻辑run和服务检查有两侧marker，每侧上限10秒。规划将这20秒列为安全预算，正常额外耗时保持未知，整任务截止继续共享。元数据预算包含marker/release、独立宿主观察和clock proof，超界在执行前拒绝。

宿主只观察计划中列明的路径，按实际Docker容器成员查PID；不重复释放、重用Go进程或跨过缺失的阶段。raw锁纳入全部marker/release；宿主观察、冻结proof及控制脚本单独归档。离线reader复核完整清单、来源、进程身份、阶段时刻与数据质量，所有计划分母仍保留。

当前复合链已完成真实受控HG全11题功能验收，33请求全部正确、66两侧观察完整，核心离线产物逐字节重建，见封存交付（本机记录未随包分发）。首个40rps同资源对比未证明整体驱动改善；解析优化后的新版本复测单独保存。旧Node A/A、余量和观测成本不赋予Go资格。宿主/Go时钟关联通过仍不授予collector/JVM细粒度时间归因、统计认证或性能结论。正式测试逐次要求HG monitor关闭和宿主swap关闭。
