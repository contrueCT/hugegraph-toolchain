# Go组件的宿主时钟证明

Go runner使用进程内单调elapsed，Linux原`CLOCK_MONOTONIC`在每个Time.Now前后采样，保存上下界。纳秒使用十进制字符串；每个phase保存实际start/end（包含排空）。独立reader检查所有原始区间与elapsed存在共同原点区间，保留区间宽度。

```text
宿主Python CLOCK_MONOTONIC             Docker内Go clock-probe
    send-before ── nonce/sequence ────> raw-before / Time.Now / raw-after
    recv-after  <── 同nonce及原区间 ──┘
        |
    /proc/hostPID/exe + start + NSpid + namespace + boot
        |
    五次交换 + 进程前后身份 + Docker终态/清理 → 原始锁 → 离线核验
```

```bash
# Linux宿主执行；只使用已经存在的镜像，明确给出binary和资源边界。
hgbench http engine clock-probe --binary /absolute/hgbench-http-go \
  --image-id sha256:LOCAL_IMMUTABLE_ID --out /new/clock-proof
# 离线操作，可搬移证据后在本机核验。
hgbench http engine clock-verify /new/clock-proof
```

生产者不发送HTTP请求。Docker限制2CPU/256MiB/64PID，无网络，只读binary/root，去掉capabilities，用户1000，固定Go GC=100/内存软限off/GODEBUG空。只允许本地immutable image，30秒共享截止加5秒清理余量；任何失败保留原件和错误，返回非零。create结果不确定时按唯一name查同一对象并核label后清理，绝不重新创建或启动。信号进入同一清理路径。

一条ready及5个nonce交换，最后一次finish；每个响应最多64KiB，整份proof最多1MiB。Host读取实际Docker root PID的exe SHA、startTicks、NSpid、boot和namespace，交换前后重新核验。Go进程直接为容器PID1，无init/wrapper。两端time namespace一致且monotonic/boottime偏移均为0。单次交互最多100ms、Go原区间最多1ms，超界保留失败。未丢弃或重采样任何一次交换。

reader核验完整文件清单/大小/SHA、生产者源码、固定Go build协议、实际exe和前后进程身份、容器限制和清理、nonce/序号、五个独立宿主区间及elapsed共同原点。文件读取拒绝symlink/special file并检查读取期间漂移。返回的内存证明不可变；复制或纯JSON不能冒充已经通过实际IO核验的对象。

当前证明范围是探针进程及其冻结时钟实现。`measurementProcessAssociationAccepted=false`、`performanceAccepted=false`。各次测量的活进程身份/phase窗口与collector/JVM关联仍需单独证据；不能把探针PID或其原点直接替换到测量进程，也不能继承旧Node运行时校准。正式测试仍要求HG monitor关闭和宿主swap关闭。

## 每次测量进程的两侧握手

新Go组件接受可选`run --identity-gate NEW-DIRECTORY`。它同步并原子发布`ready.json`和`finished.json`，分别在任何HTTP之前、全部phase及writer收尾之后等待宿主精确释放；每侧10秒，可取消，不改phase窗口。回执包含run ID/stage、随机nonce和marker原字节SHA，旧marker不复用。功能/单臂默认入口不启用等待；正式控制器接线时需同时冻结该策略及收尾预算。

宿主`go-process-observer.py`从同一Docker容器的PID清单中查找被握持的实际Go进程，独立读exe/PID/start/namespace/boot，观察前后核对后先保存证据再释放。它只枚举该容器成员，不扫描宿主进程树。调用者提供已经冻结且受截止/输出上限约束的Docker执行函数。

```bash
hgbench http engine process-verify /host-observations \
  --clock-proof /same-binary-clock-proof \
  --run /original/run.json --request /original/request.json \
  --markers /original/gate-directory
```

公开reader核对两侧实际marker/release字节、宿主采证源码、同binary/image的独立时钟证明、前后同一活进程，以及全部phase被宿主两侧时刻包围。真实Docker机制联验已验证0请求等待、释放后3个正确请求、两侧采证和清理；它只使用本机协议peer，不是HG性能结果。

该分项返回`measurementProcessAssociationAccepted=true`时，`collectorAssociationAccepted`、`jvmAssociationAccepted`、`performanceAccepted`仍为false。完整正确性/trace证据、collector/JVM关联及新工程校准继续独立判断。正式paired/preset/capacity控制器接线仍在推进。
