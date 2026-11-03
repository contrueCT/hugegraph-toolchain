# standard/full 的已确认规则与工程实现边界

quick v0.1.0及A/A004的参数、指标、门槛和原始结果保持原定义。

2026-10-23用户已确认工程验收，真实误报率统计认证单列可选；随后已确认吞吐主线方案：
standard默认全11题逐题吞吐—延迟曲线及独立复测最高观察正确吞吐，业务SLO可选，rate/预算由工程预注册。
full扩充分层及具有真实比例的混合业务；具体方案见[已确认范围](http-capacity-proposal.md)。
没有业务SLO时不发明阈值、不阻塞曲线，也不将观察最高点称为SLO容量或无条件可持续极限。
有事前冻结的SLO时，才从独立确认点派生容量下界/边界区间；固定低负载延迟为补充。
范围确认不等于具体rate、SLO、窗口和资源数值已校准；工程参数将前瞻展示、校准并冻结，不再逐项请求用户选择。

## 1. 按已确认评测目的固定新版本指标角色

| 场景                           | 推断主指标                         | 质量门与描述指标                                        |
| ------------------------------ | ---------------------------------- | ------------------------------------------------------- |
| 固定到达率、低于饱和的延迟比较 | 预声明延迟分位及完整题目×负载族    | 正确窗内吞吐、交付率、错误/丢弃、迟到；所有原始证据保留 |
| 容量/闭环吞吐比较              | 预声明正确吞吐及需要比较的延迟指标 | 正确性、可选事前SLO、错误/丢弃、驱动与观测预算          |

固定open率下所有请求都按时成功时，goodput等于给定速率，配对差异可全部为0。
现有paired分析会给degenerate warning；cohort要求整族无warning，所以一个恒定goodput会使整组unknown。
加配对、时间或bootstrap不能保证消除这种退化；不能为制造方差故意提高drop，也不能把零方差区间改称已证明等价。

新计划需固定每个指标的角色，角色/完整比较族进入内容锁。预声明质量门继续强制，退化指标和失败继续完整报告。
完整曲线、探索选点与独立复测分别留证；共有实测点可作同负载比较，驱动受限点不能当target容量上界。
任何调整只作用于新版本/新attempt，不改004或quick，不通过事后删除恒定goodput提高旧结果资格。

## 2. 区分工程校准验收与真实误报率认证（用户已确认）

standard的工程验收继续要求：来源/环境和完整原始证据绑定、正确性及负载质量、独立配对与A/A噪声、已知退化检测、驱动余量、稳定窗口及观测成本证据。所有声明的family/alpha/阈值前瞻冻结，低样本/压力/不可比保留unknown。

真实误报率的置信上界认证单列为可选强度，不能由两次A/A宣称完成。当前cohort用双侧95%Clopper–Pearson区间：0/2误判的上界约84.2%，0/20约16.8%；至少0/72才可能令上界不超过5%。这些是当前函数的理论计算，不是实测成绩；不能因此自动派发72组长任务。

若用户选择将真实误报率≤5%的认证列为必过门，则必须另行预声明足够独立实验和资源预算。代码中experiment数量下限2与强统计认证不是同一保证；`qualificationAccepted=false`和`performanceAccepted=false`当前仍是独立验收状态。

这两项的方向与范围已获确认。工程验收/统计认证分离及指标角色v2已完成本地实现和专项检查，005正在使用新冻结政策；真实工程验收尚未通过。
当前calibration-evidence只独立重验A/A贡献。clean-window、driver-headroom、observer-and-validator-cost、known-regression、warmup-stability和formal-freeze仍明确返回unknown/verified-evidence-adapter-not-connected。这些项还需要真实证据及相应可信接线，不能仅凭操作结果的passed布尔完成总验收。现有正确性、SF100规模、数据一致性、重复性和真实环境缺项仍保留，quick功能通过不替代这些条件。

## 004与quick的冻结行为（保持原政策）

- `components/http/comparison-plan.mjs`：固定为每case×load加入latency与goodput。
- `components/paired/core.mjs`：退化效应→warning及not-assessed。
- `components/http/calibration-evidence.mjs`：任一指标有warning令该experiment无法分类，cohort保留unknown。
- `components/http/analyzer.mjs`：goodput为正确窗内完成数/固定测量时长。
