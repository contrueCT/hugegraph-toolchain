# HTTP CLI 分发与依赖锁

`npm run build` 将根目录 `package-lock.json` 的原字节同步到固定资产
`release/dependency-lock.json`，并在 `package.json` 记录字节数与 SHA-256。
`npm run release:check` 和 `npm pack` 的 prepack 检查要求二者逐字节相同，
同时核对包名、版本、入口和依赖声明。更新依赖后先构建，再检查和打包。
此资产保留 npm lock 的声明语义，不改变 npm 的安装行为。

分发包携带 `src/`、`dist/` 和 `release/`。HTTP comparison 的 planner、
report 以及 workflow 库存统一绑定固定发布资产及其校验源码；运行时只读取
当前工具包内的资产。缺失、摘要不符或依赖声明不符会拒绝，消费者目录的
`package-lock.json` 不参与替代。已冻结的旧工具源及证据继续使用原有解释。
源和锁的绑定不证明安装后的第三方包完整性，也不构成性能资格。

定向验证：`node --test tests-normative/http-package.test.mjs`。
其中实际执行 `npm pack`、解包到独立临时目录，物理复制声明的两个生产依赖，
通过解包后的公开 CLI 执行 quick dry-run、plan/verify 和缺失 raw 的失败报告。
这些输入是本地协议夹具，不启动 Docker、HTTP 负载或真实目标。

分发文档保留安装、命令、协议和验收范围。本机进度日志、环境交接和运行诊断由
`.gitignore`与`docs/.npmignore`的相同清单排除，原文件仍保留在工作区。
原始证据和旧交付快照不随包分发，相应说明保留为文本，避免分发失效链接。
物理打包检查会核对README和包内文档的全部相对文件链接，并确认关键本机记录未混入，
同时检查全量扫描组件与固定核验向量仍在包内。旧候选归档和远端冻结执行不受此调整影响。
