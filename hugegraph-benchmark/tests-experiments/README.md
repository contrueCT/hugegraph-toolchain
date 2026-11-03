# 固定实验检查

这里的测试验证一次已冻结校准操作的计划与生命周期，使用其原始源码和本地草案。
通用组件与公开 CLI 的默认测试仍在 `tests-normative/`，通过 `npm test` 运行。

当前两份 headroom 测试共17项，保持原测试内容；执行需要：

- `.hgbench/http-driver-headroom-{plan,execute}.py` 及同目录固定的 known-delay helpers；
- `.hgbench/http-driver-headroom-draft-v2` 完整内容锁和原绝对路径所引用的输入；
- `evidence/phase2-http-comparison-functional-v6/{source,remote}`，含可运行的冻结 CLI 依赖；
- 当前主工作树的 `components/http/` 分窗分析器和 Node/Python。

```bash
python3 -B -m unittest discover -s tests-experiments \
  -p 'test_http_driver_headroom_*.py' -v
```

该命令离线运行协议检查和实际公共规划命令，不启动 Docker、HTTP 测量或远端任务。
缺少原实验输入会明确失败；没有跳过检查来代替通过。重建迁移后的新实验时，应使用其对应的固定材料，
不把此处引用的本机草案作为可跨主机直接执行的计划。

原始通过记录及远端使用的小包位于 `evidence/phase2-http-driver-headroom-001-materials`。
这里移动测试文件的目的在于显式区分所需输入，已封存材料中的文件与摘要均保持不变。
