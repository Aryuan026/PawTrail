# PawTrail Archive Tool

PawTrail 用来把 `conversations.json` 整理成可读、可检索、可审阅的记忆文件。

## 快速使用
1. 打开 `docs/index.html`（或本地 `ui_index.html`）。
2. 上传 `conversations.json`。
3. 勾选窗口（支持跨月份勾选）。
4. 导出：
- `导出勾选md`：纯原样合并 md（不受右侧 topic 选项影响）。
- `按窗口批量ZIP` / `按天重拼ZIP`：按右侧选项导出。

## 核心开关说明
1. `启用 topic 分段`
- 开：导出 topic 标题与标签；并自动附加 `^msg` 锚点用于定位。
- 关：导出原始批量 md（无 topic 分段）。

2. `导出主题索引CSV`
- 开：在 ZIP 里额外导出 topic 索引表（`topic_map_*.csv`）。
- 关：只导出 md。

3. `写入 frontmatter`
- 在 md 顶部写元数据草稿。
- 最终建议手工补写更稳定的检索关键词，不必照搬自动主题词。

4. `msg 锚点样式`
- 默认：`仅自定义`（全局唯一锚点，适合长期归档与跨文件检索）
- `仅经典`：`^msg-000001`
- `经典 + 全局唯一`：同时输出经典锚点和自定义锚点
- `仅自定义`：只输出自定义锚点
- 模板变量：`{scope} {conv} {window} {day} {msg} {g}`

## Topic 审阅页
1. 导入 md（按天或按窗口都可）。
2. 可选导入 topic csv。
3. 手工改标题/标签/index_id，保留或删除误标。
4. 导出：
- `修订MD`
- `修订CSV`
- `sidecar`（人工批注备份）
