# PawTrail Archive Tool

PawTrail 用来把 `conversations.json` 整理成可读、可检索、可审阅的记忆文件。

## 快速使用
1. 打开 `docs/index.html`（或本地 `ui_index.html`）。
2. 上传解压后的 `conversations.json`；如果手里只有 ChatGPT 导出的 `.zip`，也可以直接上传。
3. 选择 ChatGPT 分支处理方式（默认当前分支；小文件可选全部分支）。
4. 勾选窗口（支持跨月份勾选）。
5. 导出：
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

## ChatGPT 官方导出与大文件
ChatGPT App / 网页端导出的压缩包里通常会有 `conversations.json`。如果 `conversations.json` 本身已经有几百 MB 到 900MB，可以直接上传这份 JSON，页面会自动进入流式解析并显示读取进度。zip 只是额外支持：用户手里只有原始导出压缩包时，也可以不解压直接上传。

分支信息来自 `conversations.json` 里的 `mapping / current_node / children`。zip 只是外壳，不决定有没有分支。

分支模式：

- `当前分支`：默认模式，只导出 ChatGPT 当前打开的主分支。
- `时间最新分支`：没有可靠当前节点时，选择时间最新的一条分支。
- `全部分支`：保留所有叶子分支，适合审计“改写 / 重新生成 / 分叉”痕迹；共同祖先消息会重复出现。

导出目录里会生成 `branch_manifest.jsonl`，记录每条分支的 `branch_id`、`leaf_node`、是否为当前分支，以及原始 `node_path`。

### 高级排障

直接处理解压后的 `conversations.json`：

```bash
python3 archive_tool.py --input /path/to/conversations.json --out ./out/chatgpt --branch-mode active
```

也可以直接传入官方导出的 zip，PawTrail 会自动寻找里面的 `conversations.json`：

```bash
python3 archive_tool.py --input /path/to/chatgpt-export.zip --out ./out/chatgpt --branch-mode active
```

## Topic 审阅页
1. 导入 md（按天或按窗口都可）。
2. 可选导入 topic csv。
3. 手工改标题/标签/index_id，保留或删除误标。
4. 导出：
- `修订MD`
- `修订CSV`
- `sidecar`（人工批注备份）

## GitHub Pages 发布
本目录已准备好发布入口：`docs/index.html`。

1. 新建 GitHub 仓库并上传 `chat_archive_tool` 全部文件。
2. 仓库 `Settings -> Pages`。
3. `Build and deployment` 选择 `Deploy from a branch`。
4. Branch 选 `main`，Folder 选 `/docs`，保存。
5. 等待 1-2 分钟，打开生成的 Pages 链接即可在线使用。
