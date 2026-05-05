#!/usr/bin/env python3
import argparse
import io
import datetime as dt
import json
import os
import re
import sys
import zipfile
from contextlib import contextmanager
from json import JSONDecoder

CHUNK_SIZE = 1024 * 1024
LARGE_INPUT_FALLBACK_LIMIT = 128 * 1024 * 1024
TOPIC_TRIGGERS_DEFAULT = ["对了", "话说回来", "顺便", "另外", "再说", "哦对", "换个话题", "题外话"]


def safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def safe_slug(value, fallback="item"):
    text = str(value or "").strip()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5_-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return (text or fallback)[:80]


def normalize_timestamp(ts):
    if ts is None:
        return 0
    try:
        t = float(ts)
    except Exception:
        return 0
    if t > 1e12:
        t = t / 1000.0
    return int(t)


def extract_text_content(content):
    if isinstance(content, dict) and "parts" in content:
        parts = content.get("parts", [])
        out = []
        if isinstance(parts, list):
            for part in parts:
                if isinstance(part, str):
                    out.append(part)
                elif isinstance(part, dict):
                    text = part.get("text") or part.get("content") or part.get("name") or ""
                    if isinstance(text, str):
                        out.append(text)
        return "\n".join([item for item in out if item])
    if isinstance(content, dict) and "text" in content:
        return content.get("text", "")
    if isinstance(content, str):
        return content
    return ""


def normalize_message(msg, node_id=None):
    role = (msg.get("author", {}) or {}).get("role") or msg.get("role") or "assistant"
    content = extract_text_content(msg.get("content"))
    ts = msg.get("create_time")
    if not ts:
        meta = msg.get("metadata", {}) or {}
        ts = meta.get("timestamp") or meta.get("timestamp_")
    out = {
        "role": role,
        "content": (content or "").strip(),
        "ts": normalize_timestamp(ts)
    }
    if node_id:
        out["node_id"] = node_id
    return out


def get_node_message_timestamp(node):
    if not node or not node.get("message"):
        return 0
    msg = node.get("message") or {}
    raw_ts = msg.get("create_time")
    if not raw_ts:
        meta = msg.get("metadata", {}) or {}
        raw_ts = meta.get("timestamp") or meta.get("timestamp_")
    return normalize_timestamp(raw_ts)


def get_latest_subtree_timestamp(node_id, mapping, cache, stack):
    if not node_id or node_id not in mapping:
        return 0
    if node_id in cache:
        return cache[node_id]
    if node_id in stack:
        return get_node_message_timestamp(mapping.get(node_id))
    stack.add(node_id)
    node = mapping.get(node_id) or {}
    best_ts = get_node_message_timestamp(node)
    for child_id in node.get("children", []) or []:
        child_ts = get_latest_subtree_timestamp(child_id, mapping, cache, stack)
        if child_ts > best_ts:
            best_ts = child_ts
    stack.remove(node_id)
    cache[node_id] = best_ts
    return best_ts


def find_root_ids(mapping):
    roots = []
    for node_id, node in mapping.items():
        parent_id = (node or {}).get("parent")
        if parent_id is None or parent_id not in mapping:
            roots.append(node_id)
    return roots or list(mapping.keys())[:1]


def collect_path_to_root(mapping, node_id):
    if not node_id or node_id not in mapping:
        return []
    ids = []
    seen = set()
    while node_id and node_id not in seen and node_id in mapping:
        seen.add(node_id)
        ids.append(node_id)
        node_id = (mapping.get(node_id) or {}).get("parent")
    ids.reverse()
    return ids


def build_latest_subtree_cache(mapping):
    cache = {}
    visiting = set()
    for start_id in list(mapping.keys()):
        if start_id in cache:
            continue
        stack = [(start_id, False)]
        while stack:
            node_id, expanded = stack.pop()
            if node_id in cache or node_id not in mapping:
                continue
            node = mapping.get(node_id) or {}
            if expanded:
                best_ts = get_node_message_timestamp(node)
                for child_id in node.get("children", []) or []:
                    if child_id in mapping:
                        child_ts = cache.get(child_id, get_node_message_timestamp(mapping.get(child_id)))
                        if child_ts > best_ts:
                            best_ts = child_ts
                cache[node_id] = best_ts
                visiting.discard(node_id)
                continue
            if node_id in visiting:
                cache[node_id] = get_node_message_timestamp(node)
                continue
            visiting.add(node_id)
            stack.append((node_id, True))
            for child_id in reversed(node.get("children", []) or []):
                if child_id in mapping and child_id not in cache:
                    stack.append((child_id, False))
    return cache


def collect_latest_branch_path(mapping):
    all_ids = list(mapping.keys())
    if not all_ids:
        return []
    cache = build_latest_subtree_cache(mapping)
    root_ids = find_root_ids(mapping)
    root_id = max(root_ids, key=lambda item: (cache.get(item, 0), str(item)))
    ids = []
    visited = set()
    node_id = root_id
    while node_id and node_id not in visited and node_id in mapping:
        visited.add(node_id)
        ids.append(node_id)
        node = mapping.get(node_id) or {}
        children = [child for child in (node.get("children", []) or []) if child in mapping]
        if not children:
            break
        node_id = max(children, key=lambda item: (cache.get(item, 0), str(item)))
    return ids


def collect_leaf_branch_paths(mapping):
    if not mapping:
        return []
    cache = build_latest_subtree_cache(mapping)
    paths = []
    stack = [(root_id, [root_id]) for root_id in reversed(find_root_ids(mapping))]
    while stack:
        node_id, path = stack.pop()
        node = mapping.get(node_id) or {}
        children = [
            child for child in (node.get("children", []) or [])
            if child in mapping and child not in path
        ]
        if not children:
            paths.append(path)
            continue
        children.sort(key=lambda item: (cache.get(item, 0), str(item)), reverse=True)
        for child_id in children:
            stack.append((child_id, path + [child_id]))
    return paths


def path_to_messages(mapping, path_ids):
    out = []
    for pid in path_ids:
        node = mapping.get(pid) or {}
        msg = node.get("message")
        if not msg:
            continue
        m = normalize_message(msg, node_id=pid)
        if m.get("content"):
            out.append(m)
    return out


def build_branch_record(conv, path_ids, index, is_current):
    leaf_id = path_ids[-1] if path_ids else ""
    conv_id = conv.get("id") or conv.get("conversation_id") or conv.get("title") or "conversation"
    branch_label = "current" if is_current else f"alternate-{index:03d}"
    return {
        "branch_id": f"{safe_slug(conv_id, 'conversation')}__b{index:03d}__{safe_slug(str(leaf_id)[:12], 'leaf')}",
        "branch_label": branch_label,
        "leaf_node": leaf_id,
        "is_current_branch": bool(is_current),
        "node_path": path_ids,
        "node_count": len(path_ids)
    }


def iter_conversation_branches(conv, branch_mode="active"):
    if not conv or not conv.get("mapping"):
        raw_msgs = conv.get("messages", []) or []
        messages = [normalize_message(m) for m in raw_msgs]
        if messages:
            yield {
                "branch_id": "",
                "branch_label": "",
                "leaf_node": "",
                "is_current_branch": True,
                "node_path": [],
                "node_count": 0,
                "messages": messages
            }
        return

    mapping = conv.get("mapping") or {}
    current_path = collect_path_to_root(mapping, conv.get("current_node"))
    latest_path = collect_latest_branch_path(mapping)

    if branch_mode == "all":
        paths = collect_leaf_branch_paths(mapping)
        if current_path and tuple(current_path) not in {tuple(path) for path in paths}:
            paths.append(current_path)
    elif branch_mode == "latest":
        paths = [latest_path]
    else:
        paths = [current_path or latest_path]

    seen = set()
    clean_paths = []
    for path in paths:
        key = tuple(path or [])
        if not key or key in seen:
            continue
        seen.add(key)
        clean_paths.append(path)

    current_key = tuple(current_path or [])
    for index, path in enumerate(clean_paths, start=1):
        is_current = bool(current_key and tuple(path) == current_key)
        record = build_branch_record(conv, path, index, is_current)
        record["messages"] = path_to_messages(mapping, path)
        if record["messages"]:
            yield record


def linearize_conversation(conv):
    for branch in iter_conversation_branches(conv, branch_mode="active"):
        return branch.get("messages", [])
    return []


def parse_openai_conversations(obj, branch_mode="active"):
    if isinstance(obj, list):
        conversations = obj
    elif isinstance(obj, dict) and isinstance(obj.get("conversations"), list):
        conversations = obj.get("conversations")
    elif isinstance(obj, dict) and obj.get("mapping"):
        conversations = [obj]
    else:
        conversations = []

    for conv in conversations:
        for branch in iter_conversation_branches(conv, branch_mode=branch_mode):
            messages = branch.get("messages", [])
            if not messages:
                continue
            title = conv.get("title") or "Conversation"
            if branch.get("branch_label") and branch_mode == "all":
                title = f"{title} [{branch.get('branch_label')}]"
            yield {
                "id": conv.get("id") or conv.get("conversation_id") or "",
                "title": title,
                "messages": messages,
                "branch": {
                    "branch_id": branch.get("branch_id", ""),
                    "branch_label": branch.get("branch_label", ""),
                    "leaf_node": branch.get("leaf_node", ""),
                    "is_current_branch": branch.get("is_current_branch", False),
                    "node_path": branch.get("node_path", []),
                    "node_count": branch.get("node_count", 0),
                    "message_count": len(messages)
                }
            }


def find_zip_conversations_member(zf):
    files = [info.filename for info in zf.infolist() if not info.is_dir()]
    for name in files:
        if os.path.basename(name) == "conversations.json":
            return name
    json_files = [name for name in files if name.lower().endswith(".json")]
    if len(json_files) == 1:
        return json_files[0]
    raise ValueError("zip input does not contain conversations.json")


def input_payload_size(path):
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf:
            member = find_zip_conversations_member(zf)
            return zf.getinfo(member).file_size
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


@contextmanager
def open_input_text(path):
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf:
            member = find_zip_conversations_member(zf)
            with zf.open(member, "r") as raw:
                with io.TextIOWrapper(raw, encoding="utf-8") as text:
                    yield text
        return
    with open(path, "r", encoding="utf-8") as f:
        yield f


def iter_json_array(path):
    decoder = JSONDecoder()
    with open_input_text(path) as f:
        buf = ""
        # find array start
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                return
            buf += chunk
            idx = buf.find("[")
            if idx != -1:
                buf = buf[idx + 1:]
                break
        while True:
            # skip whitespace/commas
            i = 0
            while i < len(buf) and buf[i] in " \t\r\n,":
                i += 1
            buf = buf[i:]
            if not buf:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    return
                buf += chunk
                continue
            if buf[0] == "]":
                return
            try:
                obj, idx = decoder.raw_decode(buf)
            except json.JSONDecodeError:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    raise
                buf += chunk
                continue
            buf = buf[idx:]
            yield obj


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def to_iso(ts):
    if not ts:
        return ""
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def day_key(ts):
    if not ts:
        return "unknown"
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def month_key(ts):
    if not ts:
        return "unknown"
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m")


def write_branch_manifest_row(writer, conv):
    branch = conv.get("branch") or {}
    if not branch.get("branch_id"):
        return 0
    row = {
        "conversation_id": conv.get("id", ""),
        "title": conv.get("title", ""),
        "branch_id": branch.get("branch_id", ""),
        "branch_label": branch.get("branch_label", ""),
        "leaf_node": branch.get("leaf_node", ""),
        "is_current_branch": branch.get("is_current_branch", False),
        "message_count": branch.get("message_count", 0),
        "node_count": branch.get("node_count", 0),
        "node_path": branch.get("node_path", [])
    }
    writer.write(json.dumps(row, ensure_ascii=False) + "\n")
    return 1


def write_by_day_raw(conversations_iter, out_root, write_manifest=True, include_branch_labels=False):
    day_dir = os.path.join(out_root, "by_day")
    ensure_dir(day_dir)
    count = 0
    branch_count = 0
    manifest_path = os.path.join(out_root, "branch_manifest.jsonl")
    manifest = open(manifest_path, "w", encoding="utf-8") if write_manifest else None
    try:
        for conv in conversations_iter:
            if manifest:
                branch_count += write_branch_manifest_row(manifest, conv)
            for m in conv.get("messages", []):
                if not m.get("content"):
                    continue
                ts = m.get("ts", 0)
                dk = day_key(ts)
                raw_path = os.path.join(day_dir, f"{dk}.raw")
                with open(raw_path, "a", encoding="utf-8") as w:
                    content = (m.get("content") or "").replace("\n", " ")
                    role = m.get("role")
                    branch = conv.get("branch") or {}
                    if include_branch_labels and branch.get("branch_id"):
                        role = f"{role} [{branch.get('branch_label') or branch.get('branch_id')}]"
                    line = f"{ts}\t[{to_iso(ts)}] {role}: {content}\n"
                    w.write(line)
                count += 1
    finally:
        if manifest:
            manifest.close()
    return count, branch_count


def build_month_files(out_root):
    day_dir = os.path.join(out_root, "by_day")
    month_dir = os.path.join(out_root, "by_month")
    ensure_dir(month_dir)
    if not os.path.isdir(day_dir):
        return

    day_files = sorted([f for f in os.listdir(day_dir) if f.endswith(".raw")])
    month_counts = {}
    for day_file in day_files:
        day_path = os.path.join(day_dir, day_file)
        day = day_file.replace(".raw", "")
        if day == "unknown":
            mkey = "unknown"
        else:
            mkey = day[:7]
        out_path = os.path.join(month_dir, f"{mkey}.md")
        with open(day_path, "r", encoding="utf-8") as r:
            lines = r.readlines()
        # sort by timestamp (ts is first tab-separated token)
        def key_fn(line):
            parts = line.split("\t", 1)
            return safe_int(parts[0], 0)
        lines.sort(key=key_fn)
        with open(out_path, "a", encoding="utf-8") as w:
            if mkey not in month_counts:
                month_counts[mkey] = 0
            for line in lines:
                try:
                    _, rest = line.split("\t", 1)
                except ValueError:
                    continue
                month_counts[mkey] += 1
                anchor = f"^msg-{month_counts[mkey]:06d}"
                w.write(rest.strip() + f" {anchor}\n")


def detect_topic_boundaries(lines, gap_hours, triggers):
    markers = []
    last_ts = None
    for i, line in enumerate(lines):
        m = re.match(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", line)
        ts = None
        if m:
            try:
                ts = int(dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").timestamp())
            except Exception:
                ts = None
        if last_ts and ts:
            if ts - last_ts >= gap_hours * 3600:
                markers.append(i)
        if any(t in line for t in triggers):
            markers.append(i)
        if ts:
            last_ts = ts
    # always mark start
    markers.append(0)
    return sorted(set(markers))


def build_topic_preview(out_root, gap_hours=4, triggers=None):
    if triggers is None:
        triggers = TOPIC_TRIGGERS_DEFAULT
    month_dir = os.path.join(out_root, "by_month")
    if not os.path.isdir(month_dir):
        return
    for fname in sorted(os.listdir(month_dir)):
        if not fname.endswith(".md"):
            continue
        path = os.path.join(month_dir, fname)
        with open(path, "r", encoding="utf-8") as r:
            lines = [ln.rstrip("\n") for ln in r.readlines()]
        if not lines:
            continue
        markers = detect_topic_boundaries(lines, gap_hours, triggers)
        preview_path = os.path.join(month_dir, fname.replace(".md", ".topics.md"))
        with open(preview_path, "w", encoding="utf-8") as w:
            topic_idx = 0
            for i, line in enumerate(lines):
                if i in markers:
                    topic_idx += 1
                    date_prefix = line[1:11] if line.startswith("[") else "unknown"
                    w.write(f"[TOPIC_START] {date_prefix} Topic-{topic_idx:03d}\n")
                w.write(line + "\n")


def split_topics(out_root, month, topics_file):
    by_topic_dir = os.path.join(out_root, "by_topic", month)
    ensure_dir(by_topic_dir)
    with open(topics_file, "r", encoding="utf-8") as r:
        lines = [ln.rstrip("\n") for ln in r.readlines()]
    current = None
    buffer = []

    def flush():
        nonlocal current, buffer
        if not current:
            return
        filename = re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5_-]+", "_", current)
        path = os.path.join(by_topic_dir, f"{filename}.md")
        with open(path, "w", encoding="utf-8") as w:
            w.write("---\n")
            w.write(f"title: {current}\n")
            w.write(f"date: {current[:10] if len(current) >= 10 else ''}\n")
            w.write("---\n\n")
            for ln in buffer:
                w.write(ln + "\n")
        buffer = []

    for line in lines:
        if line.startswith("[TOPIC_START]"):
            flush()
            current = line.replace("[TOPIC_START]", "").strip()
            continue
        buffer.append(line)
    flush()

    # mapping CSV
    mapping_path = os.path.join(out_root, "by_topic", month, "event_anchor_mapping.csv")
    with open(mapping_path, "w", encoding="utf-8") as w:
        w.write("event_anchor,obsidian_path\n")
        for fname in sorted(os.listdir(by_topic_dir)):
            if not fname.endswith(".md"):
                continue
            anchor = fname.replace(".md", "")
            w.write(f"{anchor},{os.path.join('by_topic', month, fname)}\n")


def load_conversations_stream(path, branch_mode="active"):
    # streaming array only
    for obj in iter_json_array(path):
        for conv in parse_openai_conversations(obj if isinstance(obj, (dict, list)) else [], branch_mode=branch_mode):
            yield conv


def load_conversations_fallback(path, branch_mode="active"):
    # full load (large memory)
    with open_input_text(path) as f:
        data = json.load(f)
    for conv in parse_openai_conversations(data, branch_mode=branch_mode):
        yield conv


def main():
    parser = argparse.ArgumentParser(description="Chat archive tool")
    parser.add_argument("--input", required=True, help="path to conversations.json or ChatGPT export .zip")
    parser.add_argument("--out", required=True, help="output root folder")
    parser.add_argument("--stage", default="month", choices=["month", "topic-preview", "split-topics"], help="run stage")
    parser.add_argument("--month", default=None, help="month key for split-topics, e.g. 2026-01")
    parser.add_argument("--topics-file", default=None, help="path to .topics.md for split-topics")
    parser.add_argument("--gap-hours", type=int, default=4, help="topic boundary gap in hours")
    parser.add_argument("--branch-mode", default="active", choices=["active", "latest", "all"], help="OpenAI mapping branch handling")
    args = parser.parse_args()

    ensure_dir(args.out)

    if args.stage == "month":
        # attempt streaming parse, fallback to full load if file is object
        try:
            conv_iter = load_conversations_stream(args.input, branch_mode=args.branch_mode)
            count, branch_count = write_by_day_raw(
                conv_iter,
                args.out,
                include_branch_labels=args.branch_mode == "all"
            )
        except Exception as stream_error:
            if input_payload_size(args.input) >= LARGE_INPUT_FALLBACK_LIMIT:
                print(f"streaming parse failed for large input: {stream_error}", file=sys.stderr)
                sys.exit(1)
            conv_iter = load_conversations_fallback(args.input, branch_mode=args.branch_mode)
            count, branch_count = write_by_day_raw(
                conv_iter,
                args.out,
                include_branch_labels=args.branch_mode == "all"
            )
        build_month_files(args.out)
        build_topic_preview(args.out, gap_hours=args.gap_hours)
        print(f"done: wrote {count} messages and {branch_count} branch records into by_month + topic previews")
    elif args.stage == "topic-preview":
        build_topic_preview(args.out, gap_hours=args.gap_hours)
        print("done: topic previews generated")
    elif args.stage == "split-topics":
        if not args.month or not args.topics_file:
            print("split-topics requires --month and --topics-file")
            sys.exit(2)
        split_topics(args.out, args.month, args.topics_file)
        print("done: topics split")


if __name__ == "__main__":
    main()
