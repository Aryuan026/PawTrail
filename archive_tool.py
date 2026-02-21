#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import sys
from json import JSONDecoder

CHUNK_SIZE = 1024 * 1024
TOPIC_TRIGGERS_DEFAULT = ["对了", "话说回来", "顺便", "另外", "再说", "哦对", "换个话题", "题外话"]


def safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


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


def normalize_message(msg):
    role = (msg.get("author", {}) or {}).get("role") or msg.get("role") or "assistant"
    content = ""
    if isinstance(msg.get("content"), dict) and "parts" in msg.get("content", {}):
        parts = msg["content"].get("parts", [])
        if isinstance(parts, list):
            content = "\n".join([p for p in parts if isinstance(p, str)])
    elif isinstance(msg.get("content"), str):
        content = msg.get("content", "")
    elif isinstance(msg.get("content"), dict) and "text" in msg.get("content", {}):
        content = msg["content"].get("text", "")
    ts = msg.get("create_time")
    if not ts:
        meta = msg.get("metadata", {}) or {}
        ts = meta.get("timestamp") or meta.get("timestamp_")
    return {
        "role": role,
        "content": (content or "").strip(),
        "ts": normalize_timestamp(ts)
    }


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


def linearize_conversation(conv):
    if not conv or not conv.get("mapping"):
        return []
    mapping = conv.get("mapping") or {}

    def collect_path_from_current_node():
        node_id = conv.get("current_node")
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

    def collect_latest_branch_path():
        all_ids = list(mapping.keys())
        if not all_ids:
            return []
        root_id = None
        for i in all_ids:
            parent_id = (mapping.get(i) or {}).get("parent")
            if parent_id is None or parent_id not in mapping:
                root_id = i
                break
        if root_id is None:
            root_id = all_ids[0]
        ids = []
        visited = set()
        cache = {}
        stack = set()
        node_id = root_id
        while node_id and node_id not in visited and node_id in mapping:
            visited.add(node_id)
            ids.append(node_id)
            node = mapping.get(node_id) or {}
            children = node.get("children", []) or []
            if not children:
                break
            next_id = None
            best_ts = -1
            for child_id in children:
                if child_id not in mapping:
                    continue
                child_ts = get_latest_subtree_timestamp(child_id, mapping, cache, stack)
                if next_id is None or child_ts > best_ts:
                    next_id = child_id
                    best_ts = child_ts
            if not next_id:
                break
            node_id = next_id
        return ids

    def path_to_messages(path_ids):
        out = []
        for pid in path_ids:
            node = mapping.get(pid) or {}
            msg = node.get("message")
            if not msg:
                continue
            m = normalize_message(msg)
            if m.get("content"):
                out.append(m)
        return out

    primary = path_to_messages(collect_path_from_current_node())
    fallback = path_to_messages(collect_latest_branch_path())
    if not primary:
        return fallback
    if len(primary) < len(fallback):
        return fallback
    return primary


def parse_openai_conversations(obj):
    if isinstance(obj, list):
        conversations = obj
    elif isinstance(obj, dict) and isinstance(obj.get("conversations"), list):
        conversations = obj.get("conversations")
    elif isinstance(obj, dict) and obj.get("mapping"):
        conversations = [obj]
    else:
        conversations = []

    for conv in conversations:
        if conv.get("mapping"):
            messages = linearize_conversation(conv)
        else:
            raw_msgs = conv.get("messages", []) or []
            messages = [normalize_message(m) for m in raw_msgs]
        if messages:
            yield {
                "title": conv.get("title") or "Conversation",
                "messages": messages
            }


def iter_json_array(path):
    decoder = JSONDecoder()
    with open(path, "r", encoding="utf-8") as f:
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


def write_by_day_raw(conversations_iter, out_root):
    day_dir = os.path.join(out_root, "by_day")
    ensure_dir(day_dir)
    count = 0
    for conv in conversations_iter:
        for m in conv.get("messages", []):
            if not m.get("content"):
                continue
            ts = m.get("ts", 0)
            dk = day_key(ts)
            raw_path = os.path.join(day_dir, f"{dk}.raw")
            with open(raw_path, "a", encoding="utf-8") as w:
                content = (m.get("content") or "").replace("\n", " ")
                line = f"{ts}\t[{to_iso(ts)}] {m.get('role')}: {content}\n"
                w.write(line)
            count += 1
    return count


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


def load_conversations_stream(path):
    # streaming array only
    for obj in iter_json_array(path):
        for conv in parse_openai_conversations(obj if isinstance(obj, (dict, list)) else []):
            yield conv


def load_conversations_fallback(path):
    # full load (large memory)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for conv in parse_openai_conversations(data):
        yield conv


def main():
    parser = argparse.ArgumentParser(description="Chat archive tool")
    parser.add_argument("--input", required=True, help="path to conversations.json")
    parser.add_argument("--out", required=True, help="output root folder")
    parser.add_argument("--stage", default="month", choices=["month", "topic-preview", "split-topics"], help="run stage")
    parser.add_argument("--month", default=None, help="month key for split-topics, e.g. 2026-01")
    parser.add_argument("--topics-file", default=None, help="path to .topics.md for split-topics")
    parser.add_argument("--gap-hours", type=int, default=4, help="topic boundary gap in hours")
    args = parser.parse_args()

    ensure_dir(args.out)

    if args.stage == "month":
        # attempt streaming parse, fallback to full load if file is object
        try:
            conv_iter = load_conversations_stream(args.input)
            count = write_by_day_raw(conv_iter, args.out)
        except Exception:
            conv_iter = load_conversations_fallback(args.input)
            count = write_by_day_raw(conv_iter, args.out)
        build_month_files(args.out)
        build_topic_preview(args.out, gap_hours=args.gap_hours)
        print(f"done: wrote {count} messages into by_month + topic previews")
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
