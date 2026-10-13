#!/usr/bin/env python3
"""Convert one Sirius telemetry context into Quent Simulator-model events.

The quent working tree can only analyze the Simulator model (the Sirius
analyzer targets an older quent). This script replays a real Sirius query as
Simulator events so the local server and UI can display it.

What is kept: engine, worker, query, plan, operators ("pipelines"), ports,
tasks, executor threads, and GPU / host memory occupancy.
What is dropped: query groups, task queues, thread groups, manager threads,
data batches, batch placement, channels, NVTX, and the DISK memory tier.
What is synthesized: one TaskExecutor per worker (the Simulator needs a parent
for threads) and per-operator Statistics derived from the task data.

Usage:
  sirius_to_simulator.py --assets run_assets_<id> --query 1 --out converted
  sirius_to_simulator.py --context <context dir> --out converted
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path

NAMESPACE = uuid.UUID("5f0e9c52-0a54-4a53-9d8e-5d1c6f0f2a11")
RESOLVABLE_STATUSES = {"gpu_completed", "runtime_failed"}
STAT_KEYS = ("tasks_processed", "input_bytes", "wall_time_ns", "peak_memory_bytes")


# --- reading ---------------------------------------------------------------


def _order_key(event: dict) -> tuple:
    data = event["data"]
    seq = data.get("seq", 0) if isinstance(data, dict) else 0
    return (event["timestamp"], seq)


def read_stream(ctx: Path, entity: str) -> list[dict]:
    """All events of one entity type, ordered by time then sequence number."""
    events = []
    for path in sorted((ctx / entity).glob("*.ndjson")):
        with path.open() as handle:
            events.extend(json.loads(line) for line in handle if line.strip())
    return sorted(events, key=_order_key)


def group_by_id(events: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for event in events:
        grouped.setdefault(event["id"], []).append(event)
    return grouped


def fsm_state(event: dict) -> tuple[str, dict]:
    state = event["data"]["state"]
    if isinstance(state, str):
        return state, {}
    name, body = next(iter(state.items()))
    return name, body or {}


# --- small builders --------------------------------------------------------


def ref(target: str, data: dict | None = None) -> dict:
    return {"target": target, "data": data}


def make_event(entity_id: str, timestamp: int, data) -> dict:
    return {"id": entity_id, "timestamp": timestamp, "data": data}


def operator_type(instance_name: str) -> str:
    """Drop per-instance indexes: `HASH_JOIN(11) -> FILTER(12)` -> `HASH_JOIN -> FILTER`."""
    return re.sub(r"\(\d+\)", "", instance_name)


def merge_stats(a: dict, b: dict) -> dict:
    merged = {key: a.get(key, 0) + b.get(key, 0) for key in STAT_KEYS}
    merged["peak_memory_bytes"] = max(
        a.get("peak_memory_bytes", 0), b.get("peak_memory_bytes", 0)
    )
    return {key: value for key, value in merged.items() if key in a or key in b}


def statistics_event(operator_id: str, timestamp: int, stats: dict) -> dict:
    attributes = [{"key": k, "value": {"U64": int(v)}} for k, v in stats.items()]
    return make_event(
        operator_id, timestamp, {"Statistics": {"custom_attributes": attributes}}
    )


# --- tasks -----------------------------------------------------------------


@dataclass(frozen=True)
class ConvertedTask:
    events: list[dict]
    operator_id: str
    stats: dict = field(default_factory=dict)


def _memory_usage(memory_fields: dict[str, str], reservation: dict) -> dict:
    """`use_gpu_memory` / `use_host_memory` for a reservation on a mapped tier."""
    usage = {"use_gpu_memory": None, "use_host_memory": None}
    resource_id = reservation["resource_id"]
    field_name = memory_fields.get(resource_id)
    if field_name:
        nbytes = reservation["capacity"]["capacity_bytes"]
        usage[field_name] = ref(resource_id, {"bytes": nbytes})
    return usage


def convert_task(
    task_id: str, events: list[dict], memory_fields: dict[str, str]
) -> ConvertedTask | None:
    """Map a Sirius task FSM onto the Simulator task FSM.

    Created/Queued/Routing -> Queueing, Reserving -> Allocating,
    Preparing -> Loading (only when data moves between tiers),
    Computing* + Finalizing -> one Computing, Exit -> Exit.
    Returns None for tasks that never computed (they cannot be expressed:
    the Simulator FSM only reaches Exit from Computing).
    """
    states = [(e["timestamp"], *fsm_state(e)) for e in events]

    def first(name: str):
        return next((s for s in states if s[1] == name), None)

    created, reserving, preparing = first("Created"), first("Reserving"), first("Preparing")
    computing, exit_state = first("Computing"), first("Exit")
    if not (created and preparing and computing and exit_state):
        return None

    created_ts, _, created_body = created
    preparing_ts, _, prep = preparing
    computing_ts, _, comp = computing
    exit_ts = exit_state[0]
    operator_id = created_body["pipeline_uuid"]
    thread = ref(prep["executor_thread"]["resource_id"])
    memory = _memory_usage(memory_fields, prep["reservation"])

    steps: list[tuple[int, dict]] = [
        (
            created_ts,
            {
                "Queueing": {
                    "instance_name": created_body["instance_name"],
                    "operator_id": ref(operator_id),
                }
            },
        ),
        (reserving[0] if reserving else preparing_ts, {"Allocating": {"use_thread": thread}}),
    ]
    if prep["origin_tier"] != prep["target_tier"]:
        steps.append(
            (
                preparing_ts,
                {
                    "Loading": {
                        "use_thread": thread,
                        "use_storage_channel": None,
                        "use_pcie_channel": None,
                        **memory,
                    }
                },
            )
        )
    steps.append(
        (
            computing_ts,
            {
                "Computing": {
                    "instance_name": comp["instance_name"],
                    "input_bytes": comp["input_bytes"],
                    "use_thread": thread,
                    **memory,
                }
            },
        )
    )

    out = [
        make_event(task_id, ts, {name: {"seq": seq, **body}})
        for seq, (ts, state) in enumerate(steps)
        for name, body in state.items()
    ]
    out.append(make_event(task_id, exit_ts, {"Exit": {"seq": len(steps)}}))

    stats = {
        "tasks_processed": 1,
        "input_bytes": comp["input_bytes"],
        "wall_time_ns": exit_ts - computing_ts,
        "peak_memory_bytes": prep["reservation"]["capacity"]["capacity_bytes"],
    }
    return ConvertedTask(out, operator_id, stats)


def convert_plan(decl: dict, worker_id: str, is_leaf: bool) -> dict:
    parent = decl["parent"]
    return {
        "parent": {
            "query_id": ref(parent["query_id"]),
            "plan_id": ref(parent["plan_id"]) if parent["plan_id"] else None,
        },
        "instance_name": decl["instance_name"],
        "edges": [{"source": ref(x["source"]), "target": ref(x["target"])} for x in decl["edges"]],
        "worker_id": ref(worker_id) if is_leaf else None,
    }


# --- whole-context conversion ----------------------------------------------


@dataclass(frozen=True)
class Converted:
    streams: dict[str, list[dict]]
    report: dict


def _declaration_events(ctx: Path, entity: str, build) -> list[dict]:
    """One output event per Sirius Declaration; `build(decl) -> dict`."""
    return [
        make_event(e["id"], e["timestamp"], {"Declaration": build(e["data"]["Declaration"])})
        for e in read_stream(ctx, entity)
        if "Declaration" in e["data"]
    ]


def _lifecycle(ctx: Path, entity: str, build_init, entity_id: str | None = None) -> list[dict]:
    """Init/Exit entities (engine, worker). `entity_id` replaces the Sirius id."""
    out = []
    for e in read_stream(ctx, entity):
        event_id = entity_id or e["id"]
        if "Init" in e["data"]:
            out.append(make_event(event_id, e["timestamp"], {"Init": build_init(e["data"]["Init"])}))
        else:
            out.append(make_event(event_id, e["timestamp"], "Exit"))
    return out


def _first_id(events: list[dict]) -> str:
    return events[0]["id"]


def _tier_gpu_ordinal(name: str) -> int | None:
    match = re.fullmatch(r"GPU-(\d+)", name)
    return int(match.group(1)) if match else None


def convert(
    ctx: Path,
    query_number: str,
    workload: str,
    shared_engine_id: str | None = None,
    emit_engine: bool = True,
) -> Converted:
    """`shared_engine_id` puts this query under an engine shared with other queries;
    only one context per shared engine should keep `emit_engine` on."""
    engines = read_stream(ctx, "engine")
    workers = read_stream(ctx, "worker")
    query_events = read_stream(ctx, "query")
    if not (engines and workers and query_events):
        raise SystemExit(f"{ctx}: no engine/worker/query events (query was plan_rejected?)")

    engine_id = shared_engine_id or _first_id(engines)
    worker_id = _first_id(workers)  # all per-engine resources hang off the first worker
    query_id = _first_id(query_events)
    start_ts = engines[0]["timestamp"]
    streams: dict[str, list[dict]] = {}

    # Gpus, keyed by ordinal so memory tiers can find their device.
    gpu_by_ordinal: dict[int, str] = {}
    for e in read_stream(ctx, "gpu_device"):
        decl = e["data"]["Declaration"]
        gpu_by_ordinal[decl["ordinal"]] = e["id"]
    streams["Gpu"] = _declaration_events(
        ctx, "gpu_device",
        lambda d: {"instance_name": d["instance_name"], "worker_id": ref(worker_id)},
    )

    # Memory tiers become GpuMemory / HostMemory; tasks reserve against tier ids.
    memory_fields: dict[str, str] = {}
    gpu_memory, host_memory = [], []
    for e in read_stream(ctx, "memory_tier"):
        state = e["data"]["state"]
        if not isinstance(state, dict) or "MemoryTierInitializing" not in state:
            continue
        name = state["MemoryTierInitializing"]["instance_name"]
        ordinal = _tier_gpu_ordinal(name)
        if ordinal in gpu_by_ordinal:
            memory_fields[e["id"]] = "use_gpu_memory"
            decl = {"instance_name": f"{name} Memory", "gpu_id": ref(gpu_by_ordinal[ordinal])}
            gpu_memory.append(make_event(e["id"], e["timestamp"], {"Declaration": decl}))
        elif name == "HOST":
            memory_fields[e["id"]] = "use_host_memory"
            decl = {"instance_name": "Host Memory", "worker_id": ref(worker_id)}
            host_memory.append(make_event(e["id"], e["timestamp"], {"Declaration": decl}))
    streams["GpuMemory"], streams["HostMemory"] = gpu_memory, host_memory

    # One synthesized executor owns every executor thread.
    executor_id = str(uuid.uuid5(NAMESPACE, f"task-executor:{worker_id}"))
    streams["TaskExecutor"] = [
        make_event(
            executor_id, workers[0]["timestamp"],
            {"Declaration": {"instance_name": "Task Executor", "worker_id": ref(worker_id)}},
        )
    ]
    thread_events = [
        make_event(
            events[0]["id"], events[0]["timestamp"],
            {"Declaration": {
                "instance_name": fsm_state(events[0])[1]["instance_name"],
                "task_executor_id": ref(executor_id),
            }},
        )
        for events in group_by_id(read_stream(ctx, "executor_thread")).values()
    ]
    streams["TaskExecutorThread"] = thread_events

    streams["Engine"] = _lifecycle(
        ctx, "engine",
        entity_id=engine_id,
        build_init=lambda i: {
            "implementation": i["implementation"],
            "instance_name": i["instance_name"],
            "configuration": {
                "workers": len(group_by_id(workers)),
                "threads_per_worker": len(thread_events) // max(len(group_by_id(workers)), 1),
                "gpus_per_worker": len(gpu_by_ordinal),
                "num_workloads": 1,
                "num_queries": 1,
            },
        },
    )
    if not emit_engine:
        streams["Engine"] = []
    streams["Worker"] = _lifecycle(
        ctx, "worker",
        lambda i: {"parent_engine_id": ref(engine_id), "instance_name": i["instance_name"]},
    )

    # Query FSM: Init, Planning, Executing, Exit -> Done.
    query_out = []
    for seq, e in enumerate(query_events):
        name, body = fsm_state(e)
        if name == "Init":
            body = {
                "instance_name": f"Q{query_number}",
                "engine_id": ref(engine_id),
                "workload": workload,
                "query_index": int(query_number),
            }
        name = "Done" if name == "Exit" else name
        query_out.append(make_event(query_id, e["timestamp"], {name: {"seq": seq, **body}}))
    streams["Query"] = query_out
    query_end_ts = query_events[-1]["timestamp"]

    # Plan, operators, ports. A plan with no child plan is a leaf and must name its worker.
    parent_plan_ids = {
        e["data"]["Declaration"]["parent"]["plan_id"]
        for e in read_stream(ctx, "plan")
        if "Declaration" in e["data"]
    }
    streams["Plan"] = [
        make_event(
            e["id"], e["timestamp"],
            {"Declaration": convert_plan(e["data"]["Declaration"], worker_id, e["id"] not in parent_plan_ids)},
        )
        for e in read_stream(ctx, "plan")
        if "Declaration" in e["data"]
    ]
    streams["Operator"] = _declaration_events(
        ctx, "operator",
        lambda d: {
            "plan_id": ref(d["plan_id"]),
            "parent_operator_ids": [ref(p) for p in d["parent_operator_ids"]],
            "instance_name": d["instance_name"],
            "type_name": operator_type(d["instance_name"]),
            "custom_attributes": [],
        },
    )
    streams["Port"] = _declaration_events(
        ctx, "port",
        lambda d: {"operator_id": ref(d["operator_id"]), "instance_name": d["instance_name"]},
    )

    # Tasks, and the per-operator statistics derived from them.
    task_events: list[dict] = []
    stats_by_operator: dict[str, dict] = {}
    skipped = 0
    for task_id, events in group_by_id(read_stream(ctx, "task")).items():
        converted = convert_task(task_id, events, memory_fields)
        if converted is None:
            skipped += 1
            continue
        task_events.extend(converted.events)
        previous = stats_by_operator.get(converted.operator_id, {})
        stats_by_operator[converted.operator_id] = merge_stats(previous, converted.stats)
    streams["Task"] = task_events
    streams["Operator"] += [
        statistics_event(op_id, query_end_ts, stats) for op_id, stats in stats_by_operator.items()
    ]

    report = {
        "engine_id": engine_id,
        "query_id": query_id,
        "duration_ms": (query_end_ts - query_events[0]["timestamp"]) / 1e6,
        "events": {name: len(events) for name, events in streams.items() if events},
        "tasks_skipped_no_compute": skipped,
        "start_ts": start_ts,
    }
    return Converted({k: v for k, v in streams.items() if v}, report)


# --- output ----------------------------------------------------------------


# The server refuses a context without model.qmi, so a copy ships next to this script.
DEFAULT_MODEL_QMI = Path(__file__).with_name("model.qmi")


def write_context(out_root: Path, ctx_name: str, converted: Converted, model_qmi: Path | None) -> Path:
    ctx_out = out_root / ctx_name
    shutil.rmtree(ctx_out, ignore_errors=True)  # no stale entities from an earlier conversion
    for entity, events in converted.streams.items():
        directory = ctx_out / entity
        directory.mkdir(parents=True, exist_ok=True)
        ordered = sorted(events, key=_order_key)
        name = uuid.uuid5(NAMESPACE, f"{ctx_name}:{entity}")
        with (directory / f"{name}.ndjson").open("w") as handle:
            for event in ordered:
                handle.write(json.dumps(event, separators=(",", ":")) + "\n")
    ctx_out.mkdir(parents=True, exist_ok=True)
    (ctx_out / "model.qmi").write_text((model_qmi or DEFAULT_MODEL_QMI).read_text())
    return ctx_out


# --- run index -------------------------------------------------------------

INDEX_NAME = "runs-index.json"


def default_index_path(out: Path) -> Path:
    """The index sits with the data it describes, so it follows the output dir."""
    return out / INDEX_NAME
RUN_ASSETS_PREFIX = "run_assets_"


def run_id_from_assets(assets: Path | None) -> str | None:
    """`run_assets_<hex>` -> the dashed UUID BenchmarkDB shows for the run."""
    if assets is None:
        return None
    name = assets.name.removeprefix(RUN_ASSETS_PREFIX)
    if re.fullmatch(r"[0-9a-f]{32}", name):
        return str(uuid.UUID(name))
    return name


def shared_engine_id(run_id: str | None) -> str | None:
    """One engine per run, so a run's queries are listed together."""
    return str(uuid.uuid5(NAMESPACE, f"engine:{run_id}")) if run_id else None


def owns_engine(entries: list[dict], run_id: str | None, context: str) -> bool:
    """Exactly one context per run carries the shared engine's events."""
    if run_id is None:
        return True
    owners = [e["context"] for e in entries if e["run_id"] == run_id and e["owns_engine"]]
    return not owners or context in owners


def index_entry(
    run_id: str | None, query: str, context: str, report: dict, owns_engine: bool
) -> dict:
    return {
        "run_id": run_id,
        "query": query,
        "context": context,
        "engine_id": report["engine_id"],
        "query_id": report["query_id"],
        "duration_ms": report["duration_ms"],
        "owns_engine": owns_engine,
        "ui_path": f"/profile/engine/{report['engine_id']}/query/{report['query_id']}",
    }


def upsert_entry(entries: list[dict], entry: dict) -> list[dict]:
    kept = [e for e in entries if e["context"] != entry["context"]]
    return sorted([*kept, entry], key=lambda e: (e["run_id"] or "", int(e["query"])))


def load_index(path: Path) -> list[dict]:
    return json.loads(path.read_text()) if path.exists() else []


def save_index(path: Path, entries: list[dict]) -> None:
    path.write_text(json.dumps(entries, indent=2) + "\n")


def format_index(entries: list[dict]) -> str:
    rows = [("RUN", "QUERY", "CONTEXT", "UI PATH")] + [
        (e["run_id"] or "-", e["query"], e["context"], e["ui_path"]) for e in entries
    ]
    widths = [max(len(row[i]) for row in rows) for i in range(4)]
    return "\n".join("  ".join(c.ljust(w) for c, w in zip(row, widths)).rstrip() for row in rows)


# --- CLI -------------------------------------------------------------------


def resolve_context(assets: Path, query: str) -> Path:
    """Context dirs sort by time and match classification.csv row order."""
    with (next(assets.glob("*/classification.csv"))).open() as handle:
        rows = list(csv.DictReader(handle))
    contexts = sorted(p for p in (assets / "telemetry").iterdir() if p.is_dir())
    if len(rows) != len(contexts):
        raise SystemExit(f"{len(rows)} classified queries but {len(contexts)} contexts")
    for row, ctx in zip(rows, contexts):
        if row["query"] == query:
            if row["status"] not in RESOLVABLE_STATUSES:
                raise SystemExit(f"query {query} is {row['status']}: no telemetry to convert")
            return ctx
    raise SystemExit(f"query {query} not found in classification.csv")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=False)
    source.add_argument("--assets", type=Path, help="extracted run_assets_<id> directory")
    source.add_argument("--context", type=Path, help="a single Sirius context directory")
    parser.add_argument("--query", help="TPC-DS query number (required with --assets)")
    parser.add_argument("--workload", default="tpcds_sf10")
    parser.add_argument("--out", type=Path, default=Path("converted"))
    parser.add_argument("--model-qmi", type=Path, help="model.qmi to use instead of the bundled one")
    parser.add_argument("--index", type=Path, help="run index file (default: <out>/runs-index.json)")
    parser.add_argument("--list", action="store_true", help="print the run index and exit")
    args = parser.parse_args(argv)
    index_path = args.index or default_index_path(args.out)

    if args.list:
        print(format_index(load_index(index_path)))
        return 0
    if not (args.assets or args.context):
        parser.error("one of --assets or --context is required")

    if args.assets:
        if not args.query:
            parser.error("--query is required with --assets")
        ctx = resolve_context(args.assets, args.query)
        query = args.query
    else:
        ctx, query = args.context, args.query or "0"

    run_id = run_id_from_assets(args.assets)
    entries = load_index(index_path)
    owner = owns_engine(entries, run_id, ctx.name)
    converted = convert(ctx, query, args.workload, shared_engine_id(run_id), emit_engine=owner)
    out = write_context(args.out, ctx.name, converted, args.model_qmi)
    entry = index_entry(run_id, query, ctx.name, converted.report, owner)
    save_index(index_path, upsert_entry(entries, entry))
    json.dump({"context": str(ctx), "output": str(out), **converted.report}, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
