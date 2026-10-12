# benchmark-run-simulator-converter

Converts the telemetry from a Sirius benchmark run into events of quent's
Simulator model, so you can open a real query in a locally running quent
server and frontend.

It is a one-off converter, not a live replay. It reads the run's telemetry
files and writes new ndjson files that `quent-simulator-server` can load.

## Why this exists

The Sirius analyzer is pinned to an old quent revision and cannot be built
against a current quent checkout. The Simulator model is something the current
server and frontend already understand, so the telemetry is mapped onto it.

Only Sirius is supported today.

## Requirements

- Python 3.10 or newer, standard library only.
- A quent checkout, to run `quent-simulator-server` and the frontend.
- An extracted run folder (`run_assets_<run id>/`) that contains `telemetry/`
  and a `classification.csv`.

## Usage

Run it from the folder where you keep your run data:

```
python3 /path/to/sirius_to_simulator.py --assets run_assets_<run id> --query <query number> --out converted
```

- `--assets`: the extracted run folder.
- `--query`: the query number. It selects the row in `classification.csv`
  and the telemetry context at the same position, and it is also used as the
  query's name (`Q3`) and index.
- `--out`: where to write the converted data. Defaults to `converted`.
- `--workload`: workload label. Defaults to `tpcds_sf10`.
- `--context <dir>`: convert one telemetry context directly instead of using
  `--assets` and `--query`.
- `--model-qmi <file>`: use a different `model.qmi` than the bundled one.
- `--index <file>`: where the run index is kept. Defaults to
  `runs-index.json` next to the script.
- `--list`: print the run index and exit.

Only queries whose status is `gpu_completed` or `runtime_failed` have
telemetry. `plan_rejected` queries have none. Run it once per query.

## Run index

Each conversion adds a row to `runs-index.json`: run id, query number,
context folder, engine and query ids, and the frontend path. Use `--list` to
print it.

The folders in the output directory are named after the Sirius telemetry
context, not the run id. The server requires each folder name to be a UUID and
ignores any that are not, so the run id cannot be added to the name. The index
is how you tell which run a folder came from.

## One engine per run

All queries converted from the same run share one engine, so they are listed
together under it. The first query converted from a run writes the engine's
events. The rest only refer to it. If you delete that first query's folder by
hand, the others lose their engine. Reconvert that query to fix it.

## Viewing the result

Start the backend from your quent checkout:

```
cargo run -p quent-simulator-server -- --output-dir /path/to/converted --analyzer-address 127.0.0.1:18080 --cors-address http://localhost:5173
```

If the default collector port is already in use, set
`QUENT_COLLECTOR_ADDRESS`, for example `QUENT_COLLECTOR_ADDRESS=127.0.0.1:17836`.

Start the frontend from `ui/`, pointed at that backend:

```
VITE_API_TARGET=http://127.0.0.1:18080 pnpm exec vite --port 5173 --strictPort
```

Then open `/profile/engine/<engine id>/query/<query id>`. `--list` prints the
paths. Restart the backend after converting more queries.

## What is converted

- Kept: engine, worker, GPUs, GPU and host memory, executor threads, the query
  lifecycle, the plan, operators, ports, and tasks.
- Tasks: Sirius task states map onto the Simulator's queueing, allocating,
  loading, computing and exit. Loading appears only when data moves between
  tiers. Consecutive computing steps are collapsed into one. Tasks that never
  computed are skipped.
- Operator statistics are derived from the tasks (tasks processed, input
  bytes, wall time, peak memory). Sirius does not emit them.
- Dropped: query groups, channels, data batches, batch placement, task queues,
  manager threads, NVTX events, and the disk memory tier.
- The plan is one physical plan, with no logical plan.
- Everything hangs off the first worker.

## Tests

```
python3 -m unittest test_sirius_to_simulator
```
