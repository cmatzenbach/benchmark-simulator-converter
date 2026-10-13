# benchmark-simulator-converter

Converts the telemetry from a Sirius benchmark run into events of Quent's
simulator model, so you can open a real query in a locally running Quent
server and frontend.

It is a one-off converter, not a live replay. It reads the run's telemetry
files and writes new ndjson files that `quent-simulator-server` can load.

## Why this exists

The Sirius analyzer is pinned to an specific Quent revision and cannot be built
against a current Quent checkout. The simulator model is something the current
server and frontend already understand, so the telemetry is mapped onto it.

Only Sirius is supported today.

## Requirements

- Python 3.10 or newer, standard library only.
- A [Quent](https://github.com/rapidsai/quent) checkout, to run `quent-simulator-server` and the frontend.
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
  `runs-index.json` inside the `--out` directory.
- `--list`: print the run index for the `--out` directory and exit.

Only queries whose status is `gpu_completed` or `runtime_failed` have
telemetry. `plan_rejected` queries have none. Run it once per query.

## Run index

Each conversion adds a row to `runs-index.json` in the output directory: run id, query number,
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

Start the backend from your Quent repo:

```
cargo run -p quent-simulator-server -- --output-dir /path/to/converted
```

Start the frontend from `ui/`:

```
pnpm run dev
```

Then either use the UI to navigate to the run added, or open
`/profile/engine/<engine id>/query/<query id>` on the frontend's address.
Run `python3 sirius_to_simulator.py --list` to print the path for every
converted query. Restart the backend after converting more queries.

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
