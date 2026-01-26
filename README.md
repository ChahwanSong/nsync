# nsync

## Task-scoped port allocation

`nsync` derives a deterministic port set from an integer `task_id` so that master and worker
processes can coordinate without hard-coding per-run ports. Ports are computed by
hashing `task_id` with SHA-256, taking the modulo of `range_size`, and applying the
result as an offset to a base port for each channel. A retry index shifts the range
when a conflict is detected.

### Example

```bash
python -m nsync.master --task-id 202409
python -m nsync.worker --task-id 202409
```

### Port conflict retry

If the master detects `EADDRINUSE` while binding ZeroMQ sockets or the FastAPI
server port, it will recompute the port set with `retry_index=1` and retry up to
three times by default. Adjust the retry count with `--port-retry` if you want more attempts.
Workers follow the same port derivation logic and will try the same retry indices
when connecting.

```bash
python -m nsync.master --task-id 202409 --port-retry 3
python -m nsync.worker --task-id 202409 --port-retry 3
```
