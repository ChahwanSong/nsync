# nsync

nsync is a distributed, asynchronous file synchronization tool using ZeroMQ for data-plane messaging and FastAPI for operational endpoints.

## Quick start

```bash
python -m nsync.master \
  --src /data/src \
  --dst /data/dst \
  --batch-num-files 1000 \
  --batch-size 104857600 \
  --num-master-processes 4 \
  --master-scan-depth 5
```

```bash
python -m nsync.worker \
  --num-worker-processes 4 \
  --dst-host localhost \
  --master-host 127.0.0.1
```

### FastAPI endpoints

- `GET /status`
- `GET /progress`
- `GET /throughput`
- `GET /workers`
- `GET /logs`
- `GET /results`

## Testing

```bash
pytest
```
