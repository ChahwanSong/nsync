# API 상세

Base URL: `http://<master-host>:8000`

## GET /status

Master 상태와 큐 상황을 반환합니다.

### 예제 요청

```bash
curl -s http://localhost:8000/status | jq
```

### 예제 응답

```json
{
  "total_batches": 120,
  "completed_batches": 95,
  "failed_batches": 2,
  "queue_depth": 10,
  "producers_done": 4
}
```

### 필드 설명

- `total_batches`: 마스터가 생성해 큐에 등록한 전체 배치 수입니다.
- `completed_batches`: 성공적으로 완료된 배치 수입니다.
- `failed_batches`: 실패로 종료된 배치 수입니다.
- `queue_depth`: 현재 배치 큐에 대기 중인 배치 수입니다.
- `producers_done`: 배치 생성 프로듀서 프로세스 중 완료된 프로세스 수입니다.

## GET /progress

전체 진행률(완료/실패/대기)을 반환합니다.

### 예제 요청

```bash
curl -s http://localhost:8000/progress | jq
```

### 예제 응답

```json
{
  "total": 120,
  "completed": 95,
  "failed": 2,
  "pending": 23
}
```

## GET /throughput

처리량과 경과 시간을 반환합니다.

### 예제 요청

```bash
curl -s http://localhost:8000/throughput | jq
```

### 예제 응답

```json
{
  "batches_per_sec": 1.23,
  "elapsed_sec": 77.2
}
```

## GET /workers

워커 헬스비트 맵을 반환합니다.

### 예제 요청

```bash
curl -s http://localhost:8000/workers | jq
```

### 예제 응답

```json
{
  "heartbeats": {
    "worker-a-uuid": 1714372359.123,
    "worker-b-uuid": 1714372361.456
  }
}
```

## GET /logs

경고/에러 로그 집계를 반환합니다.

### 예제 요청

```bash
curl -s http://localhost:8000/logs | jq
```

### 예제 응답

```json
{
  "warnings": [
    "task 123 retry 1 exit=23"
  ],
  "errors": [
    "task 456 retry 2 exit=24"
  ]
}
```

## GET /results

모든 배치 결과 리스트를 반환합니다.

### 예제 요청

```bash
curl -s http://localhost:8000/results | jq
```

### 예제 응답

```json
{
  "results": [
    {
      "worker_id": "host-uuid",
      "task_id": 123456789,
      "status": "success",
      "retry_count": 0,
      "rsync_exit_code": 0,
      "stats": {
        "bytes_sent": 0,
        "bytes_received": 0,
        "start_ts": 1714372000.0,
        "end_ts": 1714372002.1
      },
      "errors": [],
      "received_ts": 1714372002.2
    }
  ]
}
```
