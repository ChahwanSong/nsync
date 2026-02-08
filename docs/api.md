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
  "producers_done": 4,
  "producers_total": 4
}
```

### 필드 설명

- `total_batches`: 마스터가 생성해 큐에 등록한 전체 배치 수입니다.
- `completed_batches`: 성공적으로 완료된 배치 수입니다.
- `failed_batches`: 실패로 종료된 배치 수입니다.
- `queue_depth`: 현재 배치 큐에 대기 중인 배치 수입니다.
- `producers_done`: 배치 생성 프로듀서 프로세스 중 완료된 프로세스 수입니다.
- `producers_total`: 배치 생성 프로듀서 프로세스 총 개수입니다.

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

처리량과 경과 시간을 반환합니다. 처리량은 성공적으로 완료된 배치를 기준으로 집계됩니다.

### 예제 요청

```bash
curl -s http://localhost:8000/throughput | jq
```

### 예제 응답

```json
{
  "batches_per_sec": 1.23,
  "elapsed_sec": 77.2,
  "batches_completed": 95,
  "batches_failed": 2,
  "files_processed": 12034,
  "bytes_processed": 9876543210,
  "gbytes_processed": 9.2,
  "bytes_per_sec": 12789012.3,
  "gbytes_per_sec": 0.01,
  "files_per_sec": 155.8
}
```

### 필드 설명

- `batches_per_sec`: 성공적으로 완료된 배치 기준 처리 속도입니다.
- `elapsed_sec`: 마스터 시작 이후 경과 시간(초)입니다.
- `batches_completed`: 성공적으로 완료된 배치 수입니다.
- `batches_failed`: 실패한 배치 수입니다.
- `files_processed`: 완료된 배치 기준 파일 수입니다.
- `bytes_processed`: 스캔 시 수집한 파일 크기 합계를 기준으로 계산한 바이트 처리량입니다.
- `gbytes_processed`: `bytes_processed`를 GiB(1024^3)로 환산한 값입니다.
- `bytes_per_sec`: 바이트 처리 속도입니다.
- `gbytes_per_sec`: GiB 처리 속도입니다.
- `files_per_sec`: 파일 처리 속도입니다.

## GET /workers

워커 헬스비트 맵을 반환합니다. 값은 `Asia/Seoul` 시간 문자열입니다.

### 예제 요청

```bash
curl -s http://localhost:8000/workers | jq
```

### 예제 응답

```json
{
  "heartbeats": {
    "worker-a-uuid": "2026-02-02 06:41:51",
    "worker-b-uuid": "2026-02-02 06:41:53"
  },
  "workers": {
    "worker-a-uuid": {
      "batches_in_flight": 1,
      "files_in_flight": 120,
      "directories_in_flight": 5,
      "bytes_in_flight": 123456789,
      "gbytes_in_flight": 0.115,
      "batches_completed": 42,
      "batches_failed": 1,
      "files_processed": 8400,
      "directories_processed": 410,
      "bytes_processed": 9876543210,
      "gbytes_processed": 9.200
    }
  }
}
```

### 필드 설명

- `heartbeats`: 워커별 최근 하트비트 수신 시간을 `Asia/Seoul` 기준 문자열로 반환합니다.
- `workers`: 워커별 처리 현황을 반환합니다.
- `batches_in_flight`: 현재 워커가 클레임하고 처리 중인 배치 수입니다.
- `files_in_flight`: 처리 중인 배치의 파일 수 합계입니다.
- `directories_in_flight`: 처리 중인 배치의 디렉터리 수 합계입니다.
- `bytes_in_flight`: 처리 중인 배치의 바이트 합계입니다.
- `gbytes_in_flight`: `bytes_in_flight`를 GiB(1024^3)로 환산한 값입니다.
- `batches_completed`: 성공적으로 완료된 배치 수입니다.
- `batches_failed`: 실패한 배치 수입니다.
- `files_processed`: 성공적으로 완료된 배치 기준 파일 수입니다.
- `directories_processed`: 성공적으로 완료된 배치 기준 디렉터리 수입니다.
- `bytes_processed`: 성공적으로 완료된 배치 기준 바이트 합계입니다.
- `gbytes_processed`: `bytes_processed`를 GiB(1024^3)로 환산한 값입니다.

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

최근 배치 결과 리스트를 반환합니다. 결과는 메모리 보호를 위해 최대 보관 수를 넘으면 오래된 항목부터 제거됩니다.

`task_id`는 마스터가 배치를 큐에 넣는 시점에 1부터 순차 증가로 부여합니다.
`*_iso` 시간 필드는 `Asia/Seoul` 기준입니다.

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
        "start_ts": 1714372000.0,
        "start_ts_iso": "2026-02-02 06:46:40",
        "end_ts": 1714372002.1,
        "end_ts_iso": "2026-02-02 06:46:42",
        "file_count": 500,
        "directory_count": 42,
        "estimated_bytes": 104857600
      },
      "errors": [],
      "received_ts": 1714372002.2,
      "received_ts_iso": "2026-02-02 06:46:42"
    }
  ],
  "retained_limit": 100000,
  "retained_count": 1
}
```
