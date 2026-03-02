# 운영 가이드

## 모니터링 포인트

- **진행률**: `/progress`
- **처리량**: `/throughput`
- **워커 상태**: `/workers`
- **에러/경고**: `/logs`
- **최근 결과**: `/results` (보관 상한 적용)

## 디버그 모드

- Master/Worker 모두 `--debug` 플래그를 제공하며, 배치 수신/클레임/결과 처리 등 주요 이벤트를 상세 로그로 확인할 수 있습니다.
- 워커는 종료 시점에 처리량 요약 로그(`worker_summary`)를 출력합니다.
- 기본 로그는 사람이 읽기 쉬운 포맷으로 stdout에 출력됩니다.
- 기본적으로 Master/Worker 모두 주기적으로 `progress` 로그를 INFO 레벨로 출력합니다.
- Master는 `--no-progress` 옵션으로 주기적 `progress` 로그 출력을 비활성화할 수 있습니다.
- Master는 `--quiet-fastapi` 옵션으로 FastAPI(uvicorn) 요청 로그 출력을 비활성화할 수 있습니다.
- Master는 `--exit-when-done` 모드에서 완료 시 `master_summary` 테이블을 출력하고 종료합니다.

## 로그/결과 파일

- Master는 `--output`에 지정한 로그 파일로 append 기록합니다.
- Master 결과는 `--output-result`를 지정했을 때만 JSONL 파일로 append 기록합니다.
- Worker 로그는 `--output` 접두어를 사용해 `-worker.log` 파일로 기록합니다.

## 성능 튜닝 체크리스트

- 네트워크 대역폭과 디스크 I/O가 병목인지 확인합니다.
- `batch_num_files`와 `batch_size`를 통해 배치 크기를 조정합니다.
- 워커 프로세스 수를 늘려 병렬성을 확보합니다.
- Master의 rsync 옵션(`--options`)으로 압축/체크섬/암호화 비용을 조절합니다. (모든 워커에 전달)

## 장애 대응

- 워커가 장시간 하트비트를 보내지 않으면 해당 노드 상태를 점검합니다.
- Master는 하트비트 타임아웃 감지 시 `worker_heartbeat_timeout` WARNING 로그를 출력합니다.
- Producer 종료 메시지 누락 시에도 Master가 프로세스 종료를 감지해 진행 상태를 정리합니다.
- Producer 비정상 종료 시 Master는 `producer_exit` WARNING 로그를 출력합니다.
- `/logs`에서 반복 실패 배치를 확인하고 원인(권한/네트워크/디스크)을 조사합니다.
- 필요 시 워커를 재기동하여 배치 재시도를 유도합니다.
- 하트비트 타임아웃 시 해당 워커가 클레임한 배치는 자동으로 재큐잉됩니다.
