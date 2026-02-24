# 모듈: master

## 책임

- 파일 스캔 및 배치 생성 프로세스 시작
- 배치 큐 관리 및 워커 배치 분배
- 결과/헬스 수집 및 운영 API 제공

## 주요 구성 요소

- `MasterService`: ZeroMQ 소켓을 관리하며 배치/결과/헬스비트를 수신합니다.
- `MasterState`: 진행률, 결과, 로그, 워커 상태와 처리량(파일/디렉터리/바이트)을 집계합니다.
- `create_app`: FastAPI 엔드포인트를 생성합니다.
- `_producer_main`: 스캔된 파일 목록을 받아 배치를 생성합니다.

## 동작 요약

- Master는 스캔 → 버킷 분할 → 프로듀서 실행 순서로 배치를 만듭니다.
- 워커가 클레임하면 큐에서 배치를 꺼내 전달합니다.
- 모든 프로듀서가 종료되고 큐가 비면 `done` 응답을 반환합니다.
- `--debug` 모드에서는 배치 수신/클레임/결과 처리 로그를 상세하게 출력합니다.
- 배치 큐는 `queue_threshold` 이상일 경우 잠시 대기하며, 과도한 큐 적재를 방지합니다.
- `task_id`는 마스터가 큐에 넣는 시점에 순차적으로 부여합니다.
- 로그 파일 출력은 `--log-dir`, `--log-prefix`로 지정합니다.
- 결과 보관 상한은 `nsync/constants.py`의 `MAX_RESULT_HISTORY`로 관리합니다.
- `--exit-when-done` 모드에서는 완료 시 `master_summary` 테이블을 출력합니다.
- 하트비트 타임아웃(`heartbeat_timeout`)이 발생하면 해당 워커가 클레임한 배치를 재큐잉합니다.
- 하트비트 타임아웃 감지 시 워커별 `worker_heartbeat_timeout` WARNING 로그를 출력합니다.
- Producer의 `producer_done` 메시지가 누락되어도 프로세스 종료를 감시해 완료 수를 보정합니다.
- Producer가 비정상 종료하면 `producer_exit` WARNING 로그를 출력합니다.
- 재큐잉은 `requeue_limit` 횟수까지만 수행되며, 초과 시 해당 배치를 실패 처리합니다.
- 기본적으로 `progress` INFO 로그를 주기적으로 출력하며, `--no-progress`로 비활성화할 수 있습니다.
- `--quiet-fastapi` 옵션으로 FastAPI(uvicorn) 요청/INFO 로그 출력을 비활성화할 수 있습니다.
