# 모듈: worker

## 책임

- 배치 클레임 및 rsync 실행
- 재시도 로직을 통한 실패 복구
- 결과/헬스비트 보고

## 주요 구성 요소

- `WorkerService`: 워커 쓰레드를 생성하고 수명주기를 관리합니다.
- `_worker_loop`: 배치 클레임 → rsync 실행 → 결과 보고 루프를 수행합니다.
- `_run_rsync`: 단일/다중 파일 케이스에 따라 rsync 명령을 구성합니다.
- `_ensure_destinations`: 대상 디렉터리를 생성해 rsync 실패를 줄입니다.
- `_log_summary`: 워커 종료 시 처리량 요약을 테이블 형태로 출력합니다.
- 로그 파일 출력은 `--log-dir`, `--log-prefix`로 지정합니다.

## 운영 팁

- `--rsync-args`에 `--compress`를 추가하면 WAN 환경에서 대역폭 절약에 도움이 됩니다.
- 대량의 작은 파일은 `batch_num_files`를 높여 배치 오버헤드를 줄입니다.
- `--debug` 모드에서 배치 클레임/실행/결과 처리 로그를 상세하게 확인할 수 있습니다.
