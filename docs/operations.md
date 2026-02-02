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
- Master/Worker 모두 주기적으로 `progress` 로그를 INFO 레벨로 출력합니다.
- Master는 `--exit-when-done` 모드에서 완료 시 `master_summary` 테이블을 출력하고 종료합니다.

## 로그 파일

- `--log-dir`와 `--log-prefix`를 지정하면 Master/Worker 로그를 지정 디렉터리에 파일로 남깁니다.
- 예: `--log-dir /var/log/nsync --log-prefix node-a` → `node-a-master.log`, `node-a-worker.log`

## 성능 튜닝 체크리스트

- 네트워크 대역폭과 디스크 I/O가 병목인지 확인합니다.
- `batch_num_files`와 `batch_size`를 통해 배치 크기를 조정합니다.
- 워커 프로세스 수를 늘려 병렬성을 확보합니다.
- rsync 인자(`--rsync-args`)로 압축/체크섬/암호화 비용을 조절합니다.

## 장애 대응

- 워커가 장시간 하트비트를 보내지 않으면 해당 노드 상태를 점검합니다.
- `/logs`에서 반복 실패 배치를 확인하고 원인(권한/네트워크/디스크)을 조사합니다.
- 필요 시 워커를 재기동하여 배치 재시도를 유도합니다.
- 하트비트 타임아웃 시 해당 워커가 클레임한 배치는 자동으로 재큐잉됩니다.
