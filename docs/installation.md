# 설치 및 배포

## 요구 사항

- Python 3.12+
- rsync
- (원격 대상 동기화 시) SSH 접근

## 로컬 설치

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## 운영 배포 가이드

### 네트워크/포트

- Master는 다음 포트를 열어야 합니다.
  - Claim: 5555
  - Batch: 5556
  - Result: 5557
  - Heartbeat: 5558
  - API: 8000
- 워커 노드는 Master의 위 포트에 접근 가능해야 합니다.

### 보안 권장사항

- 원격 대상 동기화 시 SSH 키 기반 인증을 사용합니다.
- API는 내부 네트워크에만 노출하거나 API 게이트웨이를 통해 보호합니다.
- rsync 대상 디렉터리에 필요한 최소 권한만 부여합니다.

### 운영 파라미터 권장값

- `batch_num_files`: 파일 수가 매우 많은 경우 500~2000 범위를 권장합니다.
- `batch_size`: 네트워크 및 디스크 성능에 맞추어 64~512MB 범위에서 튜닝합니다.
- `num_master_processes`: CPU 코어 수에 맞춰 설정합니다.
- `num_worker_processes`: 워커 노드의 CPU 코어 수와 rsync I/O 병렬성을 고려해 설정합니다.
- `debug`: 운영 중 이슈 분석이 필요할 때만 활성화합니다.
- `queue_threshold`: 마스터 배치 큐의 상한입니다. 기본값은 1000입니다.
- `log_dir/log_prefix`: 로그 파일을 저장할 경로와 파일 접두어를 지정합니다.
- `heartbeat_timeout`: 워커 하트비트가 이 시간(초) 이상 끊기면 해당 워커가 클레임한 배치를 재큐잉합니다. 기본값은 15초입니다.

기본값은 `nsync/constants.py`에서 관리하며 `--help`에서도 확인할 수 있습니다.

### 가용성 및 확장 전략

- 워커 노드를 수평 확장하여 처리량을 증가시킬 수 있습니다.
- 단일 Master가 병목이 될 경우 스캔/배치 생성 구간을 분할하거나, 소스 경로를 샤딩하는 방식으로 확장합니다.
- 결과 저장이나 장기 보관이 필요하면 Master 결과를 외부 저장소로 내보내는 패턴을 권장합니다.
