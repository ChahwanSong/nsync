# nsync

nsync는 ZeroMQ 기반의 데이터 플레인과 FastAPI 기반의 운영 API를 결합한 분산 비동기 파일 동기화 도구입니다. 대규모 파일 트리를 스캔해 배치로 분할한 뒤, 여러 워커가 병렬로 rsync 작업을 수행하도록 설계되어 있습니다.

## 시스템 아키텍처

- **Master 서비스**: 파일 스캔, 배치 생성, 워커 배치 할당, 결과 집계, 운영 API 제공을 담당합니다.
- **Worker 서비스**: Master로부터 배치를 클레임하고 rsync로 전송한 뒤 결과를 보고합니다.
- **ZeroMQ 소켓**: 배치 분배(PULL/PUSH), 결과 수집(PULL/PUSH), 워커 클레임(REQ/REP), 헬스 체크(PULL/PUSH) 채널로 사용됩니다.
- **운영 API**: FastAPI로 상태/진행률/워크로드/결과를 제공합니다.

자세한 아키텍처 구성, 포트/채널 맵, 성능 고려사항은 [docs/architecture.md](docs/architecture.md)를 참고하세요.

## Workflow

1. Master가 소스 디렉터리를 스캔하고 파일 메타데이터를 수집합니다.
2. 파일을 버킷으로 나눠 멀티 프로세스로 배치를 생성합니다.
3. 워커가 배치를 클레임하고 rsync로 대상 디렉터리에 동기화합니다.
4. 워커는 결과와 메트릭을 Master에 전송하며, Master는 API를 통해 노출합니다.

상세 흐름(에러 처리, 재시도, 종료 조건 등)은 [docs/workflow.md](docs/workflow.md)를 확인하세요.

## 설치 방법

### 요구 사항

- Python 3.12+
- rsync
- (원격 대상 동기화 시) SSH 접근

### 패키지 설치

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 서비스 실행 예시

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

운영 환경 기준 설치/배포/보안 고려사항은 [docs/installation.md](docs/installation.md)를 참고하세요.

## API 및 설명

FastAPI 운영 API는 다음 엔드포인트를 제공합니다.

- `GET /status`
- `GET /progress`
- `GET /throughput`
- `GET /workers`
- `GET /logs`
- `GET /results`

모든 엔드포인트별 상세 스키마, 예제 요청/응답은 [docs/api.md](docs/api.md)에 정리되어 있습니다.

## 모듈별 상세 문서

- [docs/modules/batcher.md](docs/modules/batcher.md)
- [docs/modules/common.md](docs/modules/common.md)
- [docs/modules/master.md](docs/modules/master.md)
- [docs/modules/worker.md](docs/modules/worker.md)

## 테스트 방법

```bash
pytest
```

추가적인 운영 점검(관측/모니터링/부하 테스트) 안내는 [docs/operations.md](docs/operations.md)를 참고하세요.
