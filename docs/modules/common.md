# 모듈: common

## 책임

- 배치/결과 데이터 모델 정의
- JSON 직렬화/역직렬화 유틸 제공
- 로깅 구성 및 ID 생성

## 주요 구성 요소

- `Batch`: 배치 메타데이터를 포함하는 데이터 클래스입니다.
- `BatchResult`: 워커 결과를 보관합니다.
- `json_dumps/json_loads`: ZeroMQ 메시지 직렬화/역직렬화에 사용합니다.
- `configure_logger`: JSON 포맷 로거를 구성합니다.
- `new_task_id/new_worker_id`: 고유 식별자 생성에 사용합니다.
