# 모듈: batcher

## 책임

- 소스 디렉터리 스캔 및 파일 메타데이터 수집
- 파일 목록을 버킷으로 분할
- 배치 생성 및 경로 압축

## 핵심 로직

- `scan_paths(root, depth)`는 상대 경로와 파일 크기를 수집합니다.
- `bucketize(files, num_buckets)`는 순차 라운드 로빈 방식으로 분할합니다.
- `iter_batches(...)`는 `max_files`, `max_bytes` 기준으로 배치를 스트리밍 생성하고, 파일 수/디렉터리 수/바이트 추정치를 함께 기록합니다.
- `compress_paths(..., max_depth)`는 경로 압축 최대 깊이를 제한할 수 있습니다.
- `build_batches(...)`는 `iter_batches(...)`를 리스트로 수집하는 헬퍼입니다.
- `compress_paths(...)`는 포함 관계를 기반으로 상위 디렉터리로 경로를 축약합니다.

## 확장 아이디어

- 대형 디렉터리의 경우 스캔을 병렬화하거나 inode 필터링을 추가할 수 있습니다.
- 배치 전략을 크기 기반 외에 디렉터리 기반으로 확장해 locality를 개선할 수 있습니다.
