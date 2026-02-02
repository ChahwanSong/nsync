# 로컬 환경 실행
## 설치
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e . 
```

## Master
```bash

python3 -m nsync.master \
  --src /home/mason/workspace/test/src \
  --dst /home/mason/workspace/test/dst \
  --batch-num-files 1 \
  --batch-size 104857600 \
  --num-master-processes 3 \
  --master-scan-depth 3 \
  --log-dir /home/mason/workspace/nsync \
  --queue-threshold 10 \
  --log-prefix abc \
  --exit-when-done
```

## Worker
```bash
python3 -m nsync.worker \
  --num-worker-processes 3 \
  --dst-host localhost \
  --master-host localhost \
  --debug
```



# 쿠버네티스 환경 실행

## 설치
```bash
pip install -e . --break-system-packages
```

## Master
- ubuntu24-0.ubuntu24-svc

```bash
python3 -m nsync.master \
  --src /workspace/test/src \
  --dst /workspace/test/dst \
  --batch-num-files 1 \
  --batch-size 104857600 \
  --num-master-processes 4 \
  --master-scan-depth 5
```

## Src Worker
- ubuntu24-1.ubuntu24-svc

Dst worker: ubuntu24-2.ubuntu24-svc

```bash
python3 -m nsync.worker \
  --num-worker-processes 4 \
  --dst-host ubuntu24-2.ubuntu24-svc \
  --master-host ubuntu24-0.ubuntu24-svc
```