# date-topic-cleaner

오래된 날짜토픽(`_YYYY_MM_DD`, `_YYYY_MM` 으로 끝나는 토픽)을 삭제하는 Kubernetes CronJob.

## 동작

```
토픽 조회 -> 삭제대상 선별 -> 유관 s3 sink 커넥터 stop -> 토픽 삭제 -> 커넥터 resume
```

커넥터를 stop 하는 이유는 삭제되는 토픽을 참조하던 s3 sink task가 timeout으로 fail 하는 것을 막기 위함이다. `pause`는 task가 살아있어 fetch를 계속 시도하므로 `stop`(Connect 3.5+, task를 완전히 해제)을 쓴다.

**커넥터 복구가 토픽 삭제보다 우선이다.**

- 삭제대상을 `topics`/`topics.regex` 로 참조하는 `S3SinkConnector` 만 제어한다.
- RUNNING 이 아닌 커넥터는 건드리지 않는다.
- Connect 조회 실패, stop 실패, stop 미반영 중 하나라도 있으면 **토픽을 삭제하지 않는다.**
- 삭제 실패·SIGTERM(노드 드레인, job 삭제) 등 어떤 경로로 빠져나가도 resume 을 시도하며,
  실패 시 3회 재시도 후 `CRITICAL` 로그를 남기고 종료코드 1로 보고한다.
  (토픽이 다 지워졌어도 resume 실패면 실패로 처리한다)

SIGTERM 후 resume 할 시간이 필요하므로 `terminationGracePeriodSeconds: 300` 을 둔다.

## 삭제대상 판정

기준일이 `오늘 - retention-days` 보다 이전인 토픽. `mylog_2026_07_15` 는 기준일 2026-07-15,
`mylog_2026_07` 은 **말일인 2026-07-31** 이다. (1일 기준이면 진행중인 달이 조기에 삭제된다)

`_` 로 시작하는 내부 토픽과 날짜 형식이 아닌 토픽은 제외된다.

## 배포

```bash
docker build -t date-topic-cleaner:0.1.0 .
kubectl apply -f cronjob.yaml   # image, args, schedule 은 환경에 맞게 수정
```

인자: `--retention-days`(필수), `--broker`, `--connect-url`
종료코드: `0` 정상, `1` 실패, `2` 인자 오류

```bash
uv run pytest
```
