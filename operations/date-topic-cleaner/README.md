# date-topic-cleaner

지정한 달의 날짜토픽(`_YYYY_MM`, `_YYYY_MM_DD`)을 삭제하는 Kubernetes CronJob.

## 동작

```
토픽 조회 -> 대상 월 선별 -> 유관 s3 sink 커넥터 stop -> 토픽 삭제 -> 커넥터 resume
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

판정 단위는 **달**이다. `--target-months-ago 2` 를 2026-09-25 에 실행하면 대상은
**2026-07 하나**이고, `_2026_07` 과 `_2026_07_01` ~ `_2026_07_31` 이 한 번에 삭제된다.
6월 이하는 건드리지 않는다.

월 단위로 묶으므로 한 달이 며칠씩 쪼개져 사라지거나, monthly 토픽이 자기 daily들보다
오래 남는 일이 없다.

`_` 로 시작하는 내부 토픽과 날짜 형식이 아닌 토픽은 제외된다.

> 대상 월만 지우므로 그보다 오래된 잔여 토픽은 이 앱이 건드리지 않는다.
> 최초 도입 시 과거 누적분은 별도로 정리해야 한다.

## 배포

### Helm (권장)

```bash
docker build -t <REGISTRY>/date-topic-cleaner:0.1.0 .

helm lint chart/
helm package chart/          # date-topic-cleaner-0.1.0.tgz

helm upgrade --install date-topic-cleaner date-topic-cleaner-0.1.0.tgz \
  --set image.repository=<REGISTRY>/date-topic-cleaner \
  --set broker=<BROKER>:9092 \
  --set connectUrl=http://<CONNECT>:8083
```

| values | 설명 | 기본값 |
|--------|------|--------|
| `targetMonthsAgo` | 몇 달 전 토픽을 삭제할지 (1 이상) | `2` |
| `schedule` | cron 표현식 | `17 4 25 * *` (매월 25일) |
| `timeZone` | 스케줄 타임존 | `Asia/Seoul` |
| `broker` | bootstrap servers | `kafka:9092` |
| `connectUrl` | Kafka Connect REST URL | `http://kafka-connect:8083` |
| `image.tag` | 비우면 `Chart.appVersion` | `""` |

### kubectl

헬름 없이 띄울 때는 `cronjob.yaml` 샘플에 `<REGISTRY>`, `<BROKER>`, `<CONNECT>` 를 채워
`kubectl apply -f cronjob.yaml`. 차트에서 뽑아 쓸 수도 있다:

```bash
helm template date-topic-cleaner chart/ \
  --set image.repository=<REGISTRY>/date-topic-cleaner | kubectl apply -f -
```

종료코드: `0` 정상, `1` 실패, `2` 인자 오류

## 개발

```bash
uv run pytest
uv run python -m date_topic_cleaner --target-months-ago 2 --broker localhost:9092
```
