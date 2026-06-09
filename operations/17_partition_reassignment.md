# Partition Reassignment(파티션 재할당)

- 특정 토픽의 파티션, 복제본들을 브로커 간 재분산하는 작업
- CLI기반으로 작업이 필요

## 필요한 상황

- 스케일아웃: 신규 broker 추가시 파티션 분배
  - 신규 topic 생성시 leader partition과 follower partition(replica)들은 각 broker에 균등하게 분배되지만
  - 클러스터에 신규 broker node 추가시, 기존 topic의 partition이 자동으로 신규 broker에 분배되지 않음
  - 이 때 수동 재할당 작업 필요
- 스케일인
  - broker 제거시 replica 수동 재분배 필요
  - 수동 재분배 하지 않고 broker만 제거시, 클러스터에서 1대가 죽은 것으로 취급됨
  - 계속 유지할 broker쪽으로 replica를 모두 옮겨준 후 broker 제거 필요
  - 이후 leader partition은 유지되는 broker쪽으로 자동 선출됨
- 토픽의 Replication Factor 변경
  - RF 변경시, 명령어와 설정을 통해서 replica의 broker 별 배치를 일일이 지정해줘야 함
  - Kafbat UI 등을 쓰면 이 과정이 자동화/간소화 되어있음
- `__consumer_offsets` 토픽의 Replication Factor 변경
  - __consumer_offsets의 replication factor는 topic 최초 생성시 broker 설정값(offsets.topic.replication.factor)에 따르고, 생성 이후엔 변경 불가
  - 이미 운영 중인 서비스에서 RF 변경 필요시, __consumer_offset을 지우고 신규생성할 수 없으므로 수동 변경이 필요
  - `파티션(레플리카) 재할당을 통해서 특정 개수만큼 복제하면 자동으로 RF도 해당 개수로 표기됨`
    - 한 토픽 내 모든 파티션은 같은 개수만큼 replica를 가져야 함. 어떤 파티션은 2개, 어떤 파티션은 3개, 이런식으로는 설정자체가 불가능.
  - `__consumer_offsets`과 같은 internal topic의 replication factor 변경은 위험하므로 각종 Kafka UI에서 변경하는 것이 차단되어 있어서, CLI작업 필요

## 재할당 방법

- kafka bin파일중, `kafka-reassgin-partitions.sh`를 사용
- 이 스크립트는 JSON 형식의 재할당 계획 파일(reassignment.json)을 입력받아 실행한다.
- 재할당 계획 JSON에는 다음이 포함된다:
  - 대상 토픽
  - 각 partition의 replica를 어느 브로커에 둘지
  - (선택적으로) 리더를 어느 브로커로 둘지 등
- 재할당 계획 JSON은 아래와 같이 `kafka-reassign-partitions.sh --generate` 옵션으로 자동 생성할 수 있다. 또는 자동생성된 포맷을 참고하여 직접 json을 작성해도 된다.

## `__consumer_offsets`의 replication factor (1=>2) 변경, 재할당 방법

- broker의 주소는 localhost:9092로 표기
- 모든 partition의 replica를 만들면 replication factor도 그에 맞게 수정된 것으로 표기되며 향후 동작도 그렇게 함

### 1. 현 상태 확인

```sh
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic __consumer_offsets
```

### 2. 대상 토픽 정의(topics.json)

```json
{
  "topics": [
    {"topic": "__consumer_offsets"}
  ],
  "version": 1
}
```

```sh
# 대상토픽 정의파일 생성  # 쓰기권한 없을시 /tmp 경로 시도
cat > topics.json <<EOF
{
  "topics": [
    {"topic": "__consumer_offsets"}
  ],
  "version": 1
}
EOF
```

### 3. 재할당 계획 정의 (reassignment.json)

```sh
# 재할당 계획 생성
kafka-reassign-partitions.sh \
  --bootstrap-server localhost:9092 \
  --topics-to-move-json-file topics.json \
  --broker-list "0,1" \
  --generate > reassignment_cur_prop.json

# broker-list에는 broker id를 입력
# --generate로 만들 수 있는 건 제한적임
# => 기본 포맷 참고용으로 쓰고, 실제 원하는 형태는 reassignment.json을 편집해야 함
# --generate로 생성된 건 current, proposed 두 json이 들어있는데 이중 proposed 부분을 별도 추출해서 사용해야 함 
# e.g.) 
# - 만약 브로커 3대인데 RF를 2로 두고 균등 분배하고 싶으면 reassigntment.json을 수동작성해야 함
# - 만약 대상토픽의 현재 RF가 1이면, --broker-list 에 여러 브로커를 지정해도 제대로 된 reassignment.json이 생성되지 않으므로 포맷만 참고한다.
```

- broker 3대(0,1,2) 중 2대에 __consumer_offsets 토픽의 파티션을 고르게 분배하는 reassignment.json 설정 예시
  - **"기존 replica 위치한 곳은 그대로 두고"** 다른 곳에 replica를 추가하는게 안정적임
  - log_dirs는 최근 버전에 있는건데, 보통은 any이고 replica 수 만큼 기술하면 됨
- 중요: `replicas` 나열 순서
  - 파티션 재할당 계획 JSON에서 replicas 배열의 첫 번째 broker id는 해당 파티션의 preferred leader 로 간주됨
  - replicas 순서가 특정 브로커에 편중되면, preferred leader가 한 브로커에 몰릴 수 있고 결과적으로 리더 부하가 불균형 가능성이 있음

```json
{
  "version": 1,
  "partitions": [
    {"topic":"__consumer_offsets","partition":0,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":1,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":2,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":3,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":4,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":5,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":6,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":7,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":8,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":9,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":10,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":11,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":12,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":13,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":14,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":15,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":16,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":17,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":18,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":19,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":20,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":21,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":22,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":23,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":24,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":25,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":26,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":27,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":28,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":29,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":30,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":31,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":32,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":33,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":34,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":35,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":36,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":37,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":38,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":39,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":40,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":41,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":42,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":43,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":44,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":45,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":46,"replicas":[1,2],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":47,"replicas":[2,0],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":48,"replicas":[0,1],"log_dirs":["any","any"]},
    {"topic":"__consumer_offsets","partition":49,"replicas":[1,2],"log_dirs":["any","any"]}
  ]
}
```

### 4. 재할당 실행

```sh
# 재할당 실행 (완료까지 꽤 시간 소요 필요)
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json \
  --execute

# 재할당 실행(쓰로틀링 50MB/s 걸기, production 권장, 완료 후 쓰로틀링 해제 필수)
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file jdbc-only-migration.json \
  --throttle 52428800 \
  --execute
```

### 5. 진행상태 모니터링

- __consumer_offsets 수준은 금방 처리되므로 꼭 필요하지 않음
- 파티션 재분배의 경우 실제 대용량 데이터를 broker간 옮겨야 하므로 꽤 오래걸리기 때문에 진행상황 모니터링이 필요할 수 있음

```sh
# 진행상황 모니터링
# 재할당이 완전히 끝난 후 이 명령어를 한번 더 쓰면 쓰로틀링이 해제된다.
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file consumer-offsets-replication.json \
  --verify
```

### 6. 리더 재분배(필요시 사용)

- 재할당 이후 실제 리더를 preferred leader 기준으로 다시 맞추려면 아래 명령어로 preferred leader election을 수행한다.

```
# 리더 재분배
kafka-leader-election.sh --bootstrap-server localhost:9092 \
  --election-type preferred --all-topic-partitions
```

### 여러 토픽에 대해 재할당 계획 Json 작성

```sh
# 예시
# 전체 토픽 목록 중 "jdbc"키워드가 포함된 것들을 RF=3으로 변경 계획
# [0,1,2], [1,2,0], [2,0,1] 순 번갈아 기술
kafka-topics.sh --bootstrap-server localhost:9092 --describe | \
awk '$1 == "Topic:" && $4 ~ /^[0-9]+$/ {print $2, $4}' | \
grep "jdbc" | \
jq -R -s -c '
  split("\n") | del(.[-1]) | map(split(" ")) | . as $orig |
  {
    "version": 1,
    "partitions": [
      range(0; $orig | length) |
      if . % 3 == 0 then
        {"topic": $orig[.][0], "partition": $orig[.][1] | tonumber, "replicas": [0, 1, 2], "log_dirs": ["any", "any", "any"]}
      elif . % 3 == 1 then
        {"topic": $orig[.][0], "partition": $orig[.][1] | tonumber, "replicas": [1, 2, 0], "log_dirs": ["any", "any", "any"]}
      else
        {"topic": $orig[.][0], "partition": $orig[.][1] | tonumber, "replicas": [2, 0, 1], "log_dirs": ["any", "any", "any"]}
      end
    ]
  }
' > jdbc-only-migration-v2.json
```