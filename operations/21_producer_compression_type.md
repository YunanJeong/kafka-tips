# `compression.type`

## ProducerConfig (Connect, Streams)

- producer 설정이기 때문에 producer 접두어는 종종 생략됨
- producer.compression.type
- compression.type

## ConsumerConfig

- 압축관련 옵션이 없으며, Topic 내 압축된 내용은 알아서 해제하여 읽음

## TopicConfig, BrokerConfig

- default: `compression.type=producer`
  - 기본값으로 두고 Producer 측 설정에 따르는 것이 일반적임
  - Producer 측 압축 연산부하는 있으나, 네트워크 대역폭과 Broker 저장공간 측면에서 이득
  - 하나의 Topic 내에서도 RecordBatch마다 압축방식이 다를 수 있음
- Topic 또는 Broker 측에서 특정 압축방식 지정 시
  - Broker는 최종 압축방식을 자신의 설정에 맞춤
  - Producer가 미압축전송시 Broker가 1회 압축
  - Producer가 Broker 측과 다른 압축방식 사용 시, 압축해제 후 재압축되어 비효율 초래

## 압축설정 직후, 정말 압축설정된 것 맞나? 체크하기

```sh
# 최근 적재된 Record 및 메타데이터 조회
# --files: 압축방식 조회는 실제 Segement 파일대상으로 조회필요
# <segment>.log: segment 경로 내 파일명숫자가 가장 큰 것이 최신데이터임
kafka-dump-log.sh \
  --files /path/to/kafka-logs/<topic>-<partition>/<segment>.log \
  --print-data-log | tail -100
```

## 권장 type

- `snappy: 원본의 1/2, 데이터엔지니어링에서 가장 무난`
- lz4: 원본의 1/3, snappy 대비 거의 비슷하며 Java 환경에서 근소하게 더 좋음. 호환성 측면에서 Kafka Client 및 유관 시스템이 모두 Java일 때만 사용
- zstd: 원본의 1/5, gzip과 snappy의 절충
- gzip: 원본의 1/10, 처리속도가 확연히 느려짐. 용량 최소화 필요시에만 사용

