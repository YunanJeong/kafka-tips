# Log Ingestion Requirement

- Kafka로 Rawdata 로그 파이프라인(JDBC Conenctior, CDC, filebeat 등)을 구축한다고 가정시
- `서비스 기획/초기개발 당시, 로그 스키마 요구사항`
- BigQuery, Snowflake 같은게 존재하는 환경이면 사실 아래처럼 고려할 정도도 아닐듯...

## RDBMS (MySQL)

### 공통컬럼

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `log_id` (로그ID) | `BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY` | Kafka 수집 및 DB 정렬용 단조증가 번호표. 또는 애플리케이션에서 시간 기반 생성하여 주입 |
| `log_uid` (로그UID) | `BINARY(16)` NOT NULL | 로그 식별용 전역 유일 ID. UUID v7, 애플리케이션 생성 |
| `event_time` (발생시간) | `DATETIME(6)` NOT NULL | 마이크로초 단위 로그 발생 시간. 애플리케이션 주입 |
| `created_at` (적재시간) | `DATETIME(6)` NOT NULL DEFAULT `CURRENT_TIMESTAMP(6)` | DB 적재 시간 |
| `source_server_id` (원천서버ID) | `SMALLINT UNSIGNED` NULL | 서버 통합 시 원천 서버 식별자 |
| `source_id` (원천ID) | `BIGINT UNSIGNED` NULL | 서버 통합 시 기존 DB에서 쓰던 원래 번호표 보존용 |
| `json_body` (로그 내용) | `JSON` NOT NULL | 변할 수 있는 로그 내용 |

- 로그 종류, 서버 번호 등 조회 조건으로 쓰는 값은 `json_body` 내부가 아닌 별도 컬럼으로 분리
- CDC 기능 활성화

### 컬럼별 보충

**`log_id` / `log_uid`** — 목적이 다른 별개 컬럼으로, 둘 다 필요

| | `log_id` | `log_uid` |
|---|---|---|
| 목적 | 수집 커서 (어디까지 읽었는지) | 로그 식별 (중복 제거) |
| 유일 범위 | 해당 DB 내 | 전역 |
| 타입 제약 | 숫자 (JDBC Source Connector `incrementing.column.name`) | `BINARY(16)` |
| 서버 통합 시 | 재발급 무관 | 유지 필요 |

- `log_id`: JDBC Source Connector가 `WHERE log_id > {마지막값} ORDER BY log_id` 로 폴링. 숫자 컬럼 필수
  - AUTO_INCREMENT는 INSERT 시점 배정 / COMMIT 시점 가시화. 동시 트랜잭션에서 작은 번호가 늦게 커밋되면 커넥터가 해당 지점을 이미 지나가 누락 발생. 애플리케이션 시간 기반 생성 주입 시 회피 가능
  - CDC 수집 시에는 커서 불필요 (binlog가 순서 제공)
- `log_uid`: Kafka는 at-least-once. 재전송 시 중복 제거 키로 사용. 서버 통합·재적재 시 ID 재발급 불필요
  - MySQL `UUID()` 함수는 v1. v7은 애플리케이션 라이브러리 사용
- `event_time` / `created_at`: `DEFAULT CURRENT_TIMESTAMP`는 적재 시각. 큐잉·재시도 시 발생 시각과 수 초~수 분 격차. 집계 기준은 `event_time`, 두 값의 차는 파이프라인 지연 지표
- `json_body`: `VARCHAR`는 행당 VARCHAR 합계 64KB 상한으로 절삭 발생. `JSON` 타입은 유효성 검증·`JSON_EXTRACT`·생성컬럼 인덱싱 가능. 원문(공백·키 순서) 보존 필요 시 `LONGTEXT`
- `source_server_id` / `source_id`: 원천 `log_id`는 DB 내에서만 유일하여 통합 시 충돌. 원천 서버 식별자와 쌍으로 보존해야 통합 후 중복 판별 및 마이그레이션 재실행 가능

### DDL 예시

```sql
CREATE TABLE log_xxx (
  log_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  log_uid    BINARY(16) NOT NULL,
  event_time DATETIME(6) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  log_type   SMALLINT UNSIGNED NOT NULL,
  server_no  SMALLINT UNSIGNED NOT NULL,
  source_server_id SMALLINT UNSIGNED NULL,
  source_id        BIGINT UNSIGNED NULL,
  json_body  JSON NOT NULL,
  PRIMARY KEY (log_id, created_at),
  UNIQUE KEY uk_uid (log_uid, created_at),
  UNIQUE KEY uk_source (source_server_id, source_id, created_at),
  KEY ix_type_time (log_type, created_at)
) ENGINE=InnoDB PARTITION BY RANGE COLUMNS(created_at) ( /* 월 또는 일 단위 */ );
```

- PK를 `log_id` 기준으로 두는 이유: 폴링 쿼리(`WHERE log_id > ? ORDER BY log_id`)와 PK 순서 일치 시 인덱스 사용
- PK가 `(log_id, created_at)` 복합인 이유: MySQL은 파티셔닝 기준 컬럼이 모든 유니크/프라이머리 키에 포함될 것을 요구. `PRIMARY KEY (log_id)` 단독 지정 시 파티셔닝 실패
- 보존 기간 만료 처리는 `DELETE` 대신 `DROP PARTITION` (대량 DELETE는 binlog 급증 및 CDC 스트림 지연 유발)

### CDC 활성화 항목

- `binlog_format=ROW`, `binlog_row_image=FULL`
- `gtid_mode=ON`, `enforce_gtid_consistency=ON`
- `server_id` 인스턴스별 유일
- `binlog_expire_logs_seconds` ≥ 재처리 허용 기간
- 대상 테이블 PK 필수 (없으면 커넥터가 메시지 키 생성 불가)
- CDC 계정 권한: `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, `REPLICATION CLIENT`
- 대용량 로그 테이블은 초기 스냅샷 회피 (`snapshot.mode=no_data`)
- 저트래픽 DB는 `heartbeat.interval.ms` 설정 (이벤트 부재 시 오프셋 정체 → binlog 만료 → 커넥터 복구 불가)

### 수집 방식

| | JDBC 폴링 | CDC (binlog) |
|---|---|---|
| 필요 조건 | `log_id` 커서 컬럼 | binlog 설정 |
| 누락 가능성 | 트랜잭션 커밋 순서에 의한 누락 | 없음 |
| DB 부하 | 폴링 쿼리 | binlog 읽기 |

- `log_uid`, `event_time`, `json_body`, 원천 ID는 수집 방식과 무관하게 필요

## 파일인 경우 (Filebeat)

- 로그의 안정성을 위해 잘 알려진 로깅 라이브러리 (Boost.Log, NLog, Log4J 등) 을 이용하여 서버 로컬 파일로 저장
  - 비동기 로깅 사용 시 종료 시점 flush 보장 및 큐 오버플로 정책(drop/block) 확정 필요
- 한 줄 = JSON 한 개 (NDJSON) 형식. 각 줄에 `log_uid`, `event_time` 포함
  - RDBMS 경로와 동일한 중복 제거 키 사용 가능
  - 스택트레이스는 문자열 필드에 포함하여 한 줄로 작성
  - 수집 커서(`log_id`)는 불필요 (파일 오프셋으로 위치 추적)
- 로그 파일명은 일반적으로 서버종류, 서버번호, 날짜 등의 정보를 포함하도록 작성합니다
  - 예: `{service}-{server_no}-{YYYYMMDD}[-{seq}].log`
- 로그 파일이 너무 큰 디스크 공간을 차지하지 않도록 보통 파일 크기 기반의 로그 로테이션을 적용합니다
  - 보관 개수/기간 상한은 디스크 점유 상한에서 역산
  - `copytruncate` 방식은 복사·절단 사이 기록분 유실/중복 발생. rename 방식 사용
  - 압축은 Filebeat 처리 완료 후 적용 (gzip 파일 읽기 불가). harvest glob에서 `.gz` 제외
  - Kafka 장애 시 로컬 파일이 버퍼 역할. 보관 기간은 브로커 장애 허용 시간보다 길게
- **구체적인 파일명이나 로테이션 방식에 대해서는 추가 협의가 필요합니다.**
- 생성된 로그 전송을 위해 각 서버에 로그 파일 전송 에이전트 (Filebeat 등) 의 설치가 필요합니다
  - registry(상태 파일)는 영구 볼륨에 배치 (재시작 시 초기화되면 전체 재전송)
  - `close_inactive`, `ignore_older`, `clean_removed`, `scan_frequency` 값 확정 필요
  - `output.kafka`: 토픽, `required_acks`, `compression`, `max_message_bytes`(브로커 설정과 일치)
- 개인정보·자격증명은 기록 시점 마스킹

## 시간 표기 (공통)

- 직렬화되는 모든 시각은 ISO 8601 / RFC 3339 준수
  - 예: `2026-09-09T04:12:33.481725Z`
- 소수점 이하 최소 3자리 필요, 6자리 이상 권장
- 소수점 자리수는 필드 내 고정 (`.5` / `.500` 혼용 시 문자열 정렬과 시간 순서 불일치)
- 타임존 오프셋 표기 필수 (`Z` 또는 `+09:00`). 오프셋 없는 naive 시각 불허
- MySQL `DATETIME(6)` 컬럼은 저장값을 UTC로 통일
- 파일명·로테이션 접미사는 `YYYYMMDD` (basic format)
- 전 서버 NTP/chrony 동기화

## 확정 필요 항목

- 로그 종류 목록 및 `log_type` 코드
- 로그별 예상 건수·크기 (peak TPS, 일일 GB)
- 보존 기간 및 파티션 단위
- Kafka 토픽 명명 규칙, 파티션 수, 파티션 키
- 로그 파일명 규칙, 회전 크기, 보관 개수
- CDC 재처리 허용 기간 (= binlog 보존 기간)
- 마스킹 대상 필드