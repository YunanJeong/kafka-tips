"""Timezone 테스트.

# db.timezone 옵션에 따른다.

# Kafka broker 및 connect 프로세스 서비스의 시간은 상관없다.
# Kafka 실행환경 서버시간도 상관없다.
# 대상 DB 실행환경 서버시간도 상관없다.

# 기본적으로 대상 DB의 row에 기입된 EventTime의 시간을 그대로 가져오며,
# 해당 시간을 db.timezone 옵션에 따라 해석한다.
# db.timezone의 default는 UTC이다.
# e.g.) Event Time이 '2023년 1월 17일 0시 0분 0초' 일 때,
 - "db.timezone": "UTC" 이면 UTC로 '2023년 1월 17일 0시 0분 0초'에 해당하는 timestamp가 Kafka에 입력된다.
 - "db.timezone": "Asia/Seoul" 이면,
    - KST로 '2023년 1월 17일 0시 0분 0초'
    - 즉, UTC로 '2023년 1월 16일 15시 0분 0초'에 해당하는 timestamp가 Kafka에 입력된다.

# jdbc로 시간처리를 하면, Kafka topic에는 항상 timestamp형태로 저장된다는 점을 기억하자!
"""

import json
import requests

####################################
# 보안 주의. git push 주의
####################################
connect_ip = 'localhost:8083'

db_ip = 'localhost:1433'
db_name = 'TutorialDB'
db_user = 'tester'
db_pass = 'tester^381'
db_table = 'Customers'
incrementing_col = 'CustomerId'
timestamp_col = 'regdate'
incrementing_initial = "RegDate > CONVERT(DATETIME, '2022-05-11 00:00:01.000')"
####################################

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}


db_tables = ['TutorialDB.dbo.Customers', ]
body = {
    "name": "jdbc-timezonetest",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
      "tasks.max": "1",         # 한 커넥터에서 처리하는 테이블 개수

      "connection.url": f"jdbc:sqlserver://{db_ip};databaseName={db_name}",
      "connection.user": f"{db_user}",
      "connection.password": f"{db_pass}",


      "db.timezone": "UTC",
      "table.whitelist": ','.join(db_tables),  # 특정 테이블들만 조회
      "mode": "bulk",
      "topic.prefix": "jdbc-timezonetest",


      # JsonConverter에서 스키마 미사용하기
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": False,

      "poll.interval.ms": 60000,  # default: 60000ms (1분)
      "batch.max.rows": 2000,    # default: 100개

      "transaction.isolation.mode": "READ_UNCOMMITED",  # sql server default: READ_COMMITED
  }
}

kafka_connect = 'http://'+connect_ip+'/connectors'
response = requests.post(kafka_connect, json.dumps(body, ensure_ascii=False), headers=headers)
print(response)
