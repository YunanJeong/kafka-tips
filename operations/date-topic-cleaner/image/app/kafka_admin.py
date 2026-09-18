"""Kafka AdminClient: 토픽 조회 및 삭제."""

import logging

from confluent_kafka.admin import AdminClient

log = logging.getLogger(__name__)


class TopicAdmin:
    """토픽 목록 조회와 삭제만 담당."""

    def __init__(self, broker, request_timeout=60.0):
        self.admin = AdminClient({'bootstrap.servers': broker})
        self.request_timeout = request_timeout

    def list_topics(self):
        md = self.admin.list_topics(timeout=self.request_timeout)
        return list(md.topics.keys())

    def delete_topics(self, topics, operation_timeout=60.0):
        """토픽 삭제.

        Args:
            - topics (list): 삭제할 토픽명
            - operation_timeout (float): 브로커측 삭제 완료 대기 시간(초)

        Returns:
            (deleted, failed): deleted는 토픽명 리스트,
                               failed는 [(토픽명, 에러문자열), ...]
        """
        if not topics:
            return [], []
        futures = self.admin.delete_topics(
            list(topics),
            operation_timeout=operation_timeout,
            request_timeout=self.request_timeout,
        )
        deleted, failed = [], []
        for topic, fut in futures.items():
            try:
                fut.result()
                deleted.append(topic)
            except Exception as e:  # noqa: BLE001 - 토픽별로 계속 진행해야 함
                failed.append((topic, str(e)))
        return deleted, failed
