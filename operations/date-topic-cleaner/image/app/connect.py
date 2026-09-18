"""Kafka Connect REST API: 유관 커넥터 탐색 및 stop/resume."""

import logging
import re
import time

import requests

log = logging.getLogger(__name__)

S3_SINK_CLASS_HINT = 'S3SinkConnector'


class ConnectClient:
    """Kafka Connect REST 클라이언트."""

    def __init__(self, url, timeout=30):
        self.url = url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()

    def _req(self, method, path):
        res = self.session.request(
            method, f'{self.url}{path}', timeout=self.timeout)
        res.raise_for_status()
        return res

    def list_connectors_expanded(self):
        """커넥터 이름 -> config dict."""
        res = self._req('GET', '/connectors?expand=info')
        return {name: body['info']['config']
                for name, body in res.json().items()}

    def state(self, name):
        """커넥터 상태 문자열 (RUNNING, STOPPED, FAILED ...)."""
        res = self._req('GET', f'/connectors/{name}/status')
        return res.json()['connector']['state']

    def stop(self, name):
        """커넥터 정지. task를 완전히 해제한다 (Connect 3.5+)."""
        self._req('PUT', f'/connectors/{name}/stop')

    def resume(self, name):
        """정지된 커넥터 재개."""
        self._req('PUT', f'/connectors/{name}/resume')

    def wait_state(self, name, want, timeout, interval=2.0):
        """커넥터가 want 상태가 될 때까지 대기.

        Returns:
            bool: 도달 여부
        """
        deadline = time.monotonic() + timeout
        while True:
            try:
                cur = self.state(name)
            except requests.RequestException as e:
                cur = None
                log.warning('%s 상태 조회 실패: %s', name, e)
            if cur == want:
                return True
            if time.monotonic() >= deadline:
                log.warning('%s 상태 대기 timeout (want=%s, cur=%s)',
                            name, want, cur)
                return False
            time.sleep(interval)


def _topic_matchers(config):
    """sink 커넥터 config에서 (명시토픽 set, topics.regex 패턴) 추출."""
    names = set()
    raw = config.get('topics')
    if raw:
        names = {t.strip() for t in raw.split(',') if t.strip()}

    pattern = None
    raw_re = config.get('topics.regex')
    if raw_re:
        try:
            pattern = re.compile(raw_re)
        except re.error as e:
            log.warning('topics.regex 컴파일 실패 (%r): %s', raw_re, e)
    return names, pattern


def find_related_connectors(configs, topics):
    """삭제대상 토픽을 참조하는 s3 sink 커넥터명을 반환.

    Args:
        - configs (dict): 커넥터명 -> config dict
        - topics (iterable): 삭제대상 토픽명

    Returns:
        정렬된 커넥터명 리스트.

    Note:
        - topics.regex 는 Connect가 부분매칭이 아닌 fullmatch로 다루므로
          fullmatch 로 판정한다.
    """
    topics = set(topics)
    related = []
    for name, config in configs.items():
        if S3_SINK_CLASS_HINT not in config.get('connector.class', ''):
            continue
        names, pattern = _topic_matchers(config)
        if names & topics:
            related.append(name)
            continue
        if pattern and any(pattern.fullmatch(t) for t in topics):
            related.append(name)
    return sorted(related)
