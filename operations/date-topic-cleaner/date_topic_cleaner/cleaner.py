"""날짜토픽 삭제 실행 흐름.

유관 s3 sink 커넥터를 stop -> 토픽 삭제 -> 커넥터 resume 순으로 진행한다.
커넥터를 stop 하는 이유는, 삭제되는 토픽을 참조하던 sink task가
metadata/fetch 요청에서 timeout을 맞고 fail 하는 것을 막기 위함이다.
"""

import logging
import signal
import time
from datetime import date

import requests

from .connect import find_related_connectors
from .dates import select_expired

log = logging.getLogger(__name__)


RESUME_ATTEMPTS = 3
RESUME_RETRY_INTERVAL = 5.0


class ConnectorStopTimeout(Exception):
    """커넥터가 제한시간 내 STOPPED 상태에 도달하지 못함."""

    def __init__(self, name):
        super().__init__(f'커넥터 {name}: STOPPED 전이 timeout — 삭제 중단')


class ConnectorGuard:
    """삭제 구간 동안 유관 커넥터를 정지시키는 컨텍스트 매니저.

    커넥터를 되살리는 것이 토픽 삭제보다 우선이다. 정지시킨 커넥터는
    어떤 경로로 빠져나가도 반드시 resume 을 시도하고, 끝내 실패하면
    resume_failed 에 남겨 호출부가 실패로 처리하게 한다.

    Note:
        - stop 은 task를 완전히 해제하므로(Connect 3.5+) 삭제 대상 토픽에 대한
          fetch가 멈춘다. pause 는 task가 살아있어 fetch를 계속 시도하므로
          쓰지 않는다.
        - 원래 RUNNING 이 아니었던 커넥터(사용자가 의도적으로 내려둔 것)는
          건드리지 않는다.
    """

    def __init__(self, client, names, wait_timeout=120):
        self.client = client
        self.names = names
        self.wait_timeout = wait_timeout
        self.stopped = []
        self.resume_failed = []

    def __enter__(self):
        try:
            for name in self.names:
                if self.client.state(name) != 'RUNNING':
                    log.info('커넥터 %s: RUNNING 아님, 건너뜀', name)
                    continue
                self.client.stop(name)
                self.stopped.append(name)
                log.info('커넥터 %s: stop 요청', name)
            for name in self.stopped:
                if not self.client.wait_state(name, 'STOPPED',
                                              self.wait_timeout):
                    # 정지 못한 채 삭제하면 막으려던 task fail이 그대로 난다
                    raise ConnectorStopTimeout(name)
        except BaseException:
            # __enter__ 에서 예외가 나면 __exit__ 은 호출되지 않는다.
            # 직접 되돌리지 않으면 앞서 정지시킨 커넥터가 STOPPED로 방치된다.
            # SIGTERM(SystemExit), Ctrl-C 도 잡아야 하므로 BaseException.
            self._resume_stopped()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        self._resume_stopped()
        return False

    def _resume_stopped(self):
        """정지시킨 커넥터를 RUNNING 으로 되돌린다. 실패해도 예외를 내지 않는다.

        삭제 실패나 SIGTERM 으로 빠져나오는 중에도 호출되므로, 여기서 예외를
        던지면 남은 커넥터를 못 살린다. 실패는 resume_failed 로만 보고한다.
        """
        pending, self.stopped = self.stopped, []
        for name in pending:
            if self._resume_one(name):
                continue
            self.resume_failed.append(name)
            log.critical('커넥터 %s: resume 최종 실패 — 수동 복구 필요 '
                         '(PUT /connectors/%s/resume)', name, name)

    def _resume_one(self, name):
        for attempt in range(1, RESUME_ATTEMPTS + 1):
            try:
                self.client.resume(name)
                if self.client.wait_state(name, 'RUNNING', self.wait_timeout):
                    log.info('커넥터 %s: resume 완료', name)
                    return True
                log.warning('커넥터 %s: resume 후 RUNNING 미도달 (%d/%d)',
                            name, attempt, RESUME_ATTEMPTS)
            except requests.RequestException as e:
                log.warning('커넥터 %s resume 실패 (%d/%d): %s',
                            name, attempt, RESUME_ATTEMPTS, e)
            if attempt < RESUME_ATTEMPTS:
                time.sleep(RESUME_RETRY_INTERVAL)
        return False


def install_sigterm_handler():
    """SIGTERM 을 SystemExit 으로 바꿔 ConnectorGuard 가 풀리게 한다.

    기본 SIGTERM 은 프로세스를 즉시 끝내므로 __exit__ 이 실행되지 않아
    커넥터가 STOPPED 로 방치된다. 노드 드레인이나 job 삭제 시 발생한다.
    """
    def handler(signum, frame):
        raise SystemExit(f'signal {signum} 수신')

    signal.signal(signal.SIGTERM, handler)


def run(admin, connect_client, retention_days, today=None,
        connector_wait_timeout=120, delete_timeout=120.0):
    """날짜토픽 삭제를 수행.

    Args:
        - admin (TopicAdmin): 토픽 조회/삭제
        - connect_client (ConnectClient): 커넥터 제어
        - retention_days (int): 보존일수
        - today (datetime.date): 기준일 (default: 오늘)
        - connector_wait_timeout (int): 커넥터 상태전이 대기 시간(초)
        - delete_timeout (float): 브로커측 삭제 완료 대기 시간(초)

    Returns:
        int: 종료코드. 0=정상, 1=일부 실패
    """
    today = today or date.today()
    all_topics = admin.list_topics()
    log.info('전체 토픽 %d개', len(all_topics))

    expired = select_expired(all_topics, retention_days, today)
    if not expired:
        log.info('삭제대상 없음 (기준일수 %d일)', retention_days)
        return 0

    log.info('삭제대상 %d개 (기준일수 %d일)', len(expired), retention_days)
    for topic, topic_date, kind in expired:
        log.info('  %s (%s, %s)', topic, topic_date.isoformat(), kind)

    targets = [t for t, _, _ in expired]

    configs = connect_client.list_connectors_expanded()
    related = find_related_connectors(configs, targets)
    log.info('유관 커넥터 %d개: %s', len(related), related or '-')

    guard = ConnectorGuard(connect_client, related, connector_wait_timeout)
    with guard:
        deleted, failed = admin.delete_topics(targets, delete_timeout)

    for topic in deleted:
        log.info('삭제 완료: %s', topic)
    for topic, err in failed:
        log.error('삭제 실패: %s — %s', topic, err)
    log.info('결과: 삭제 %d개, 실패 %d개', len(deleted), len(failed))

    if guard.resume_failed:
        # 커넥터가 내려가 있는 것이 토픽이 안 지워진 것보다 심각하다
        log.critical('resume 실패 커넥터: %s', guard.resume_failed)
        return 1
    return 1 if failed else 0
