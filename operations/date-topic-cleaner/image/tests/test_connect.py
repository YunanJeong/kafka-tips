"""유관 커넥터 탐색 및 ConnectorGuard 테스트."""

import pytest
import requests

from app import cleaner
from app.cleaner import ConnectorGuard, ConnectorStopTimeout
from app.connect import find_related_connectors

S3 = 'io.confluent.connect.s3.S3SinkConnector'
JDBC = 'io.confluent.connect.jdbc.JdbcSourceConnector'


def test_find_by_topics_list():
    configs = {
        'sink-a': {'connector.class': S3, 'topics': 'log_2026_07_15,other'},
        'sink-b': {'connector.class': S3, 'topics': 'unrelated'},
    }
    assert find_related_connectors(configs, ['log_2026_07_15']) == ['sink-a']


def test_find_by_topics_regex_fullmatch():
    configs = {
        'sink-a': {'connector.class': S3, 'topics.regex': 'log_.*'},
        'sink-b': {'connector.class': S3, 'topics.regex': 'log'},
    }
    assert find_related_connectors(configs, ['log_2026_07_15']) == ['sink-a']


def test_find_skips_non_s3():
    configs = {'src': {'connector.class': JDBC, 'topics': 'log_2026_07_15'}}
    assert find_related_connectors(configs, ['log_2026_07_15']) == []


def test_find_ignores_broken_regex():
    configs = {'sink-a': {'connector.class': S3, 'topics.regex': 'log_(['}}
    assert find_related_connectors(configs, ['log_2026_07_15']) == []


class FakeClient:
    """ConnectClient 대역."""

    def __init__(self, states):
        self.states = dict(states)
        self.calls = []

    def state(self, name):
        return self.states[name]

    def stop(self, name):
        self.calls.append(('stop', name))
        self.states[name] = 'STOPPED'

    def resume(self, name):
        self.calls.append(('resume', name))
        self.states[name] = 'RUNNING'

    def wait_state(self, name, want, timeout, interval=2.0):
        return self.states[name] == want


def test_guard_stops_and_resumes():
    c = FakeClient({'sink-a': 'RUNNING', 'sink-b': 'RUNNING'})
    with ConnectorGuard(c, ['sink-a', 'sink-b']):
        assert c.states == {'sink-a': 'STOPPED', 'sink-b': 'STOPPED'}
    assert c.states == {'sink-a': 'RUNNING', 'sink-b': 'RUNNING'}
    assert c.calls == [
        ('stop', 'sink-a'), ('stop', 'sink-b'),
        ('resume', 'sink-a'), ('resume', 'sink-b'),
    ]


def test_guard_skips_non_running():
    """사용자가 의도적으로 내려둔 커넥터는 건드리지 않는다."""
    c = FakeClient({'sink-a': 'PAUSED', 'sink-b': 'RUNNING'})
    with ConnectorGuard(c, ['sink-a', 'sink-b']):
        pass
    assert c.calls == [('stop', 'sink-b'), ('resume', 'sink-b')]
    assert c.states['sink-a'] == 'PAUSED'


def test_guard_resumes_on_exception():
    c = FakeClient({'sink-a': 'RUNNING'})
    with pytest.raises(RuntimeError):
        with ConnectorGuard(c, ['sink-a']):
            raise RuntimeError('삭제 중 실패')
    assert c.states['sink-a'] == 'RUNNING'
    assert ('resume', 'sink-a') in c.calls


def test_guard_empty_names_is_noop():
    c = FakeClient({})
    with ConnectorGuard(c, []):
        pass
    assert c.calls == []


def test_guard_resumes_when_stop_fails_midway():
    """__enter__ 중 실패해도 앞서 정지시킨 커넥터는 되돌려야 한다.

    __enter__ 에서 예외가 나면 __exit__ 은 호출되지 않으므로,
    이걸 직접 처리하지 않으면 sink-a 가 STOPPED로 방치된다.
    """
    c = FakeClient({'sink-a': 'RUNNING', 'sink-b': 'RUNNING'})

    def boom(name):
        if name == 'sink-b':
            raise RuntimeError('stop 실패')
        c.calls.append(('stop', name))
        c.states[name] = 'STOPPED'

    c.stop = boom
    with pytest.raises(RuntimeError):
        with ConnectorGuard(c, ['sink-a', 'sink-b']):
            pass
    assert c.states['sink-a'] == 'RUNNING'
    assert ('resume', 'sink-a') in c.calls


def test_guard_resumes_on_sigterm():
    """SIGTERM(SystemExit)으로 빠져나가도 커넥터를 되살린다.

    SystemExit은 Exception이 아니라 BaseException이라 except Exception
    으로는 안 잡힌다. 잡지 못하면 Pod이 죽으며 커넥터가 STOPPED로 남는다.
    """
    c = FakeClient({'sink-a': 'RUNNING'})
    g = ConnectorGuard(c, ['sink-a'])
    with pytest.raises(SystemExit):
        with g:
            raise SystemExit('signal 15 수신')
    assert c.states['sink-a'] == 'RUNNING'
    assert g.resume_failed == []


def test_guard_retries_resume_then_succeeds(monkeypatch):
    monkeypatch.setattr(cleaner, 'RESUME_RETRY_INTERVAL', 0)
    c = FakeClient({'sink-a': 'RUNNING'})
    calls = []
    real_resume = c.resume

    def flaky(name):
        calls.append(name)
        if len(calls) < 3:
            raise requests.ConnectionError('connect 재시작 중')
        real_resume(name)

    c.resume = flaky
    g = ConnectorGuard(c, ['sink-a'])
    with g:
        pass
    assert c.states['sink-a'] == 'RUNNING'
    assert g.resume_failed == []
    assert len(calls) == 3


def test_guard_reports_resume_failure(monkeypatch):
    """끝내 못 살리면 예외 대신 resume_failed 로 보고한다."""
    monkeypatch.setattr(cleaner, 'RESUME_RETRY_INTERVAL', 0)
    c = FakeClient({'sink-a': 'RUNNING'})

    def dead(name):
        raise requests.ConnectionError('connect 응답 없음')

    c.resume = dead
    g = ConnectorGuard(c, ['sink-a'])
    with g:  # 예외가 새어나오면 안 된다
        pass
    assert g.resume_failed == ['sink-a']


def test_guard_resumes_others_when_one_fails(monkeypatch):
    """한 커넥터 resume 실패가 나머지 복구를 막으면 안 된다."""
    monkeypatch.setattr(cleaner, 'RESUME_RETRY_INTERVAL', 0)
    c = FakeClient({'sink-a': 'RUNNING', 'sink-b': 'RUNNING'})
    real_resume = c.resume

    def partial(name):
        if name == 'sink-a':
            raise requests.ConnectionError('실패')
        real_resume(name)

    c.resume = partial
    g = ConnectorGuard(c, ['sink-a', 'sink-b'])
    with g:
        pass
    assert g.resume_failed == ['sink-a']
    assert c.states['sink-b'] == 'RUNNING'


def test_guard_aborts_when_stop_does_not_take_effect():
    """STOPPED 전이를 확인 못하면 삭제로 진입하지 않고 되돌린다."""
    c = FakeClient({'sink-a': 'RUNNING'})
    c.wait_state = lambda name, want, timeout, interval=2.0: want != 'STOPPED'

    entered = False
    with pytest.raises(ConnectorStopTimeout):
        with ConnectorGuard(c, ['sink-a']):
            entered = True
    assert entered is False
    assert c.states['sink-a'] == 'RUNNING'
