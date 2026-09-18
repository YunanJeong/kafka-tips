"""삭제 실행 흐름 테스트."""

from datetime import date

from app.cleaner import run

S3 = 'io.confluent.connect.s3.S3SinkConnector'
TODAY = date(2026, 9, 25)


class FakeAdmin:
    def __init__(self, topics, fail=()):
        self.topics = list(topics)
        self.fail = set(fail)
        self.deleted_calls = []

    def list_topics(self):
        return list(self.topics)

    def delete_topics(self, topics, operation_timeout=60.0):
        self.deleted_calls.append(list(topics))
        ok = [t for t in topics if t not in self.fail]
        ng = [(t, 'timeout') for t in topics if t in self.fail]
        return ok, ng


class FakeConnect:
    def __init__(self, configs):
        self.configs = configs
        self.events = []
        self.states = {n: 'RUNNING' for n in configs}

    def list_connectors_expanded(self):
        return self.configs

    def state(self, n):
        return self.states[n]

    def stop(self, n):
        self.events.append(('stop', n))
        self.states[n] = 'STOPPED'

    def resume(self, n):
        self.events.append(('resume', n))
        self.states[n] = 'RUNNING'

    def wait_state(self, n, want, timeout, interval=2.0):
        return self.states[n] == want


def test_run_deletes_whole_month_and_cycles_connector():
    """monthly와 daily가 한 번의 stop 구간에서 함께 삭제된다."""
    admin = FakeAdmin(['log_2026_07', 'log_2026_07_15',
                       'log_2026_09_25', 'plain'])
    connect = FakeConnect({'sink': {'connector.class': S3,
                                    'topics.regex': 'log_.*'}})
    rc = run(admin, connect, target_months_ago=2, today=TODAY)
    assert rc == 0
    assert admin.deleted_calls == [['log_2026_07', 'log_2026_07_15']]
    assert connect.events == [('stop', 'sink'), ('resume', 'sink')]


def test_run_leaves_older_months_alone():
    """대상 월만 지운다. 더 오래된 달은 건드리지 않는다."""
    admin = FakeAdmin(['log_2026_05_01', 'log_2026_06', 'log_2026_07_15'])
    connect = FakeConnect({'sink': {'connector.class': S3,
                                    'topics.regex': 'log_.*'}})
    assert run(admin, connect, target_months_ago=2, today=TODAY) == 0
    assert admin.deleted_calls == [['log_2026_07_15']]


def test_run_no_target_skips_connector_entirely():
    admin = FakeAdmin(['log_2026_09_25', 'plain'])
    connect = FakeConnect({'sink': {'connector.class': S3,
                                    'topics.regex': 'log_.*'}})
    assert run(admin, connect, target_months_ago=2, today=TODAY) == 0
    assert admin.deleted_calls == []
    assert connect.events == []


def test_run_returns_1_on_partial_failure_but_resumes():
    admin = FakeAdmin(['log_2026_07_01', 'log_2026_07_02'],
                      fail=['log_2026_07_02'])
    connect = FakeConnect({'sink': {'connector.class': S3,
                                    'topics.regex': 'log_.*'}})
    assert run(admin, connect, target_months_ago=2, today=TODAY) == 1
    assert connect.states['sink'] == 'RUNNING'


def test_run_no_related_connector_leaves_others_alone():
    admin = FakeAdmin(['log_2026_07_01'])
    connect = FakeConnect({'sink': {'connector.class': S3,
                                    'topics': 'unrelated'}})
    assert run(admin, connect, target_months_ago=2, today=TODAY) == 0
    assert admin.deleted_calls == [['log_2026_07_01']]
    assert connect.events == []
