"""날짜토픽 파싱/선별 로직 테스트."""

from datetime import date

from date_topic_cleaner.dates import (
    parse_month_key, select_targets, target_month)

TODAY = date(2026, 9, 25)


def test_parse_daily_and_monthly_share_key():
    """일단위와 월단위 토픽은 같은 달 키로 묶인다."""
    assert parse_month_key('mylog_2026_07_15') == (2026, 7)
    assert parse_month_key('mylog_2026_07') == (2026, 7)


def test_parse_invalid():
    assert parse_month_key('mylog') is None
    assert parse_month_key('mylog_2026') is None
    assert parse_month_key('mylog_2026_13') is None      # 13월
    assert parse_month_key('mylog_2026_00') is None      # 0월
    assert parse_month_key('mylog_2026_02_30') is None   # 없는 날짜
    assert parse_month_key('mylog_2026_7_15') is None    # 0패딩 없음
    assert parse_month_key('2026_07_15') is None         # prefix 없음
    assert parse_month_key('mylog_2026_07_15_extra') is None


def test_target_month():
    assert target_month(TODAY, 2) == (2026, 7)
    assert target_month(TODAY, 1) == (2026, 8)


def test_target_month_crosses_year():
    assert target_month(date(2026, 1, 25), 1) == (2025, 12)
    assert target_month(date(2026, 1, 25), 2) == (2025, 11)
    assert target_month(date(2026, 2, 25), 14) == (2024, 12)


def test_select_targets_takes_whole_month():
    """대상 월의 monthly와 daily가 전부 함께 잡힌다."""
    topics = [
        'log_2026_07',
        'log_2026_07_01',
        'log_2026_07_31',
    ]
    targets, key = select_targets(topics, 2, TODAY)
    assert key == (2026, 7)
    assert targets == ['log_2026_07', 'log_2026_07_01', 'log_2026_07_31']


def test_select_targets_only_that_month():
    """대상 월만 잡고 더 오래된 달은 건드리지 않는다."""
    topics = [
        'log_2026_06_30',   # 더 오래됨 -> 제외
        'log_2026_06',      # 더 오래됨 -> 제외
        'log_2026_07_15',   # 대상
        'log_2026_08_01',   # 최근 -> 제외
        'log_2026_09_25',   # 이번 달 -> 제외
    ]
    targets, _ = select_targets(topics, 2, TODAY)
    assert targets == ['log_2026_07_15']


def test_select_targets_skips_internal_and_non_date():
    topics = [
        '__consumer_offsets',
        '_schemas_2026_07_01',
        'connect-offsets',
        'plain_topic',
        'log_2026_07_01',
    ]
    targets, _ = select_targets(topics, 2, TODAY)
    assert targets == ['log_2026_07_01']


def test_select_targets_empty_when_month_already_clean():
    targets, key = select_targets(['log_2026_09_25'], 2, TODAY)
    assert targets == []
    assert key == (2026, 7)


def test_select_targets_multiple_prefixes():
    """서로 다른 토픽 계열도 같은 달이면 함께 잡힌다."""
    topics = ['a_2026_07_01', 'b_2026_07', 'c_2026_08_01']
    targets, _ = select_targets(topics, 2, TODAY)
    assert targets == ['a_2026_07_01', 'b_2026_07']
