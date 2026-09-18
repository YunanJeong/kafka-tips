"""날짜토픽 파싱/선별 로직 테스트."""

from datetime import date

from date_topic_cleaner.dates import parse_topic_date, select_expired


def test_parse_daily():
    assert parse_topic_date('mylog_2026_07_15') == (date(2026, 7, 15), 'daily')


def test_parse_monthly_uses_last_day():
    assert parse_topic_date('mylog_2026_07') == (date(2026, 7, 31), 'monthly')
    assert parse_topic_date('mylog_2024_02') == (date(2024, 2, 29), 'monthly')
    assert parse_topic_date('mylog_2026_02') == (date(2026, 2, 28), 'monthly')


def test_parse_daily_wins_over_monthly():
    """_2026_07_15 는 monthly 정규식에도 걸리지만 daily로 잡혀야 한다."""
    assert parse_topic_date('a_2026_07_15')[1] == 'daily'


def test_parse_invalid():
    assert parse_topic_date('mylog') is None
    assert parse_topic_date('mylog_2026') is None
    assert parse_topic_date('mylog_2026_13') is None      # 13월
    assert parse_topic_date('mylog_2026_00') is None      # 0월
    assert parse_topic_date('mylog_2026_02_30') is None   # 없는 날짜
    assert parse_topic_date('mylog_2026_7_15') is None    # 0패딩 없음
    assert parse_topic_date('2026_07_15') is None         # prefix 없음
    assert parse_topic_date('mylog_2026_07_15_extra') is None


def test_select_expired_boundary():
    """cutoff 당일은 남기고 그 이전만 삭제한다."""
    today = date(2026, 9, 18)
    topics = [
        'a_2026_07_20',  # cutoff 당일 -> 보존
        'a_2026_07_19',  # cutoff 이전 -> 삭제
        'a_2026_07_21',  # 보존
    ]
    got = [t for t, _, _ in select_expired(topics, 60, today)]
    assert got == ['a_2026_07_19']


def test_select_expired_monthly_current_month_kept():
    """진행중인 달의 월단위 토픽은 말일 기준이라 삭제되지 않는다."""
    today = date(2026, 9, 18)
    got = [t for t, _, _ in select_expired(['a_2026_09', 'a_2026_06'], 60, today)]
    assert got == ['a_2026_06']  # 2026-06-30 < 2026-07-20


def test_select_expired_skips_internal_and_non_date():
    today = date(2026, 9, 18)
    topics = [
        '__consumer_offsets',
        '_schemas_2020_01_01',
        'connect-offsets',
        'plain_topic',
        'a_2020_01_01',
    ]
    got = [t for t, _, _ in select_expired(topics, 60, today)]
    assert got == ['a_2020_01_01']


def test_select_expired_sorted_by_date():
    today = date(2026, 9, 18)
    topics = ['a_2020_03_01', 'a_2020_01_01', 'a_2020_02_01']
    got = [t for t, _, _ in select_expired(topics, 60, today)]
    assert got == ['a_2020_01_01', 'a_2020_02_01', 'a_2020_03_01']


def test_select_expired_zero_retention_keeps_today():
    today = date(2026, 9, 18)
    topics = ['a_2026_09_18', 'a_2026_09_17']
    got = [t for t, _, _ in select_expired(topics, 0, today)]
    assert got == ['a_2026_09_17']
