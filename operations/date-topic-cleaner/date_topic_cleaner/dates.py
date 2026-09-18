"""날짜토픽 이름 파싱 및 삭제대상 판정."""

import calendar
import re
from datetime import date

# 토픽명 끝의 _YYYY_MM_DD
DAILY_RE = re.compile(r'^.+_(?P<y>\d{4})_(?P<m>\d{2})_(?P<d>\d{2})$')
# 토픽명 끝의 _YYYY_MM
MONTHLY_RE = re.compile(r'^.+_(?P<y>\d{4})_(?P<m>\d{2})$')


def parse_topic_date(topic):
    """날짜토픽명에서 기준일을 파싱.

    Args:
        - topic (str): 토픽명

    Returns:
        (datetime.date, str) 또는 None.
        - date: 해당 토픽이 담는 기간의 마지막 날.
                일단위 토픽은 그 날짜, 월단위 토픽은 그 달의 말일.
        - str: 'daily' 또는 'monthly'

    Note:
        - 월단위 토픽에 말일을 쓰는 이유: 진행중인 달의 토픽이
          조기에 삭제대상으로 잡히는 것을 막기 위함.
        - _YYYY_MM_DD 를 먼저 검사한다. _2026_07_15 는
          MONTHLY_RE 에도 걸리므로(prefix=..._2026, y=07 불가) 순서가 중요.
    """
    m = DAILY_RE.match(topic)
    if m:
        d = _to_date(int(m['y']), int(m['m']), int(m['d']))
        return (d, 'daily') if d else None

    m = MONTHLY_RE.match(topic)
    if m:
        y, mm = int(m['y']), int(m['m'])
        if not 1 <= mm <= 12:
            return None
        return date(y, mm, calendar.monthrange(y, mm)[1]), 'monthly'

    return None


def _to_date(y, m, d):
    try:
        return date(y, m, d)
    except ValueError:  # 2026_02_30 같은 존재하지 않는 날짜
        return None


def select_expired(topics, retention_days, today):
    """삭제대상 날짜토픽을 선별.

    Args:
        - topics (iterable): 전체 토픽명
        - retention_days (int): 보존일수. 기준일이 today-retention_days 보다
                                이전인 토픽을 삭제대상으로 본다.
        - today (datetime.date): 기준 오늘 날짜

    Returns:
        [(topic, date, kind), ...] 기준일 오름차순 정렬.
    """
    cutoff = date.fromordinal(today.toordinal() - retention_days)
    expired = []
    for topic in topics:
        if topic.startswith('_'):  # __consumer_offsets 등 내부 토픽
            continue
        parsed = parse_topic_date(topic)
        if parsed is None:
            continue
        topic_date, kind = parsed
        if topic_date < cutoff:
            expired.append((topic, topic_date, kind))
    expired.sort(key=lambda x: (x[1], x[0]))
    return expired
