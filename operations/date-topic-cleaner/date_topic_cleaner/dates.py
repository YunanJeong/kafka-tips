"""날짜토픽 이름 파싱 및 삭제대상 판정.

판정 단위는 '달'이다. `_YYYY_MM` 과 `_YYYY_MM_DD` 를 같은 달 키로 묶어
한 덩어리로 다루므로 한 달이 며칠씩 쪼개져 삭제되는 일이 없다.
"""

import re
from datetime import date

# 토픽명 끝의 _YYYY_MM_DD
DAILY_RE = re.compile(r'^.+_(?P<y>\d{4})_(?P<m>\d{2})_(?P<d>\d{2})$')
# 토픽명 끝의 _YYYY_MM
MONTHLY_RE = re.compile(r'^.+_(?P<y>\d{4})_(?P<m>\d{2})$')


def parse_month_key(topic):
    """날짜토픽명에서 (년, 월) 을 파싱.

    Args:
        - topic (str): 토픽명

    Returns:
        (int, int) 또는 None. 날짜토픽이 아니면 None.

    Note:
        - _YYYY_MM_DD 를 먼저 검사한다. `a_2026_07_15` 는 MONTHLY_RE 에도
          걸리지만(y=2026_07 불가하므로 실제로는 y=0715 형태로 오독) 순서로 막는다.
        - 일단위든 월단위든 같은 달이면 같은 키를 반환한다.
          `a_2026_07` 과 `a_2026_07_15` 는 둘 다 (2026, 7).
    """
    m = DAILY_RE.match(topic)
    if m:
        y, mm, dd = int(m['y']), int(m['m']), int(m['d'])
        try:
            date(y, mm, dd)  # 2026_02_30 같은 없는 날짜 거르기
        except ValueError:
            return None
        return y, mm

    m = MONTHLY_RE.match(topic)
    if m:
        y, mm = int(m['y']), int(m['m'])
        if not 1 <= mm <= 12:
            return None
        return y, mm

    return None


def target_month(today, months_ago):
    """삭제대상 월을 계산.

    Args:
        - today (datetime.date): 기준일
        - months_ago (int): 몇 달 전을 대상으로 볼지

    Returns:
        (int, int): (년, 월)

    Note:
        - 2026-09-18 기준 months_ago=2 이면 (2026, 7).
    """
    total = today.year * 12 + (today.month - 1) - months_ago
    y, m = divmod(total, 12)
    return y, m + 1


def select_targets(topics, months_ago, today):
    """삭제대상 날짜토픽을 선별.

    대상 월 '하나'에 속한 토픽만 고른다. 그보다 오래된 달은 건드리지 않는다.

    Args:
        - topics (iterable): 전체 토픽명
        - months_ago (int): 몇 달 전을 대상으로 볼지
        - today (datetime.date): 기준일

    Returns:
        (targets, month_key): targets 는 정렬된 토픽명 리스트,
                              month_key 는 대상 (년, 월)
    """
    want = target_month(today, months_ago)
    targets = []
    for topic in topics:
        if topic.startswith('_'):  # __consumer_offsets 등 내부 토픽
            continue
        if parse_month_key(topic) == want:
            targets.append(topic)
    return sorted(targets), want
