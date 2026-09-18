"""CLI 엔트리포인트."""

import argparse
import logging
import sys

from .cleaner import install_sigterm_handler, run
from .connect import ConnectClient
from .kafka_admin import TopicAdmin


def build_parser():
    p = argparse.ArgumentParser(
        prog='date-topic-cleaner',
        description='지정한 달의 날짜토픽(_YYYY_MM, _YYYY_MM_DD)을 삭제한다.')
    p.add_argument(
        '--target-months-ago', type=int, required=True,
        help='몇 달 전의 날짜토픽을 삭제할지. 2026-09-25 에 2를 주면 2026-07 의 '
             '_2026_07 과 _2026_07_DD 를 전부 삭제한다. 그보다 오래된 달은 '
             '건드리지 않는다.')
    p.add_argument(
        '--broker', default='localhost:9092',
        help='bootstrap servers (default: localhost:9092)')
    p.add_argument(
        '--connect-url', default='http://localhost:8083',
        help='Kafka Connect REST URL (default: http://localhost:8083)')
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')

    if args.target_months_ago < 1:
        # 0이면 이번 달, 즉 지금 쓰이는 중인 토픽을 지우게 된다
        print('--target-months-ago 는 1 이상이어야 합니다.', file=sys.stderr)
        return 2

    install_sigterm_handler()

    try:
        return run(
            TopicAdmin(args.broker),
            ConnectClient(args.connect_url),
            target_months_ago=args.target_months_ago,
        )
    except SystemExit as e:  # SIGTERM. 커넥터는 이미 guard가 되돌렸다
        logging.error('중단: %s', e)
        return 1
    except Exception as e:  # noqa: BLE001 - cron에서 종료코드로 판단
        logging.exception('실행 실패: %s', e)
        return 1


if __name__ == '__main__':
    sys.exit(main())
