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
        description='오래된 날짜토픽(_YYYY_MM_DD, _YYYY_MM)을 삭제한다.')
    p.add_argument(
        '--retention-days', type=int, required=True,
        help='보존일수. 기준일이 (오늘 - 이 값) 보다 이전인 날짜토픽을 삭제한다.')
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

    if args.retention_days < 0:
        print('--retention-days 는 0 이상이어야 합니다.', file=sys.stderr)
        return 2

    install_sigterm_handler()

    try:
        return run(
            TopicAdmin(args.broker),
            ConnectClient(args.connect_url),
            retention_days=args.retention_days,
        )
    except SystemExit as e:  # SIGTERM. 커넥터는 이미 guard가 되돌렸다
        logging.error('중단: %s', e)
        return 1
    except Exception as e:  # noqa: BLE001 - cron에서 종료코드로 판단
        logging.exception('실행 실패: %s', e)
        return 1


if __name__ == '__main__':
    sys.exit(main())
