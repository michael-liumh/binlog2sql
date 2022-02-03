# !/usr/bin/env python3
# -*- coding:utf8 -*-

import argparse
import getpass
import json
import sys
import pymysql
import os
import re
import time
import logging
import colorlog
import logging.handlers
from datetime import datetime
# 测试运行时：export PYTHONPATH=../utils/

base_dir = os.path.dirname(os.path.abspath(__file__))
sep = '/' if '/' in sys.argv[0] else os.sep
py_file_pre = sys.argv[0].split(sep)[-1].split('.')[0]

logger = logging.getLogger('execute_mysql_dml_v1_single')
logger.setLevel(logging.INFO)

# set logger color
log_colors_config = {
    'DEBUG': 'bold_purple',
    'INFO': 'bold_green',
    'WARNING': 'bold_yellow',
    'ERROR': 'bold_red',
    'CRITICAL': 'bold_red',
}

# set logger format
console_format = colorlog.ColoredFormatter(
    "%(log_color)s[%(asctime)s] [%(module)s:%(funcName)s] [%(lineno)d] [%(levelname)s] %(message)s",
    log_colors=log_colors_config
)

# add console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_format)
logger.addHandler(console_handler)
fei_shu_utils_object = ''

try:
    from feishu_utils import FeiShuUtils

    fei_shu_utils_object = FeiShuUtils(
        app_id="cli_a0d68082a2b9500e",
        app_secret="Pby78SUlpJtL3OPbdnQzxgjKtmq0i71c"
    )
except ModuleNotFoundError:
    pass


def add_file_handler():
    global logger

    logfile_format = logging.Formatter(
        "[%(asctime)s] [%(module)s:%(funcName)s] [%(lineno)d] [%(levelname)s] %(message)s"
    )

    # add rotate file handler
    logs_dir = os.path.join(base_dir, 'logs')
    if not os.path.isdir(logs_dir):
        os.makedirs(logs_dir, exist_ok=True)

    logfile = os.path.join(logs_dir, py_file_pre + '.log')
    file_maxsize = 1024 * 1024 * 100  # 100m

    file_handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=file_maxsize, backupCount=10,
                                                        encoding='utf8')
    file_handler.setFormatter(logfile_format)
    logger.addHandler(file_handler)


def parse_args():
    """parse args to connect MySQL"""

    parser = argparse.ArgumentParser(description='Parse MySQL Connect Settings', add_help=False,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-u', '--user', dest='user', type=str, default='root', help='MySQL User')
    connect_setting.add_argument('-p', '--password', dest='password', type=str, nargs='*', default='',
                                 help='MySQL Password')
    connect_setting.add_argument('-h', '--host', dest='host', type=str, default='127.0.0.1', help='MySQL Host')
    connect_setting.add_argument('-P', '--port', dest='port', type=int, default=3306, help='MySQL Port')
    connect_setting.add_argument('-S', '--socket', dest='socket', type=str, default='', help='MySQL Socket')
    connect_setting.add_argument('-C', '--charset', dest='charset', type=str, default='utf8mb4', help='MySQL Charset')

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--database', dest='database', type=str, default='', help='Connect MySQL Database')

    sql_file = parser.add_argument_group('sql file')
    sql_file.add_argument('-f', '--file', dest='file_path', type=str, nargs='*', default='',
                          help='SQL file you want to execute')
    sql_file.add_argument('-fd', '--file-dir', dest='file_dir', type=str, default='.',
                          help='SQL file dir')
    sql_file.add_argument('-fr', '--file-regex', dest='file_regex', type=str, default='.*\\.sql',
                          help="SQL file regex, use to find SQL file in file dir. ")
    sql_file.add_argument('-efr', '--exclude-file-regex', dest='exclude_file_regex', type=str,
                          default='executed_.*',
                          help="SQL file exclude regex, use to exclude file you don't want it. ")
    sql_file.add_argument('--start-file', dest='start_file', type=str, default='',
                          help='Start file in SQL file dir')
    sql_file.add_argument('--stop-file', dest='stop_file', type=str, default='',
                          help='Stop file in SQL file dir')
    sql_file.add_argument('--check', dest='check', action='store_true', default=False,
                          help='Check SQL file list if you want')
    sql_file.add_argument('-ma', '--minutes-ago', dest='minutes_ago', type=int, default=1,
                          help='Only files whose last modification time was number minutes ago are executed')

    sep = '/' if '/' in sys.argv[0] else os.sep
    committed_file = base_dir + sep + 'logs' + sep + 'committed_' + sys.argv[0].split(sep)[-1].split('.')[0] + '.json'
    sql_file.add_argument('--save', dest='result_file', type=str,
                          help='file for save committed line.', default=committed_file)

    execute = parser.add_argument_group('execute method')
    execute.add_argument('--chunk', dest='chunk', type=int, default=2000,
                         help="Execute chunk of line sql in one transaction.")
    execute.add_argument('--interval', dest='interval', type=float, default=1.0,
                         help="Sleep time after execute chunk of line sql. set it to 0 if do not need sleep ")
    execute.add_argument('--reset', dest='reset', action='store_true', default=False,
                         help='Do not ignore committed line')
    execute.add_argument('--skip-error-regex', dest='skip_error_regex', type=str,
                         help='specify regex to skip some errors if the regex match the error msg.')

    alert = parser.add_argument_group('alert threads')
    alert.add_argument('--fei-shu-url', dest='fei_shu_url', type=str, nargs='*',
                       help='If execute fail, send a msg to fei shu group')
    alert.add_argument('--title', dest='title', type=str,
                       help='Send a msg to fei shu group with specify title')
    alert.add_argument('--at-all', dest='at_all', action='store_true', default=False,
                       help='Send to fei shu group with at all person?')
    alert.add_argument('--at-user-ids', dest='at_user_ids', type=str, nargs='*',
                       help='Send to fei shu group with at specify person id??')
    alert.add_argument('--test', dest='test', action='store_true', default=False,
                       help="Set to test mode, we won't send alert")

    action = parser.add_argument_group('action threads')
    action.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                        help='Never stop executed file or file in file dir if file increasing')
    action.add_argument('--sleep', dest='sleep', type=int, default=60,
                        help='When you use stop never options, we will sleep specify seconds after '
                             'finished every time.')
    action.add_argument('--delete-file', dest='delete_executed_file', action='store_true', default=False,
                        help='Delete SQL file after executed successfully')
    action.add_argument('--delete-record', dest='delete_not_exists_file_record', action='store_true',
                        help='Delete not exists file record in result file ', default=False)
    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)

    if not args.check:
        if not args.password:
            args.password = getpass.getpass()
        else:
            args.password = args.password[0]

    if args.file_path:
        for f in args.file_path:
            if not os.path.exists(f):
                logger.error(f'File {f} does not exists.')
                sys.exit(1)
    if args.file_dir and not os.path.isdir(args.file_dir):
        logger.error(f'File dir {args.file_dir} does not exists.')
        sys.exit(1)

    if args.sleep < 0:
        logger.error(f'Invalid value of sleep')
        sys.exit(1)
    return args


def connect2mysql(args):
    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        unix_socket=args.socket,
        user=args.user,
        password=args.password,
        db=args.database,
        charset=args.charset,
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn


def ts_now(is_millisecond: bool = False, is_microsecond: bool = False) -> int:
    """
    获取当前时间戳
    :param is_millisecond: 指定返回毫秒级时间戳
    :param is_microsecond: 指定返回微秒级时间戳
    :return 整形时间戳
    """
    if is_millisecond:
        return int(datetime.now().timestamp() * 1000)
    elif is_microsecond:
        return int(datetime.now().timestamp() * 1000 * 1000)
    else:
        return int(datetime.now().timestamp())


def ts_interval(ts1: int = 0, ts2: int = 0, ts_set: int = 0,
                is_millisecond: bool = False, is_microsecond: bool = False) -> str:
    """
    获取两个时间戳之间的查异，以格式化的方式返回
    :param ts1: 指定时间戳1
    :param ts2: 指定时间戳2
    :param ts_set: 指定传入计算好的时间间隔
    :param is_millisecond: 指定传入的时间戳为毫秒级时间戳
    :param is_microsecond: 指定传入的时间戳为微秒级时间戳
    :return 格式化的后的时间间隔
    """
    if ts1 and ts2:
        ts = abs(ts2 - ts1)
    elif ts_set:
        ts = ts_set
    else:
        ts = 0

    milliseconds = 0
    microseconds = 0
    if is_millisecond:
        milliseconds = ts % 1000
        ts /= 1000
    elif is_microsecond:
        milliseconds = ts % (1000 * 1000) // 1000
        microseconds = ts % 1000
        ts /= (1000 * 1000)

    days = int(ts // 86400)
    hours = int(ts // 3600 % 24)
    minutes = int(ts // 60 % 60)
    seconds = int(ts % 60)

    ts_str = ''
    if days:
        ts_str = '%s days %s hours %s minutes ' % (days, hours, minutes)
    elif hours:
        ts_str = '%s hours %s minutes ' % (hours, minutes)
    elif minutes:
        ts_str = '%s minutes ' % minutes
    ts_str += '%s seconds ' % seconds

    if is_millisecond:
        ts_str += '%s milliseconds' % milliseconds
    elif is_microsecond:
        ts_str += '%s milliseconds %s microseconds' % (milliseconds, microseconds)
    return ts_str


def file_handle(filename):
    if not os.path.exists(filename):
        logger.error(filename + " does not exists!!!")
        return ''

    with open(filename, 'r', encoding='utf8') as fh:
        for line in fh:
            yield line.strip().replace('\n', '')


def read_file(filename):
    if not os.path.exists(filename):
        logger.error(filename + " does not exists!!!")
        return ''

    with open(filename, 'r', encoding='utf8') as f:
        return json.loads(f.read())


def get_sql_file_list(args):
    file_list = []
    if args.file_dir and not args.file_path:
        for current_dir, sub_dir, files in os.walk(args.file_dir):
            for f in files:
                if args.start_file and f < args.start_file:
                    continue
                if args.stop_file and f > args.stop_file:
                    break
                if re.search(args.file_regex, f) is None and re.search(args.exclude_file_regex, f) is not None:
                    continue

                sql_file = os.path.join(current_dir, f)
                if int(time.time() - os.path.getmtime(sql_file)) < args.minutes_ago * 60:
                    continue
                file_list.append(sql_file)
    else:
        for f in args.file_path:
            if re.search(args.file_regex, f) is not None and re.search(args.exclude_file_regex, f) is None:
                if not f.startswith('/') and args.file_dir:
                    if not os.path.join(args.file_dir, f).startswith('.'):
                        sql_file = os.path.join(args.file_dir, f)
                    else:
                        sql_file = f
                else:
                    sql_file = f
                if int(time.time() - os.path.getmtime(sql_file)) < args.minutes_ago * 60:
                    continue
                file_list.append(sql_file)
    file_list.sort()
    return file_list


def get_log_format(args, sql_file):
    if args.socket:
        info_format = '[{socket}] [{db}] [{file}] '.format(
            socket=args.socket,
            db=args.database,
            file=sql_file
        )
    else:
        info_format = '[{host}] [{port}] [{db}] [{file}] '.format(
            host=args.host,
            port=args.port,
            db=args.database,
            file=sql_file
        )
    finished_info = info_format + 'finished'
    info_format += '[Read line count: {read_cnt}] [Committed line count: {cnt}]'
    base_format = '[%s] ' % sql_file
    return base_format, info_format, finished_info


def get_committed_cnt(args, sql_file):
    executed_result = read_file(args.result_file) if os.path.exists(args.result_file) else {}
    committed_cnt_read = executed_result.get(sql_file) if sql_file in executed_result else 0
    if args.reset:
        committed_cnt_read = 0
    return committed_cnt_read


def save_executed_result(result_file, sql_file, committed_cnt_save, is_finished,
                         delete_not_exists_file_record: bool = False):
    executed_result = read_file(result_file) if os.path.exists(result_file) else {}
    executed_result[sql_file] = committed_cnt_save
    if delete_not_exists_file_record and is_finished:
        for f in executed_result.copy().keys():
            if not os.path.exists(f):
                del executed_result[f]
    msg = json.dumps(executed_result, ensure_ascii=False, indent=4) + '\n'
    with open(result_file, 'w', encoding='utf8') as f:
        f.write(msg)
    return


def execute_sql_from_file(cursor, args, sql_file):
    if not os.path.exists(sql_file):
        logger.error(f'File {sql_file} does not exists.')
        return

    base_format, info_format, finished_info = get_log_format(args, sql_file)
    ignore_line_cnt = get_committed_cnt(args, sql_file)
    committed_cnt_save = ignore_line_cnt
    affected_rows = 0

    i = ignore_line_cnt
    line = ''
    is_finished = False

    if ignore_line_cnt and not args.stop_never:
        logger.warning(base_format + 'Ignore committed lines %s' % ignore_line_cnt)

    try:
        for i, line in enumerate(file_handle(sql_file)):
            if i < ignore_line_cnt != 0:
                continue

            if line == '':
                logger.warning(base_format + '[Ignore null content line: %s] %s' % (i + 1, line))
                ignore_line_cnt += 1
                continue

            line_split = line.split()
            if line_split:
                sql_type = line_split[0].upper()
                if sql_type not in ['INSERT', 'UPDATE', 'DELETE', 'REPLACE']:
                    logger.warning(base_format + '[Ignore line: %s] %s' % (i + 1, line))
                    ignore_line_cnt += 1
                    continue

            try:
                cursor.execute(line)
                affected_rows += cursor.rowcount
            except Exception as e:
                if args.skip_error_regex and re.search(args.skip_error_regex, str(e)) is not None:
                    pass
                else:
                    raise e

            if ((i + 1) % args.chunk) == 0:
                cursor.execute('commit')
                committed_cnt_save = i + 1
                logger.info(info_format.format(cnt=(i + 1 - ignore_line_cnt), read_cnt=i + 1) +
                            f' [Affected rows: {affected_rows}]')
                affected_rows = 0
                time.sleep(args.interval)
        else:
            cursor.execute('commit')
            committed_cnt_save = i + 1
            if not (args.stop_never and i + 1 - ignore_line_cnt == 0):
                logger.info(info_format.format(cnt=(i + 1 - ignore_line_cnt), read_cnt=i + 1) +
                            f' [Affected rows: {affected_rows}]')
                logger.info(finished_info)
            is_finished = True
            if args.delete_executed_file:
                os.remove(sql_file)
    except Exception as e:
        if args.test:
            logger.error(base_format + str(e))
        else:
            logger.exception(base_format + str(e))

        err_msg = base_format + '[Error line: %s] %s' % (i + 1, line)
        logger.error(err_msg)
        if not args.test and args.fei_shu_url and fei_shu_utils_object:
            logger.info('Sending alert...')
            for url in args.fei_shu_url:
                err_msg = f'【报错文件】：{base_format.replace("[", "").replace("]", "")}\n' \
                          f'【报错行数】: {i + 1}\n' \
                          f'【报错原因】：{e}\n' \
                          f'【具体内容】：{line}'
                fei_shu_utils_object.send_msg(url=url, msg=err_msg, title=args.title, is_at_all=args.at_all,
                                              at_user_id_list=args.at_user_ids)
    finally:
        save_executed_result(args.result_file, sql_file, committed_cnt_save, is_finished,
                             args.delete_not_exists_file_record)
    return


def main(args, execute_file_list):
    conn = connect2mysql(args)
    try:
        while True:
            with conn.cursor() as cursor:
                for sql_file in execute_file_list:
                    logger.info(f'Execute commands from file [{sql_file}]')
                    execute_sql_from_file(cursor, args, sql_file)
            if not args.stop_never:
                break
            time.sleep(args.sleep)
            execute_file_list = get_sql_file_list(args)
    finally:
        conn.close()
    return


if __name__ == "__main__":
    command_line_args = command_line_args(sys.argv[1:])
    add_file_handler()

    sql_file_list = get_sql_file_list(command_line_args)
    if command_line_args.check:
        from pprint import pprint

        pprint(sql_file_list)
        sys.exit(1)

    if not sql_file_list:
        logger.error(f'No sql files select or the sql files whose last modification time is '
                     f'less than {command_line_args.minutes_ago} minutes. Or you can add -ma 0 options. '
                     f'Or the file is not match file regex {command_line_args.file_regex}')
        if not command_line_args.stop_never:
            sys.exit(1)

    assert command_line_args.database, "No database select."

    ts_start = ts_now()
    main(command_line_args, sql_file_list)
    logger.info('Total used time: %s' % (ts_interval(ts_now(), ts_start)))
