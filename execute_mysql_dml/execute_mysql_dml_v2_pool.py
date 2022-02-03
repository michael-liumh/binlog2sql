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
from urllib import parse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime
if sys.platform == 'linux':
    from signal import signal, SIGHUP, SIGTERM
# 测试运行时：export PYTHONPATH=../utils/

base_dir = os.path.dirname(os.path.abspath(__file__))
sep = '/' if '/' in sys.argv[0] else os.sep
py_file_pre = sys.argv[0].split(sep)[-1].split('.')[0]

logger = logging.getLogger('execute_mysql_dml_v2_pool')
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


tmp_result_file = ''
tmp_sql_file = ''
tmp_committed_cnt_save = 0
tmp_is_finished = False
tmp_delete_not_exists_file_record = False


def exit_handler(sig, frame):
    logger.info('Got exit signal, auto save committed count in result file.')
    save_executed_result(tmp_result_file, tmp_sql_file, tmp_committed_cnt_save, tmp_is_finished,
                         tmp_delete_not_exists_file_record)
    sys.exit(0)


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

    pool = parser.add_argument_group('multi threads')
    pool.add_argument('--pool', dest='pool', action='store_true', default=False,
                      help='Use multi threads?')
    pool.add_argument('--threads', dest='threads', type=int, default=5,
                      help="When you use multi threads, set num of threads for execute sql.")
    pool.add_argument('--exit', dest='exit', action='store_true', default=False,
                      help="Exit when failed to execute sql.")

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


def connect2mysql_pool(args):
    conn_str = "mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset={charset}".format(
        user=args.user,
        password=parse.quote_plus(args.password),
        host=args.host,
        port=args.port,
        db=args.database,
        charset=args.charset,
    )
    engine = create_engine(
        conn_str,
        max_overflow=5,  # 超过连接池大小外最多创建的连接
        pool_size=args.threads,  # 连接池大小
        pool_timeout=600,  # 池中没有线程最多等待的时间，否则报错
        pool_recycle=1,  # 多久之后对线程池中的线程进行一次连接的回收（重置）
    )
    session_factory = sessionmaker(bind=engine)
    session = scoped_session(session_factory)
    return session


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
    return executed_result, committed_cnt_read


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


def fix_json_col(col_list):
    """
        本函数存在原因是拆分 SQL 的时候，根据空格或者拆分时，可能会把 JSON 字段给拆分成 2 段或多段，这里将它还原成 1 段
    :param col_list:
    :return:
    """

    # 左括号数量 与 右括号数量
    json_mark_left_cnt = 0
    json_mark_right_cnt = 0
    json_col = ''
    col_list_new = []
    for col in col_list:
        if isinstance(col, str):
            col = col.strip()

        if re.search('{', col) is not None:
            json_mark_left_cnt += col.count('{')
            if re.search('}', col) is not None:
                json_mark_right_cnt += col.count('}')
            json_col += col + ','
            if json_mark_left_cnt == json_mark_right_cnt and "}'" in col:
                col_list_new.append(json_col[:-1])
                json_col = ''
                json_mark_left_cnt = json_mark_right_cnt = 0
        elif json_mark_left_cnt != json_mark_right_cnt:
            if re.search('}', col) is not None:
                json_mark_right_cnt += col.count('}')
            json_col += col + ','
            if (json_mark_left_cnt == json_mark_right_cnt and "}'" in col) or "}'" in col:
                col_list_new.append(json_col[:-1])
                json_col = ''
                json_mark_left_cnt = json_mark_right_cnt = 0
        elif json_col != '':
            if '`=' not in col:
                json_col += col + ','
            else:
                # 将误判定为 json 的字段加回去
                col_list_new.append(json_col[:-1])
                json_col = ''
                json_mark_left_cnt = json_mark_right_cnt = 0
                col_list_new.append(col)

            if "}'" in col:
                col_list_new.append(json_col[:-1])
                json_col = ''
                json_mark_left_cnt = json_mark_right_cnt = 0
        else:
            col_list_new.append(col)
    return col_list_new


def get_hex_value(value: str, base_format: str = ''):
    if not isinstance(value, str):
        logger.error(base_format + '%s is not string' % value)
        return value

    if "'" in value:
        left_mark_idx = value.find("'")
        right_mark_idx = value.rfind("'")
    else:
        left_mark_idx = value.find('"')
        right_mark_idx = value.rfind('"')
    value = '0x' + value[left_mark_idx + 1:right_mark_idx].encode('utf8').hex().upper()
    return value


def col_list_to_dict(col_list, base_format: str):
    col_dict = {}
    others = []
    err_set = 0
    for col in col_list:
        if '=' in col:
            sep = '='
            col_split = col.split('=')
            key = col_split[0]
            value = sep.join(col_split[1:])
        elif ' IS NULL' in col and len(col.split()) == 3:
            sep = ' IS '
            col_split = col.split()
            key = col_split[0]
            value = "".join(col_split[2:])
        else:
            others.append(col)
            key = ''
            value = ''
            sep = ''
        key_strip = key.strip()
        if key_strip and key_strip not in col_dict:
            col_dict[key_strip] = {
                'key': key_strip,
                'sep': sep,
                'value': value.strip(),
            }
    if others:
        logger.error(base_format + 'Could not convert list to dict. col_list: %s, other part: %s'
                     % (col_list, others))
        err_set = 1
    return col_dict, err_set


def get_where_col_list(where_col_part):
    # where 条件没有根据逗号进行分割，所以无需调用 fix_json_col
    if 'AND' in where_col_part:
        where_col_list = list(map(lambda s: s.strip(), where_col_part.split(' AND ')))
    elif 'and' in where_col_part:
        where_col_list = list(map(lambda s: s.strip(), where_col_part.split(' and ')))
    else:
        where_col_list = [where_col_part]

    if 'LIMIT' in where_col_list[-1]:
        limit_idx = where_col_list[-1].find('LIMIT')
    else:
        limit_idx = where_col_list[-1].find('limit')

    if limit_idx != -1:
        limit_part = ' ' + where_col_list[-1][limit_idx:].strip().upper()
        where_col_list[-1] = where_col_list[-1][:limit_idx].strip()
    else:
        limit_part = ''

    return where_col_list, limit_part


def fix_insert_sql(sql: str, base_format: str):
    value_part_list_new = []
    left_mark_idx = sql.upper().find('VALUES (')
    right_mask_idx = sql.rfind(')')
    value_part_list = sql[left_mark_idx + 8:right_mask_idx].split(',')

    if '{' in sql:
        value_part_list = fix_json_col(value_part_list)

    for value_part in value_part_list:
        if ':' in value_part:
            value_part = get_hex_value(value_part, base_format)
        value_part_list_new.append(value_part)

    new_sql = sql[:left_mark_idx + 8] + ', '.join(value_part_list_new) + sql[right_mask_idx:]
    return new_sql


def fix_update_sql(sql: str, base_format: str) -> str:
    if 'WHERE' in sql:
        sql_split = sql.split('WHERE')
    elif 'where' in sql:
        sql_split = sql.split('where')
    else:
        logger.error(base_format + 'Could not find [where] keyword in sql %s' % sql)
        return sql

    update_col_part = sql_split[0]
    where_col_part = sql_split[1]

    if 'SET' in update_col_part:
        begin_idx = update_col_part.find('SET')
    elif 'set' in update_col_part:
        begin_idx = update_col_part.find('set')
    else:
        logger.error(base_format + 'Could not find [set] keyword in sql %s' % sql)
        return sql

    update_prefix = update_col_part[:begin_idx + 3]
    update_suffix = update_col_part[begin_idx + 3:]
    update_col_list = update_suffix.split(', `')
    if "{" in str(update_col_list):
        update_col_list = fix_json_col(update_col_list)

    update_col_dict, err_set = col_list_to_dict(update_col_list, base_format)
    if err_set == 1:
        return sql

    where_col_list, limit_part = get_where_col_list(where_col_part)
    where_col_dict, err_set = col_list_to_dict(where_col_list, base_format)
    if err_set == 1:
        return sql

    update_col_list_new, where_col_list_new = [], []
    for key, new_value in update_col_dict.items():
        if ':' in new_value['value']:
            new_value['value'] = get_hex_value(new_value['value'], base_format)

        old_value = where_col_dict.get(key, {})
        if old_value and ':' in old_value['value']:
            old_value['value'] = get_hex_value(old_value['value'], base_format)

        if key not in update_col_list_new:
            update_col_list_new.append(new_value['sep'].join([key, new_value['value']]))

        if old_value and key not in where_col_list_new:
            where_col_list_new.append(old_value['sep'].join([key, old_value['value']]))

    new_sql = "".join(update_prefix) + ' ' + ', `'.join(update_col_list_new) + \
              ' WHERE ' + ' AND '.join(where_col_list_new) + limit_part
    return new_sql


def fix_delete_sql(sql: str, base_format: str) -> str:
    if 'WHERE' in sql:
        sql_split = sql.split('WHERE')
    elif 'where' in sql:
        sql_split = sql.split('where')
    else:
        logger.error(base_format + 'Could not find [where] keyword in sql %s' % sql)
        return sql

    where_col_list, limit_part = get_where_col_list(sql_split[1])
    where_col_dict, err_set = col_list_to_dict(where_col_list, base_format)
    if err_set == 1:
        return sql

    where_col_list_new = []
    for key, value in where_col_dict.items():
        if ':' in value['value']:
            value['value'] = get_hex_value(value['value'], base_format)

        if key not in where_col_list_new:
            where_col_list_new.append(value['sep'].join([key, value['value']]))

    new_sql = "".join(sql_split[0]) + 'WHERE ' + ' AND '.join(where_col_list_new) + limit_part
    return new_sql


def fix_invalid_sql(sql: str, base_format: str):
    if sql.upper().startswith('INSERT') or sql.upper().startswith('REPLACE'):
        new_sql = fix_insert_sql(sql, base_format)
    elif sql.upper().startswith('UPDATE'):
        new_sql = fix_update_sql(sql, base_format)
    elif sql.upper().startswith('DELETE'):
        new_sql = fix_delete_sql(sql, base_format)
    else:
        new_sql = sql
    return new_sql


def execute_sql_from_file(cursor, args, sql_file):
    if not os.path.exists(sql_file):
        logger.error(f'File {sql_file} does not exists.')
        if args.pool:
            cursor.remove()
        return

    global tmp_result_file, tmp_sql_file, tmp_committed_cnt_save, tmp_is_finished, tmp_delete_not_exists_file_record

    # if not args.pool:
    tmp_result_file = args.result_file
    tmp_sql_file = sql_file
    tmp_delete_not_exists_file_record = args.delete_not_exists_file_record

    base_format, info_format, finished_info = get_log_format(args, sql_file)
    executed_result, ignore_line_cnt = get_committed_cnt(args, sql_file)
    tmp_committed_cnt_save = committed_cnt_save = ignore_line_cnt
    affected_rows = 0
    # if not args.pool:
    #     tmp_committed_cnt_save = committed_cnt_save

    i = ignore_line_cnt
    line = ''
    is_finished = False
    retry_set = 0
    err_set = 0

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

            skip_error_flag = False
            try:
                cursor.execute(line)
            except Exception as e:
                # 特殊处理
                if args.pool and ':' in line:
                    try:
                        cursor.execute(text(line.replace(':', '\\:')))
                    except:
                        line = fix_invalid_sql(line, base_format)
                        try:
                            cursor.execute(line)
                        except:
                            if '`=0x' in line:
                                begin_idx = line.find('`=0x')
                                while begin_idx != -1:
                                    begin_idx += + len('`=')
                                    end_idx = line.find(', ', begin_idx)
                                    line = line[:begin_idx] + "'" + line[begin_idx: end_idx] + "'" + line[end_idx:]
                                    begin_idx = line.find('`=0x', end_idx)
                            try:
                                cursor.execute(line)
                            except Exception as e:
                                if args.skip_error_regex and re.search(args.skip_error_regex, str(e)) is not None:
                                    skip_error_flag = True
                                    pass
                                else:
                                    raise e
                elif args.skip_error_regex and re.search(args.skip_error_regex, str(e)) is not None:
                    skip_error_flag = True
                    pass
                else:
                    raise e

            if not args.pool and not skip_error_flag:
                affected_rows += cursor.rowcount

            if ((i + 1) % args.chunk) == 0:
                cursor.execute('commit')
                committed_cnt_save = i + 1
                if not args.pool:
                    tmp_committed_cnt_save = committed_cnt_save
                    logger.info(info_format.format(cnt=(i + 1 - ignore_line_cnt), read_cnt=i + 1) +
                                f' [Affected rows: {affected_rows}]')
                    affected_rows = 0
                else:
                    logger.info(info_format.format(cnt=(i + 1 - ignore_line_cnt), read_cnt=i + 1))
                time.sleep(args.interval)
        else:
            cursor.execute('commit')
            committed_cnt_save = i + 1
            if not args.pool:
                tmp_committed_cnt_save = committed_cnt_save
            if not (args.stop_never and i + 1 - ignore_line_cnt == 0):
                if not args.pool:
                    logger.info(info_format.format(cnt=(i + 1 - ignore_line_cnt), read_cnt=i + 1) +
                                f' [Affected rows: {affected_rows}]')
                else:
                    logger.info(info_format.format(cnt=(i + 1 - ignore_line_cnt), read_cnt=i + 1))
                logger.info(finished_info)
            is_finished = True
            tmp_is_finished = True
            if args.delete_executed_file:
                os.remove(sql_file)
    except pymysql.err.OperationalError:
        logger.exception('Detect Database Connect Timeout Error, now set the retry flag to 1.')
        retry_set = 1
        err_set = 1
    except pymysql.err.InterfaceError:
        logger.exception('Detect SQL Execute Timeout Error, now set the retry flag to 1.')
        retry_set = 1
        err_set = 1
    except Exception as e:
        err_set = 1
        if args.test:
            logger.error(base_format + str(e))
        else:
            logger.exception(base_format + str(e))

        if not args.pool:
            err_msg = base_format + '[Error line: %s] %s' % (i + 1, line)
        else:
            err_msg = base_format + '[Error line: %s]' % (i + 1)

        logger.error(err_msg)
        if not args.test and args.fei_shu_url and fei_shu_utils_object and retry_set == 0:
            logger.info('Sending alert...')
            for url in args.fei_shu_url:
                if url == 'https://open.feishu.cn/open-apis/bot/v2/hook/fdb483f9-f3db-45e6-89e9-5ed0e0869b4a':
                    err_msg = f'【报错文件】：{base_format.replace("[", "").replace("]", "")}\n' \
                              f'【报错行数】: {i + 1}\n' \
                              f'【报错原因】：{e}\n' \
                              f'【具体内容】：{line}'
                else:
                    err_msg = f'【报错文件】：{base_format.strip("[").strip("]")}\n' \
                              f'【报错行数】: {i + 1}\n' \
                              f'【报错原因】：{e}'
                fei_shu_utils_object.send_msg(url=url, msg=err_msg, title=args.title, is_at_all=args.at_all,
                                              at_user_id_list=args.at_user_ids)
    finally:
        save_executed_result(args.result_file, sql_file, committed_cnt_save, is_finished,
                             args.delete_not_exists_file_record)
        if retry_set == 1:
            logger.info('Detect retry flag is set, now try to re-run the process')
            if not args.pool:
                conn = connect2mysql(args)
                cursor = conn.cursor()
            else:
                cursor = connect2mysql_pool(args)
            execute_sql_from_file(cursor, args, sql_file)

        if args.pool:
            cursor.remove()

        if args.exit and err_set == 1:
            logger.error('Exit process!')
            os.popen(f'kill {os.getpid()}')
            sys.exit(1)
    return


def execute_missing_file(session, args, execute_file_list):
    pool = ThreadPool(args.threads)
    execute_sql_from_file_func = partial(execute_sql_from_file, session, args)
    pool.map_async(execute_sql_from_file_func, set(execute_file_list))
    pool.close()
    pool.join()
    return


def main(args, execute_file_list):
    if not args.pool:
        conn = connect2mysql(args)
    else:
        conn = connect2mysql_pool(args)

    try:
        while True:
            if not args.pool:
                with conn.cursor() as cursor:
                    for sql_file in execute_file_list:
                        logger.info(f'Execute commands from file [{sql_file}]')
                        execute_sql_from_file(cursor, args, sql_file)
            else:
                execute_missing_file(conn, args, execute_file_list)

                # execute missing file
                while True:
                    executed_file = read_file(args.result_file)
                    missing_files = []
                    for f in execute_file_list:
                        if f not in list(executed_file.keys()):
                            logger.warning('%s is missing. we will re-run script to execute this file.' % f)
                            missing_files.append(f)

                    if missing_files:
                        execute_missing_file(conn, args, missing_files)
                    else:
                        break
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
    if (command_line_args.stop_never and not command_line_args.pool) or command_line_args.pool:
        signal(SIGHUP, exit_handler)
        signal(SIGTERM, exit_handler)

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
