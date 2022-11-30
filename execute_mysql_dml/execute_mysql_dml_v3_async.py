# !/usr/bin/env python3
# -*- coding:utf8 -*-

import argparse
import asyncio
import getpass
import json
import sys
import aiomysql
import os
import re
import time
import logging
import colorlog
import logging.handlers
from copy import deepcopy
from datetime import datetime

# 测试运行时：export PYTHONPATH=../utils/

base_dir = os.path.dirname(os.path.abspath(__file__))
sep = '/' if '/' in sys.argv[0] else os.sep
py_file_pre = sys.argv[0].split(sep)[-1].split('.')[0]

logger = logging.getLogger('execute_mysql_dml_v3_async')
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


class AttrDict(dict):
    """Dict that can get attribute by dot, and doesn't raise KeyError"""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            return None


class AttrDictCursor(aiomysql.DictCursor):
    dict_type = AttrDict


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
    sql_file.add_argument('--save', dest='result_file', type=str, default=committed_file,
                          help='file for save committed parts.')

    execute = parser.add_argument_group('execute method')
    execute.add_argument('--chunk', dest='chunk', type=int, default=2000,
                         help="Execute chunk of line sql in one transaction.")
    execute.add_argument('--interval', dest='interval', type=float, default=0.1,
                         help="Sleep time after execute chunk of line sql. set it to 0 if do not need sleep ")
    execute.add_argument('--reset', dest='reset', action='store_true', default=False,
                         help='Do not ignore committed line')
    execute.add_argument('--file-per-thread', dest='file_per_thread', action='store_true', default=False,
                         help="If set to true, we won't separate file part to execute sql, "
                              "unless you give more than one file. Only one thread per file.")
    execute.add_argument('--threads', dest='threads', type=int, default=3,
                         help="Only execute number of file part at the same time, "
                              "0 means execute all parts at the same time.")
    execute.add_argument('--no-limit', dest='no_limit', action='store_true', default=False,
                         help='Execute all parts at the same time, no limit for threads.'
                              'Warning: It will cause server overlay.')
    execute.add_argument('--skip-error-regex', dest='skip_error_regex', type=str,
                         help='specify regex to skip some errors if the regex match the error msg.')
    execute.add_argument('--save-per-commit', dest='save_per_commit', action='store_true', default=False,
                         help='Once commit one part, save it into result file. '
                              'If set to True, the execute time will be much longer.')
    execute.add_argument('--no-log-bin', dest='no_log_bin', action='store_true', default=False,
                         help='disable variable sql_log_bin when execute sql from file')

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


def get_sql_file_list(args):
    file_list = []
    if args.file_dir and not args.file_path:
        for current_dir, sub_dir, files in os.walk(args.file_dir):
            for f in files:
                if args.start_file and f < args.start_file:
                    continue
                if args.stop_file and f > args.stop_file:
                    break
                if (args.file_regex and re.search(args.file_regex, f) is None) or \
                        (args.exclude_file_regex and re.search(args.exclude_file_regex, f) is not None):
                    continue

                sql_file = os.path.join(current_dir, f)
                if int(time.time() - os.path.getmtime(sql_file)) < args.minutes_ago * 60:
                    continue
                file_list.append(sql_file)
    else:
        for f in args.file_path:
            if (args.file_regex and re.search(args.file_regex, f) is not None) and \
                    (args.exclude_file_regex and re.search(args.exclude_file_regex, f) is None):
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
    logger.info(f'Total file count: {len(file_list)}')
    return file_list


async def connect2mysql_async(loop, args):
    conn = await aiomysql.connect(
        host=args.host,
        port=args.port,
        unix_socket=args.socket,
        user=args.user,
        password=args.password,
        db=args.database,
        charset=args.charset,
        loop=loop,
        init_command='set sql_log_bin=0;' if args.no_log_bin else None,
    )
    return conn


async def read_file(filename):
    with open(filename, 'r', encoding='utf8') as f:
        return json.loads(f.read())


async def get_log_format(args, sql_file):
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
    base_format = '[%s] ' % sql_file
    return base_format, info_format, finished_info


async def send_alert(args, base_format, line_index, err_msg, line):
    logger.info('Sending alert...')
    for url in args.fei_shu_url:
        err_msg = f'【报错文件】：{base_format.replace("[", "").replace("]", "")}\n' \
                  f'【报错行数】: {line_index + 1}\n' \
                  f'【报错原因】：{err_msg}\n' \
                  f'【具体内容】：{line}'
        fei_shu_utils_object.send_msg(url=url, msg=err_msg, title=args.title, is_at_all=args.at_all,
                                      at_user_id_list=args.at_user_ids)


async def get_file_record_part_start_end(part):
    start_part = []
    end_part = []
    for value in part:
        if '-' in value:
            value_split = value.split('-')
            start_line = value_split[0]
            end_line = value_split[1]
        else:
            start_line = end_line = value
        start_part.append(start_line)
        end_part.append(end_line)
    return start_part, end_part


async def get_file_executed_record(args, sql_file):
    executed_result = await read_file(args.result_file) if os.path.exists(args.result_file) else {}
    committed_part = executed_result.get(sql_file, [])

    if args.reset:
        committed_part = []
        committed_part_start = []
        committed_part_end = []
    else:
        committed_part_start, committed_part_end = await get_file_record_part_start_end(committed_part)
    return committed_part, committed_part_start, committed_part_end


async def save_executed_result(result_file, sql_file, committed_part, delete_not_exists_file_record=False,
                               executed_all_parts=False):
    executed_result = await read_file(result_file) if os.path.exists(result_file) else {}
    executed_result[sql_file] = committed_part
    if delete_not_exists_file_record and executed_all_parts:
        for f in executed_result.copy().keys():
            if not os.path.exists(f):
                del executed_result[f]
    msg = json.dumps(executed_result, ensure_ascii=False, indent=4) + '\n'
    with open(result_file, 'w', encoding='utf8') as f:
        f.write(msg)
    return


def check_line_whether_executable(line, line_index, base_format, ignore_part_start, ignore_part_end,
                                  ignore_line_idx_list):
    for part_start, part_end in zip(ignore_part_start, ignore_part_end):
        if int(part_start) <= line_index <= int(part_end):
            return False

    if line == '':
        logger.warning(base_format + '[Ignore null content line: %s] %s' % (line_index, line))
        ignore_line_idx_list.append(line_index)
        return False

    sql_type = line.strip()[:7].strip().upper()
    if sql_type not in ['INSERT', 'UPDATE', 'DELETE', 'REPLACE']:
        logger.warning(base_format + '[Ignore line: %s] %s' % (line_index, line))
        ignore_line_idx_list.append(line_index)
        return False
    return True


def modify_idx_record_list(idx_record_list):
    tmp_list = []
    for i, idx_record in enumerate(idx_record_list):
        if i == 0:
            if isinstance(idx_record, int):
                start_idx = idx_record
                last_idx = idx_record
            elif isinstance(idx_record, str):
                idx_record_split = idx_record.split('-')
                start_idx = int(idx_record_split[0])
                last_idx = int(idx_record_split[-1])
            else:
                logger.error(idx_record)
            continue

        if isinstance(idx_record, int):
            if abs(idx_record - last_idx) != 1:
                tmp_record = f'{start_idx}-{last_idx}'
                tmp_list.append(tmp_record)
                start_idx = idx_record
            last_idx = idx_record
        elif isinstance(idx_record, str):
            idx_record_split = idx_record.split('-')
            idx_record_start = int(idx_record_split[0])
            idx_record_end = int(idx_record_split[-1])
            if abs(idx_record_start - last_idx) != 1:
                tmp_record = f'{start_idx}-{last_idx}'
                tmp_list.append(tmp_record)
                start_idx = idx_record_start
            last_idx = idx_record_end
        else:
            logger.error(f'{idx_record} is not index or index range')
    else:
        if idx_record_list:
            tmp_record = f'{start_idx}-{last_idx}'
            tmp_list.append(tmp_record)
    return list(set(tmp_list))


def file_handle(filename, base_format, committed_part, ignore_part_start, ignore_part_end, args):
    sql_list = []   # SQL 列表：用于保存可执行的 SQL
    sql_idx_list = []   # SQL 行数列表：用于保存可执行的 SQL 在原文件中的行数，报错时能准确知道错误 SQL 的所在行
    ignore_line_idx_list = []   # 被跳过的行数列表

    if committed_part and not args.stop_never and not args.reset:
        file_lines = "1-" + str(os.popen("wc -l " + filename + " | awk '{print $1}'").read().strip())
        if committed_part[0] == file_lines:
            logger.warning(f'File {filename} had been executed all line parts, skip it.')
            return sql_list, sql_idx_list
        else:
            logger.warning(base_format + 'Ignore committed line parts: %s' % committed_part)

    with open(filename, 'r', encoding='utf8') as fh:
        for idx, line in enumerate(fh):
            idx = idx + 1
            line = line.strip().replace('\n', '')

            executable = check_line_whether_executable(
                line, idx, base_format, ignore_part_start, ignore_part_end, ignore_line_idx_list
            )
            if not executable:
                continue

            sql_list.append(line)
            sql_idx_list.append(idx)

            if idx != 0 and idx % args.chunk == 0:
                yield sql_list, sql_idx_list
                sql_list = []
                sql_idx_list = []
        else:
            if sql_list != [] and sql_idx_list != []:
                yield sql_list, sql_idx_list
                sql_list = []

            yield sql_list, ignore_line_idx_list


async def execute_sql(sql_list, sql_idx_list, loop, args, base_format, info_format):
    is_finished = False
    affected_rows = 0
    conn = await connect2mysql_async(loop, args)
    cursor = await conn.cursor(AttrDictCursor)
    try:
        for sql, sql_idx in zip(sql_list, sql_idx_list):
            try:
                await cursor.execute(sql)
            except Exception as e:
                if args.skip_error_regex and re.search(args.skip_error_regex, str(e)) is not None:
                    pass
                elif 'Deadlock found when trying to get lock' in str(e) or \
                        'Lock wait timeout exceeded; try restarting transaction' in str(e):
                    while True:
                        try:
                            logger.warning('Deadlock found or lock wait timeout, try to restart transaction.')
                            await asyncio.sleep(1)
                            await cursor.execute(sql)
                            logger.info('Successfully executed retry transaction.')
                            break
                        except:
                            pass
                else:
                    raise e
            affected_rows += cursor.rowcount
        else:
            await cursor.execute('commit')
            committed_line_range = ",".join(modify_idx_record_list(sql_idx_list))
            logger.info(info_format + f'[Committed line range: {committed_line_range}] '
                                      f'[Affected rows: {affected_rows}]')
            is_finished = True
    except Exception as e:
        if args.test:
            logger.error(base_format + str(e))
        else:
            logger.exception(base_format + str(e))

        err_msg = base_format + '[Error line: %s] %s' % (sql_idx, sql)
        logger.error(err_msg)
        if not args.test and args.fei_shu_url and fei_shu_utils_object:
            await send_alert(args, base_format, sql_idx, e, sql)
    finally:
        await cursor.close()
        conn.close()
    return is_finished, sql_idx_list


def sort_start(key):
    if isinstance(key, str):
        key_split = key.split('-')
        return int(key_split[0])
    else:
        return key


async def execute_task(task, committed_part, unfinished_line_parts, args, sql_file):
    is_finished, sql_idx_list = await task
    if is_finished:
        committed_part += sql_idx_list
        if args.save_per_commit:
            committed_part.sort(key=sort_start)
            await save_executed_result(args.result_file, sql_file, modify_idx_record_list(committed_part))
    else:
        unfinished_line_parts.extend(modify_idx_record_list(sql_idx_list))


async def execute_sql_from_file(loop, args, sql_file):
    if not os.path.exists(sql_file):
        logger.error(f'File {sql_file} does not exists.')
        return

    logger.info(f'Execute commands from file [{sql_file}]')
    base_format, info_format, finished_info = await get_log_format(args, sql_file)
    committed_part, committed_part_start, committed_part_end = await get_file_executed_record(args, sql_file)
    tasks = []
    unfinished_line_parts = []
    executed_all_parts = False

    try:
        for sql_list, sql_idx_list in file_handle(sql_file, base_format, committed_part, deepcopy(committed_part_start),
                                                  deepcopy(committed_part_end), args):
            if sql_list:
                if args.no_limit:
                    tasks.append(
                        asyncio.create_task(
                            execute_sql(sql_list, sql_idx_list, loop, args, base_format, info_format))
                    )
                elif args.file_per_thread:
                    task = execute_sql(
                        sql_list, sql_idx_list, loop, args, base_format, info_format
                    )
                    await execute_task(task, committed_part, unfinished_line_parts, args, sql_file)
                    await asyncio.sleep(args.interval)
                else:
                    if len(tasks) < args.threads:
                        tasks.append(
                            asyncio.create_task(
                                execute_sql(sql_list, sql_idx_list, loop, args, base_format, info_format))
                        )
                    else:
                        for task in asyncio.as_completed(tasks):
                            await execute_task(task, committed_part, unfinished_line_parts, args, sql_file)
                        await asyncio.sleep(args.interval)
                        tasks = [asyncio.create_task(
                            execute_sql(sql_list, sql_idx_list, loop, args, base_format, info_format))]
            else:
                committed_part += sql_idx_list
        else:
            for task in asyncio.as_completed(tasks):
                await execute_task(task, committed_part, unfinished_line_parts, args, sql_file)

            if unfinished_line_parts:
                logger.error(info_format + f'Not all tasks finished, unfinished line parts: '
                                           f'[{",".join(unfinished_line_parts)}]')
            else:
                executed_all_parts = True
                logger.info(finished_info)
                if args.delete_executed_file and int(time.time() - os.path.getmtime(sql_file)) > 60:
                    os.remove(sql_file)
    finally:
        committed_part.sort(key=sort_start)
        committed_part = modify_idx_record_list(committed_part)
        await save_executed_result(args.result_file, sql_file, committed_part, args.delete_not_exists_file_record,
                                   executed_all_parts)
    return


async def main_work(loop, args, execute_file_list):
    while True:
        tasks = []
        for sql_file in execute_file_list:
            if args.no_limit:
                tasks.append(
                    asyncio.create_task(execute_sql_from_file(loop, args, sql_file))
                )
            elif args.file_per_thread:
                if len(tasks) < args.threads:
                    tasks.append(
                        asyncio.create_task(execute_sql_from_file(loop, args, sql_file))
                    )
                else:
                    for task in asyncio.as_completed(tasks):
                        await task
                    await asyncio.sleep(args.interval)
                    tasks.append(
                        asyncio.create_task(execute_sql_from_file(loop, args, sql_file))
                    )
            else:
                await execute_sql_from_file(loop, args, sql_file)
        else:
            for task in asyncio.as_completed(tasks):
                await task

        if not args.stop_never:
            break
        # await asyncio.sleep(args.sleep)
        time.sleep(args.sleep)
        execute_file_list = get_sql_file_list(args)


def main(args, execute_file_list):
    ts_start = ts_now()
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main_work(loop, args, execute_file_list))
    finally:
        logger.info('Total used time: %s' % (ts_interval(ts_now(), ts_start)))
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
    main(command_line_args, sql_file_list)
