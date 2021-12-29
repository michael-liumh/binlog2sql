#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import argparse
import datetime
import getpass
import json
import logging
from functools import partial

import chardet
from contextlib import contextmanager
import colorlog
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)

if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False

# create a logger
base_dir = os.path.dirname(os.path.abspath(__file__))
sep = '/' if '/' in sys.argv[0] else os.sep
py_file_pre = sys.argv[0].split(sep)[-1].split('.')[0]
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set logger color
log_colors_config = {
    'DEBUG': 'bold_purple',
    'INFO': 'bold_green',
    'WARNING': 'bold_yellow',
    'ERROR': 'bold_red',
    'CRITICAL': 'red',
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


def set_log_format():
    import logging.handlers

    global logger

    # set logger format
    logfile_format = logging.Formatter(
        "[%(asctime)s] [%(module)s:%(funcName)s] [%(lineno)d] [%(levelname)s] %(message)s"
    )

    # add rotate file handler
    logs_dir = os.path.join(base_dir, 'logs')
    if not os.path.isdir(logs_dir):
        os.makedirs(logs_dir, exist_ok=True)

    sep = '/' if '/' in sys.argv[0] else os.sep
    logfile = logs_dir + sep + sys.argv[0].split(sep)[-1].split('.')[0] + '.log'
    file_maxsize = 1024 * 1024 * 100  # 100m

    file_handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=file_maxsize, backupCount=10)
    file_handler.setFormatter(logfile_format)
    logger.addHandler(file_handler)


def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False


def create_unique_file(filename, path=None):
    version = 0
    result_file = filename
    # if we have to try more than 1000 times, something is seriously wrong
    while os.path.exists(result_file) and version < 1000:
        result_file = filename + '.' + str(version)
        version += 1
    if version >= 1000:
        raise OSError('cannot create unique file %s.[0-1000]' % filename)
    if path:
        result_file = os.path.join(path, result_file)
    return result_file


@contextmanager
def temp_open(filename, mode):
    f = open(filename, mode)
    try:
        yield f
    finally:
        f.close()
        os.remove(filename)


def parse_args():
    """parse args for binlog2sql"""

    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str, nargs='*',
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)

    interval = parser.add_argument_group('interval filter')
    interval.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    interval.add_argument('--start-position', '--start-pos', dest='start_pos', type=int,
                          help='Start position of the --start-file', default=4)
    interval.add_argument('--stop-file', '--end-file', dest='end_file', type=str,
                          help="Stop binlog file to be parsed. default: '--start-file'", default='')
    interval.add_argument('--stop-position', '--end-pos', dest='end_pos', type=int,
                          help="Stop position. default: latest position of '--stop-file'", default=0)
    interval.add_argument('--start-datetime', dest='start_time', type=str,
                          help="Start time. format %%Y-%%m-%%d %%H:%%M:%%S", default='')
    interval.add_argument('--stop-datetime', dest='stop_time', type=str,
                          help="Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;", default='')
    interval.add_argument('--include-gtids', dest='include_gtids', type=str,
                          help="Include Gtids. format @server_uuid:1-10[:20-30][:...]", default='')
    interval.add_argument('--exclude-gtids', dest='exclude_gtids', type=str,
                          help="Exclude Gtids. format @server_uuid:1-10[:20-30][:...]", default='')

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-id', '--ignore-databases', dest='ignore_databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')
    schema.add_argument('-it', '--ignore-tables', dest='ignore_tables', type=str, nargs='*',
                        help='tables you want to ignore', default='')
    schema.add_argument('-ic', '--ignore-columns', dest='ignore_columns', type=str, nargs='*',
                        help='columns you want to ignore', default='')

    event = parser.add_argument_group('type filter')
    event.add_argument('--only-dml', dest='only_dml', action='store_true', default=False,
                       help='only print dml, ignore ddl')
    event.add_argument('--sql-type', dest='sql_type', type=str, nargs='*', default=['INSERT', 'UPDATE', 'DELETE'],
                       help='Sql type you want to process, support INSERT, UPDATE, DELETE.')
    event.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                       help="Continuously parse binlog. default: stop at the latest event when you start.")

    # exclusive = parser.add_mutually_exclusive_group()
    event.add_argument('-K', '--no-primary-key', dest='no_pk', action='store_true',
                       help='Generate insert sql without primary key if exists', default=False)
    event.add_argument('-KK', '--only-primary-key', dest='only_pk', action='store_true', default=False,
                       help='Only key primary key condition when sql type is UPDATE and DELETE')
    event.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                       help='Flashback data to start_position of start_file', default=False)
    event.add_argument('--back-interval', dest='back_interval', type=float, default=1.0,
                       help="Sleep time between chunks of 1000 rollback sql. set it to 0 if do not need sleep")
    event.add_argument('--replace', dest='replace', action='store_true',
                       help='Use REPLACE INTO instead of INSERT INTO.', default=False)
    event.add_argument('--insert-ignore', dest='insert_ignore', action='store_true',
                       help='Insert rows with INSERT IGNORE.', default=False)

    result = parser.add_argument_group('result filter')
    result.add_argument('--need-comment', dest='need_comment', type=int, default=1,
                        help='Choice need comment like [#start 268435860 end 268436724 time 2021-12-01 16:40:16] '
                             'or not, 0 means not need, 1 means need')
    result.add_argument('--rename-db', dest='rename_db', type=str,
                        help='Rename source dbs to one db.')
    result.add_argument('--remove-not-update-col', dest='remove_not_update_col', action='store_true', default=False,
                        help='If set, we will remove not update column in update statements (exclude primary key)')
    result.add_argument('--result-file', dest='result_file', type=str,
                        help='If set, we will save result sql in this file instead print into stdout.'
                             '(Tip: we will ignore path if give a result file with relative path or absolute path,'
                             'please use --result-dir to set path)')
    result.add_argument('--result-dir', dest='result_dir', type=str, default='./',
                        help='Give a dir to save result_file.')
    result.add_argument('--table-per-file', dest='table_per_file', action='store_true', default=False,
                        help='If set, we will save result sql in table per file instead of result file')

    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)

    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)

    if args.result_file and args.table_per_file:
        logger.error('Could not use --result-file and --table-per-file at the same time.')
        sys.exit(1)

    if args.result_file and sep in args.result_file:
        logger.warning('we will ignore path if give a result file with relative path or absolute path, '
                       'please use --result-dir to set path.')

    if not args.start_file:
        raise ValueError('Lack of parameter: start_file')
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    if (args.start_time and not is_valid_datetime(args.start_time)) or \
            (args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.password:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]

    if args.result_file and not os.path.exists(args.result_dir):
        os.makedirs(args.result_dir, exist_ok=True)
    args.result_file = os.path.join(args.result_dir, args.result_file.split(sep)[-1]) if args.result_file else \
        args.result_file
    return args


def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def fix_object_bytes(value: bytes, is_bytes_column: bool = True):
    if is_bytes_column:
        value = '0x' + value.hex().upper()
        return value

    try:
        encoding = chardet.detect(value).get('encoding', '')
        if not encoding:
            encoding = 'utf8'
        return value.decode(encoding)
    except Exception:
        value = '0x' + value.hex().upper()
        return value


def fix_object_array(value: list):
    new_list = []
    for v in value:
        # list里可能同时存在string、bytes(划重点)、array、json
        if isinstance(v, bytes):
            v = fix_object_bytes(v, False)
        elif isinstance(v, list):
            v = fix_object_array(v)
        elif isinstance(v, dict):
            v = fix_object_json(v)

        # string直接原封不动存储
        new_list.append(v)
    return new_list


def fix_object_json(value: dict):
    new_dict = {}
    for k, v in value.items():
        # json内部 key 可能是字符串或bytes，如果是bytes，则跳转到bytes解析
        if isinstance(k, bytes):
            k = fix_object_bytes(k, False)

        # json内部的 value 则多种多样，可能为字符串、bytes(划重点)、array、json
        if isinstance(v, bytes):
            v = fix_object_bytes(v, False)
        elif isinstance(v, list):
            v = fix_object_array(v)
        elif isinstance(v, dict):
            v = fix_object_json(v)

        # 字符串直接赋值即可
        new_dict[k] = v
    return new_dict


def fix_object(value, is_return_type: bool = False):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if is_return_type:
        return type(value)

    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return fix_object_bytes(value)
    # 添加json数据解析
    elif PY3PLUS and isinstance(value, dict):
        return fix_object_json(value)
    # json里的数组解析
    elif PY3PLUS and isinstance(value, list):
        return fix_object_array(value)
    # python2 unicode
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


def is_dml_event(event):
    if isinstance(event, WriteRowsEvent) or isinstance(event, UpdateRowsEvent) or isinstance(event, DeleteRowsEvent):
        return True
    else:
        return False


def event_type(event):
    t = None
    if isinstance(event, WriteRowsEvent):
        t = 'INSERT'
    elif isinstance(event, UpdateRowsEvent):
        t = 'UPDATE'
    elif isinstance(event, DeleteRowsEvent):
        t = 'DELETE'
    return t


def handle_list(value: list):
    new_list = []
    for v in value:
        if isinstance(v, dict) or isinstance(v, list):
            try:
                v = json.dumps(v, ensure_ascii=False)
            except Exception as e:
                logger.error("Failed to dump dict or list value to string. Error is:" + str(e))
                logger.error("Error value is:" + str(v))
                sys.exit(1)
        new_list.append(v)
    return new_list


def fix_hex_values(sql: str, values: list, types: list):
    begin = 0
    new_sql = ''
    while sql.find("'0x", begin) > 0:
        # 拿第1个引号的下标
        quote_begin_idx = sql.find("'0x", begin)
        # 拿第2个引号的下标
        quote_end_idx = sql.find("'", quote_begin_idx + 1)
        # 获取 0x 开头的值
        quote_value = sql[quote_begin_idx + 1:quote_end_idx]

        # 确认 0x 开头的值是十六进制的值，还是字符串
        quote_value_idx = values.index(quote_value)
        quote_value_type = types[quote_value_idx]
        if quote_value_type == str:
            # 保留原样
            new_sql += sql[begin:quote_end_idx + 1]
        else:
            # 去除 十六进制值 前后的引号
            new_sql += sql[begin:quote_begin_idx] + sql[quote_begin_idx + 1:quote_end_idx]

        begin = quote_end_idx + 1

    if new_sql:
        new_sql += sql[quote_end_idx + 1:]

    return new_sql


def concat_sql_from_binlog_event(cursor, binlog_event, row=None, e_start_pos=None, flashback=False, no_pk=False,
                                 rename_db=None, only_pk=False, only_return_sql=True, ignore_columns=None,
                                 replace=False, insert_ignore=False, ignore_virtual_columns=False,
                                 remove_not_update_col=False, binlog_gtid=None):
    if flashback and no_pk:
        raise ValueError('only one of flashback or no_pk can be True')
    if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent)
            or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
        raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = ''
    db = ''
    table = ''
    if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) \
            or isinstance(binlog_event, DeleteRowsEvent):
        # 会调用 fix_object 函数生成sql
        (pattern, db, table), types = generate_sql_pattern(
            binlog_event, row=row, flashback=flashback, no_pk=no_pk, rename_db=rename_db, only_pk=only_pk,
            ignore_columns=ignore_columns, replace=replace, insert_ignore=insert_ignore, return_type=True,
            ignore_virtual_columns=ignore_virtual_columns, remove_not_update_col=remove_not_update_col
        )

        # cursor.mogrify 处理 value 时，会返回一个字符串，如果 value 里包含 dict，则会报错
        if isinstance(pattern['values'], list):
            pattern_values = handle_list(pattern['values'])
        else:
            pattern_values = pattern['values']
        sql = cursor.mogrify(pattern['template'], pattern_values)
        if "'0x" in str(sql):
            sql = fix_hex_values(sql, pattern_values, types)
        time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
        sql += ' #start %s end %s time %s' % (e_start_pos, binlog_event.packet.log_pos, time)
        if binlog_gtid:
            sql += ' gtid %s' % binlog_gtid
    elif flashback is False and isinstance(binlog_event, QueryEvent) and binlog_event.query != 'BEGIN' \
            and binlog_event.query != 'COMMIT':
        if binlog_event.schema:
            if isinstance(binlog_event.schema, bytes):
                sql = 'USE {0};\n'.format(binlog_event.schema.decode('utf8'))
            else:
                sql = 'USE {0};\n'.format(binlog_event.schema)
        sql += '{0};'.format(fix_object(binlog_event.query))

    if not only_return_sql:
        return sql, db, table
    else:
        return sql


def generate_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False, rename_db=None, only_pk=False,
                         ignore_columns=None, replace=False, insert_ignore=False, ignore_virtual_columns=False,
                         remove_not_update_col=False, return_type=False):
    if ignore_columns and is_dml_event(binlog_event):
        if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, DeleteRowsEvent):
            for k in row['values'].copy():
                if k in ignore_columns:
                    row['values'].pop(k)
        else:
            for k in row['before_values'].copy():
                if k in ignore_columns:
                    row['before_values'].pop(k)
            for k in row['after_values'].copy():
                if k in ignore_columns:
                    row['after_values'].pop(k)
    elif ignore_virtual_columns and is_dml_event(binlog_event):
        if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, DeleteRowsEvent):
            for k in row['values'].copy():
                if re.search('__dropped_col_\d+__', k) is not None:
                    row['values'].pop(k)
        else:
            for k in row['before_values'].copy():
                if re.search('__dropped_col_\d+__', k) is not None:
                    row['before_values'].pop(k)
            for k in row['after_values'].copy():
                if re.search('__dropped_col_\d+__', k) is not None:
                    row['after_values'].pop(k)

    if remove_not_update_col and isinstance(binlog_event, UpdateRowsEvent):
        for k, old_v in row['before_values'].copy().items():
            new_v = row['after_values'].copy().get(k)
            if old_v == new_v:
                if k == binlog_event.primary_key:
                    row['after_values'].pop(k)
                else:
                    row['before_values'].pop(k)
                    row['after_values'].pop(k)

    template = ''
    values = []
    types = []
    db = binlog_event.schema
    table = binlog_event.table
    fix_object_new = partial(fix_object, is_return_type=True)
    if flashback is True:
        if isinstance(binlog_event, WriteRowsEvent):
            db = rename_db if rename_db else binlog_event.schema
            if not only_pk:
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                    db, binlog_event.table,
                    ' AND '.join(map(compare_items, row['values'].items()))
                )
                values = map(fix_object, row['values'].values())
                types = map(fix_object_new, row['values'].values())
            else:
                pk_item = {
                    binlog_event.primary_key: row['values'].get(binlog_event.primary_key)
                }
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                    db, binlog_event.table,
                    ' AND '.join(map(compare_items, pk_item.items()))
                )
                values = map(fix_object, pk_item.values())
                types = map(fix_object_new, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            db = rename_db if rename_db else binlog_event.schema
            if replace:
                template = 'REPLACE INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    db, binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
            elif insert_ignore:
                template = 'INSERT IGNORE INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    db, binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
            else:
                template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    db, binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
            values = map(fix_object, row['values'].values())
            types = map(fix_object_new, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            db = rename_db if rename_db else binlog_event.schema
            if not only_pk:
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    db, binlog_event.table,
                    ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                    ' AND '.join(map(compare_items, row['after_values'].items())))
                values = map(fix_object, list(row['before_values'].values()) + list(row['after_values'].values()))
            else:
                pk_item = {
                    binlog_event.primary_key: row['after_values'].get(binlog_event.primary_key)
                }
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    db, binlog_event.table,
                    ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                    ' AND '.join(map(compare_items, pk_item.items())))
                values = map(fix_object, list(row['before_values'].values()) + list(pk_item.values()))
                types = map(fix_object_new, list(row['before_values'].values()) + list(pk_item.values()))
    else:
        if isinstance(binlog_event, WriteRowsEvent):
            if no_pk:
                # print binlog_event.__dict__
                # tableInfo = (binlog_event.table_map)[binlog_event.table_id]
                # if tableInfo.primary_key:
                #     row['values'].pop(tableInfo.primary_key)
                if binlog_event.primary_key:
                    row['values'].pop(binlog_event.primary_key)

            db = rename_db if rename_db else binlog_event.schema
            if replace:
                template = 'REPLACE INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    db, binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )

            elif insert_ignore:
                template = 'INSERT IGNORE INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    db, binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
            else:
                template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    db, binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
            values = map(fix_object, row['values'].values())
            types = map(fix_object_new, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            db = rename_db if rename_db else binlog_event.schema
            if not only_pk:
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                    db, binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
                values = map(fix_object, row['values'].values())
                types = map(fix_object_new, row['values'].values())
            else:
                pk_item = {
                    binlog_event.primary_key: row['values'].get(binlog_event.primary_key)
                }
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                    db, binlog_event.table, ' AND '.join(map(compare_items, pk_item.items())))
                values = map(fix_object, pk_item.values())
                types = map(fix_object_new, pk_item.values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            db = rename_db if rename_db else binlog_event.schema
            if not only_pk:
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    db, binlog_event.table,
                    ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                    ' AND '.join(map(compare_items, row['before_values'].items()))
                )
                values = map(fix_object, list(row['after_values'].values()) + list(row['before_values'].values()))
                types = map(fix_object_new, list(row['after_values'].values()) + list(row['before_values'].values()))
            else:
                pk_item = {
                    binlog_event.primary_key: row['before_values'].get(binlog_event.primary_key)
                }
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    db, binlog_event.table,
                    ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                    ' AND '.join(map(compare_items, pk_item.items()))
                )
                values = map(fix_object, list(row['after_values'].values()) + list(pk_item.values()))
                types = map(fix_object_new, list(row['after_values'].values()) + list(pk_item.values()))

    result = (
        {'template': template, 'values': list(values)},
        db.replace('`', ''),
        table.replace('`', '')
    )
    if return_type:
        return result, list(types)
    return result


def reversed_lines(fin):
    """Generate the lines of file in reverse order."""
    part = ''
    for block in reversed_blocks(fin):
        if PY3PLUS:
            # block = block.decode("utf-8")
            block = fix_object(block)
        for c in reversed(block):
            if c == '\n' and part:
                yield part[::-1]
                part = ''
            part += c
    if part:
        yield part[::-1]


def reversed_blocks(fin, block_size=4096):
    """Generate blocks of file's contents in reverse order."""
    # 调到文件末尾
    fin.seek(0, os.SEEK_END)
    # 获取文件末尾的流位置
    here = fin.tell()
    # 如果文件非空
    while 0 < here:
        # 一次取固定大小的文件信息
        delta = min(block_size, here)
        here -= delta
        fin.seek(here, os.SEEK_SET)
        yield fin.read(delta)


def get_gtid_set(include_gtids, exclude_gtids):
    # gtid 示例
    # 35191261-90cd-11e9-9398-00163e0ef40e:2840-134906:134908-183611:183613-351746:360220-364062,
    # 6ea67fc8-c260-11eb-8c17-00163e0ef40e:1-99954068,
    # b1f3ee7b-b46d-11eb-9806-00163e0ef40e:4790-196015:196017-2588749,
    # fcb79f76-b484-11eb-9d4c-00163e047dcb:7273871-7277930
    gtid_set = {}
    if include_gtids:
        gtid_set['include'] = {}
        gtids = include_gtids.split(',')
        for gtid in gtids:
            gtid_splited = gtid.split(':')
            uuid = gtid_splited[0]
            if uuid not in gtid_set:
                gtid_set['include'][uuid] = []
            txn_range = gtid_splited[1:]
            gtid_set['include'][uuid].extend(txn_range)

    if exclude_gtids:
        gtid_set['exclude'] = {}
        gtids = exclude_gtids.split(',')
        for gtid in gtids:
            gtid_splited = gtid.split(':')
            uuid = gtid_splited[0]
            if uuid not in gtid_set:
                gtid_set['exclude'][uuid] = []
            txn_range = gtid_splited[1:]
            gtid_set['exclude'][uuid].extend(txn_range)

    return gtid_set


def is_want_gtid(gtid_set, gtid):
    gtid_splited = gtid.split(':')
    uuid = gtid_splited[0]
    txn = int(gtid_splited[1])
    if 'include' in gtid_set and uuid in gtid_set['include']:
        txn_ranges = gtid_set['include'][uuid]
        for txn_range in txn_ranges:
            txn_split = txn_range.split('-')
            txn_min = int(txn_split[0])
            txn_max = int(txn_split[1]) if len(txn_split) > 1 else txn_min
            if txn_min <= txn <= txn_max:
                return True
        else:
            return False
    elif 'exclude' in gtid_set and uuid in gtid_set['exclude']:
        txn_ranges = gtid_set['exclude'][uuid]
        for txn_range in txn_ranges:
            txn_split = txn_range.split('-')
            txn_min = int(txn_split[0])
            txn_max = int(txn_split[1]) if len(txn_split) > 1 else txn_min
            if txn_min <= txn <= txn_max:
                return False
        else:
            return True
