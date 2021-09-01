# !/usr/bin/env python3
# -*- coding:utf8 -*-

import argparse
import sys
import os
import chardet
import re

# create a logger
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def set_log_format():
    import logging.handlers
    import colorlog

    global logger

    # set logger color
    log_colors_config = {
        'DEBUG': 'bold_puple',
        'INFO': 'bold_green',
        'WARNING': 'bold_yellow',
        'ERROR': 'bold_red',
        'CRITICAL': 'bold_red',
    }

    # set logger format
    log_format = colorlog.ColoredFormatter(
        "%(log_color)s[%(asctime)s] [%(module)s:%(funcName)s] [%(lineno)d] [%(levelname)s] %(message)s",
        log_colors=log_colors_config
    )

    # add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # add rotate file handler
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, 'logs')
    if not os.path.isdir(logs_dir):
        os.makedirs(logs_dir, exist_ok=True)

    logfile = logs_dir + os.sep + sys.argv[0].split(os.sep)[-1].split('.')[0] + '.log'
    file_maxsize = 1024 * 1024 * 100  # 100m
    # logfile_size = os.path.getsize(logfile) if os.path.exists(logfile) else 0

    file_handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=file_maxsize, backupCount=10)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)


def parse_args():
    """parse args to connect MySQL"""

    parser = argparse.ArgumentParser(description='Parse MySQL Connect Settings', add_help=False)
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    #sql_file = parser.add_argument_group('sql file')
    #sql_file.add_argument('-f', '--file', dest='sql_file', type=str,
    #                      help='sql file you want to execute', default='')
    sql_file = parser.add_argument_group('sql file')
    sql_file.add_argument('-f', '--file', dest='sql_file', type=str,
                          help='sql file you want to execute', default='')
    
    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    return args


def detect_file_encoding(filename):
    with open(filename, 'rb') as f:
        data = f.read()
    encoding = chardet.detect(data).get('encoding', 'unknown encoding')
    logger.info('file [' + filename + '] encoding is ' + encoding)
    return encoding


def read_file(filename, is_yield=False):
    if not os.path.exists(filename):
        print(filename + " does not exists!!!")
        sys.exit(1)

    if is_yield:
        with open(filename, 'r', encoding='utf8') as f:
            for line in f:
                yield line.strip()
    else:
        with open(filename, 'r', encoding='utf8') as f:
            info = f.readlines()
        return info


def filter_update_sql(sql, keep_col_list=None):
    sql_tmp = re.subn(',|AND|LIMIT 1', '', sql)[0]
    sql_part = sql_tmp.split()
    sql_type = sql_part[0]
    if sql_type.upper() == 'UPDATE':
        table = sql_part[1]
        start_idx = 3
    else:
        logger.warning('This function only support update')
        return sql

    condition_idx = sql_part.index('WHERE')
    comment_idx = sql_part.index('#start')
    new_col_value = sql_part[start_idx:condition_idx]
    old_col_value = sql_part[condition_idx+1:comment_idx]
    comment_value = " ".join(sql_part[comment_idx:])

    new_sql = sql_type.upper() + ' ' + table + ' SET'
    condition = 'WHERE ' + old_col_value[0]

    for i, (new_value, old_value) in enumerate(zip(new_col_value, old_col_value)):
        if new_value != old_value:
            if i == 0 or new_sql.endswith('SET'):
                new_sql += ' ' + new_value
            else:
                new_sql += ', ' + new_value
            condition += ' AND ' + old_value
        elif keep_col_list:
            for col in keep_col_list:
                if re.search(col, old_value):
                    condition += ' AND ' + old_value

    new_sql += ' ' + condition + '; ' + comment_value
    return new_sql


def main(args):
    sql_file = args.sql_file
    cnt = 0
    info_format = '[{file}] '.format(
        file=sql_file
    )
    finished_info = info_format + 'finished'
    info_format += '[Filtered line count: {cnt}]'

    keep_col_list = [
        '`corp_id`',
    ]
    try:
        for line in read_file(sql_file, is_yield=True):
            new_sql = filter_update_sql(line, keep_col_list=keep_col_list)
            print(new_sql)
            cnt += 1
            if (cnt % 2000) == 0:
                logger.info(info_format.format(cnt=cnt))
        logger.info(info_format.format(cnt=cnt))
        logger.info(finished_info)
    except Exception as e:
        logger.error('Detect error in line: [' + str(line) + '], err_msg is: ' + str(e))


if __name__ == "__main__":
    args = command_line_args(sys.argv[1:])
    assert args.sql_file, "Missing sql file, we need [-f|--file] argument."
    set_log_format()
    main(args)
