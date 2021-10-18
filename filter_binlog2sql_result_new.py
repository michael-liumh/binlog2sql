# !/usr/bin/env python3
# -*- coding:utf8 -*-

import argparse
import json
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
        'DEBUG': 'bold_purple',
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

    parser = argparse.ArgumentParser(description='Parse MySQL Connect Settings', add_help=False,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    parser.add_argument('-f', '--file', dest='sql_file', type=str,
                        help='sql file you want to filter', default='')
    parser.add_argument('-p', '--pk', dest='primary_key', type=str,
                        help='choose a column to be primary key', default='`id`')
    parser.add_argument('-k', '--kcl', dest='keep_col_list', type=str, nargs='*',
                        help='choose multi column that you want to keep, these column will not be filter', default=[])
    parser.add_argument('-o', '--out-file', dest='out_file', type=str,
                        help='file that save the result', default='')
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


def fix_json_col(col_list):
    # 左括号数量 与 右括号数量
    json_mark_left_cnt = 0
    json_mark_right_cnt = 0
    json_col = ''
    col_list_new = []
    for i, col in enumerate(col_list):
        if isinstance(col, str):
            col = col.strip()

        if i < 3:
            col_list_new.append(col)
            continue

        if re.search('{', col) is not None:
            json_mark_left_cnt += col.count('{')
            if re.search('}', col) is not None:
                json_mark_right_cnt += col.count('}')
            json_col += col + ','
            if json_mark_left_cnt == json_mark_right_cnt:
                col_list_new.append(json_col[:-1])
                json_col = ''
            continue
        elif json_mark_left_cnt != json_mark_right_cnt:
            if re.search('}', col) is not None:
                json_mark_right_cnt += col.count('}')
            json_col += col + ','
            if json_mark_left_cnt == json_mark_right_cnt:
                col_list_new.append(json_col[:-1])
                json_col = ''
            continue
        else:
            col_list_new.append(col)
    return col_list_new


def col_list_to_dict(col_list):
    col_dict = {}
    others = []
    for col in col_list:
        if '=' in col:
            sep = '='
            col_split = col.split('`=')
            key = col_split[0] + '`'
            value = col_split[1]
        elif 'IS NULL' in col:
            sep = ' IS '
            col_split = col.split()
            key = col_split[0]
            value = col_split[-1]
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
        logger.error(others)
    return col_dict


def filter_update(sql: str, primary_key: str = None, keep_col_list: list = None) -> str:
    if not sql.startswith('UPDATE'):
        return sql

    if primary_key is None:
        primary_key = '`id`'
    if keep_col_list is None:
        keep_col_list = []
    if primary_key not in keep_col_list:
        keep_col_list.append(primary_key)

    sql_split = sql.split('WHERE')
    update_col_part = sql_split[0]
    where_col_part = sql_split[1]
    begin_idx = update_col_part.find('SET')
    update_prefix = update_col_part[:begin_idx + 3]
    update_suffix = update_col_part[begin_idx + 3:]
    update_col_list = update_suffix.split(',')
    if "{" in str(update_col_list):
        update_col_list = fix_json_col(update_col_list)
    update_col_dict = col_list_to_dict(update_col_list)
    # print(json.dumps(update_col_dict, indent=4))

    where_col_list = list(map(lambda s: s.strip(), where_col_part.split(' AND ')))
    limit_idx = where_col_list[-1].find('LIMIT')
    comment_idx = where_col_list[-1].find('#')
    comment = '; ' + where_col_list[-1][comment_idx:].strip() if comment_idx > 0 else ''
    where_col_list[-1] = where_col_list[-1][:limit_idx].strip()
    where_col_dict = col_list_to_dict(where_col_list)
    # print(json.dumps(where_col_dict, indent=4))

    update_col_list_new, where_col_list_new = [], []
    for key, new_value in update_col_dict.items():
        old_value = where_col_dict.get(key, '')
        # logger.info('new_value:%s old_value:%s %s' % (new_value['value'], old_value['value'],
        #                                               old_value['value'] == new_value['value']))
        if old_value and old_value['value'] == new_value['value']:
            if key in keep_col_list:
                where_col_list_new.append(old_value['sep'].join([key, old_value['value']]))
            continue
        if new_value['value'] != 'NULL' and key not in update_col_list_new:
            update_col_list_new.append(new_value['sep'].join([key, new_value['value']]))
        if old_value['value'] != 'NULL' and key not in where_col_list_new:
            where_col_list_new.append(old_value['sep'].join([key, old_value['value']]))

    new_sql = "".join(update_prefix) + ' ' + ','.join(update_col_list_new) + \
              ' WHERE ' + ' AND '.join(where_col_list_new) + comment
    return new_sql


def get_file_lines(filename):
    logger.info('getting file %s lines' % filename)
    # cnt = 0
    # with open(filename, encoding='utf8') as f:
    #     while True:
    #         buffer = f.read(4096)
    #         if not buffer:
    #             break
    #         cnt += buffer.count('\n')
    cnt = os.popen('grep -Ev "^--|^#" %s | wc -l' % filename).read()
    return cnt


def main(args):
    sql_file = args.sql_file
    keep_col_list = args.keep_col_list
    primary_key = args.primary_key
    out_file = args.out_file
    logger.warning('This function only filter update statement')

    sql_file_len = get_file_lines(sql_file)
    cnt = 0
    info_format = '[{file}] '.format(
        file=sql_file
    )
    finished_info = info_format + 'finished'
    info_format += '[Filtered line count: {cnt} / {sql_file_len}]'

    f = open(out_file, 'w', encoding='utf8') if out_file else ''
    try:
        for line in read_file(sql_file, is_yield=True):
            if line.startswith('--') or line.startswith('#'):
                logger.warning('Ignore comment line')
                new_sql = line
            else:
                new_sql = filter_update(line, primary_key=primary_key, keep_col_list=keep_col_list)

            if f:
                f.write(new_sql + '\n')
            else:
                print(new_sql)

            cnt += 1
            if (cnt % 10000) == 0:
                logger.info(info_format.format(cnt=cnt, sql_file_len=sql_file_len))

        logger.info(info_format.format(cnt=cnt, sql_file_len=sql_file_len))
        logger.info(finished_info)

        if out_file:
            logger.warning("The result saved in %s" % out_file)
    except Exception as e:
        logger.error('Detect error in line: [' + str(line) + '], err_msg is: ' + str(e))
    finally:
        if f:
            f.close()


if __name__ == "__main__":
    args = command_line_args(sys.argv[1:])
    assert args.sql_file, "Missing sql file, we need [-f|--file] argument."
    set_log_format()
    main(args)
