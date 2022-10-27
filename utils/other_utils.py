# !/usr/bin/env python3
# -*- coding:utf8 -*-
import logging
import os
import sys
import colorlog
import re
import time
import uuid
from datetime import datetime as dt
from contextlib import contextmanager


# create a logger
logger = logging.getLogger('json_utils')
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
    "[%(asctime)s] [%(module)s:%(funcName)s] [%(lineno)d] [%(levelname)s] %(log_color)s%(message)s",
    log_colors=log_colors_config
)

# add console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_format)
logger.addHandler(console_handler)


def create_unique_file(filename, path=None):
    result_file = filename + '_' + str(uuid.uuid4())
    # version = 0
    # if we have to try more than 1000 times, something is seriously wrong
    # while os.path.exists(result_file) and version < 1000:
    #     result_file = filename + '.' + str(version)
    #     version += 1
    # if version >= 1000:
    #     raise OSError('cannot create unique file %s.[0-1000]' % filename)
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
        if os.path.exists(filename):
            os.remove(filename)


def read_file(filename):
    if not os.path.exists(filename):
        print(filename + " does not exists!!!")
        return []

    with open(filename, 'r', encoding='utf8') as f:
        return list(map(lambda s: s.strip('\n'), f.readlines()))


def save_executed_result(result_file, result_list):
    result_list = list(map(lambda s: s + '\n', result_list))
    with open(result_file, 'w', encoding='utf8') as f:
        f.writelines(result_list)
    return


def get_binlog_file_list(args):
    binlog_file_list = []
    executed_file_list = read_file(args.record_file) if args.stop_never and os.path.exists(args.record_file) else []
    if args.file_dir and not args.file_path:
        for f in sorted(os.listdir(args.file_dir)):
            if args.start_file and f < args.start_file:
                continue
            if args.stop_file and f > args.stop_file:
                break
            if re.search(args.file_regex, f) is not None:
                binlog_file = os.path.join(args.file_dir, f)
                if args.stop_never and \
                        (int(time.time() - os.path.getmtime(binlog_file)) < args.minutes_ago * 60 or
                         binlog_file in executed_file_list):
                    continue
                binlog_file_list.append(binlog_file)
    else:
        binlog_file_list.extend(args.file_path)

    for f in executed_file_list.copy():
        if not os.path.exists(f):
            executed_file_list.remove(f)

    return binlog_file_list, executed_file_list


def is_valid_datetime(string):
    try:
        dt.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False


def timestamp_to_datetime(ts: int, datetime_format: str = None) -> str:
    """
    将时间戳转换为指定格式的时间字符串
    :param ts: 传入时间戳
    :param datetime_format: 传入指定的时间格式
    :return 指定格式的时间字符串
    """
    if datetime_format is None:
        datetime_format = '%Y-%m-%d %H:%M:%S'

    datetime_obj = dt.fromtimestamp(ts)
    datetime_str = datetime_obj.strftime(datetime_format)

    return datetime_str


def fix_json_col(col_list):
    # 左括号数量 与 右括号数量
    json_mark_left_cnt = 0
    json_mark_right_cnt = 0
    json_col = ''
    col_list_new = []
    for i, col in enumerate(col_list):
        if isinstance(col, str):
            col = col.strip()

        if re.search('{', col) is not None:
            json_mark_left_cnt += col.count('{')
            if re.search('}', col) is not None:
                json_mark_right_cnt += col.count('}')
            json_col += col + ','
            if json_mark_left_cnt == json_mark_right_cnt:
                col_list_new.append(json_col[:-1])
                json_col = ''
        elif json_mark_left_cnt != json_mark_right_cnt:
            if re.search('}', col) is not None:
                json_mark_right_cnt += col.count('}')
            json_col += col + ','
            if json_mark_left_cnt == json_mark_right_cnt:
                col_list_new.append(json_col[:-1])
                json_col = ''
        else:
            col_list_new.append(col)
    return col_list_new


def parse_split_condition(cond, condition_list):
    cond = re.sub(' [iI][sS] ', ' IS ', cond)
    cond = re.sub(' [iI][nN] ', ' IN ', cond)
    if '>=' in cond:
        calc_type = '>='
    elif '<=' in cond:
        calc_type = '<='
    elif '!=' in cond:
        calc_type = '!='
    elif '<>' in cond:
        calc_type = '<>'
    elif '=' in cond:
        calc_type = '='
    elif '>' in cond:
        calc_type = '>'
    elif '<' in cond:
        calc_type = '<'
    elif ' IS ' in cond:
        calc_type = ' IS '
        cond = re.sub(' [nN][uU][lL][lL]', ' NULL', cond)
    elif ' IN ' in cond:
        calc_type = ' IN '
    else:
        logger.warning(f"Ignore condition: {cond} !!! We Don't support condition like that.")
        return

    cond_split = cond.split(calc_type)
    value = calc_type.join(cond_split[1:]).strip()
    if calc_type == ' IN ':
        left_quote_idx = value.find('(')
        right_quote_idx = value.rfind(')')
        quote_part = value[left_quote_idx + 1: right_quote_idx]
        quote_part_value = quote_part.split(',')
        if '{' in quote_part:
            json_mark_left_cnt = quote_part.count('{')
            json_mark_right_cnt = quote_part.count('}')
            if json_mark_left_cnt == json_mark_right_cnt:
                quote_part_value = fix_json_col(quote_part_value)
        value = []
        for v in quote_part_value:
            try:
                if isinstance(v, str):
                    v = int(v)
            except ValueError:
                pass
            if value in ['""', "''"]:
                value.append('')
            else:
                value.append(v)

    try:
        if isinstance(value, str):
            value = int(value)
    except ValueError:
        pass

    condition_list.append({
        "column": cond_split[0].strip().replace('`', ''),
        "calc_type": calc_type.strip(),
        "value": '' if value in ['""', "''"] else value,
    })


def split_condition(src_conditions):
    """拆分 WHERE 条件
    WHERE 条件：
        and
        or
        in
        between ... and ...
        > < >= <= = !=
        is null

    思路：
        不需要有 and， 用 nargs='*' 即可实现 and 的效果，需要 or 的话，在单个条件里加上即可
        不需要有 between ... and ... ，用两个分别具备 >= 和 <= 的条件实现即可
    最终效果：
        最终的条件组合就是一个数组，数组里面的每个元素是一个 json(dict)
        json 的组成：
            key: column, value: 列名,
            key: calc_type, value: 条件符号，如 > < >= <= = != IS in
            key: value, value: 条件值
    """
    condition_list = []
    for condition in src_conditions:
        if ' AND ' in condition.upper():
            logger.error(f"""Invalid condition {condition}. Multi conditions format：--where 'c1=v1' 'c2=v2' """)
            sys.exit(1)
        if condition.lstrip().startswith('(') and condition.rstrip().endswith(')'):
            logger.error(f"Invalid condition: {condition} !!! Don't use parentheses before and after")
            sys.exit(1)

        condition = re.sub(' [oO][rR] ', ' OR ', condition)
        condition_split = condition.split(' OR ')
        if len(condition_split) == 1:
            for cond in condition_split:
                parse_split_condition(cond, condition_list)
        else:
            condition_list_tmp = []
            for cond in condition_split:
                parse_split_condition(cond, condition_list_tmp)
            condition_list.append(tuple(condition_list_tmp))
    return condition_list


def merge_rename_args(rename_args_list: list):
    rename_args_dict = dict()
    for rename_arg in rename_args_list:
        rename_arg_split = rename_arg.split()
        if len(rename_arg_split) > 1:
            old_arg = rename_arg_split[0]
            new_arg = rename_arg_split[1]
        else:
            old_arg = "*"
            new_arg = rename_arg_split[0]
        rename_args_dict[old_arg] = new_arg
    return rename_args_dict
