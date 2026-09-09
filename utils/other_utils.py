# !/usr/bin/env python3
# -*- coding:utf8 -*-
import json
import os
import sys
import re
import time
import uuid
from datetime import datetime as dt
from contextlib import contextmanager
from loguru import logger
from pathlib import Path

py_file_path = Path(sys.argv[0])
py_file_pre = py_file_path.parts[-1].replace('.py', '')
log_file = py_file_path.parent / 'logs' / f'{py_file_pre}.log'
Path(log_file).parent.mkdir(exist_ok=True, parents=True)
logger.add(log_file, rotation='100MB', retention=10, compression='zip')
sep = '/' if '/' in sys.argv[0] else os.sep


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
def temp_open(filename, mode, encoding=None, errors=None):
    open_kwargs = {}
    if encoding:
        open_kwargs['encoding'] = encoding
    if errors:
        open_kwargs['errors'] = errors
    f = open(filename, mode, **open_kwargs)
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


def split_in_values(src: str):
    """按逗号拆分 IN (...) 内的值。

    忽略 JSON 对象 {}、数组 [] 以及引号字符串内部的逗号，
    避免 JSON 值里的逗号被误当作分隔符。
    """
    values = []
    start = 0
    brace_depth = 0
    bracket_depth = 0
    in_quote = None
    escaped = False
    for i, ch in enumerate(src):
        if in_quote:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == in_quote:
                in_quote = None
            continue
        if ch in ('"', "'"):
            in_quote = ch
        elif ch == '{':
            brace_depth += 1
        elif ch == '}':
            brace_depth = max(brace_depth - 1, 0)
        elif ch == '[':
            bracket_depth += 1
        elif ch == ']':
            bracket_depth = max(bracket_depth - 1, 0)
        elif ch == ',' and brace_depth == 0 and bracket_depth == 0:
            values.append(src[start:i])
            start = i + 1
    values.append(src[start:])
    return values


def parse_in_value(v: str):
    """把 IN 列表里的单个值解析成合适的 Python 类型。

    支持 JSON 对象/数组、数字、布尔、null、引号字符串及裸字符串。
    """
    v = v.strip()
    if v in ('""', "''"):
        return ''
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        inner = v[1:-1]
        # 带引号的 JSON 值也要解析成 dict/list，便于与 binlog 里的 JSON 列匹配
        try:
            parsed = json.loads(inner)
            if isinstance(parsed, (dict, list)):
                return parsed
        except ValueError:
            pass
        return inner
    try:
        return json.loads(v)
    except ValueError:
        pass
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        return v


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
        if left_quote_idx == -1 or right_quote_idx <= left_quote_idx:
            logger.warning(f"Ignore condition: {cond} !!! Invalid IN condition.")
            return
        quote_part = value[left_quote_idx + 1: right_quote_idx]
        value = [parse_in_value(v) for v in split_in_values(quote_part) if v.strip()]
    elif calc_type == ' IS ' and value.upper() == 'NULL':
        value = None
    elif value in ('""', "''"):
        value = ''
    else:
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                pass

    condition_list.append({
        "column": cond_split[0].strip().replace('`', ''),
        "calc_type": calc_type.strip(),
        "value": value,
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
