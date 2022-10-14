# !/usr/bin/env python3
# -*- coding:utf8 -*-
# Author:           Michael Liu
# Created on:       2022-04-21
import argparse
import logging
import os
import sys
import re
import uuid
import colorlog
from rich.progress import track

# create a logger
logger = logging.getLogger('sort_binlog2sql_result_utils')
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


def parse_args():
    """Parse args"""

    parser = argparse.ArgumentParser(description='Parse Args', add_help=False,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--help', dest='help', action='store_true', default=False,
                        help='help information')

    args = parser.add_argument_group('Arg setting')
    args.add_argument('-sf', '--src-file', dest='src_file', type=str,
                      help='Src file')
    args.add_argument('-df', '--dst-file', dest='dst_file', type=str,
                      help='Dst file')
    args.add_argument('-e', '--encoding', dest='encoding', type=str, default='utf8',
                      help='File encoding')
    args.add_argument('-t', '--sort-type', dest='sort_type', type=str, default='reverse_seq',
                      help='Sort type. Valid choice is: reverse_seq, sort_by_time')
    args.add_argument('-td', '--tmp-dir', dest='tmp_dir', type=str, default='tmp',
                      help='Tmp dir for store tmp result.')
    args.add_argument('-c', '--chunk-size', dest='chunk_size', type=int, default=1000,
                      help='File chunk size.')
    return parser


def parse_command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(args.src_file):
        logger.error(args.src_file + " does not exists!!!")
        return

    if not args.dst_file:
        args.dst_file = args.src_file + '.new'

    os.makedirs(args.tmp_dir, exist_ok=True)
    while not check_dir_if_empty(args.tmp_dir):
        args.tmp_dir = os.path.join(args.tmp_dir, 'tmp/')
        os.makedirs(args.tmp_dir, exist_ok=True)

    if args.sort_type not in ['reverse_seq', 'sort_by_time']:
        logger.error(f'Invalid sort type: [{args.sort_type}]')
        sys.exit(1)
    return args


def check_dir_if_empty(dir_path):
    try:
        next(os.scandir(dir_path))
        return False
    except StopIteration:
        return True


def read_file(filename, encoding: str = 'utf8'):
    with open(filename, 'r', encoding=encoding) as f:
        info = f.readlines()
    return info


def yield_file(filename, encoding: str = 'utf8', chunk_size: int = 1000):
    with open(filename, 'r', encoding=encoding) as f:
        tmp_list = []
        for i, line in enumerate(f):
            if chunk_size > 1:
                tmp_list.append(line)
                if i != 0 and i % chunk_size == 0:
                    yield tmp_list
                    tmp_list = []
            else:
                yield line
        else:
            if tmp_list:
                yield tmp_list


def save_to_file(filename, msg, encoding: str = 'utf8', mode: str = 'w'):
    with open(filename, mode, encoding=encoding) as f:
        if isinstance(msg, str):
            f.write(msg)
        elif isinstance(msg, list):
            f.writelines(msg)
    return


def sort_by_index(x: list):
    return x[0]


def get_sql_time(line):
    return re.search('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line).group()


def sort_by_time(x: str):
    x_split = x.split('#start ')[-1]
    return get_sql_time(x_split)


def sort_by_min_val(x: list):
    return x[1]


def get_min_max_val(tmp_list: list):
    tmp_list.sort(key=sort_by_time)
    min_val = get_sql_time(tmp_list[0])
    max_val = get_sql_time(tmp_list[-1])
    return min_val, max_val


def get_file_line_count(filename):
    logger.info(f'Getting line count of file {filename} ...')
    command = f'wc -l {filename} | awk ' + "'{print $1}'"
    return int(os.popen(command).read())


def init_tmp_dir(tmp_dir):
    version = 0
    max_depth = 100
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir, exist_ok=True)
    while not check_dir_if_empty(tmp_dir) and version < max_depth:
        tmp_dir = os.path.join(tmp_dir, 'tmp')
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir, exist_ok=True)
        version += 1
    if version >= max_depth:
        raise OSError(f'Create too many tmp dirs.')
    return tmp_dir


def reversed_seq(src_file, chunk_size, tmp_dir, dst_file, encoding='utf8', delete_tmp_dir=True):
    file_line_count = get_file_line_count(src_file)
    if file_line_count == 0:
        logger.error(f'{src_file} is empty.')
        return

    total_part = file_line_count // chunk_size + 1
    try:
        record_list = []
        tmp_dir = init_tmp_dir(tmp_dir)
        for i, file_lines in track(enumerate(yield_file(src_file, encoding, chunk_size)),
                                   total=total_part, description='reversing...'):
            tmp_filepath = os.path.join(tmp_dir, str(uuid.uuid4()))
            record_list.append([i, tmp_filepath])

            file_lines_tmp = []
            for ii, line in enumerate(file_lines):
                file_lines_tmp.append([ii, line])
            file_lines_tmp.sort(key=sort_by_index, reverse=True)
            file_lines_new = [x[1] for x in file_lines_tmp]
            save_to_file(tmp_filepath, file_lines_new, encoding=encoding)

        record_list.sort(key=sort_by_index, reverse=True)
        for i, (_, filepath) in track(enumerate(record_list), total=len(record_list), description='re-saving...'):
            file_lines = read_file(filepath, encoding)
            if i == 0:
                save_to_file(dst_file, file_lines, encoding=encoding, mode='w')
            else:
                save_to_file(dst_file, file_lines, encoding=encoding, mode='a')
            os.remove(filepath)
    finally:
        if delete_tmp_dir:
            try:
                os.removedirs(tmp_dir)
            except:
                pass


def sort_file_by_time(src_file, chunk_size, tmp_dir, dst_file, encoding='utf8'):
    try:
        total_part = get_file_line_count(src_file) // chunk_size
        record_dict = dict()
        for tmp_list in track(yield_file(src_file, encoding, chunk_size),
                              total=total_part, description='sorting...'):
            # 获取最小值和最大值的时候，会排序
            min_val, max_val = get_min_max_val(tmp_list)
            if record_dict:
                for _ in tmp_list.copy():
                    line = tmp_list.pop(0)
                    sql_time = sort_by_time(line)
                    break_sign = 0

                    # tmp_list 里的数据已经排好序了，如果轮询到某行匹配不到前面的最小值和最大值区间的话，
                    # 后面的就不需要再匹配了
                    for filename, (record_min_val, record_max_val) in record_dict.items():
                        if record_min_val <= sql_time <= record_max_val:
                            filepath = os.path.join(tmp_dir, filename)
                            save_to_file(filepath, line, encoding, 'a')
                            break
                    else:
                        break_sign = 1

                    if break_sign == 1:
                        tmp_list.append(line)
                        min_val, max_val = get_min_max_val(tmp_list)
                        break

            tmp_filename = str(uuid.uuid4())
            record_dict[tmp_filename] = [min_val, max_val]

            tmp_filepath = os.path.join(tmp_dir, tmp_filename)
            save_to_file(tmp_filepath, tmp_list, encoding)

        record_list = []
        for i, (filename, (record_min_val, _)) in enumerate(record_dict.items()):
            record_list.append([filename, record_min_val])
        record_list.sort(key=sort_by_min_val)

        for i, (filename, _) in track(enumerate(record_list), total=len(record_list), description='re-saving...'):
            filepath = os.path.join(tmp_dir, filename)
            file_lines = read_file(filepath, encoding)
            file_lines.sort(key=sort_by_time)
            if i == 0:
                save_to_file(dst_file, file_lines, encoding=encoding, mode='w')
            else:
                save_to_file(dst_file, file_lines, encoding=encoding, mode='a')
            os.remove(filepath)
    finally:
        try:
            os.removedirs(tmp_dir)
        except:
            pass


def main(args):
    if args.sort_type == 'reverse_seq':
        reversed_seq(args.src_file, args.chunk_size, args.tmp_dir, args.dst_file, args.encoding)
    elif args.sort_type == 'sort_by_time':
        sort_file_by_time(args.src_file, args.chunk_size, args.tmp_dir, args.dst_file, args.encoding)


if __name__ == '__main__':
    command_line_args = parse_command_line_args(sys.argv[1:])
    main(command_line_args)
    logger.info('done')
