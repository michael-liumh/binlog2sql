# !/usr/bin/env python3
# -*- coding:utf8 -*-
# Author:           Michael Liu
# Created on:       2022-04-21
import argparse
import os
import sys
import re
import uuid
from rich.progress import track
parent_dir = '/'.join(os.path.dirname(os.path.abspath(__file__)).split(os.sep)[0:-1])
sys.path.append(parent_dir)
from binlog2sql_util import logger


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

    if not os.path.isdir(args.tmp_dir):
        os.makedirs(args.tmp_dir, exist_ok=True)

    if args.sort_type not in ['reverse_seq', 'sort_by_time']:
        logger.error(f'Invalid sort type: [{args.sort_type}]')
        sys.exit(1)
    return args


def read_file(filename, encoding: str = 'utf8'):
    with open(filename, 'r', encoding=encoding) as f:
        info = f.readlines()
    return info


def yield_file(filename, encoding: str = 'utf8', chunk_size: int = 1000):
    with open(filename, 'r', encoding=encoding) as f:
        tmp_list = []
        for i, line in enumerate(f):
            tmp_list.append(line)
            if i != 0 and i % chunk_size == 0:
                yield tmp_list
                tmp_list = []
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
    logger.info('Getting file line count...')
    command = f'wc -l {filename} | awk ' + "'{print $1}'"
    return int(os.popen(command).read())


def main(args):
    total_part = get_file_line_count(args.src_file) // args.chunk_size
    if args.sort_type == 'reverse_seq':
        record_list = []
        for i, file_lines in track(enumerate(yield_file(args.src_file, args.encoding, args.chunk_size)),
                                   total=total_part, description='sorting...'):
            tmp_filepath = os.path.join(args.tmp_dir, str(uuid.uuid4()))
            record_list.append([i, tmp_filepath])

            file_lines_tmp = []
            for ii, line in enumerate(file_lines):
                file_lines_tmp.append([ii, line])
            file_lines_tmp.sort(key=sort_by_index, reverse=True)
            file_lines_new = [x[1] for x in file_lines_tmp]
            save_to_file(tmp_filepath, file_lines_new, encoding=args.encoding)

        record_list.sort(key=sort_by_index, reverse=True)
        for i, (_, filepath) in track(enumerate(record_list), total=len(record_list), description='re-saving...'):
            file_lines = read_file(filepath, args.encoding)
            if i == 0:
                save_to_file(args.dst_file, file_lines, encoding=args.encoding, mode='w')
            else:
                save_to_file(args.dst_file, file_lines, encoding=args.encoding, mode='a')
            os.remove(filepath)

    elif args.sort_type == 'sort_by_time':
        record_dict = dict()
        for tmp_list in track(yield_file(args.src_file, args.encoding, args.chunk_size),
                              total=total_part, description='sorting...'):
            # 获取最小值和最大值的时候，会排序
            min_val, max_val = get_min_max_val(tmp_list)
            if record_dict:
                for _ in tmp_list.copy():
                    line = tmp_list.pop(0)
                    tmp_result = sort_by_time(line)
                    break_sign = 0

                    # tmp_list 里的数据已经排好序了，如果轮询到某行匹配不到前面的最小值和最大值区间的话，
                    # 后面的就不需要再匹配了
                    for filename, (record_min_val, record_max_val) in record_dict.items():
                        if record_min_val <= tmp_result <= record_max_val:
                            filepath = os.path.join(args.tmp_dir, filename)
                            save_to_file(filepath, line, args.encoding, 'a')
                            break
                    else:
                        break_sign = 1

                    if break_sign == 1:
                        tmp_list.append(line)
                        min_val, max_val = get_min_max_val(tmp_list)
                        break

            tmp_filename = str(uuid.uuid4())
            record_dict[tmp_filename] = [min_val, max_val]

            tmp_filepath = os.path.join(args.tmp_dir, tmp_filename)
            save_to_file(tmp_filepath, tmp_list, args.encoding)

        record_list = []
        for i, (filename, (record_min_val, _)) in enumerate(record_dict.items()):
            record_list.append([filename, record_min_val])
        record_list.sort(key=sort_by_min_val)

        for i, (filename, _) in track(enumerate(record_list), total=len(record_list), description='re-saving...'):
            filepath = os.path.join(args.tmp_dir, filename)
            file_lines = read_file(filepath, args.encoding)
            file_lines.sort(key=sort_by_time)
            if i == 0:
                save_to_file(args.dst_file, file_lines, encoding=args.encoding, mode='w')
            else:
                save_to_file(args.dst_file, file_lines, encoding=args.encoding, mode='a')
            os.remove(filepath)


if __name__ == '__main__':
    command_line_args = parse_command_line_args(sys.argv[1:])
    main(command_line_args)
    logger.info('done')
