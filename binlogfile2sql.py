#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import sys
import time
import pymysql
import re
from binlogfile2sql_util import command_line_args, BinLogFileReader
from binlog2sql_util import concat_sql_from_binlog_event, create_unique_file, reversed_lines, is_dml_event, \
    event_type, logger, set_log_format
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from signal import signal, SIGHUP, SIGTERM

result_sql_file = ''
f_result_sql_file = ''


def exit_handler(sig, frame):
    if result_sql_file:
        logger.exception('Got KeyboardInterrupt signal, delete result sql file')
        os.remove(result_sql_file)
        sys.exit(1)
    else:
        logger.exception('')
        sys.exit(1)


class BinlogFile2sql(object):
    def __init__(self, file_path, connection_settings, start_pos=None, end_pos=None, start_time=None,
                 stop_time=None, only_schemas=None, only_tables=None, no_pk=False, flashback=False,
                 only_dml=True, sql_type=None, result_dir=None, stop_never=False, need_comment=1,
                 rename_db=None):
        """
        connection_settings: {'host': 127.0.0.1, 'port': 3306, 'user': slave, 'passwd': slave}
        """
        # if not startFile:
        #    raise ValueError('lack of parameter,startFile.')

        self.file_path = file_path
        self.connection_settings = connection_settings
        self.start_pos = start_pos if start_pos else 4  # use binlog v4
        self.end_pos = end_pos
        self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") if start_time else \
            datetime.datetime.strptime('1970-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        self.stop_time = datetime.datetime.strptime(stop_time, "%Y-%m-%d %H:%M:%S") if stop_time else \
            datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.no_pk, self.flashback, self.stop_never = (no_pk, flashback, stop_never)

        self.only_dml = only_dml
        self.sql_type = [t.upper() for t in sql_type] if sql_type else []

        self.binlog_file_list = []
        self.connection = pymysql.connect(**self.connection_settings)

        self.result_dir = result_dir
        self.need_comment = need_comment
        self.rename_db = rename_db

    def process_binlog(self):
        stream = BinLogFileReader(self.file_path, ctl_connection_settings=self.connection_settings,
                                  log_pos=self.start_pos, only_schemas=self.only_schemas,
                                  only_tables=self.only_tables, resume_stream=True)

        cur = self.connection.cursor()
        # to simplify code, we do not use file lock for tmp_file.
        tmp_file = create_unique_file('%s.%s' % (self.connection_settings['host'], self.connection_settings['port']))
        if self.stop_never:
            global result_sql_file, f_result_sql_file

            sep = '/' if '/' in sys.argv[0] else os.sep
            result_sql_file = self.file_path.split(sep)[-1].replace('.', '_').replace('-', '_') + '.sql'
            result_sql_file = os.path.join(self.result_dir, result_sql_file)
            f_result_sql_file = open(result_sql_file, 'a')
        f_tmp = open(tmp_file, "w")
        flag_last_event = False
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        try:
            for binlog_event in stream:
                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    sql = concat_sql_from_binlog_event(cursor=cur, binlog_event=binlog_event,
                                                       flashback=self.flashback, no_pk=self.no_pk,
                                                       rename_db=self.rename_db)
                    if sql:
                        if self.need_comment != 1:
                            sql = re.sub('; #.*', ';', sql)
                        if f_result_sql_file:
                            f_result_sql_file.write(sql + '\n')
                        else:
                            print(sql)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        sql = concat_sql_from_binlog_event(cursor=cur, binlog_event=binlog_event, row=row,
                                                           flashback=self.flashback, no_pk=self.no_pk,
                                                           e_start_pos=e_start_pos, rename_db=self.rename_db)
                        if sql:
                            if self.need_comment != 1:
                                sql = re.sub('; #.*', ';', sql)
                            if self.flashback:
                                f_tmp.write(sql + '\n')
                            else:
                                if f_result_sql_file:
                                    f_result_sql_file.write(sql + '\n')
                                else:
                                    print(sql)

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break
            f_tmp.close()
            if f_result_sql_file:
                f_result_sql_file.close()

            if self.flashback:
                self.print_rollback_sql(tmp_file)
        except KeyboardInterrupt:
            if result_sql_file:
                logger.exception('Got KeyboardInterrupt signal, delete result sql file')
                os.remove(result_sql_file)
                sys.exit(1)
            else:
                logger.exception('')
                sys.exit(1)
        finally:
            os.remove(tmp_file)
        cur.close()
        stream.close()
        return True

    def print_rollback_sql(self, fin):
        """print rollback sql from tmp_file"""
        with open(fin) as f_tmp:
            sleep_interval = 1000
            i = 0
            for line in reversed_lines(f_tmp):
                print(line.rstrip())
                if i >= sleep_interval:
                    print('SELECT SLEEP(1);')
                    i = 0
                else:
                    i += 1

    def __del__(self):
        pass


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
    executed_file_list = read_file(args.result_file) if args.stop_never and os.path.exists(args.result_file) else []
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
        for f in args.file_path:
            if re.search(args.file_regex, f) is not None:
                if not f.startswith('/') and args.file_dir:
                    binlog_file = os.path.join(args.file_dir, f)
                else:
                    binlog_file = f
                binlog_file_list.append(binlog_file)

    for f in executed_file_list.copy():
        if not os.path.exists(f):
            executed_file_list.remove(f)

    return binlog_file_list, executed_file_list


def main(args):
    connection_settings = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password}
    binlog_file_list, executed_file_list = get_binlog_file_list(args)

    if args.check:
        from pprint import pprint
        pprint(binlog_file_list)
        sys.exit(1)

    if not args.stop_never:
        for binlog_file in binlog_file_list:
            logger.info('parsing binlog file: %s' % binlog_file)
            bin2sql = BinlogFile2sql(file_path=binlog_file, connection_settings=connection_settings,
                                     start_pos=args.start_pos, end_pos=args.end_pos,
                                     start_time=args.start_time, stop_time=args.stop_time,
                                     only_schemas=args.databases, need_comment=args.need_comment,
                                     only_tables=args.tables, no_pk=args.no_pk, flashback=args.flashback,
                                     only_dml=args.only_dml, sql_type=args.sql_type, rename_db=args.rename_db)
            bin2sql.process_binlog()
    else:
        while True:
            for binlog_file in binlog_file_list:
                logger.info('parsing binlog file: %s' % binlog_file)
                bin2sql = BinlogFile2sql(file_path=binlog_file, connection_settings=connection_settings,
                                         start_pos=args.start_pos, end_pos=args.end_pos,
                                         start_time=args.start_time, stop_time=args.stop_time,
                                         only_schemas=args.databases, result_dir=args.result_dir,
                                         only_tables=args.tables, no_pk=args.no_pk, flashback=args.flashback,
                                         only_dml=args.only_dml, sql_type=args.sql_type, stop_never=args.stop_never,
                                         need_comment=args.need_comment, rename_db=args.rename_db)
                r = bin2sql.process_binlog()
                if r is True:
                    executed_file_list.append(binlog_file)
                    save_executed_result(args.result_file, executed_file_list)
            binlog_file_list, executed_file_list = get_binlog_file_list(args)
            if not binlog_file_list:
                # logger.info('All file has been executed, sleep 60 seconds to get other new files.')
                time.sleep(60)


if __name__ == '__main__':
    command_line_args = command_line_args(sys.argv[1:])
    set_log_format()
    if command_line_args.stop_never:
        signal(SIGHUP, exit_handler)
        signal(SIGTERM, exit_handler)
    main(command_line_args)
