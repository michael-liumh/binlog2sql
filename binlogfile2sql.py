#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import sys
import pymysql
from binlogfile2sql_util import command_line_args, BinLogFileReader
from binlog2sql_util import concat_sql_from_binlog_event, create_unique_file, reversed_lines, logger, set_log_format
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)


class BinlogFile2sql(object):
    def __init__(self, file_path, connection_settings, start_pos=None, end_pos=None, start_time=None,
                 stop_time=None, only_schemas=None, only_tables=None, no_pk=False, flashback=False, stop_never=False):
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

        self.binlog_list = []
        self.connection = pymysql.connect(**self.connection_settings)

    def process_binlog(self):
        stream = BinLogFileReader(self.file_path, ctl_connection_settings=self.connection_settings,
                                  log_pos=self.start_pos, only_schemas=self.only_schemas,
                                  only_tables=self.only_tables, resume_stream=True)

        cur = self.connection.cursor()
        # to simplify code, we do not use file lock for tmp_file.
        tmp_file = create_unique_file('%s.%s' % (self.connection_settings['host'], self.connection_settings['port']))
        f_tmp = open(tmp_file, "w")
        flag_last_event = False
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        try:
            for binlog_event in stream:
                if not self.stop_never:
                    if datetime.datetime.fromtimestamp(binlog_event.timestamp) < self.start_time:
                        if not (isinstance(binlog_event, RotateEvent) or
                                isinstance(binlog_event, FormatDescriptionEvent)):
                            last_pos = binlog_event.packet.log_pos
                        continue
                    elif datetime.datetime.fromtimestamp(binlog_event.timestamp) >= self.stop_time:
                        break
                    else:
                        pass

                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, QueryEvent):
                    sql = concat_sql_from_binlog_event(cursor=cur, binlog_event=binlog_event, flashback=self.flashback,
                                                       no_pk=self.no_pk)
                    if sql:
                        print(sql)
                elif isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) or \
                        isinstance(binlog_event, DeleteRowsEvent):
                    for row in binlog_event.rows:
                        sql = concat_sql_from_binlog_event(cursor=cur, binlog_event=binlog_event, row=row,
                                                           flashback=self.flashback, no_pk=self.no_pk,
                                                           e_start_pos=e_start_pos)
                        if self.flashback:
                            f_tmp.write(sql + '\n')
                        else:
                            print(sql)

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break
            f_tmp.close()

            if self.flashback:
                self.print_rollback_sql(tmp_file)
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


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    set_log_format()
    connectionSettings = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password}
    bin2sql = BinlogFile2sql(file_path=args.file_path[0], connection_settings=connectionSettings,
                             start_pos=args.startPos, end_pos=args.endPos,
                             start_time=args.startTime, stop_time=args.stopTime, only_schemas=args.databases,
                             only_tables=args.tables, no_pk=args.nopk, flashback=args.flashback,
                             stop_never=args.stopnever)
    bin2sql.process_binlog()
