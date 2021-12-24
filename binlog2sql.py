#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import sys
import datetime
import pymysql
import os
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent, GtidEvent
from binlog2sql_util import command_line_args, concat_sql_from_binlog_event, is_dml_event, event_type, logger, \
    set_log_format, get_gtid_set, is_want_gtid
from binlogfile2sql_util import save_result_sql

sep = '/' if '/' in sys.argv[0] else os.sep


class Binlog2sql(object):

    def __init__(self, connection_settings, start_file=None, start_pos=None, end_file=None, end_pos=None,
                 start_time=None, stop_time=None, only_schemas=None, only_tables=None, no_pk=False,
                 flashback=False, stop_never=False, back_interval=1.0, only_dml=True, sql_type=None,
                 need_comment=1, rename_db=None, only_pk=False, ignore_databases=None, ignore_tables=None,
                 ignore_columns=None, replace=False, insert_ignore=False, remove_not_update_col=False,
                 table_per_file=False, result_file=None, result_dir=None,
                 include_gtids=None, exclude_gtids=None):
        """
        conn_setting: {'host': 127.0.0.1, 'port': 3306, 'user': user, 'passwd': passwd, 'charset': 'utf8'}
        """

        if not start_file:
            raise ValueError('Lack of parameter: start_file')

        self.conn_setting = connection_settings
        self.start_file = start_file
        self.start_pos = start_pos if start_pos else 4  # use binlog v4
        self.end_file = end_file if end_file else start_file
        self.end_pos = end_pos
        if start_time:
            self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.datetime.strptime('1980-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        if stop_time:
            self.stop_time = datetime.datetime.strptime(stop_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.stop_time = datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.no_pk, self.flashback, self.stop_never, self.back_interval = (no_pk, flashback, stop_never, back_interval)
        self.only_dml = only_dml
        self.sql_type = [t.upper() for t in sql_type] if sql_type else []

        self.binlogList = []
        self.connection = pymysql.connect(**self.conn_setting)
        self.need_comment = need_comment
        self.rename_db = rename_db
        self.only_pk = only_pk
        self.ignore_databases = ignore_databases
        self.ignore_tables = ignore_tables
        self.ignore_columns = ignore_columns
        self.replace = replace
        self.insert_ignore = insert_ignore
        self.remove_not_update_col = remove_not_update_col
        self.result_file = result_file
        self.result_dir = result_dir
        self.table_per_file = table_per_file
        self.gtid_set = get_gtid_set(include_gtids, exclude_gtids)

        with self.connection as cursor:
            cursor.execute("SHOW MASTER STATUS")
            self.eof_file, self.eof_pos = cursor.fetchone()[:2]
            cursor.execute("SHOW MASTER LOGS")
            bin_index = [row[0] for row in cursor.fetchall()]
            if self.start_file not in bin_index:
                raise ValueError('parameter error: start_file %s not in mysql server' % self.start_file)
            binlog2i = lambda x: x.split('.')[1]
            for binary in bin_index:
                if binlog2i(self.start_file) <= binlog2i(binary) <= binlog2i(self.end_file):
                    self.binlogList.append(binary)

            cursor.execute("SELECT @@server_id")
            self.server_id = cursor.fetchone()[0]
            if not self.server_id:
                raise ValueError('missing server_id in %s:%s' % (self.conn_setting['host'], self.conn_setting['port']))

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.conn_setting, server_id=self.server_id,
                                    log_file=self.start_file, log_pos=self.start_pos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, resume_stream=True, blocking=True,
                                    ignored_schemas=self.ignore_databases, ignored_tables=self.ignore_tables)

        f_result_sql_file = ''
        mode = 'w'
        if self.result_file:
            result_sql_file = self.result_file
            logger.info(f'Saving result into file: [{result_sql_file}]')
            f_result_sql_file = open(result_sql_file, mode)
        elif self.table_per_file:
            logger.info(f'Saving table per file into dir: [{self.result_dir}]')

        binlog_gtid = ''
        gtid_set = True if self.gtid_set else False
        flag_last_event = False
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        with self.connection as cursor:
            for binlog_event in stream:
                # 返回的 EVENT 顺序
                # RotateEvent
                # FormatDescriptionEvent
                # GtidEvent
                # QueryEvent
                # TableMapEvent
                # UpdateRowsEvent
                # XidEvent
                # GtidEvent
                # QueryEvent
                # TableMapEvent
                # UpdateRowsEvent
                # XidEvent
                # GtidEvent
                # ...
                if not self.stop_never:
                    try:
                        event_time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
                    except OSError:
                        event_time = datetime.datetime(1980, 1, 1, 0, 0)
                    if (stream.log_file == self.end_file and stream.log_pos == self.end_pos) or \
                            (stream.log_file == self.eof_file and stream.log_pos == self.eof_pos):
                        flag_last_event = True
                    elif event_time < self.start_time:
                        if not (isinstance(binlog_event, RotateEvent)
                                or isinstance(binlog_event, FormatDescriptionEvent)):
                            last_pos = binlog_event.packet.log_pos
                        continue
                    elif (stream.log_file not in self.binlogList) or \
                            (self.end_pos and stream.log_file == self.end_file and stream.log_pos > self.end_pos) or \
                            (stream.log_file == self.eof_file and stream.log_pos > self.eof_pos) or \
                            (event_time >= self.stop_time):
                        break
                    # else:
                    #     raise ValueError('unknown binlog file or position')

                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, GtidEvent):
                    binlog_gtid = str(binlog_event.gtid)

                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    if binlog_gtid and gtid_set and not is_want_gtid(self.gtid_set, binlog_gtid):
                        continue

                    sql, db, table = concat_sql_from_binlog_event(
                        cursor=cursor, binlog_event=binlog_event, only_return_sql=False,
                        flashback=self.flashback, no_pk=self.no_pk, rename_db=self.rename_db, only_pk=self.only_pk,
                        ignore_columns=self.ignore_columns, replace=self.replace, insert_ignore=self.insert_ignore,
                        remove_not_update_col=self.remove_not_update_col, binlog_gtid=binlog_gtid
                    )
                    if sql:
                        if self.need_comment != 1:
                            sql = re.sub('; #.*', ';', sql)

                        if f_result_sql_file:
                            f_result_sql_file.write(sql + '\n')
                        elif self.table_per_file and db and table:
                            result_sql_file = os.path.join(self.result_dir, db + '.' + table + '.sql')
                            save_result_sql(result_sql_file, sql + '\n')
                        elif self.table_per_file:
                            result_sql_file = os.path.join(self.result_dir, 'others.sql')
                            save_result_sql(result_sql_file, sql + '\n')
                        else:
                            print(sql)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        if binlog_gtid and gtid_set and not is_want_gtid(self.gtid_set, binlog_gtid):
                            continue

                        sql, db, table = concat_sql_from_binlog_event(
                            cursor=cursor, binlog_event=binlog_event, no_pk=self.no_pk, row=row,
                            flashback=self.flashback, e_start_pos=e_start_pos, rename_db=self.rename_db,
                            only_pk=self.only_pk, ignore_columns=self.ignore_columns, replace=self.replace,
                            insert_ignore=self.insert_ignore, remove_not_update_col=self.remove_not_update_col,
                            only_return_sql=False, binlog_gtid=binlog_gtid,
                        )
                        try:
                            if sql:
                                if self.need_comment != 1:
                                    sql = re.sub('; #.*', ';', sql)

                                if f_result_sql_file:
                                    f_result_sql_file.write(sql + '\n')
                                elif self.table_per_file and db and table:
                                    result_sql_file = os.path.join(self.result_dir, db + '.' + table + '.sql')
                                    save_result_sql(result_sql_file, sql + '\n')
                                elif self.table_per_file:
                                    result_sql_file = os.path.join(self.result_dir, 'others.sql')
                                    save_result_sql(result_sql_file, sql + '\n')
                                else:
                                    print(sql)
                        except Exception:
                            logger.exception('')
                            logger.error('Error sql: %s' % sql)
                            continue

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break

            stream.close()
        return True

    def __del__(self):
        pass


def main(args):
    conn_setting = {
        'host': args.host,
        'port': args.port,
        'user': args.user,
        'passwd': args.password,
        'charset': 'utf8mb4'
    }

    binlog2sql = Binlog2sql(
        connection_settings=conn_setting, start_file=args.start_file, start_pos=args.start_pos,
        end_file=args.end_file, end_pos=args.end_pos, start_time=args.start_time,
        stop_time=args.stop_time, only_schemas=args.databases, only_tables=args.tables,
        no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never,
        back_interval=args.back_interval, only_dml=args.only_dml, sql_type=args.sql_type,
        need_comment=args.need_comment, rename_db=args.rename_db, only_pk=args.only_pk,
        ignore_databases=args.ignore_databases, ignore_tables=args.ignore_tables,
        ignore_columns=args.ignore_columns, replace=args.replace, insert_ignore=args.insert_ignore,
        remove_not_update_col=args.remove_not_update_col, table_per_file=args.table_per_file,
        result_file=args.result_file, result_dir=args.result_dir,
        include_gtids=args.include_gtids, exclude_gtids=args.exclude_gtids,
    )
    binlog2sql.process_binlog()


if __name__ == '__main__':
    command_line_args = command_line_args(sys.argv[1:])
    set_log_format()
    main(command_line_args)
