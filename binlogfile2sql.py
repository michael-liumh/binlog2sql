#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import sys
import time
import pymysql
import re
from binlogfile2sql_util import command_line_args, BinLogFileReader, get_binlog_file_list, timestamp_to_datetime, \
    save_executed_result
from binlog2sql_util import concat_sql_from_binlog_event, is_dml_event, event_type, logger, set_log_format, \
    get_gtid_set, is_want_gtid, save_result_sql, dt_now
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent, GtidEvent

sep = '/' if '/' in sys.argv[0] else os.sep


class BinlogFile2sql(object):
    def __init__(self, file_path, connection_settings, start_pos=None, end_pos=None,
                 start_time=None, stop_time=None, only_schemas=None, only_tables=None, no_pk=False,
                 flashback=False, stop_never=False, only_dml=True, sql_type=None, result_dir=None, need_comment=1,
                 rename_db=None, only_pk=False, result_file=None, table_per_file=False, insert_ignore=False,
                 ignore_databases=None, ignore_tables=None, ignore_columns=None, replace=False,
                 ignore_virtual_columns=False, file_index=0, remove_not_update_col=False,
                 include_gtids=None, exclude_gtids=None):
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
        self.only_pk = only_pk
        self.result_file = result_file
        self.table_per_file = table_per_file
        self.ignore_databases = ignore_databases
        self.ignore_tables = ignore_tables
        self.ignore_columns = ignore_columns
        self.replace = replace
        self.insert_ignore = insert_ignore
        self.ignore_virtual_columns = ignore_virtual_columns
        self.file_index = file_index
        self.remove_not_update_col = remove_not_update_col
        self.gtid_set = get_gtid_set(include_gtids, exclude_gtids)

    def process_binlog(self):
        stream = BinLogFileReader(self.file_path, ctl_connection_settings=self.connection_settings,
                                  log_pos=self.start_pos, only_schemas=self.only_schemas, stop_pos=self.end_pos,
                                  only_tables=self.only_tables, ignored_schemas=self.ignore_databases,
                                  ignored_tables=self.ignore_tables, ignore_virtual_columns=self.ignore_virtual_columns)
        cur = self.connection.cursor()

        result_sql_file = ''
        f_result_sql_file = ''
        if self.stop_never and not self.table_per_file:
            result_sql_file = self.file_path.split(sep)[-1].replace('.', '_').replace('-', '_') + '.sql'
            result_sql_file = os.path.join(self.result_dir, result_sql_file)
        elif self.result_file and not self.table_per_file:
            result_sql_file = self.result_file

        mode = 'w' if self.file_index == 0 else 'a'
        if result_sql_file and not self.table_per_file:
            if self.file_index == 0:
                save_result_sql(result_sql_file, '', mode)
                logger.info(f'Saving result into file: [{result_sql_file}]')
            f_result_sql_file = open(result_sql_file, mode)

        if self.table_per_file:
            logger.info(f'Saving table per file into dir: [{self.result_dir}]')

        binlog_gtid = ''
        gtid_set = True if self.gtid_set else False
        flag_last_event = False
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        try:
            for binlog_event in stream:
                if not self.stop_never:
                    try:
                        event_time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
                    except OSError:
                        event_time = datetime.datetime(1980, 1, 1, 0, 0)
                    if event_time < self.start_time:
                        if not (isinstance(binlog_event, RotateEvent)
                                or isinstance(binlog_event, FormatDescriptionEvent)):
                            last_pos = binlog_event.packet.log_pos
                        continue
                    elif (self.end_pos and stream.log_pos > self.end_pos) or \
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
                        cursor=cur, binlog_event=binlog_event, flashback=self.flashback, no_pk=self.no_pk,
                        rename_db=self.rename_db, only_pk=self.only_pk, only_return_sql=False,
                        ignore_columns=self.ignore_columns, replace=self.replace, insert_ignore=self.insert_ignore,
                        ignore_virtual_columns=self.ignore_virtual_columns, binlog_gtid=binlog_gtid,
                        remove_not_update_col=self.remove_not_update_col
                    )
                    if sql:
                        if self.need_comment != 1:
                            sql = re.sub('; #.*', ';', sql)
                        if f_result_sql_file:
                            f_result_sql_file.write(sql + '\n')
                        elif self.table_per_file and db and table:
                            result_sql_file = os.path.join(self.result_dir, db + '.' + table + f'_{dt_now()}.sql')
                            save_result_sql(result_sql_file, sql + '\n')
                        elif self.table_per_file:
                            result_sql_file = os.path.join(self.result_dir, f'others_{dt_now()}.sql')
                            save_result_sql(result_sql_file, sql + '\n')
                        else:
                            print(sql)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        if binlog_gtid and gtid_set and not is_want_gtid(self.gtid_set, binlog_gtid):
                            continue

                        sql, db, table = concat_sql_from_binlog_event(
                            cursor=cur, binlog_event=binlog_event, row=row, flashback=self.flashback, no_pk=self.no_pk,
                            e_start_pos=e_start_pos, rename_db=self.rename_db, only_pk=self.only_pk,
                            only_return_sql=False, ignore_columns=self.ignore_columns, replace=self.replace,
                            insert_ignore=self.insert_ignore, ignore_virtual_columns=self.ignore_virtual_columns,
                            remove_not_update_col=self.remove_not_update_col, binlog_gtid=binlog_gtid,
                        )
                        if sql:
                            if self.need_comment != 1:
                                sql = re.sub('; #.*', ';', sql)

                            if f_result_sql_file:
                                f_result_sql_file.write(sql + '\n')
                            elif self.table_per_file and db and table:
                                result_sql_file = os.path.join(self.result_dir, db + '.' + table + f'_{dt_now()}.sql')
                                save_result_sql(result_sql_file, sql + '\n')
                            elif self.table_per_file:
                                result_sql_file = os.path.join(self.result_dir, f'others_{dt_now()}.sql')
                                save_result_sql(result_sql_file, sql + '\n')
                            else:
                                print(sql)

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break

        finally:
            if f_result_sql_file:
                f_result_sql_file.close()
            stream.close()
            cur.close()
        return True

    def __del__(self):
        pass


def main(args):
    connection_settings = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password}
    binlog_file_list, executed_file_list = get_binlog_file_list(args)

    if args.check:
        from pprint import pprint
        pprint(binlog_file_list)
        sys.exit(1)

    if not binlog_file_list:
        logger.error('No file select.')
        if not args.supervisor:
            sys.exit(1)

    if not args.stop_never:
        for i, binlog_file in enumerate(binlog_file_list):
            logger.info('parsing binlog file: %s [%s]' %
                        (binlog_file, timestamp_to_datetime(os.stat(binlog_file).st_mtime)))
            bin2sql = BinlogFile2sql(
                file_path=binlog_file, connection_settings=connection_settings, start_pos=args.start_pos,
                end_pos=args.end_pos, start_time=args.start_time, stop_time=args.stop_time, only_schemas=args.databases,
                need_comment=args.need_comment, only_tables=args.tables, no_pk=args.no_pk, flashback=args.flashback,
                only_dml=args.only_dml, sql_type=args.sql_type, rename_db=args.rename_db, only_pk=args.only_pk,
                result_file=args.result_file, table_per_file=args.table_per_file, result_dir=args.result_dir,
                ignore_databases=args.ignore_databases, ignore_tables=args.ignore_tables,
                ignore_columns=args.ignore_columns, replace=args.replace, insert_ignore=args.insert_ignore,
                ignore_virtual_columns=args.ignore_virtual_columns, file_index=i,
                remove_not_update_col=args.remove_not_update_col,
                include_gtids=args.include_gtids, exclude_gtids=args.exclude_gtids,
            )
            bin2sql.process_binlog()
    else:
        while True:
            for binlog_file in binlog_file_list:
                logger.info('parsing binlog file: %s [%s]' %
                            (binlog_file, timestamp_to_datetime(os.stat(binlog_file).st_mtime)))
                bin2sql = BinlogFile2sql(
                    file_path=binlog_file, connection_settings=connection_settings, start_pos=args.start_pos,
                    end_pos=args.end_pos, start_time=args.start_time, stop_time=args.stop_time,
                    only_schemas=args.databases, result_dir=args.result_dir, only_tables=args.tables, no_pk=args.no_pk,
                    flashback=args.flashback, only_dml=args.only_dml, sql_type=args.sql_type,
                    stop_never=args.stop_never, need_comment=args.need_comment, rename_db=args.rename_db,
                    only_pk=args.only_pk, result_file=args.result_file, table_per_file=args.table_per_file,
                    ignore_databases=args.ignore_databases, ignore_tables=args.ignore_tables,
                    ignore_columns=args.ignore_columns, replace=args.replace, insert_ignore=args.insert_ignore,
                    ignore_virtual_columns=args.ignore_virtual_columns,
                    remove_not_update_col=args.remove_not_update_col,
                    include_gtids=args.include_gtids, exclude_gtids=args.exclude_gtids,
                )
                r = bin2sql.process_binlog()
                if r is True:
                    executed_file_list.append(binlog_file)
                    save_executed_result(args.record_file, executed_file_list)
            binlog_file_list, executed_file_list = get_binlog_file_list(args)
            if not binlog_file_list:
                # logger.info('All file has been executed, sleep 60 seconds to get other new files.')
                time.sleep(60)


if __name__ == '__main__':
    command_line_args = command_line_args(sys.argv[1:])
    set_log_format()
    main(command_line_args)
