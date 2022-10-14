# -*- coding: utf-8 -*-
import os
import pymysql
import struct
import argparse
import getpass
import sys
from pymysql.cursors import DictCursor
from utils.binlog2sql_util import is_valid_datetime, logger, sep, extend_parser
from pymysqlreplication.packet import BinLogPacketWrapper
from pymysqlreplication.constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT
from pymysqlreplication.event import (
    QueryEvent, RotateEvent, FormatDescriptionEvent,
    XidEvent, GtidEvent, StopEvent,
    BeginLoadQueryEvent, ExecuteLoadQueryEvent,
    HeartbeatLogEvent, NotImplementedEvent)
from pymysqlreplication.row_event import (
    UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent, TableMapEvent)

try:
    from pymysql.constants.COMMAND import COM_BINLOG_DUMP_GTID
except ImportError:
    # Handle old pymysql versions
    # See: https://github.com/PyMySQL/PyMySQL/pull/261
    COM_BINLOG_DUMP_GTID = 0x1e

from io import BytesIO
from pymysql.util import byte2int

# 2013 Connection Lost
# 2006 MySQL server has gone away
MYSQL_EXPECTED_ERROR_CODES = [2013, 2006]


class StringIOAdvance(BytesIO):
    def advance(self, length):
        self.seek(self.tell() + length)


class BinLogFileReader(object):
    """Connect to replication stream and read event
    """
    report_slave = None
    _expected_magic = b'\xfebin'

    def __init__(self, file_path, ctl_connection_settings=None, resume_stream=False, blocking=False, only_events=None,
                 log_file=None, log_pos=None, filter_non_implemented_events=True, stop_pos=None, ignored_events=None,
                 auto_position=None, only_tables=None, ignored_tables=None, only_schemas=None, ignored_schemas=None,
                 freeze_schema=False, skip_to_timestamp=None, slave_uuid=None, pymysql_wrapper=None,
                 fail_on_table_metadata_unavailable=False, slave_heartbeat=None, ignore_virtual_columns=False):

        # open file
        self._file = None
        self._file_path = file_path
        self._pos = None

        self.__connected_stream = False
        self.__connected_ctl = False
        self.__resume_stream = resume_stream
        self.__blocking = blocking
        self._ctl_connection = None
        self._ctl_connection_settings = ctl_connection_settings
        if ctl_connection_settings:
            self._ctl_connection_settings.setdefault("charset", "utf8mb4")

        self.__only_tables = only_tables
        self.__ignored_tables = ignored_tables
        self.__only_schemas = only_schemas
        self.__ignored_schemas = ignored_schemas
        self.__freeze_schema = freeze_schema
        self.__allowed_events = self._allowed_event_list(
            only_events, ignored_events, filter_non_implemented_events)
        self.__fail_on_table_metadata_unavailable = fail_on_table_metadata_unavailable

        # We can't filter on packet level TABLE_MAP and rotate event because
        # we need them for handling other operations
        self.__allowed_events_in_packet = frozenset(
            [TableMapEvent, RotateEvent]).union(self.__allowed_events)

        # Store table meta information
        self.table_map = {}
        self.log_pos = log_pos
        self.start_pos = log_pos
        self.stop_pos = stop_pos
        self.log_file = log_file
        self.auto_position = auto_position
        self.skip_to_timestamp = skip_to_timestamp

        self.report_slave = None
        self.slave_uuid = slave_uuid
        self.slave_heartbeat = slave_heartbeat
        self.ignore_virtual_columns = ignore_virtual_columns

        if pymysql_wrapper:
            self.pymysql_wrapper = pymysql_wrapper
        else:
            self.pymysql_wrapper = pymysql.connect

        # checksum with database
        self.__use_checksum = self.__checksum_enabled()

    def close(self):
        if self._file:
            self._file.close()
            self._file_path = None
        if self.__connected_ctl:
            self._ctl_connection._get_table_information = None
            self._ctl_connection.close()
            self.__connected_ctl = False

    def __connect_to_ctl(self):
        self._ctl_connection_settings["db"] = "information_schema"
        self._ctl_connection_settings["cursorclass"] = DictCursor
        self._ctl_connection = self.pymysql_wrapper(**self._ctl_connection_settings)
        self._ctl_connection._get_table_information = self.__get_table_information
        self.__connected_ctl = True

    def __checksum_enabled(self):
        """Return True if binlog-checksum = CRC32. Only for MySQL > 5.6"""
        try:
            if not self.__connected_ctl and self._ctl_connection_settings:
                self.__connect_to_ctl()

            cur = self._ctl_connection.cursor()
            cur.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")
            _result = cur.fetchone()
            cur.close()
            if _result is None:
                return False
            value = _result.get('Value', 'NONE')
            if value == 'NONE':
                return False
            return True
        except Exception:
            return False

    def __connect_to_stream(self):
        if self._file is None:
            self._file = open(self._file_path, 'rb+')
            self._pos = self._file.tell()
            assert self._pos == 0
        # read magic
        if self._pos == 0:
            magic = self._file.read(4)
            if magic == self._expected_magic:
                self._pos += len(magic)
            else:
                messagefmt = 'Magic bytes {0!r} did not match expected {1!r}'
                message = messagefmt.format(magic, self._expected_magic)
                raise BadMagicBytesError(message)

    def fetchone(self):
        while True:
            if not self._file:
                self.__connect_to_stream()

            if not self.__connected_ctl and self._ctl_connection_settings:
                self.__connect_to_ctl()

            # read pkt
            pkt = StringIOAdvance()
            # headerlength 19
            header = self._file.read(19)
            if not header:
                break

            unpacked = struct.unpack('<IcIIIH', header)
            timestamp = unpacked[0]
            event_type = byte2int(unpacked[1])
            server_id = unpacked[2]
            event_size = unpacked[3]
            log_pos = unpacked[4]
            flags = unpacked[5]

            body = self._file.read(event_size - 19)
            pkt.write(b'0')
            pkt.write(header)
            pkt.write(body)
            pkt.seek(0)

            binlog_event = BinLogPacketWrapper(pkt, self.table_map,
                                               self._ctl_connection,
                                               self.__use_checksum,
                                               self.__allowed_events_in_packet,
                                               self.__only_tables,
                                               self.__ignored_tables,
                                               self.__only_schemas,
                                               self.__ignored_schemas,
                                               self.__freeze_schema,
                                               self.__fail_on_table_metadata_unavailable)

            if not binlog_event.event or binlog_event.log_pos < self.start_pos:
                continue

            if self.stop_pos and binlog_event.log_pos >= self.stop_pos:
                break

            if binlog_event.event_type == ROTATE_EVENT:
                self.log_pos = binlog_event.event.position
                self.log_file = binlog_event.event.next_binlog
                # Table Id in binlog are NOT persistent in MySQL - they are in-memory identifiers
                # that means that when MySQL master restarts, it will reuse same table id for different tables
                # which will cause errors for us since our in-memory map will try to decode row data with
                # wrong table schema.
                # The fix is to rely on the fact that MySQL will also rotate to a new binlog file every time it
                # restarts. That means every rotation we see *could* be a sign of restart and so potentially
                # invalidates all our cached table id to schema mappings. This means we have to load them all
                # again for each logfile which is potentially wasted effort but we can't really do much better
                # without being broken in restart case
                self.table_map = {}
            elif binlog_event.log_pos:
                self.log_pos = binlog_event.log_pos

            # This check must not occur before clearing the ``table_map`` as a
            # result of a RotateEvent.
            #
            # The first RotateEvent in a binlog file has a timestamp of
            # zero.  If the server has moved to a new log and not written a
            # timestamped RotateEvent at the end of the previous log, the
            # RotateEvent at the beginning of the new log will be ignored
            # if the caller provided a positive ``skip_to_timestamp``
            # value.  This will result in the ``table_map`` becoming
            # corrupt.
            #
            # https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
            # From the MySQL Internals Manual:
            #
            #   ROTATE_EVENT is generated locally and written to the binary
            #   log on the master. It is written to the relay log on the
            #   slave when FLUSH LOGS occurs, and when receiving a
            #   ROTATE_EVENT from the master. In the latter case, there
            #   will be two rotate events in total originating on different
            #   servers.
            #
            #   There are conditions under which the terminating
            #   log-rotation event does not occur. For example, the server
            #   might crash.
            if self.skip_to_timestamp and binlog_event.timestamp < self.skip_to_timestamp:
                continue

            if binlog_event.event_type == TABLE_MAP_EVENT and \
                    binlog_event.event is not None:
                self.table_map[binlog_event.event.table_id] = \
                    binlog_event.event.get_table()

            # event is none if we have filter it on packet level
            # we filter also not allowed events
            if binlog_event.event is None or (binlog_event.event.__class__ not in self.__allowed_events):
                continue

            return binlog_event.event

    def _allowed_event_list(self, only_events, ignored_events,
                            filter_non_implemented_events):
        if only_events is not None:
            events = set(only_events)
        else:
            events = set((
                QueryEvent,
                RotateEvent,
                StopEvent,
                FormatDescriptionEvent,
                XidEvent,
                GtidEvent,
                BeginLoadQueryEvent,
                ExecuteLoadQueryEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                DeleteRowsEvent,
                TableMapEvent,
                HeartbeatLogEvent,
                NotImplementedEvent,
            ))
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)
        if filter_non_implemented_events:
            try:
                events.remove(NotImplementedEvent)
            except KeyError:
                pass
        return frozenset(events)

    def __get_table_information(self, schema, table):
        for i in range(1, 3):
            try:
                if not self.__connected_ctl:
                    self.__connect_to_ctl()

                cur = self._ctl_connection.cursor()
                if self.ignore_virtual_columns:
                    sql = """
                        SELECT
                            COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                            COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY, ORDINAL_POSITION
                        FROM
                            information_schema.columns
                        WHERE
                            EXTRA != 'VIRTUAL GENERATED'
                            AND table_schema = '%s' 
                            AND table_name = '%s'
                        ORDER BY ORDINAL_POSITION
                    """ % (schema, table)
                else:
                    sql = """
                        SELECT
                            COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                            COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY, ORDINAL_POSITION
                        FROM
                            information_schema.columns
                        WHERE
                            table_schema = '%s' 
                            AND table_name = '%s'
                        ORDER BY ORDINAL_POSITION
                    """ % (schema, table)

                cur.execute(sql)
                return cur.fetchall()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self.__connected_ctl = False
                    continue
                else:
                    raise error

    def __iter__(self):
        return iter(self.fetchone, None)


class BadMagicBytesError(Exception):
    '''The binlog file magic bytes did not match the specification'''
    pass


class EventSizeTooSmallError(Exception):
    '''The event size was smaller than the length of the event header'''
    pass


def parse_args():
    """parse args for binlog2sql"""
    parser = argparse.ArgumentParser(description='Parse MySQL binlog file to SQL you want', add_help=False,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)
    extend_parser(parser, True)

    binlog_file_filter = parser.add_argument_group('binlog file filter')
    binlog_file_filter.add_argument('-f', '--file-path', dest='file_path', type=str, nargs='*',
                                    help='Binlog file path.', default=[])
    binlog_file_filter.add_argument('-fd', '--file-dir', dest='file_dir', type=str,
                                    help='Binlog file dir. Please give us absolute path', default='')
    binlog_file_filter.add_argument('-fr', '--file-regex', dest='file_regex', type=str,
                                    help="Binlog file regex, use to find binlog file in file dir. "
                                         "(default is: mysql-bin.\\d+)",
                                    default='mysql-bin.\\d+')
    binlog_file_filter.add_argument('--start-file', dest='start_file', type=str,
                                    help='Start file in binlog file dir', default='')
    binlog_file_filter.add_argument('--stop-file', dest='stop_file', type=str,
                                    help='Stop file in binlog file dir', default='')
    binlog_file_filter.add_argument('--check', dest='check', action='store_true',
                                    help='Check binlog file list if you want', default=False)
    binlog_file_filter.add_argument('--supervisor', dest='supervisor', action='store_true',
                                    help="When use supervisor manage binlogfile2sql process, we won't exit "
                                         "if no file select", default=False)
    binlog_file_filter.add_argument('--record-file', dest='record_file', type=str, default='executed_records.txt',
                                    help='When you use --stop-never, we will save executed record in this file'
                                         '(Tip: we will ignore path if give a record file with relative path '
                                         'or absolute path, please use --result-dir to set path)')
    binlog_file_filter.add_argument('-ma', '--minutes-ago', dest='minutes_ago', type=int, default=3,
                                    help='When you use --stop-never, we only parse specify minutes ago of '
                                         'modify time of file.')

    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)

    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)

    if args.result_file and args.table_per_file:
        logger.error('Could not use --result-file and --table-per-file at the same time.')
        sys.exit(1)

    if args.result_file and sep in args.result_file:
        logger.warning('we will ignore path if give a result file with relative path or absolute path, '
                       'please use --result-dir to set path.')

    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or nopk can be True')
    if (args.start_time and not is_valid_datetime(args.start_time)) or (
            args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.check:
        if not args.password:
            args.password = getpass.getpass('Password: ')
        else:
            args.password = args.password[0]

        if args.sync:
            if not args.sync_password:
                args.sync_password = getpass.getpass('Sync Password: ')
            else:
                args.sync_password = args.sync_password[0]

    if args.minutes_ago < 1:
        logger.error('Args --minutes-ago must not lower than 1.')
        sys.exit(1)

    if (args.result_file or args.stop_never or args.table_per_file) and not os.path.exists(args.result_dir):
        os.makedirs(args.result_dir, exist_ok=True)
    args.result_file = os.path.join(args.result_dir, args.result_file.split(sep)[-1]) \
        if args.result_file and args.result_dir else args.result_file

    # record file 放到不同的目录里，防止起多个解析进程时冲突
    args.record_file = os.path.join(args.result_dir, args.record_file.split(sep)[-1]) \
        if args.record_file and args.result_dir else args.record_file

    return args
