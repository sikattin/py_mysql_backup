#!/usr/bin/python3
#-------------------------------------------------------------------------------
# Name:        local_backup.py
# Purpose:     for daily backup mysql.
#
# Author:      shikano.takeki
#
# Created:     22/12/2017
# Copyright:   (c) shikano.takeki 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# -*- coding: utf-8 -*-
## import modules.
# mysql operation
from py_mysql.mysql_custom import MySQLDB
# date operation
from datetime_skt.datetime_orig import dateArithmetic
# file/dir operation
from osfile import fileope
# logger
from mylogger import logger
from mylogger.factory import StdoutLoggerFactory, \
                             FileLoggerFactory, \
                             RotationLoggerFactory
# read/write operation to file
from iomod import rwfile
from my_utils import my_utils
# python standard modules
from os.path import split, join, getsize
from socket import gethostname
import subprocess
import time
import os


DUMP_OPTS = '--quick --quote-names'
DUMP_SP_OPTS = '--quote-names --routines --events --no-data --no-create-info'
FULLDUMP_OPTS = '--quote-names --skip-lock-tables --single-transaction --events --routines'
# base log file name.
LOGFILE = 'mariadb_dailybup.log'
# config file name.
# default config file path is <python lib>/dist|site-packages/daily_backup/config
CONFIG_FILE = 'backup.json'
INST_DIR = ''
CONFIG_PATH = ''

class localBackup(object):
    """
    """

    def __new__(cls, loglevel=None, handler=None):
        self = super().__new__(cls)
        # JSONファイルから各種データの読み込み、インスタンス変数にセット.
        self.parsed_json = {}
        self._get_pylibdir()
        self.rwfile = rwfile.RWFile()
        self.pj = rwfile.ParseJSON()
        self._load_json()
        self._set_data()
        self.logfile = "{0}/{1}".format(self.log_root, LOGFILE)

        # set up logger.
        # loglevel 20 = info level.
        if loglevel is None:
            loglevel = 20
        if handler is None:
            handler = 'rotation'
        self._logger = None
        self._handler = handler
        # ロガーを作る前にログファイル出力のディレクトリをあらかじめ作っておかなければ
        # エラーが出てしまうので改修予定
        # create file logger.
        if self._handler == 'file':
            filelogger_fac = FileLoggerFactory(loglevel=loglevel)
            self._logger = filelogger_fac.create(file=self.logfile)
        # create stdout logger.
        elif self._handler == 'console':
            stdlogger_fac = StdoutLoggerFactory(loglevel=loglevel)
            self._logger = stdlogger_fac.create()
        # create rotation logger.
        else:
            rotlogger_fac = RotationLoggerFactory(loglevel=loglevel)
            self._logger = rotlogger_fac.create(file=self.logfile, bcount=self.b_count)

        self.date_arith = dateArithmetic()
        self.year = self.date_arith.get_year()
        self.month = self.date_arith.get_month()
        self.day = self.date_arith.get_day()
        self.ym = "{0}{1}".format(self.year, self.month)
        self.md = "{0}{1}".format(self.month, self.day)
        self.ymd = "{0}{1}{2}".format(self.year, self.month, self.day)
        self.bk_dir = join(self.bk_root, self.ym)
        self.bk_dir = join(self.bk_dir, self.md)

        return self

    def _get_pylibdir(self):
            import daily_backup

            global INST_DIR, CONFIG_PATH
            INST_DIR = split(daily_backup.__file__)[0]
            CONFIG_PATH = "{0}/config/{1}".format(INST_DIR, CONFIG_FILE)

    def _load_json(self):
        """jsonファイルをパースする."""
        self.parsed_json = self.pj.load_json(file=r"{}".format(CONFIG_PATH))

    def _set_data(self):
        """パースしたJSONオブジェクトから必要なデータを変数にセットする."""
        import os
        # PATH
        self.bk_root = self.parsed_json['default_path']['BK_ROOT']
        self.log_root = self.parsed_json['default_path']['LOG_ROOT']
        self.log_file = os.path.join(self.log_root, LOGFILE)
        self.key_path = self.parsed_json['default_path']['KEY_PATH']
        self.cred_path = self.parsed_json['default_path']['CRED_PATH']

        # MYSQL
        self.myuser = self.parsed_json['mysql']['MYSQL_USER']
        self.mydb = self.parsed_json['mysql']['MYSQL_DB']
        self.myhost = self.parsed_json['mysql']['MYSQL_HOST']
        self.myport = self.parsed_json['mysql']['MYSQL_PORT']

        # Log
        self.b_count = self.parsed_json['log']['backup_count']

    def _decrypt_string(self, line: str):
        import codecs

        decrypted = codecs.decode(line, 'rot_13')
        return decrypted

    def _decrypt_credentialfile(self):
        cmd_decrypt = 'openssl rsautl -decrypt -inkey {key_path} -in {file_path}'.format(
                           key_path=self.key_path,
                           file_path=self.cred_path)
        try:
            result = subprocess.check_output(args=cmd_decrypt, shell=True)
        except subprocess.CalledProcessError as e:
            self._logger.error("Failed to decrypt credential file." \
                                "check {0} and {1} exists".format(self.cred_path,
                                                                  self.key_path))
            self._logger.error(e.output)
            raise e
        else:
            result = (result.decode()).rstrip()
            return result

    def _remove_old_backup(self, preserved_day=None):
        """旧バックアップデータを削除する.

        Args:
            param1 preserved_day: バックアップを保存しておく日数. デフォルトは3日
                type: int
        """
        if preserved_day is None:
            preserved_day = 3
        # バックアップルートにあるディレクトリ名一覧を取得する.
        dir_names = fileope.get_dir_names(dir_path=self.bk_root)
        if len(dir_names) == 0:
            return
        for dir_name in dir_names:
            # バックアップ用ディレクトリ以外は除外.
            if not self.rwfile.is_matched(line=dir_name, search_objs=['^[0-9]{6}$']):
                continue
            # 日毎のバックアップディレクトリ名一覧の取得.
            monthly_bkdir = join(self.bk_root, dir_name)
            daily_bkdirs = fileope.get_dir_names(dir_path=monthly_bkdir)
            # 日毎のバックアップディレクトリがひとつも存在しない場合は
            # 月毎のバックアップディレクトリ自体を削除する.
            if len(daily_bkdirs) == 0:
                fileope.f_remove_dirs(monthly_bkdir)
                self._logger.info("Delete old backup directory. {}".format(monthly_bkdir))
                continue
            for daily_bkdir in daily_bkdirs:
                # 現在の日付と対象となるディレクトリのタイムスタンプの日数差を計算する.
                backup_dir = "{0}/{1}".format(monthly_bkdir, daily_bkdir)
                sub_days = self.date_arith.subtract_target_from_now(backup_dir)
                self._logger.debug("sub_days = {}".format(sub_days))
                    # 作成されてから3日以上経過しているバックアップディレクトリを削除する.
                if sub_days >= preserved_day:
                    try:
                        fileope.f_remove_dirs(path=backup_dir)
                    except OSError as e:
                        error = "raise error! failed to trying remove {}".format(backup_dir)
                        self._logger.error(error)
                        raise e
                    else:
                        stdout = "remove old dump files: {}".format(backup_dir)
                        self._logger.info(stdout)

    ''' ログローテーション機能はLoggerモジュールで実装したためこれはさようなら

    def _remove_old_log(self, type, elapsed_days=None):
        """一定日数経過したログファイルを削除する.

        Args:
            param1 type: 削除対象のログを選択する.
                指定可能な値 ... 1 | 2
                1 ... 標準ログ
                2 ... エラーログ
            param1 elapsed_days: ログファイルを削除する規定経過日数. デフォルトは5日.
        """
        if type == 1:
            path = self.log_root
        elif type == 2:
            path = self.errlog_root
        else:
            raise TypeError("引数 'type' は 1 又は 2 を入力してください。")

        if elapsed_days is None:
            elapsed_days = 5

        # ログファイル格納ディレクトリからログファイル名一覧を取得する.
        log_files = fileope.get_file_names(dir_path=path)
        for log_file in log_files:
            target = "{0}{1}".format(path, log_file)
            # 現在の日付とログファイルのタイムスタンプを比較する.
            days = self.date_arith.subtract_target_from_now(target)
            # 5日以上経過しているログファイルは削除する.
            if days >= elapsed_days:
                try:
                    fileope.rm_filedir(path=target)
                except OSError as e:
                    error = "raise error! failed to trying remove file {}".format(target)
                    self.output_errlog(error)
                    raise e
                else:
                    stdout = "remove a old log file. {}".format(target)
                    self.output_logfile(stdout)
    '''

    def _mk_backupdir(self):
        """バックアップ用ディレクトリを作成する.
        """
        # make a directory for putting log files.
        if not fileope.dir_exists(path=r"{}".format(self.log_root)):
            try:
                fileope.make_dirs(path=r"{}".format(self.log_root))
            except OSError as e:
                error = "raise error! failed to trying create a log directory."
                self._logger.error(error)
                raise e
            else:
                self._logger.info("create a log directory: {}".format(self.log_root))
                # set permissions.
                from os import chmod
                from stat import S_IRUSR, S_IWUSR, S_IRGRP, S_IROTH
                mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
                chmod(path=self.log_root, mode=mode)

        # make a directory for db backup.
        dbs = self.get_dbs_and_tables()
        for db in dbs.keys():
            db_bkdir = fileope.join_path(self.bk_dir, db)
            if not fileope.dir_exists(path=r"{}".format(db_bkdir)):
                try:
                    fileope.make_dirs(path=r"{}".format(db_bkdir))
                except OSError as e:
                    error = "raise error! failed to trying create a backup directory."
                    self._logger.error(error)
                    raise e
                else:
                    self._logger.info("create a backup directory: {}".format(db_bkdir))

    def get_dbs_and_tables(self):
        """MYSQLに接続してデータベース名とテーブル名を取得する.

            Returns:
                データベース名とテーブル名を対応させた辞書.
                {'db1': (db1_table1, db1_table2, ...), 'db2': (db2_table1, ...)}
        """
        results = {}
        try:
            # MySQLに接続する.
            with MySQLDB(host=self.myhost,
                         dst_db=self.mydb,
                         myuser=self.myuser,
                         mypass=self._decrypt_credentialfile(),
                         port=self.myport) as mysqldb:
                # SHOW DATABASES;
                self._logger.info("Database names now acquireing...")
                sql = mysqldb.escape_statement("SHOW DATABASES;")
                cur_showdb = mysqldb.execute_sql(sql)
                for db_name in cur_showdb.fetchall():
                    for db_str in db_name:
                        # information_schema と peformance_schema DBはバックアップ対象から除外.
                        if db_str.lower() in {'information_schema', 'performance_schema'}:
                            continue
                        # DBに接続する.
                        mysqldb.change_database(db_str)
                        # SHOW TABLES;
                        sql = mysqldb.escape_statement("SHOW TABLES;")
                        cur_showtb = mysqldb.execute_sql(sql)
                        for table_name in cur_showtb.fetchall():
                            for table_str in table_name:
                                # 辞書にキーとバリューの追加.
                                results.setdefault(db_str, []).append(table_str)
        except:
            print("Failed to connect MySQL server.Please checking MYSQL section of configuration file " \
                  "{}".format(CONFIG_PATH))
            self._logger.error("Failed to connect MySQL server.")
            raise
        else:
            self._logger.info("succeeded acquireing database names.")
            return results

    def stop_slave(self):
        """Stop DB reeplication."""
        try:
            with MySQLDB(host=self.myhost,
                         dst_db=self.mydb,
                         myuser=self.myuser,
                         mypass=self._decrypt_credentialfile(),
                         port=self.myport) as mysqldb:
                self._logger.info("Stop Slave.")
                sql = mysqldb.escape_statement("STOP SLAVE;")
                try:
                    mysqldb.execute_sql(sql)
                except:
                    self._logger.info("Not Replication environment. stop slave is not run.")
        except:
            print("Failed to connect MySQL server.Please checking MYSQL section of configuration file " \
                  "{}".format(CONFIG_PATH))
            self._logger.error("Failed to connect MySQL server.")
            raise

    def start_slave(self):
        """Start DB reeplication."""
        try:
            with MySQLDB(host=self.myhost,
                         dst_db=self.mydb,
                         myuser=self.myuser,
                         mypass=self._decrypt_credentialfile(),
                         port=self.myport) as mysqldb:
                self._logger.info("Start Slave.")
                sql = mysqldb.escape_statement("START SLAVE;")
                try:
                    mysqldb.execute_sql(sql)
                except:
                    self._logger.info("Not Replication environment. start slave is not run.")
        except:
            print("Failed to connect MySQL server.Please checking MYSQL section of configuration file " \
                  "{}".format(CONFIG_PATH))
            self._logger.error("Failed to connect MySQL server.")
            raise

    def mk_cmd(self, params):
        """実行するLinuxコマンドを成形する.

        Args:
            param1 params: パラメータ.

        Return.
            tupple command.
        """
        self._logger.info("creating mysql dump command...")
        cmds = tuple()
        fulldump_cmd = ''
        for db, tables in params.items():
            for table in tables:
                self._logger.debug(table)
                output_path = "{0}/{1}/{2}_{3}.sql".format(self.bk_dir,
                                                           db,
                                                           self.ymd,
                                                           table)
                mysqldump_cmd = (
                                "mysqldump -u{0} -p'{1}' {2} {3} {4} > " \
                                "{5}".format(self.myuser,
                                             self._decrypt_credentialfile(),
                                             FULLDUMP_OPTS,
                                             db,
                                             table,
                                             output_path)
                                )
                split_cmd = mysqldump_cmd.split()
                cmds += (split_cmd,)
            # get a dump only SP.
            spdump_path = "{0}/{1}_{2}SP.sql".format(self.bk_dir, self.ymd, db)
            mysqldump_sp = "mysqldump -u{0} -p'{1}' {2} {3} > {4}".format(self.myuser,
                                                               self._decrypt_credentialfile(),
                                                               DUMP_SP_OPTS,
                                                               db,
                                                               spdump_path)
            cmds += (mysqldump_sp.split(),)
        return cmds

    def do_backup(self, exc_cmds: tuple):
        """mysqldumpコマンドをサーバで実行することによりバックアップを取得する.

            Args:
                param1 exc_cmd: 実行するコマンド タプル.

            Returns:

        """
        statement = "backup start. Date: {}".format(self.ymd)
        self._logger.info(statement)
        print(statement)
        for exc_cmd in exc_cmds:
            try:
                subprocess.check_call(args=' '.join(exc_cmd), shell=True)
            except subprocess.CalledProcessError as e:
                error = "an error occured during execution of following command.\n{}".format(e.cmd)
                self._logger.error(error)
            else:
                stdout = "mysqldump succeeded. dumpfile is saved {}".format(exc_cmd[len(exc_cmd) - 1])
                self._logger.info(stdout)
        self._logger.info("complete backup process.")

    def compress_backup(self, del_flag=None):
        """取得したバックアップファイルを圧縮処理にかける.

        Args:
            param1 del_flag: 圧縮後、元ファイルを削除するかどうかのフラグ.
                             デフォルトでは削除する.
        """
        self._logger.info("start compression.")
        if del_flag is None:
            del_flag = True
        # DBのディレクトリ名を取得.
        dir_list = fileope.get_dir_names(self.bk_dir)
        # gzip圧縮処理
        for dir_name in dir_list:
            target_dir = fileope.join_path(self.bk_dir, dir_name)
            file_list = fileope.get_file_names(r'{}'.format(target_dir))
            for file_name in file_list:
                target_file = fileope.join_path(target_dir, file_name)
                try:
                    fileope.compress_gz(r'{}'.format(target_file))
                except OSError as oserr:
                    error = oserr.strerror
                    # output error line... "Error: {} failed to compress.".format(target_file))
                    self._logger.error(error)
                except ValueError as valerr:
                    error = valerr
                    # "Error: {} failed to compress.".format(target_file))
                    self._logger.error(error)
                else:
                    if del_flag:
                        fileope.rm_filedir(target_file)
        self._logger.info("complete compressing dump files.")

    def main(self):
        """main.
        """
        start = time.time()
        # バックアップ用ディレクトリの作成.
        self._mk_backupdir()
        # 旧バックアップデータの削除.
        self._remove_old_backup()
        # ログファイルの削除.
        #self._remove_old_log(type=1)
        #self._remove_old_log(type=2)
        # DB名とテーブル名一覧の取得.
        dbs_tables = self.get_dbs_and_tables()
        # 実行するLinuxコマンドを生成.
        commands = self.mk_cmd(params=dbs_tables)
        # stop replication
        self.stop_slave()
        # mysqldumpの実行.
        self.do_backup(commands)
        
        # start replication.
        self.start_slave()
        # 圧縮処理
        self.compress_backup()

        elapsed_time = time.time() - start
        line = "elapsed time is {0} sec. {1} finished.".format(elapsed_time, __file__)
        self._logger.info(line)
        print(line)
        # close
        #self._logger.close()
        # 転送先の定期的なファイル削除処理と転送元のzipファイルを削除する。

if __name__ == '__main__':
    import argparse
    import daily_backup

    def parse_json():
        parse_j = rwfile.ParseJSON()
        obj_json = {}
        obj_json = parse_j.load_json(file=CONFIG_PATH)
        return obj_json

    def set_path(obj_json: dict):
        # Log file PATH.
        global log_path, sshconn_info
        log_path = {"log_root": obj_json['default_path']['LOG_ROOT']}
        sshconn_info = {"Enabled": obj_json['ssh']['Enabled'],
                        "ssh_host": obj_json['ssh']['hostname'],
                        "ssh_user": obj_json['ssh']['username'],
                        "private_key": obj_json['ssh']['private_key'],
                        "remote_path": obj_json['ssh']['remote_path']}
        if not isinstance(sshconn_info["ssh_host"], list):
            sshconn_info["ssh_host"] = [sshconn_info["ssh_host"]]

    lib_dir = split(daily_backup.__file__)[0]
    with open(fileope.join_path(lib_dir, 'README')) as f:
        description = f.read()

    epilog = 'example command.\n\n' \
             '`python3 local_backup.py` ... log is outputted to specified file. log level is INFO.\n' \
             '`python3 local_backup.py --loglevel 30 --handler console` ... log is outputted to console.' \
             'log level is WARNING'

    argparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                        description=description,
                                        epilog=epilog)
    argparser.add_argument('-l', '--loglevel', type=int, required=False,
                           default=20,
                           help='log level. need to set int value 10 ~ 50.\n' \
                           'default is INFO. 10:DEBUG, 20:INFO, 30:WARNING, 40:ERROR, 50:CRITICAL')
    argparser.add_argument('-H', '--handler', type=str, required=False,
                           default='rotation',
                           help="settings the handler of log outputed.\n" \
                                "default handler is 'rotation'. log is outputed in file.\n" \
                                 "a valid value is 'file' | 'console' | 'rotation'")
    argparser.add_argument('--upload_s3', action='store_true',
                                          required=False,
                                          help='enable to upload to S3 bucket'
    )
    argparser.add_argument('--key_name', metavar='<KEY_NAME>',
                                         type=str,
                                         required=False,
                                         default=None,
                                         help='Key name of the uploading dump file' \
                                         'default value is None' \
                                         'dump will be uploaded root of bucket by default.'
    )
    argparser.add_argument('--no_compress', action='store_true',
                           required=False,
                           help='not compress the target file.')
    args = argparser.parse_args()

    db_backup = localBackup(loglevel=args.loglevel, handler=args.handler)
    db_backup.main()

    # Log file path
    log_path = {}
    # information about SSH connection
    sshconn_info = {}
    conf_s3 = {}
    conf_mail = {}
    json_dict = parse_json()
    set_path(json_dict)
    conf_s3 = {"bucket": json_dict['s3']['bucket']}
    conf_mail = {"from_addr": json_dict['mail']['from_address'],
                    "to_addr": json_dict['mail']['to_address'],
                    "cc_addr": json_dict['mail']['cc_address'],
                    "smtp_server": json_dict['mail']['smtp_server'],
                    "ses_access": json_dict['mail']['ses_access'],
                    "ses_secret": json_dict['mail']['ses_secret'],
                    "smtp_port": json_dict['mail']['smtp_port']
    }
    mail_args = (conf_mail['from_addr'],
                 conf_mail['to_addr'],
                 conf_mail['cc_addr'])

    if sshconn_info["Enabled"]:
        # transfer data local to remote
        from datatransfer.datatransfer import DataTransfer
        for host in sshconn_info["ssh_host"]:
            d_trans = DataTransfer(hostname=host,
                                   username=sshconn_info["ssh_user"],
                                   keyfile_path=sshconn_info["private_key"],
                                   logger=db_backup._logger,
                                   loglevel=args.loglevel)
            d_trans.transfer_files(targets=[db_backup.bk_dir],
                                   remote_path=sshconn_info["remote_path"],
                                   delete=True)
    logger = db_backup._logger
    # create S3Uploader Object
    if args.upload_s3:
        from transfer_s3.transfer_s3 import TransferS3Notification
        
        src = db_backup.bk_dir
        key_name = args.key_name
        try:
            trans_s3 = TransferS3Notification(conf_s3['bucket'],
                                            conf_mail['smtp_server'],
                                            *mail_args,
                                            smtp_port=conf_mail['smtp_port'],
                                            logger=logger,
                                            handler=args.handler,
                                            ses_accesskey=conf_mail['ses_access'],
                                            ses_secretkey=conf_mail['ses_secret'],
                                            is_ses_auth=True,
                                            is_remove=True)
            if not args.no_compress:
                src = trans_s3.compress_srcfile(src)
            trans_s3.upload(src, key_name=key_name)
        except Exception as e:
            logger.error("Failed uploading to S3!")
            logger.exception(str(e))
            raise e
        finally:
            # logger close
            db_backup._logger.close()
            logger.info("Finished daily backup.")

