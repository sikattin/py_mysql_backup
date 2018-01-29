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
from py_mysql.mysql_custom import MySQLDB
from datetime_skt.datetime_orig import dateArithmetic
from osfile import fileope
from mylogger.logger import Logger
from iomod import rwfile
from connection.sshconn import SSHConn
import subprocess
import time

# バックアップ用フォルダのルート
BK_ROOT = '/data2/backup/'
LOG_ROOT = '/data2/backup/daily_backuplog/'
ERRLOG_ROOT = '/data2/backup/error_log/'
MYSQL_USER = "root"
MYSQL_PASSWORD = "mYXe2S6ejG1x5kW!"
MYSQL_DB = "mysql"
MYSQL_HOST = "localhost"
MYSQL_PORT = "13306"
CONFIG_FILE = '/etc/backup/backup.json'

class localBackup(object):
    """
    """

    def __new__(cls, loglevel=None):
        self = super().__new__(cls)
        # ロガーのセットアップ.
        if loglevel is None:
            loglevel = 30
        Logger.loglevel = loglevel
        self._logger = Logger(str(self))

        self.rwfile = rwfile.RWFile()
        self.pj = rwfile.ParseJSON()

        self.date_arith = dateArithmetic()
        self.year = self.date_arith.get_year()
        self.month = self.date_arith.get_month()
        self.day = self.date_arith.get_day()
        self.ym = "{0}{1}".format(self.year, self.month)
        self.md = "{0}{1}".format(self.month, self.day)
        self.ymd = "{0}{1}{2}".format(self.year, self.month, self.day)
        self.bk_dir = "{0}{1}/{2}".format(BK_ROOT, self.ym, self.md)

        self.parsed_json = {}
        self._load_json()
        return self

    def _load_json(self):
        """jsonファイルをパースする."""
        self.parsed_json = self.pj.load_json(file=r"{}".format(CONFIG_FILE))

    def _set_data(self):
        """パースしたJSONオブジェクトから必要なデータを変数にセットする."""
        self.bk_root = self.parsed_json['default_path']['BK_ROOT']
        self.log_root = self.parsed_json['default_path']['LOG_ROOT']
        self.errlog_root = self.parsed_json['default_path']['ERRLOG_ROOT']

        self.myuser = self.parsed_json['mysql']['MYSQL_USER']
        self.mypass = self.parsed_json['mysql']['MYSQL_PASSWORD']
        self.mydb = self.parsed_json['mysql']['MYSQL_DB']
        self.myhost = self.parsed_json['mysql']['MYSQL_HOST']
        self.myport = self.parsed_json['mysql']['MYSQL_PORT']

    def _remove_old_backup(self, preserved_day=None):
        """旧バックアップデータを削除する.

        Args:
            param1 preserved_day: バックアップを保存しておく日数. デフォルトは3日
                type: int
        """
        if preserved_day is None:
            preserved_day = 3
        # バックアップルートにあるディレクトリ名一覧を取得する.
        dir_names = fileope.get_dir_names(dir_path=BK_ROOT)
        if len(dir_names) == 0:
            return
        for dir_name in dir_names:
            # バックアップ用ディレクトリ以外は除外.
            if not self.rwfile.is_matched(line=dir_name, search_objs=['^[0-9]{6}$']):
                continue
            # 日毎のバックアップディレクトリ名一覧の取得.
            monthly_bkdir = "{0}{1}".format(BK_ROOT, dir_name)
            daily_bkdirs = fileope.get_dir_names(dir_path=monthly_bkdir)
            # 日毎のバックアップディレクトリがひとつも存在しない場合は
            # 月毎のバックアップディレクトリ自体を削除する.
            if len(daily_bkdirs) == 0:
                fileope.remove_dir(monthly_bkdir)
                continue
            for daily_bkdir in daily_bkdirs:
                # 現在の日付と対象となるディレクトリのタイムスタンプの日数差を計算する.
                backup_dir = "{0}/{1}".format(monthly_bkdir, daily_bkdir)
                sub_days = self.date_arith.subtract_target_from_now(backup_dir)
                self._logger.debug("sub_days = {}".format(sub_days))
                    # 作成されてから3日以上経過しているバックアップディレクトリを削除する.
                    # ログファイルも削除する.
                if sub_days >= preserved_day:
                    try:
                        fileope.f_remove_dirs(path=backup_dir)
                    except OSError as e:
                        error = "raise error! failed to trying remove {}".format(backup_dir)
                        self.output_errlog(error)
                        raise e
                    else:
                        stdout = "remove old backup files. {}".format(backup_dir)
                        self.output_logfile(stdout)

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
            path = LOG_ROOT
        elif type == 2:
            path = ERRLOG_ROOT
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

    def _mk_backupdir(self):
        """バックアップ用ディレクトリを作成する.
        """
        dbs = self.get_dbs_and_tables()
        for db in dbs.keys():
            db_bkdir = "{0}/{1}".format(self.bk_dir, db)
            if not fileope.dir_exists(path=r"{}".format(db_bkdir)):
                try:
                    fileope.make_dirs(path=r"{}".format(db_bkdir))
                except OSError as e:
                    error = "raise error! failed to trying create a backup directory. "
                    self.output_errlog(error)
                else:
                    self.output_logfile("create a backup directory: {}".format(db_bkdir))
        if not fileope.dir_exists(path=r"{}".format(ERRLOG_ROOT)):
            fileope.make_dirs(path=r"{}".format(ERRLOG_ROOT))
        if not fileope.dir_exists(path=r"{}".format(LOG_ROOT)):
            fileope.make_dirs(path=r"{}".format(LOG_ROOT))

    def get_dbs_and_tables(self):
        """MYSQLに接続してデータベース名とテーブル名を取得する.

            Returns:
                データベース名とテーブル名を対応させた辞書.
                {'db1': (db1_table1, db1_table2, ...), 'db2': (db2_table1, ...)}
        """
        results = {}
        # MySQLに接続する.
        with MySQLDB(host=MYSQL_HOST,
                     dst_db=MYSQL_DB,
                     myuser=MYSQL_USER,
                     mypass=MYSQL_PASSWORD,
                     port=MYSQL_PORT) as mysqldb:
            # SHOW DATABASES;
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
        return results

    def mk_cmd(self, params):
        """実行するLinuxコマンドを成形する.

        Args:
            param1 params: パラメータ.

        Return.
            tupple command.
        """
        cmds = tuple()
        for db, tables in params.items():
            for table in tables:
                self._logger.debug(table)
                output_path = "{0}/{1}/{2}_{3}.sql".format(self.bk_dir,
                                                           db,
                                                           self.ymd,
                                                           table)
                mysqldump_cmd = (
                                "mysqldump -u{0} -p{1} -q --skip-opt -R {2} {3} > "
                                "{4}".format(MYSQL_USER,
                                             MYSQL_PASSWORD,
                                             db,
                                             table,
                                             output_path)
                                )
                split_cmd = mysqldump_cmd.split()
                cmds += (split_cmd,)

        return cmds

    def do_backup(self, exc_cmds: tuple):
        """mysqldumpコマンドをサーバで実行することによりバックアップを取得する.

            Args:
                param1 exc_cmd: 実行するコマンド タプル.

            Returns:

        """
        self.output_logfile("backup start. Date: {}".format(self.ymd))
        for exc_cmd in exc_cmds:
            try:
                subprocess.check_call(args=' '.join(exc_cmd), shell=True)
            except subprocess.CalledProcessError as e:
                self._logger.debug("raise error!")
                self._logger.debug("executed command: {}".format(e.cmd))
                self._logger.debug("output: {}".format(e.output))
                error = "Error: an error occured during execution of following command.\n{}\n".format(e.cmd)
                self.output_errlog(error)
            else:
                stdout = "mysqldump was executed. backupfile is saved {}".format(exc_cmd[len(exc_cmd) - 1])
                self.output_logfile(stdout)
        self.output_logfile(__file__ + ' is ended.')

    def compress_backup(self, del_flag=None):
        """取得したバックアップファイルを圧縮処理にかける.

        Args:
            param1 del_flag: 圧縮後、元ファイルを削除するかどうかのフラグ.
                             デフォルトでは削除する.
        """
        self.output_logfile("start compression.")
        if del_flag is None:
            del_flag = True
        # DBのディレクトリ名を取得.
        dir_list = fileope.get_dir_names(self.bk_dir)
        # gzip圧縮処理
        for dir_name in dir_list:
            target_dir = '{0}/{1}'.format(self.bk_dir, dir_name)
            file_list = fileope.get_file_names(r'{}'.format(target_dir))
            for file_name in file_list:
                target_file = '{0}/{1}'.format(target_dir, file_name)
                try:
                    fileope.compress_gz(r'{}'.format(target_file))
                except OSError as oserr:
                    error = "Error: {}\n".format(oserr.strerror)
                    self.output_errlog(error)
                    self.output_errlog("Error: {} failed to compress.".format(target_file))
                except ValueError as valerr:
                    error = "Error: {}\n".format(valerr)
                    self.output_errlog(error)
                    self.output_errlog("Error: {} failed to compress.".format(target_file))
                else:
                    stdout = "{}: complete compress.".format(target_file)
                    self.output_logfile(stdout)
                    if del_flag:
                        fileope.rm_filedir(target_file)

    def output_logfile(self, line: str):
        """open a log file and write standard output in a log file.

        Args:
            param1 line: string of standard output
                type: string
        """
        print(line)
        with open(r"{0}{1}_backup.log".format(LOG_ROOT, self.ymd), 'a') as f:
            f.write(line)

    def output_errlog(self, line: str):
        """open a error log file and write standard error output in a error log
        file.

        Args:
            param1 line: string of standard error output
                type: string
        """
        print(line)
        with open(r"{0}{1}_error.log".format(ERRLOG_ROOT, self.ymd), 'a') as f:
            f.write(line)

    def main(self):
        """main.
        """
        start = time.time()
        # バックアップ用ディレクトリの作成.
        self._mk_backupdir()
        # 旧バックアップデータの削除.
        self._remove_old_backup()
        # ログファイルの削除.
        self._remove_old_log(type=1)
        self._remove_old_log(type=2)
        # DB名とテーブル名一覧の取得.
        dbs_tables = self.get_dbs_and_tables()
        # 実行するLinuxコマンドを生成.
        commands = self.mk_cmd(params=dbs_tables)
        # mysqldumpの実行.
        self.do_backup(commands)
        # 圧縮処理
        self.compress_backup()
        elapsed_time = time.time() - start
        print("elapsed time: {}sec.".format(elapsed_time))


if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser(description='MySQL backup script.')
    argparser.add_argument('-l', '--loglevel', type=int, required=False,
                           default=30,
                           help='ログレベルの指定.デフォルトはWARNING. 0,10,20,30...')
    args = argparser.parse_args()

    db_backup = localBackup(loglevel=args.loglevel)
    db_backup.main()



    def get_transfer_files():
        """obtain transfer files path."""
        dirs = get_transfer_dirs()
        for dir in dirs:
            filenames = fileope.get_file_names(dir_path=dir)
            for filename in filenames:
                target_file = "{0}/{1}".format(target_dir, filename)
                target_files.append(target_file)
        return target_files

    def get_transfer_dirs():
        """obtain transfer directories path."""
        target_dirs = list()
        # obtain backup directory path.
        path = db_backup.bk_dir
        # obtain Database directory path.
        db_dirnames = fileope.get_dir_names(dir_path=path)
        # generates full path of directory.
        for db_dirname in db_dirnames:
            target_dir = "{0}/{1}".format(path, db_dirname)
            target_dirs.append(target_dir)
        return target_dirs

    def transfer_files(targets: list, remote_path: str):
        """transfer local data to remoto host."""
        with SSHConn(hostname='', username='') as dtrans:
            for target in targets:
                # if the target data is directory, its comporess to gz format.
                if fileope.dir_exists(target):
                    fileope.compress_gz(target)
                    target = "{}.gz".format(target)
                try:
                    # execute scp.
                    dtrans.scp_put(local_path=target, remote_path=remote_path)
                except:
                    print("Error: failed to transfer files/dir to remote host.")
                    print("target file to failed to transfer is written in /data2/backup/error_log/")
                    with open(r"{0}{1}_error.log".format(ERRLOG_ROOT, db_backup.ymd), 'a') as f:
                        f.write("{} was not transfer to remote host.".format(target))
                    continue






