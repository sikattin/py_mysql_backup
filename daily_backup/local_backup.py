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
import subprocess

# バックアップ用フォルダのルート
BK_ROOT = '/tmp/shikano/backup/'
LOG_ROOT = '/tmp/shikano/error_log/'
MYSQL_USER = "root"
MYSQL_PASSWORD = "mYXe2S6ejG1x5kW!"
MYSQL_DB = "mysql"
MYSQL_HOST = "localhost"
MYSQL_PORT = "13306"


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

        self.date_arith = dateArithmetic()
        self.year = self.date_arith.get_year()
        self.month = self.date_arith.get_month()
        self.day = self.date_arith.get_day()
        self.ym = "{0}{1}".format(self.year, self.month)
        self.md = "{0}{1}".format(self.month, self.day)
        self.ymd = "{0}{1}{2}".format(self.year, self.month, self.day)
        self.bk_dir = "{0}{1}/{2}".format(BK_ROOT, self.ym, self.md)
        return self

    def _remove_old_backup(self):
        """旧バックアップデータを削除する.
        """
        # バックアップルートにあるバックアップ用ディレクトリ名一覧を取得する.
        dir_names = fileope.get_dir_names(dir_path=BK_ROOT)
        if len(dir_names) == 0:
            return
        for dir_name in dir_names:
            # 今月分のバックアップ用ディレクトリは対象外
            if dir_name == "{}".format(self.ym):
                continue
            # 日毎のバックアップディレクトリ名一覧の取得.
            monthly_bkdir = "{0}{1}".format(BK_ROOT, self.ym)
            daily_bkdirs = fileope.get_dir_names(dir_path=monthly_bkdir)
            for daily_bkdir in daily_bkdirs:
                # 現在の日付と対象となるディレクトリのタイムスタンプの日数差を計算する.
                backup_dir = "{0}/{1}".format(monthly_bkdir, daily_bkdir)
                sub_days = self.date_arith.subtract_target_from_now(backup_dir)
                self._logger.debug("sub_days = {}".format(sub_days))
                    # 30日以上経過しているバックアップディレクトリを削除する.
                if sub_days >= 30:
                    try:
                        fileope.f_remove_dirs(path=backup_dir)
                    except OSError as e:
                        print("raise error! {}".format(backup_dir))
                        raise e

    def _mk_backupdir(self):
        """バックアップ用ディレクトリを作成する.
        """
        dbs = self.get_dbs_and_tables()
        for db in dbs.keys():
            db_bkdir = "{0}/{1}".format(self.bk_dir, db)
            if not fileope.dir_exists(path=r"{}".format(db_bkdir)):
                fileope.make_dirs(path=r"{}".format(db_bkdir))
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
                                "mysqldump -u{0} -p{1} -q --skip-opt {2} {3} > "
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
        for exc_cmd in exc_cmds:
            try:
                subprocess.check_call(args=' '.join(exc_cmd), shell=True)
            except subprocess.CalledProcessError as e:
                self._logger.debug("raise error!")
                self._logger.debug("executed command: {}".format(e.cmd))
                self._logger.debug("output: {}".format(e.output))
                f = open(r"{0}{1}_error.log".format(LOG_ROOT, self.ymd), 'a')
                f.write("Error: {}\n".format(e.cmd))
                f.close()

            else:
                print("mysqldump was executed. backupfile is saved {}".format(self.bk_dir))


    def main(self):
        """main.
        """
        # バックアップ用ディレクトリの作成.
        self._mk_backupdir()
        # 旧バックアップデータの削除.
        self._remove_old_backup()
        # DB名とテーブル名一覧の取得.
        dbs_tables = self.get_dbs_and_tables()
        # 実行するLinuxコマンドを生成.
        commands = self.mk_cmd(params=dbs_tables)
        # mysqldumpの実行.
        self.do_backup(commands)

if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser(description='MySQL backup script.')
    argparser.add_argument('-l', '--loglevel', type=int, required=False,
                           default=30,
                           help='ログレベルの指定.デフォルトはWARNING. 0,10,20,30...')
    args = argparser.parse_args()

    db_backup = localBackup(loglevel=args.loglevel)
    db_backup.main()
