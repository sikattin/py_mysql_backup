#!/usr/bin/python3
#-------------------------------------------------------------------------------
# Name:        update_mysqlpass.py
# Purpose:     Update mysql credential file.
#
# Author:      shikano.takeki
#
# Created:     2018/06/21
# Copyright:   shikano.takeki 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# -*- coding: utf-8 -*-
import subprocess
import shlex
import codecs
from os.path import split, join
from os import remove
from getpass import getpass
from iomod import rwfile

TMPFILE_PATH = '/tmp/tmp_forpyscript'
TIMEOUT_SEC = 10


def _get_packagedir():
    import daily_backup
    return split(daily_backup.__file__)[0]

def _parse_json():
    pj = rwfile.ParseJSON()
    # make path
    package_confdirpath = join(_get_packagedir(), "config")
    conf_path = join(package_confdirpath, "backup.json")
    # parse .json
    parsed_json = pj.load_json(file=conf_path)
    del pj

    return parsed_json

def _encrypt(key_path: str, target_str: str, outfile_path: str):
    cmd_encrypt = "openssl rsautl -encrypt -inkey {0} -in {1}".format(key_path,
                                                                  TMPFILE_PATH)
    with codecs.open(r"{}".format(TMPFILE_PATH), mode="w") as f:
        f.write(target_str)
    with codecs.open(outfile_path, mode="w") as f:
        try:
            popen_obj = subprocess.Popen(shlex.split(cmd_encrypt), stdout=f)
        except OSError as os_e:
            print("Credentialファイルの更新に失敗しました。")
            raise os_e
        except ValueError as val_e:
            print("Credentialファイルの更新に失敗しました。不正な引数です。")
            raise val_e
        except subprocess.SubprocessError as subp_e:
            print("Credentialファイルの更新に失敗しました。コマンド実行中にエラーが発生しました。")
            raise subp_e
        except:
            raise
        else:
            # wait until the process executing command is finale.
            ret_code = popen_obj.wait(timeout=TIMEOUT_SEC)
            if ret_code == 0:
                # return for test
                print("Credentialファイルの更新に成功しました。")
                return True
        finally:
            f.close()
            remove(TMPFILE_PATH)

if __name__ == "__main__":
    # get value from configuration file.
    dic = _parse_json()
    try:
        my_user = dic["mysql"]["MYSQL_USER"]
        key_path = dic["default_path"]["KEY_PATH"]
        cred_path = dic["default_path"]["CRED_PATH"]
    except KeyError as key_e:
        print("設定ファイルに存在しないKeyが参照されました。")
        raise key_e

    # ask for new mysql password
    plainpass = getpass("新しいMySQL/MariaDBの{}ユーザパスワード: ".format(my_user))

    print("MySQL User: {}".format(my_user))
    print("Keyfile path(using for updating credential file): {}".format(key_path))
    print("credential file path: {}".format(cred_path))
    print("※もしエラーが出る場合は、上記パスや情報に誤りがないか設定ファイルから値を確認してみてください。")
    # update credential file.
    _encrypt(key_path=key_path, target_str=plainpass, outfile_path=cred_path)
