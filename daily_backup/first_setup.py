#!/usr/bin/python3
#-------------------------------------------------------------------------------
# Name:        first_setup.py
# Purpose:     setup config of daily_backup.
#
# Author:      shikano.takeki
#
# Created:     2018/06/08
# Copyright:   shikano.takeki 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# -*- coding: utf-8 -*-
import subprocess
import shlex
import codecs
import tempfile
from os.path import split, join
from iomod import rwfile
from getpass import getpass


CRED_FILENAME = 'credential.enc'


def _get_packagedir():
    import daily_backup
    return split(daily_backup.__file__)[0]

def _encrypt(key_path: str, target_str: str, outfile_path: str):
    outfile_path = join(outfile_path, CRED_FILENAME)
    with tempfile.NamedTemporaryFile() as temp_f:
        with codecs.open(outfile_path, mode="w") as f:
            cmd_encrypt = "openssl rsautl -encrypt -inkey {0} -in {1}".format(key_path,
                                                                       temp_f.name)
            try:    
                subprocess.Popen(shlex.split(cmd_encrypt), stdout=f)
            except:
                print("Credentialファイルの暗号化に失敗しました。")
                print("暗号化に使用されたコマンド: {}".format(cmd_encrypt))
                raise
            else:
                # テスト用の戻り値
                return True



def _write_to_config(key_path: str, cred_path: str):
    pj = rwfile.ParseJSON()
    # make path
    package_dir = _get_packagedir()
    package_confdir = join(package_dir, "config")
    package_confpath = join(package_confdir, "backup.json")
    # parse json file.
    parsed_json = pj.load_json(file=package_confpath)
    # add new key & value
    parsed_json['default_path']['KEY_PATH'] = key_path
    parsed_json['default_path']['CRED_PATH'] = encfile_path
    # remove existing json file.

    # write in new file.
    try:
        import json
        pj.out_json(file=package_confpath, content=parsed_json)
    except FileNotFoundError as notfound_e:
        print("ファイル出力される指定パスが見つかりませんでした。")
        print("{}".format(package_confpath))
        raise notfound_e
    except json.JSONDecodeError as decode_e:
        print("jsonファイルへのデコードに失敗しました。")
        raise decode_e
    else:
        del pj
        # テスト用の戻り値
        return True

if __name__ == "__main__":
    # input key file path used encryption.
    key_path = input("パスワードの暗号化/複合化に使用する鍵のパス: ")
    # input encrypted password file path.
    encfile_path = input("パスワード暗号化ファイルの保存先パス: ")
    # input mysql/mariadb password as plain text.
    plainpass = getpass("MySQL/MariaDBのrootパスワード: ")
    # create encrypted credential file.
    _encrypt(key_path=key_path, target_str=plainpass, outfile_path=encfile_path)
    # write to config file.
    _write_to_config(key_path=key_path, cred_path=encfile_path)