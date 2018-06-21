# -*- coding: utf-8 -*-
import unittest
from daily_backup import first_setup


PACKAGE_DIR = '/usr/local/lib/python3.4/dist-packages/daily_backup'
KEY_PATH = '/home/Nxj_ToS_TS1/.ssh/id_rsa'
ENC_PATH = '/home/Nxj_ToS_TS1'
ENC_STR = '12345678'


class TestFirstSetup(unittest.TestCase):
    """first_setup.py test cases."""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__get_packagedir(self):
        result = first_setup._get_packagedir()
        print(result)
        self.assertEqual(PACKAGE_DIR, result)

    def test__encrypt(self):
        result = first_setup._encrypt(key_path=KEY_PATH,
                                      target_str=ENC_STR,
                                      outfile_path=ENC_PATH)
        self.assertTrue(result)

    def test__write_to_config(self):
        result = first_setup._write_to_config(key_path=KEY_PATH, cred_path=ENC_PATH)
        self.assertTrue(result)