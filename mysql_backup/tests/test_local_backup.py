# -*- coding: utf-8 -*-
import unittest
from daily_backup import local_backup

class TestlocalBackup(unittest.TestCase):
    """Basic test cases."""

    def setUp(self):
        self.lb = local_backup.localBackup(handler='console')

    def tearDown(self):
        del self.lb

    def test__decrypt_credentialfile(self):
        result = self.lb._decrypt_credentialfile()
        print(result)
        self.assertIsNotNone(result)
        
if __name__ == '__main__':
    unittest.main()