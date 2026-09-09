import unittest
from tools.maintenance.quota import limited, retry_after


class QuotaTests(unittest.TestCase):
    def test_http_date_numeric_invalid_and_extreme_reset_hints(self):
        for value, expected in [('120', 120), (0, 60), (True, None), ('NaN', None), ('inf', None),
                                ('bad date', None), ('99999999', 604800),
                                ('Wed, 09 Sep 2026 00:03:00 GMT', 120)]:
            with self.subTest(value=value):
                self.assertEqual(retry_after(value, 1788912060), expected)

    def test_account_errors_do_not_automatically_retry_or_enable_paid_fallback(self):
        for code in ['1002', '1112', '1113', '1309', '1311', '1313']:
            self.assertFalse(limited(429, {'error': {'code': code, 'message': 'private'}}))
        for code in ['1302', '1303', '1304', '1305', '1308', '1310', '1312']:
            self.assertTrue(limited(400, {'error': {'code': code, 'message': 'private'}}))
        self.assertTrue(limited(429))
        self.assertFalse(limited(401))
        self.assertFalse(limited(500, {'error': 'private'}))
