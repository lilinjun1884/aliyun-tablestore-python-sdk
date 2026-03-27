# -*- coding: utf8 -*-

from tests.lib.api_test_base import APITestBase
from tablestore import *
from tablestore.protocol import OTSProtocol

import logging


class ClientTest(APITestBase):
    def setUp(self):
        pass  # no need to set up client

    def tearDown(self):
        pass  # no need to tearDown client

    def test_validate_parameter(self):
        test_endpoint = "https://test-inst.test-region.xxx"
        test_ak_id = "test_id"
        test_ak_secret = "test_key"
        test_instance = "test-inst"
        test_region = "test-region"

        # pass
        OTSClient(test_endpoint, test_ak_id, test_ak_secret, test_instance, region=test_region, enable_native=True, native_fallback=False)
        OTSClient(test_endpoint, test_ak_id, test_ak_secret, test_instance, region=None, enable_native=True, native_fallback=False)

        with self.assertRaisesRegex(OTSClientError, "end_point is not str or is empty."):
            OTSClient("", test_ak_id, test_ak_secret, test_instance, region=test_region, enable_native=True, native_fallback=False)
        with self.assertRaisesRegex(OTSClientError, "end_point is not str or is empty."):
            OTSClient(1, test_ak_id, test_ak_secret, test_instance, region=test_region, enable_native=True, native_fallback=False)

        with self.assertRaisesRegex(OTSClientError, "access_key_id is not str or is empty."):
            OTSClient(test_endpoint, "", test_ak_secret, test_instance, region=test_region, enable_native=True, native_fallback=False)
        with self.assertRaisesRegex(OTSClientError, "access_key_id is not str or is empty."):
            OTSClient(test_endpoint, 1, test_ak_secret, test_instance, region=test_region, enable_native=True, native_fallback=False)

        with self.assertRaisesRegex(OTSClientError, "access_key_secret is not str or is empty."):
            OTSClient(test_endpoint, test_ak_id, "", test_instance, region=test_region, enable_native=True, native_fallback=False)
        with self.assertRaisesRegex(OTSClientError, "access_key_secret is not str or is empty."):
            OTSClient(test_endpoint, test_ak_id, 1, test_instance, region=test_region, enable_native=True, native_fallback=False)

        with self.assertRaisesRegex(OTSClientError, "instance_name is not str or is empty."):
            OTSClient(test_endpoint, test_ak_id, test_ak_secret, "", region=test_region, enable_native=True, native_fallback=False)
        with self.assertRaisesRegex(OTSClientError, "instance_name is not str or is empty."):
            OTSClient(test_endpoint, test_ak_id, test_ak_secret, 1, region=test_region, enable_native=True, native_fallback=False)

        with self.assertRaisesRegex(OTSClientError, "region is not str or is empty."):
            OTSClient(test_endpoint, test_ak_id, test_ak_secret, test_instance, region="", enable_native=True, native_fallback=False)
        with self.assertRaisesRegex(OTSClientError, "region is not str or is empty."):
            OTSClient(test_endpoint, test_ak_id, test_ak_secret, test_instance, region=1, enable_native=True, native_fallback=False)

        with self.assertRaisesRegex(OTSClientError, "protocol of end_point must be 'http' or 'https', e.g. https://instance.cn-hangzhou.ots.aliyun.com."):
            test_client = OTSClient("tcp://instance.cn-hangzhou.ots.aliyun.com.", test_ak_id, test_ak_secret, test_instance, region=test_region, enable_native=True, native_fallback=False)

        test_client = OTSClient(test_endpoint, test_ak_id, test_ak_secret, test_instance, region=test_region, enable_native=True, native_fallback=False)
        self.assertEqual(test_ak_id, test_client.credentials_provider.get_credentials().get_access_key_id())
        self.assertEqual(test_region, test_client._signer.sign_region)
        self.assertEqual(True, test_client._signer.auto_update_v4_sign)

        test_date = "20250101"
        test_client = OTSClient(test_endpoint, test_ak_id, test_ak_secret, test_instance, region=test_region, sign_date=test_date, auto_update_v4_sign=False, enable_native=True, native_fallback=False)
        self.assertEqual(test_ak_id, test_client.credentials_provider.get_credentials().get_access_key_id())
        self.assertEqual(test_region, test_client._signer.sign_region)
        self.assertEqual(test_date, test_client._signer.sign_date)
        self.assertEqual(False, test_client._signer.auto_update_v4_sign)
