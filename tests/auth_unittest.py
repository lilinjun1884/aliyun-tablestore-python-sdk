# -*- coding: utf8 -*-

from tests.lib.api_test_base import APITestBase
from tablestore.credentials import StaticCredentialsProvider
from tablestore.auth import *


class AuthTest(APITestBase):
    def setUp(self):
        self.test_ak_id = "test_id"
        self.test_ak_secret = "test_key"
        self.test_sts_token = "test_token"
        self.test_encoding = "utf-8"
        self.test_region = "test-region"
        self.test_sign_date = "20250410"
        self.signature_string = "test_signature_string"
        self.test_query = "test_query"
        self.headers = {"x-ots-test": "test"}

    def tearDown(self):
        pass  # no need to tearDown client

    def test_calculate_signature(self):
        actual_sha1_sign = "C845ef7UjNGL0gExNlQhp+3B/gY="
        sha1_sign = call_signature_method_sha1(self.test_ak_secret, self.signature_string, self.test_encoding)
        self.assertEqual(28, len(sha1_sign))
        self.assertEqual(actual_sha1_sign, sha1_sign)

        actual_sha256_sign = "c+lCAaaQVSCVlc0u0JBEPoIzyxplf4xEIBH8sdWUOjo="
        sha256_sign = call_signature_method_sha256(self.test_ak_secret, self.signature_string, self.test_encoding)
        self.assertEqual(44, len(sha256_sign))
        self.assertEqual(actual_sha256_sign, sha256_sign)

    def test_SignClass(self):
        cred = StaticCredentialsProvider(self.test_ak_id, self.test_ak_secret, self.test_sts_token)
        request_context: RequestContext = RequestContext(cred.get_credentials(), self.test_sign_date)

        # test v2 sign
        v2_signer = SignV2(self.test_encoding)
        self.assertEqual(self.test_encoding, v2_signer.encoding)
        test_headers = self.headers.copy()
        v2_signer.make_request_signature_and_add_headers(self.test_query, test_headers, request_context)
        v2_request_signature = test_headers[consts.OTS_HEADER_SIGNATURE]
        actual_v2_request_signature = "QDhzLv7VESBJtYQY4Li0IhSUOdg="
        self.assertEqual(28, len(v2_request_signature))
        self.assertEqual(actual_v2_request_signature, v2_request_signature)
        v2_response_signature = v2_signer.make_response_signature(
            self.test_query, self.headers, v2_signer.get_signing_key(request_context))
        actual_v2_response_signature = "UjJK/SWed0n9o6JYxvApHGaQABo="
        self.assertEqual(actual_v2_response_signature, v2_response_signature)

        # test v4 sign
        with self.assertRaisesRegex(OTSClientError, "region is not str or is empty."):
            SignV4(self.test_encoding)
        v4_signer = SignV4(self.test_encoding, region=self.test_region, sign_date=self.test_sign_date)
        self.assertEqual(self.test_encoding, v4_signer.encoding)
        actual_v4_signing_key = b"nToxlXr" + b"xgCm0L" + b"5J0nr/q" + b"q/GmtgN9" + b"GVBhiR" + b"LzdL" + b"aVUP0="
        self.assertEqual(actual_v4_signing_key, v4_signer.get_signing_key(request_context))
        test_headers = self.headers.copy()
        v4_signer.make_request_signature_and_add_headers(self.test_query, test_headers, request_context)
        self.assertEqual(self.test_region, test_headers[consts.OTS_HEADER_SIGN_REGION])
        self.assertEqual(v4_signer.sign_date, test_headers[consts.OTS_HEADER_SIGN_DATE])
        v4_request_signature = test_headers[consts.OTS_HEADER_SIGNATURE_V4]
        actual_v4_request_signature = "yXnOpODWaU1EYAlLP3l25ksj010uGHS7uxIt5Qiwz4o="
        self.assertEqual(44, len(v4_request_signature))
        self.assertEqual(actual_v4_request_signature, v4_request_signature)
        v4_response_signature = v4_signer.make_response_signature(
            self.test_query, self.headers, v4_signer.get_signing_key(request_context))
        actual_v4_response_signature = "vIhaUGwv/JSg8ctLNyxbNeNv69A="
        self.assertEqual(actual_v4_response_signature, v4_response_signature)

        self.assertEqual(
            v2_signer.make_response_signature(
                self.test_query, self.headers, v4_signer.get_signing_key(request_context)),
            v4_signer.make_response_signature(
                self.test_query, self.headers, v4_signer.get_signing_key(request_context))
        )
