# -*- coding: utf8 -*-

from tests.lib.api_test_base import APITestBase
from tablestore import metadata
from tablestore.metadata import SSESpecification, SSEKeyType
from tablestore.protobuf import search_pb2

class EnumTest(APITestBase):
    def setUp(self):
        pass # no need to set up client

    def tearDown(self):
        pass # no need to tearDown client

    def test_IntEnum_equal_int(self):
        self.assert_equal(metadata.HighlightEncoder.PLAIN_MODE, search_pb2.PLAIN_MODE)
        self.assert_equal(metadata.HighlightEncoder.HTML_MODE, search_pb2.HTML_MODE)


class TestSSESpecification(APITestBase):
    def setUp(self):
        pass # no need to set up client

    def tearDown(self):
        pass # no need to tearDown client

    def test_check_valid_arguments(self):
        # Test case 1: SSE disabled
        sse = SSESpecification()
        sse.enable = False
        sse.key_type = None
        sse.key_id = None
        sse.role_arn = None
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNone(err)

        # Test case 2: SSE enabled with KMS service
        sse = SSESpecification()
        sse.enable = True
        sse.key_type = SSEKeyType.SSE_KMS_SERVICE
        sse.key_id = None
        sse.role_arn = None
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNone(err)

        # Test case 3: SSE enabled with BYOK
        sse = SSESpecification()
        sse.enable = True
        sse.key_type = SSEKeyType.SSE_BYOK
        sse.key_id = "test-key-id"
        sse.role_arn = "test-role-arn"
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNone(err)

    def test_check_invalid_arguments(self):
        # Test case 2: Key type set when enable is false
        sse = SSESpecification()
        sse.enable = False
        sse.key_type = SSEKeyType.SSE_KMS_SERVICE
        sse.key_id = None
        sse.role_arn = None
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNotNone(err)
        self.assertEqual("key type cannot be set when enable is false", str(err))

        # Test case 3: Key type not set when enable is true
        sse = SSESpecification()
        sse.enable = True
        sse.key_type = None
        sse.key_id = None
        sse.role_arn = None
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNotNone(err)
        self.assertEqual("key type is required when enable is true", str(err))

        # Test case 4: Key ID set when key type is not SSE_BYOK
        sse = SSESpecification()
        sse.enable = True
        sse.key_type = SSEKeyType.SSE_KMS_SERVICE
        sse.key_id = "test-key-id"
        sse.role_arn = None
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNotNone(err)
        self.assertEqual("key id and role arn cannot be set when key type is not SSE_BYOK", str(err))

        # Test case 5: Role ARN set when key type is not SSE_BYOK
        sse = SSESpecification()
        sse.enable = True
        sse.key_type = SSEKeyType.SSE_KMS_SERVICE
        sse.key_id = None
        sse.role_arn = "test-role-arn"
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNotNone(err)
        self.assertEqual("key id and role arn cannot be set when key type is not SSE_BYOK", str(err))

        # Test case 6: Key ID and Role ARN not set when key type is SSE_BYOK
        sse = SSESpecification()
        sse.enable = True
        sse.key_type = SSEKeyType.SSE_BYOK
        sse.key_id = None  # Missing key_id
        sse.role_arn = None  # Missing role_arn
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNotNone(err)
        self.assertEqual("key id and role arn are required when key type is not SSE_KMS_SERVICE", str(err))

        # Test case 7: Only key ID set when key type is SSE_BYOK
        sse = SSESpecification()
        sse.enable = True
        sse.key_type = SSEKeyType.SSE_BYOK
        sse.key_id = "test-key-id"  # Only key_id set
        sse.role_arn = None  # Missing role_arn
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNotNone(err)
        self.assertEqual("key id and role arn are required when key type is not SSE_KMS_SERVICE", str(err))

        # Test case 8: Only role ARN set when key type is SSE_BYOK
        sse = SSESpecification()
        sse.enable = True
        sse.key_type = SSEKeyType.SSE_BYOK
        sse.key_id = None  # Missing key_id
        sse.role_arn = "test-role-arn"  # Only role_arn set
        try:
            sse.check_arguments()
            err = None
        except Exception as e:
            err = e
        self.assertIsNotNone(err)
        self.assertEqual("key id and role arn are required when key type is not SSE_KMS_SERVICE", str(err))
