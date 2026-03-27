# -*- coding: utf8 -*-

"""
Tests for native C++ encoder/decoder integration (方案B).

These tests verify:
1. The NativeEncodedBytes wrapper works correctly with protocol.py
2. Python encoder produces correct output with and without native encoder
3. Python decoder produces correct output with and without native decoder
4. Fallback behavior when native module is unavailable
5. End-to-end encode/decode consistency between native and Python implementations
"""

import unittest
import unittest.mock
import logging
import random
import string
import sys
from tests.lib import test_config

from tablestore.metadata import (
    Row, Condition, RowExistenceExpectation, CapacityUnit,
    ReturnType, Direction, INF_MIN, INF_MAX, PK_AUTO_INCR,
    TableMeta, TableOptions, ReservedThroughput,
    PutRowItem, UpdateRowItem, DeleteRowItem,
    TableInBatchGetRowItem, BatchGetRowRequest,
    TableInBatchWriteRowItem, BatchWriteRowRequest,
    BatchWriteRowResponseItem, RowDataItem,
    SearchQuery, MatchAllQuery, TermQuery,
    ScanQuery, ColumnsToGet, ColumnReturnType,
)
from tablestore.encoder import (
    OTSProtoBufferEncoder, NativeEncodedBytes,
    NATIVE_ENCODER_AVAILABLE,
)
from tablestore.decoder import (
    OTSProtoBufferDecoder,
    NATIVE_DECODER_AVAILABLE,
)
import tablestore.protobuf.table_store_pb2 as pb2
import tablestore.protobuf.search_pb2 as search_pb2

logger = logging.getLogger(__name__)


class TestNativeEncodedBytes(unittest.TestCase):
    """Test the NativeEncodedBytes wrapper class."""

    def test_serialize_to_string_returns_data(self):
        data = b'\x01\x02\x03\x04'
        wrapper = NativeEncodedBytes(data)
        self.assertEqual(wrapper.SerializeToString(), data)
        self.assertEqual(wrapper.data, data)

    def test_empty_data(self):
        wrapper = NativeEncodedBytes(b'')
        self.assertEqual(wrapper.SerializeToString(), b'')

    def test_isinstance_check(self):
        wrapper = NativeEncodedBytes(b'test')
        self.assertIsInstance(wrapper, NativeEncodedBytes)
        self.assertNotIsInstance(pb2.GetRowRequest(), NativeEncodedBytes)


class TestEncoderFallback1(unittest.TestCase):
    """Test that encoder falls back to Python when native is unavailable."""

    def setUp(self):
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

    def test_encode_get_row_returns_valid_proto(self):
        primary_key = [('pk1', 'value1'), ('pk2', 123)]
        result = self.encoder._encode_get_row(
            'test_table', primary_key,
            columns_to_get=None, column_filter=None,
            max_version=1, time_range=None,
            start_column=None, end_column=None,
            token=None, transaction_id=None,
        )
        if NATIVE_ENCODER_AVAILABLE:
            self.assertIsInstance(result, NativeEncodedBytes)
            self.assertIsInstance(result.data, bytes)
            self.assertGreater(len(result.data), 0)
        else:
            self.assertNotIsInstance(result, NativeEncodedBytes)
            serialized = result.SerializeToString()
            self.assertIsInstance(serialized, bytes)
            self.assertGreater(len(serialized), 0)

    def test_encode_put_row_returns_valid_proto(self):
        primary_key = [('pk1', 'value1')]
        attribute_columns = [('col1', 'str_val'), ('col2', 42)]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        result = self.encoder._encode_put_row(
            'test_table', row, condition,
            return_type=None, transaction_id=None,
        )
        if NATIVE_ENCODER_AVAILABLE:
            self.assertIsInstance(result, NativeEncodedBytes)
        else:
            serialized = result.SerializeToString()
            self.assertIsInstance(serialized, bytes)

    def test_encode_update_row_returns_valid_proto(self):
        primary_key = [('pk1', 'value1')]
        update_columns = {'PUT': [('col1', 'new_val')]}
        row = Row(primary_key, update_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        result = self.encoder._encode_update_row(
            'test_table', row, condition,
            return_type=None, transaction_id=None,
        )
        if NATIVE_ENCODER_AVAILABLE:
            self.assertIsInstance(result, NativeEncodedBytes)
        else:
            serialized = result.SerializeToString()
            self.assertIsInstance(serialized, bytes)

    def test_encode_delete_row_returns_valid_proto(self):
        primary_key = [('pk1', 'value1')]
        condition = Condition(RowExistenceExpectation.IGNORE)
        result = self.encoder._encode_delete_row(
            'test_table', primary_key, condition,
            return_type=None, transaction_id=None,
        )
        if NATIVE_ENCODER_AVAILABLE:
            self.assertIsInstance(result, NativeEncodedBytes)
        else:
            serialized = result.SerializeToString()
            self.assertIsInstance(serialized, bytes)

    def test_encode_get_range_returns_valid_proto(self):
        start_pk = [('pk1', INF_MIN), ('pk2', INF_MIN)]
        end_pk = [('pk1', INF_MAX), ('pk2', INF_MAX)]
        result = self.encoder._encode_get_range(
            'test_table', 'FORWARD',
            start_pk, end_pk,
            columns_to_get=None, limit=10,
            column_filter=None, max_version=1,
            time_range=None, start_column=None,
            end_column=None, token=None,
            transaction_id=None,
        )
        if NATIVE_ENCODER_AVAILABLE:
            self.assertIsInstance(result, NativeEncodedBytes)
        else:
            serialized = result.SerializeToString()
            self.assertIsInstance(serialized, bytes)


class TestEncoderWithColumnsToGet(unittest.TestCase):
    """Test encoder with columns_to_get parameter."""

    def setUp(self):
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

    def test_encode_get_row_with_columns_to_get(self):
        primary_key = [('pk1', 'value1')]
        result = self.encoder._encode_get_row(
            'test_table', primary_key,
            columns_to_get=['col1', 'col2'],
            column_filter=None,
            max_version=1, time_range=None,
            start_column=None, end_column=None,
            token=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertIsInstance(body, bytes)
        self.assertGreater(len(body), 0)

    def test_encode_get_row_with_time_range_tuple(self):
        primary_key = [('pk1', 'value1')]
        result = self.encoder._encode_get_row(
            'test_table', primary_key,
            columns_to_get=None, column_filter=None,
            max_version=None, time_range=(1000, 2000),
            start_column=None, end_column=None,
            token=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertIsInstance(body, bytes)

    def test_encode_get_row_with_time_range_int(self):
        primary_key = [('pk1', 'value1')]
        result = self.encoder._encode_get_row(
            'test_table', primary_key,
            columns_to_get=None, column_filter=None,
            max_version=None, time_range=1500,
            start_column=None, end_column=None,
            token=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertIsInstance(body, bytes)


class TestEncoderPrimaryKeyTypes(unittest.TestCase):
    """Test encoder with various primary key types."""

    def setUp(self):
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

    def test_encode_get_row_with_string_pk(self):
        primary_key = [('pk1', 'hello')]
        result = self.encoder._encode_get_row(
            'test_table', primary_key,
            columns_to_get=None, column_filter=None,
            max_version=1, time_range=None,
            start_column=None, end_column=None,
            token=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_encode_get_row_with_integer_pk(self):
        primary_key = [('pk1', 12345)]
        result = self.encoder._encode_get_row(
            'test_table', primary_key,
            columns_to_get=None, column_filter=None,
            max_version=1, time_range=None,
            start_column=None, end_column=None,
            token=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_encode_get_row_with_binary_pk(self):
        primary_key = [('pk1', bytearray(b'\x01\x02\x03'))]
        result = self.encoder._encode_get_row(
            'test_table', primary_key,
            columns_to_get=None, column_filter=None,
            max_version=1, time_range=None,
            start_column=None, end_column=None,
            token=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_encode_get_range_with_inf_min_max(self):
        start_pk = [('pk1', INF_MIN)]
        end_pk = [('pk1', INF_MAX)]
        result = self.encoder._encode_get_range(
            'test_table', 'FORWARD',
            start_pk, end_pk,
            columns_to_get=None, limit=100,
            column_filter=None, max_version=1,
            time_range=None, start_column=None,
            end_column=None, token=None,
            transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)


class TestEncoderAttributeColumnTypes(unittest.TestCase):
    """Test encoder with various attribute column value types."""

    def setUp(self):
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

    def test_put_row_with_string_column(self):
        row = Row([('pk1', 'val')], [('col1', 'string_value')])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_integer_column(self):
        row = Row([('pk1', 'val')], [('col1', 42)])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_double_column(self):
        row = Row([('pk1', 'val')], [('col1', 3.14)])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_boolean_column(self):
        row = Row([('pk1', 'val')], [('col1', True)])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_binary_column(self):
        row = Row([('pk1', 'val')], [('col1', bytearray(b'\xde\xad\xbe\xef'))])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_timestamp(self):
        row = Row([('pk1', 'val')], [('col1', 'value', 1234567890000)])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_multiple_columns(self):
        row = Row(
            [('pk1', 'val')],
            [
                ('str_col', 'hello'),
                ('int_col', 100),
                ('float_col', 2.718),
                ('bool_col', False),
                ('bin_col', bytearray(b'\x00\xff')),
            ]
        )
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)


class TestEncoderConditions(unittest.TestCase):
    """Test encoder with various condition types."""

    def setUp(self):
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

    def test_put_row_with_ignore_condition(self):
        row = Row([('pk1', 'val')], [('col1', 'v')])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_expect_exist_condition(self):
        row = Row([('pk1', 'val')], [('col1', 'v')])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.EXPECT_EXIST),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_expect_not_exist_condition(self):
        row = Row([('pk1', 'val')], [('col1', 'v')])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.EXPECT_NOT_EXIST),
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_none_condition(self):
        row = Row([('pk1', 'val')], [('col1', 'v')])
        result = self.encoder._encode_put_row(
            'test_table', row,
            condition=None,
            return_type=None, transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_put_row_with_return_type_pk(self):
        row = Row([('pk1', 'val')], [('col1', 'v')])
        result = self.encoder._encode_put_row(
            'test_table', row,
            Condition(RowExistenceExpectation.IGNORE),
            return_type=ReturnType.RT_PK,
            transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)


class TestEncoderGetRange(unittest.TestCase):
    """Test encoder for GetRange API."""

    def setUp(self):
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

    def test_get_range_forward(self):
        start_pk = [('pk1', INF_MIN)]
        end_pk = [('pk1', INF_MAX)]
        result = self.encoder._encode_get_range(
            'test_table', 'FORWARD',
            start_pk, end_pk,
            columns_to_get=None, limit=100,
            column_filter=None, max_version=1,
            time_range=None, start_column=None,
            end_column=None, token=None,
            transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_get_range_backward(self):
        start_pk = [('pk1', INF_MAX)]
        end_pk = [('pk1', INF_MIN)]
        result = self.encoder._encode_get_range(
            'test_table', 'BACKWARD',
            start_pk, end_pk,
            columns_to_get=None, limit=100,
            column_filter=None, max_version=1,
            time_range=None, start_column=None,
            end_column=None, token=None,
            transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)

    def test_get_range_with_columns_and_limit(self):
        start_pk = [('pk1', 'a')]
        end_pk = [('pk1', 'z')]
        result = self.encoder._encode_get_range(
            'test_table', 'FORWARD',
            start_pk, end_pk,
            columns_to_get=['col1', 'col2'], limit=50,
            column_filter=None, max_version=3,
            time_range=None, start_column='col1',
            end_column='col3', token=None,
            transaction_id=None,
        )
        body = result.SerializeToString()
        self.assertGreater(len(body), 0)


class TestDecoderFallback1(unittest.TestCase):
    """Test that decoder falls back to Python when native is unavailable."""

    def setUp(self):
        self.decoder = OTSProtoBufferDecoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

    def test_decode_get_row_empty_response(self):
        proto = pb2.GetRowResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 0
        proto.row = b''
        body = proto.SerializeToString()

        result, _ = self.decoder._decode_get_row(body, 'test-request-id')
        consumed, return_row, next_token = result
        self.assertEqual(consumed.read, 1)
        self.assertEqual(consumed.write, 0)
        self.assertIsNone(return_row)

    def test_decode_put_row_empty_response(self):
        proto = pb2.PutRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        body = proto.SerializeToString()

        result, _ = self.decoder._decode_put_row(body, 'test-request-id')
        consumed, return_row = result
        self.assertEqual(consumed.read, 0)
        self.assertEqual(consumed.write, 1)
        self.assertIsNone(return_row)

    def test_decode_update_row_empty_response(self):
        proto = pb2.UpdateRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        body = proto.SerializeToString()

        result, _ = self.decoder._decode_update_row(body, 'test-request-id')
        consumed, return_row = result
        self.assertEqual(consumed.write, 1)
        self.assertIsNone(return_row)

    def test_decode_delete_row_empty_response(self):
        proto = pb2.DeleteRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        body = proto.SerializeToString()

        result, _ = self.decoder._decode_delete_row(body, 'test-request-id')
        consumed, return_row = result
        self.assertEqual(consumed.write, 1)
        self.assertIsNone(return_row)

    def test_decode_get_range_empty_response(self):
        proto = pb2.GetRangeResponse()
        proto.consumed.capacity_unit.read = 5
        proto.consumed.capacity_unit.write = 0
        proto.rows = b''
        body = proto.SerializeToString()

        result, _ = self.decoder._decode_get_range(body, 'test-request-id')
        capacity_unit, next_start_pk, row_list, next_token = result
        self.assertEqual(capacity_unit.read, 5)
        self.assertIsNone(next_start_pk)
        self.assertEqual(len(row_list), 0)

    def test_decode_batch_get_row_empty_response(self):
        proto = pb2.BatchGetRowResponse()
        body = proto.SerializeToString()

        result, _ = self.decoder._decode_batch_get_row(body, 'test-request-id')
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 0)

    def test_decode_batch_write_row_empty_response(self):
        proto = pb2.BatchWriteRowResponse()
        body = proto.SerializeToString()

        result, _ = self.decoder._decode_batch_write_row(body, 'test-request-id')
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 0)


class TestEncodeRequestDispatch(unittest.TestCase):
    """Test that encode_request dispatches correctly."""

    def setUp(self):
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

    def test_encode_request_get_row(self):
        primary_key = [('pk1', 'value1')]
        result = self.encoder.encode_request(
            'GetRow',
            'test_table', primary_key,
            None, None, 1, None, None, None, None, None,
        )
        body = result.SerializeToString()
        self.assertIsInstance(body, bytes)
        self.assertGreater(len(body), 0)

    def test_encode_request_put_row(self):
        row = Row([('pk1', 'value1')], [('col1', 'val')])
        condition = Condition(RowExistenceExpectation.IGNORE)
        result = self.encoder.encode_request(
            'PutRow',
            'test_table', row, condition, None, None,
        )
        body = result.SerializeToString()
        self.assertIsInstance(body, bytes)

    def test_encode_request_delete_row(self):
        primary_key = [('pk1', 'value1')]
        condition = Condition(RowExistenceExpectation.IGNORE)
        result = self.encoder.encode_request(
            'DeleteRow',
            'test_table', primary_key, condition, None, None,
        )
        body = result.SerializeToString()
        self.assertIsInstance(body, bytes)

    def test_encode_request_get_range(self):
        start_pk = [('pk1', INF_MIN)]
        end_pk = [('pk1', INF_MAX)]
        result = self.encoder.encode_request(
            'GetRange',
            'test_table', 'FORWARD',
            start_pk, end_pk,
            None, 10, None, 1, None, None, None, None, None,
        )
        body = result.SerializeToString()
        self.assertIsInstance(body, bytes)


class TestNativeAvailabilityFlags(unittest.TestCase):
    """Test that availability flags are properly set."""

    def test_native_encoder_flag_is_boolean(self):
        self.assertIsInstance(NATIVE_ENCODER_AVAILABLE, bool)

    def test_native_decoder_flag_is_boolean(self):
        self.assertIsInstance(NATIVE_DECODER_AVAILABLE, bool)

    def test_native_encoded_bytes_class_exists(self):
        self.assertTrue(hasattr(NativeEncodedBytes, 'SerializeToString'))
        instance = NativeEncodedBytes(b'test')
        self.assertTrue(hasattr(instance, 'data'))


@unittest.skipUnless(NATIVE_ENCODER_AVAILABLE, "Native C++ encoder not available")
class TestNativeEncoderConsistency(unittest.TestCase):
    """Test that native encoder produces output consistent with Python encoder.

    These tests only run when the native C++ module is available.
    They compare the deserialized protobuf messages from both implementations.
    """

    def _encode_with_python(self, method_name, *args, **kwargs):
        """Force Python encoding by temporarily disabling native encoder."""
        encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)
        original_flag = encoder._use_native_encoder
        encoder._use_native_encoder = False
        try:
            self.assertFalse(encoder._use_native_encoder)
            method = getattr(encoder, method_name)
            result = method(*args, **kwargs)
            return result.SerializeToString()
        finally:
            encoder._use_native_encoder = original_flag

    def _encode_with_native(self, method_name, *args, **kwargs):
        """Force native encoding."""
        encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)
        original_flag = encoder._use_native_encoder
        encoder._use_native_encoder = True
        try:
            self.assertTrue(encoder._use_native_encoder)
            method = getattr(encoder, method_name)
            result = method(*args, **kwargs)
            self.assertIsInstance(result, NativeEncodedBytes)
            return result.SerializeToString()
        finally:
            encoder._use_native_encoder = original_flag

    def test_get_row_consistency(self):
        primary_key = [('pk1', 'value1'), ('pk2', 123)]
        args = (
            'test_table', primary_key,
            ['col1', 'col2'], None,
            1, None, None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_row', *args)
        native_body = self._encode_with_native('_encode_get_row', *args)

        python_proto = pb2.GetRowRequest()
        python_proto.ParseFromString(python_body)
        native_proto = pb2.GetRowRequest()
        native_proto.ParseFromString(native_body)

        self.assertEqual(python_proto.table_name, native_proto.table_name)
        self.assertEqual(list(python_proto.columns_to_get), list(native_proto.columns_to_get))
        self.assertEqual(python_proto.max_versions, native_proto.max_versions)

    def test_delete_row_consistency(self):
        primary_key = [('pk1', 'value1')]
        condition = Condition(RowExistenceExpectation.IGNORE)
        args = ('test_table', primary_key, condition, None, None)

        python_body = self._encode_with_python('_encode_delete_row', *args)
        native_body = self._encode_with_native('_encode_delete_row', *args)

        python_proto = pb2.DeleteRowRequest()
        python_proto.ParseFromString(python_body)
        native_proto = pb2.DeleteRowRequest()
        native_proto.ParseFromString(native_body)

        self.assertEqual(python_proto.table_name, native_proto.table_name)
        self.assertEqual(
            python_proto.condition.row_existence,
            native_proto.condition.row_existence,
        )



    # === put_row encoder consistency tests ===

    def test_update_row_put_with_timestamp(self):
        """PUT 更新（添加/覆盖列），带 timestamp"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = {
            'PUT': [('col1', 'val1', 1234567890), ('col2', 123, 1234567891), ('col3', 45.67)],
        }
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_update_row', *args)
        native_body = self._encode_with_native('_encode_update_row', *args)
        self.assertEqual(python_body, native_body)


    # === put_row / update_row / delete_row / get_row encoder consistency tests ===

    def test_put_row_basic(self):
        """基础场景：单列字符串值 + IGNORE 条件"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = [('col1', 'val1')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_put_row', *args)
        native_body = self._encode_with_native('_encode_put_row', *args)
        self.assertEqual(python_body, native_body)

    def test_put_row_multi_types(self):
        """多类型列：string/integer/double/boolean/binary 混合列"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = [
            ('col_str', 'string_value'),
            ('col_int', 123),
            ('col_double', 45.67),
            ('col_bool', True),
            ('col_binary', bytearray(b'binary_data'))
        ]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_put_row', *args)
        native_body = self._encode_with_native('_encode_put_row', *args)
        self.assertEqual(python_body, native_body)

    def test_put_row_with_timestamp(self):
        """列值带 timestamp"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = [
            ('col1', 'val1', 1234567890),
            ('col2', 456, 1234567891)
        ]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_put_row', *args)
        native_body = self._encode_with_native('_encode_put_row', *args)
        self.assertEqual(python_body, native_body)

    def test_put_row_expect_exist(self):
        """EXPECT_EXIST 条件"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = [('col1', 'val1')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.EXPECT_EXIST)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_put_row', *args)
        native_body = self._encode_with_native('_encode_put_row', *args)
        self.assertEqual(python_body, native_body)

    def test_put_row_expect_not_exist(self):
        """EXPECT_NOT_EXIST 条件"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = [('col1', 'val1')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_put_row', *args)
        native_body = self._encode_with_native('_encode_put_row', *args)
        self.assertEqual(python_body, native_body)

    def test_put_row_return_pk(self):
        """return_type=RT_PK"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = [('col1', 'val1')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_PK
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_put_row', *args)
        native_body = self._encode_with_native('_encode_put_row', *args)
        self.assertEqual(python_body, native_body)

    def test_put_row_multi_pk(self):
        """多主键列（string + integer + binary）"""
        table_name = 'test_table'
        primary_key = [
            ('pk1', 'value1'),
            ('pk2', 123),
            ('pk3', bytearray(b'\x00\x01\x02'))
        ]
        attribute_columns = [('col1', 'val1')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_put_row', *args)
        native_body = self._encode_with_native('_encode_put_row', *args)
        self.assertEqual(python_body, native_body)

    def test_put_row_with_transaction_id(self):
        """带事务 ID"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = [('col1', 'val1')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        transaction_id = 'txn_123456'
        args = (table_name, row, condition, return_type, transaction_id)
        python_body = self._encode_with_python('_encode_put_row', *args)
        native_body = self._encode_with_native('_encode_put_row', *args)
        self.assertEqual(python_body, native_body)

    def test_update_row_put(self):
        """PUT 更新（添加/覆盖列）"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = {
            'PUT': [('col1', 'val1'), ('col2', 123), ('col3', 45.67)],
        }
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_update_row', *args)
        native_body = self._encode_with_native('_encode_update_row', *args)
        self.assertEqual(python_body, native_body)

    def test_update_row_delete(self):
        """DELETE 更新（删除指定列的指定版本）"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = {
            'DELETE': [('col1', None, 1234567890), ('col2', None, 1234567891)],
        }
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_update_row', *args)
        native_body = self._encode_with_native('_encode_update_row', *args)
        self.assertEqual(python_body, native_body)

    def test_update_row_delete_all(self):
        """DELETE_ALL 更新"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = {
            'DELETE_ALL': ['col1', 'col2'],
        }
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_update_row', *args)
        native_body = self._encode_with_native('_encode_update_row', *args)
        self.assertEqual(python_body, native_body)

    def test_update_row_mixed(self):
        """PUT + DELETE + DELETE_ALL 混合"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = {
            'PUT': [('col1', 'val1'), ('col2', 123), ('col5', bytearray(b'binary_data'))],
            'DELETE': [('col3', None, 1234567890)],
            'DELETE_ALL': ['col4'],
        }
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_update_row', *args)
        native_body = self._encode_with_native('_encode_update_row', *args)
        self.assertEqual(python_body, native_body)

    def test_update_row_expect_exist(self):
        """带 EXPECT_EXIST 条件"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = {'PUT': [('col1', 'val1')]}
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.EXPECT_EXIST)
        return_type = ReturnType.RT_NONE
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_update_row', *args)
        native_body = self._encode_with_native('_encode_update_row', *args)
        self.assertEqual(python_body, native_body)

    def test_update_row_return_pk(self):
        """return_type=RT_PK"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        attribute_columns = {'PUT': [('col1', 'val1')]}
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_PK
        args = (table_name, row, condition, return_type, None)
        python_body = self._encode_with_python('_encode_update_row', *args)
        native_body = self._encode_with_native('_encode_update_row', *args)
        self.assertEqual(python_body, native_body)

    def test_delete_row_multi_pk_types(self):
        """多主键类型：string + integer + binary"""
        table_name = 'test_table'
        primary_key = [
            ('pk1', 'value1'),
            ('pk2', 123),
            ('pk3', bytearray(b'\x00\x01\x02'))
        ]
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        args = (table_name, primary_key, condition, return_type, None)
        python_body = self._encode_with_python('_encode_delete_row', *args)
        native_body = self._encode_with_native('_encode_delete_row', *args)
        self.assertEqual(python_body, native_body)

    def test_delete_row_expect_exist(self):
        """EXPECT_EXIST 条件"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        condition = Condition(RowExistenceExpectation.EXPECT_EXIST)
        return_type = ReturnType.RT_NONE
        args = (table_name, primary_key, condition, return_type, None)
        python_body = self._encode_with_python('_encode_delete_row', *args)
        native_body = self._encode_with_native('_encode_delete_row', *args)
        self.assertEqual(python_body, native_body)

    def test_delete_row_return_pk(self):
        """return_type=RT_PK"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_PK
        args = (table_name, primary_key, condition, return_type, None)
        python_body = self._encode_with_python('_encode_delete_row', *args)
        native_body = self._encode_with_native('_encode_delete_row', *args)
        self.assertEqual(python_body, native_body)

    def test_delete_row_with_transaction_id(self):
        """带事务 ID"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_NONE
        transaction_id = 'txn_123456'
        args = (table_name, primary_key, condition, return_type, transaction_id)
        python_body = self._encode_with_python('_encode_delete_row', *args)
        native_body = self._encode_with_native('_encode_delete_row', *args)
        self.assertEqual(python_body, native_body)

    def test_get_row_no_columns_to_get(self):
        """不指定返回列（columns_to_get=None）"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        columns_to_get = None
        column_filter = None
        max_version = 1
        time_range = None
        start_column = None
        end_column = None
        token = None
        transaction_id = None
        args = (table_name, primary_key, columns_to_get, column_filter, max_version,
                time_range, start_column, end_column, token, transaction_id)
        python_body = self._encode_with_python('_encode_get_row', *args)
        native_body = self._encode_with_native('_encode_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_get_row_time_range_tuple(self):
        """time_range 为 (start, end) 元组"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        columns_to_get = ['col1', 'col2']
        column_filter = None
        max_version = 1
        time_range = (1234567890, 1234567990)
        start_column = None
        end_column = None
        token = None
        transaction_id = None
        args = (table_name, primary_key, columns_to_get, column_filter, max_version,
                time_range, start_column, end_column, token, transaction_id)
        python_body = self._encode_with_python('_encode_get_row', *args)
        native_body = self._encode_with_native('_encode_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_get_row_time_range_int(self):
        """time_range 为整数（指定版本时间戳）"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        columns_to_get = ['col1', 'col2']
        column_filter = None
        max_version = 1
        time_range = 1234567890
        start_column = None
        end_column = None
        token = None
        transaction_id = None
        args = (table_name, primary_key, columns_to_get, column_filter, max_version,
                time_range, start_column, end_column, token, transaction_id)
        python_body = self._encode_with_python('_encode_get_row', *args)
        native_body = self._encode_with_native('_encode_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_get_row_start_end_column(self):
        """指定 start_column 和 end_column"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        columns_to_get = ['col1', 'col2', 'col3']
        column_filter = None
        max_version = 1
        time_range = None
        start_column = 'col1'
        end_column = 'col3'
        token = None
        transaction_id = None
        args = (table_name, primary_key, columns_to_get, column_filter, max_version,
                time_range, start_column, end_column, token, transaction_id)
        python_body = self._encode_with_python('_encode_get_row', *args)
        native_body = self._encode_with_native('_encode_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_get_row_binary_pk(self):
        """bytearray 类型主键"""
        table_name = 'test_table'
        primary_key = [('pk1', bytearray(b'\x00\x01\x02\x03'))]
        columns_to_get = ['col1', 'col2']
        column_filter = None
        max_version = 1
        time_range = None
        start_column = None
        end_column = None
        token = None
        transaction_id = None
        args = (table_name, primary_key, columns_to_get, column_filter, max_version,
                time_range, start_column, end_column, token, transaction_id)
        python_body = self._encode_with_python('_encode_get_row', *args)
        native_body = self._encode_with_native('_encode_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_get_row_multi_pk(self):
        """3 个主键列"""
        table_name = 'test_table'
        primary_key = [
            ('pk1', 'value1'),
            ('pk2', 123),
            ('pk3', bytearray(b'\x00\x01\x02'))
        ]
        columns_to_get = ['col1', 'col2']
        column_filter = None
        max_version = 1
        time_range = None
        start_column = None
        end_column = None
        token = None
        transaction_id = None
        args = (table_name, primary_key, columns_to_get, column_filter, max_version,
                time_range, start_column, end_column, token, transaction_id)
        python_body = self._encode_with_python('_encode_get_row', *args)
        native_body = self._encode_with_native('_encode_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_get_row_with_transaction_id(self):
        """带事务 ID"""
        table_name = 'test_table'
        primary_key = [('pk1', 'value1')]
        columns_to_get = ['col1', 'col2']
        column_filter = None
        max_version = 1
        time_range = None
        start_column = None
        end_column = None
        token = None
        transaction_id = 'txn_123456'
        args = (table_name, primary_key, columns_to_get, column_filter, max_version,
                time_range, start_column, end_column, token, transaction_id)
        python_body = self._encode_with_python('_encode_get_row', *args)
        native_body = self._encode_with_native('_encode_get_row', *args)
        self.assertEqual(python_body, native_body)


    # === batch_get_row / batch_write_row / get_range encoder consistency tests ===

    def test_batch_get_row_single_table_single_row(self):
        """Test batch_get_row with single table and single row."""
        request = BatchGetRowRequest()
        item = TableInBatchGetRowItem('test_table', [
            [('pk1', 'value1')],
        ])
        request.add(item)
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_get_row', *args)
        native_body = self._encode_with_native('_encode_batch_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_get_row_single_table_multi_rows(self):
        """Test batch_get_row with single table and multiple rows."""
        request = BatchGetRowRequest()
        item = TableInBatchGetRowItem('test_table', [
            [('pk1', 'value1')],
            [('pk1', 'value2')],
            [('pk1', 'value3')],
        ])
        request.add(item)
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_get_row', *args)
        native_body = self._encode_with_native('_encode_batch_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_get_row_multi_tables(self):
        """Test batch_get_row with multiple tables."""
        request = BatchGetRowRequest()
        item1 = TableInBatchGetRowItem('table1', [
            [('pk1', 'value1')],
        ])
        item2 = TableInBatchGetRowItem('table2', [
            [('pk1', 'value2')],
        ])
        request.add(item1)
        request.add(item2)
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_get_row', *args)
        native_body = self._encode_with_native('_encode_batch_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_get_row_with_columns_to_get(self):
        """Test batch_get_row with columns_to_get specified."""
        request = BatchGetRowRequest()
        item = TableInBatchGetRowItem('test_table', [
            [('pk1', 'value1')],
        ], columns_to_get=['col1', 'col2'])
        request.add(item)
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_get_row', *args)
        native_body = self._encode_with_native('_encode_batch_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_get_row_with_max_version(self):
        """Test batch_get_row with max_version specified."""
        request = BatchGetRowRequest()
        item = TableInBatchGetRowItem('test_table', [
            [('pk1', 'value1')],
        ], max_version=2)
        request.add(item)
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_get_row', *args)
        native_body = self._encode_with_native('_encode_batch_get_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_write_row_single_put(self):
        """Test batch_write_row with single table and single PUT operation."""
        request = BatchWriteRowRequest()
        row = Row([('pk1', 'value1')], [('col1', 'attr_value')])
        put_item = PutRowItem(row, Condition(RowExistenceExpectation.IGNORE))
        request.add(TableInBatchWriteRowItem('test_table', [put_item]))
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_write_row', *args)
        native_body = self._encode_with_native('_encode_batch_write_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_write_row_multi_puts(self):
        """Test batch_write_row with single table and multiple PUT operations."""
        request = BatchWriteRowRequest()
        row1 = Row([('pk1', 'value1')], [('col1', 'attr1')])
        row2 = Row([('pk1', 'value2')], [('col1', 'attr2')])
        row3 = Row([('pk1', 'value3')], [('col1', 'attr3')])
        put_item1 = PutRowItem(row1, Condition(RowExistenceExpectation.IGNORE))
        put_item2 = PutRowItem(row2, Condition(RowExistenceExpectation.IGNORE))
        put_item3 = PutRowItem(row3, Condition(RowExistenceExpectation.IGNORE))
        request.add(TableInBatchWriteRowItem('test_table', [put_item1, put_item2, put_item3]))
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_write_row', *args)
        native_body = self._encode_with_native('_encode_batch_write_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_write_row_multi_tables_mixed(self):
        """Test batch_write_row with multiple tables and mixed operations."""
        request = BatchWriteRowRequest()
        row1 = Row([('pk1', 'value1')], [('col1', 'attr1')])
        put_item = PutRowItem(row1, Condition(RowExistenceExpectation.IGNORE))
        update_row = Row([('pk1', 'value2')], {'PUT': [('col1', 'new_attr')]})
        update_item = UpdateRowItem(update_row, Condition(RowExistenceExpectation.IGNORE))
        delete_row = Row([('pk1', 'value3')])
        delete_item = DeleteRowItem(delete_row, Condition(RowExistenceExpectation.IGNORE))
        request.add(TableInBatchWriteRowItem('table1', [put_item]))
        request.add(TableInBatchWriteRowItem('table2', [update_item, delete_item]))
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_write_row', *args)
        native_body = self._encode_with_native('_encode_batch_write_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_write_row_with_condition(self):
        """Test batch_write_row with different conditions."""
        request = BatchWriteRowRequest()
        row1 = Row([('pk1', 'value1')], [('col1', 'attr1')])
        put_item1 = PutRowItem(row1, Condition(RowExistenceExpectation.EXPECT_EXIST))
        row2 = Row([('pk1', 'value2')], [('col1', 'attr2')])
        put_item2 = PutRowItem(row2, Condition(RowExistenceExpectation.EXPECT_NOT_EXIST))
        row3 = Row([('pk1', 'value3')], [('col1', 'attr3')])
        put_item3 = PutRowItem(row3, Condition(RowExistenceExpectation.IGNORE))
        request.add(TableInBatchWriteRowItem('test_table', [put_item1, put_item2, put_item3]))
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_write_row', *args)
        native_body = self._encode_with_native('_encode_batch_write_row', *args)
        self.assertEqual(python_body, native_body)

    def test_batch_write_row_with_return_pk(self):
        """Test batch_write_row with return_type=RT_PK."""
        request = BatchWriteRowRequest()
        row = Row([('pk1', 'value1')], [('col1', 'attr1')])
        put_item = PutRowItem(row, Condition(RowExistenceExpectation.IGNORE), ReturnType.RT_PK)
        request.add(TableInBatchWriteRowItem('test_table', [put_item]))
        args = (request,)
        python_body = self._encode_with_python('_encode_batch_write_row', *args)
        native_body = self._encode_with_native('_encode_batch_write_row', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_forward(self):
        """Test get_range with FORWARD direction."""
        start_pk = [('pk1', 'a')]
        end_pk = [('pk1', 'z')]
        args = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 100, None, 1, None, None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_backward(self):
        """Test get_range with BACKWARD direction."""
        start_pk = [('pk1', 'z')]
        end_pk = [('pk1', 'a')]
        args = (
            'test_table', Direction.BACKWARD, start_pk, end_pk,
            None, 100, None, 1, None, None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_inf_boundary(self):
        """Test get_range with INF_MIN and INF_MAX boundaries."""
        start_pk = [('pk1', INF_MIN)]
        end_pk = [('pk1', INF_MAX)]
        args = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 100, None, 1, None, None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_with_columns_to_get(self):
        """Test get_range with columns_to_get specified."""
        start_pk = [('pk1', 'a')]
        end_pk = [('pk1', 'z')]
        args = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            ['col1', 'col2'], 100, None, 1, None, None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_with_limit(self):
        """Test get_range with limit specified."""
        start_pk = [('pk1', 'a')]
        end_pk = [('pk1', 'z')]
        args = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 50, None, 1, None, None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_with_start_end_column(self):
        """Test get_range with start_column and end_column specified."""
        start_pk = [('pk1', 'a')]
        end_pk = [('pk1', 'z')]
        args = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 100, None, 1, None, 'col1', 'col3', None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_with_token(self):
        """Test get_range with token specified."""
        start_pk = [('pk1', 'a')]
        end_pk = [('pk1', 'z')]
        token = b'\x00\x01\x02\x03'
        args = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 100, None, 1, None, None, None, token, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_with_time_range(self):
        """Test get_range with time_range (tuple and int)."""
        start_pk = [('pk1', 'a')]
        end_pk = [('pk1', 'z')]
        # Test with tuple
        args_tuple = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 100, None, None, (1000, 2000), None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args_tuple)
        native_body = self._encode_with_native('_encode_get_range', *args_tuple)
        self.assertEqual(python_body, native_body)
        # Test with int
        args_int = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 100, None, None, 1500, None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args_int)
        native_body = self._encode_with_native('_encode_get_range', *args_int)
        self.assertEqual(python_body, native_body)

    def test_get_range_multi_pk(self):
        """Test get_range with multiple primary key columns."""
        start_pk = [('pk1', 'a'), ('pk2', 1)]
        end_pk = [('pk1', 'z'), ('pk2', 100)]
        args = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 100, None, 1, None, None, None, None, None,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

    def test_get_range_with_transaction_id(self):
        """Test get_range with transaction_id specified."""
        start_pk = [('pk1', 'a')]
        end_pk = [('pk1', 'z')]
        transaction_id = 'test-tx-id-12345'
        args = (
            'test_table', Direction.FORWARD, start_pk, end_pk,
            None, 100, None, 1, None, None, None, None, transaction_id,
        )
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)


    # === search / parallel_scan encoder consistency tests ===

    def test_search_match_all(self):
        """Test search encoder with MatchAllQuery."""
        query = MatchAllQuery()
        search_query = SearchQuery(query)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_term_query(self):
        """Test search encoder with TermQuery."""
        query = TermQuery('field_name', 'field_value')
        search_query = SearchQuery(query)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_with_limit(self):
        """Test search encoder with limit."""
        query = MatchAllQuery()
        search_query = SearchQuery(query, limit=10)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_with_offset(self):
        """Test search encoder with offset."""
        query = MatchAllQuery()
        search_query = SearchQuery(query, offset=5)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_get_total_count(self):
        """Test search encoder with get_total_count=True."""
        query = MatchAllQuery()
        search_query = SearchQuery(query, get_total_count=True)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_columns_to_get_none(self):
        """Test search encoder with ColumnsToGet NONE."""
        query = MatchAllQuery()
        search_query = SearchQuery(query)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_columns_to_get_all(self):
        """Test search encoder with ColumnsToGet ALL."""
        query = MatchAllQuery()
        search_query = SearchQuery(query)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_columns_to_get_specified(self):
        """Test search encoder with ColumnsToGet SPECIFIED."""
        query = MatchAllQuery()
        search_query = SearchQuery(query)
        columns_to_get = ColumnsToGet(column_names=['col1', 'col2'], return_type=ColumnReturnType.SPECIFIED)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_columns_to_get_all_from_index(self):
        """Test search encoder with ColumnsToGet ALL_FROM_INDEX."""
        query = MatchAllQuery()
        search_query = SearchQuery(query)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_with_routing_keys(self):
        """Test search encoder with routing_keys."""
        query = MatchAllQuery()
        search_query = SearchQuery(query)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        routing_keys = [[('pk1', 'v1')], [('pk1', 'v2')]]
        args = ('test_table', 'test_index', search_query, columns_to_get, routing_keys, None)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_search_with_timeout(self):
        """Test search encoder with timeout_s."""
        query = MatchAllQuery()
        search_query = SearchQuery(query)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', search_query, columns_to_get, None, 30)
        python_body = self._encode_with_python('_encode_search', *args)
        native_body = self._encode_with_native('_encode_search', *args)
        self.assertEqual(python_body, native_body)

    def test_parallel_scan_match_all(self):
        """Test parallel_scan encoder with MatchAllQuery."""
        query = MatchAllQuery()
        scan_query = ScanQuery(query, limit=10, next_token=None, current_parallel_id=0, max_parallel=1)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', scan_query, None, columns_to_get, None)
        python_body = self._encode_with_python('_encode_parallel_scan', *args)
        native_body = self._encode_with_native('_encode_parallel_scan', *args)
        self.assertEqual(python_body, native_body)

    def test_parallel_scan_term_query(self):
        """Test parallel_scan encoder with TermQuery."""
        query = TermQuery('field_name', 'field_value')
        scan_query = ScanQuery(query, limit=10, next_token=None, current_parallel_id=0, max_parallel=1)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', scan_query, None, columns_to_get, None)
        python_body = self._encode_with_python('_encode_parallel_scan', *args)
        native_body = self._encode_with_native('_encode_parallel_scan', *args)
        self.assertEqual(python_body, native_body)

    def test_parallel_scan_with_session_id(self):
        """Test parallel_scan encoder with session_id."""
        query = MatchAllQuery()
        scan_query = ScanQuery(query, limit=10, next_token=None, current_parallel_id=0, max_parallel=1)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', scan_query, 'session_123', columns_to_get, None)
        python_body = self._encode_with_python('_encode_parallel_scan', *args)
        native_body = self._encode_with_native('_encode_parallel_scan', *args)
        self.assertEqual(python_body, native_body)

    def test_parallel_scan_different_parallel_params(self):
        """Test parallel_scan encoder with different parallel params."""
        query = MatchAllQuery()
        scan_query = ScanQuery(query, limit=10, next_token=None, current_parallel_id=2, max_parallel=5)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', scan_query, None, columns_to_get, None)
        python_body = self._encode_with_python('_encode_parallel_scan', *args)
        native_body = self._encode_with_native('_encode_parallel_scan', *args)
        self.assertEqual(python_body, native_body)

    def test_parallel_scan_columns_to_get_none(self):
        """Test parallel_scan encoder with ColumnsToGet NONE."""
        query = MatchAllQuery()
        scan_query = ScanQuery(query, limit=10, next_token=None, current_parallel_id=0, max_parallel=1)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', scan_query, None, columns_to_get, None)
        python_body = self._encode_with_python('_encode_parallel_scan', *args)
        native_body = self._encode_with_native('_encode_parallel_scan', *args)
        self.assertEqual(python_body, native_body)

    def test_parallel_scan_columns_to_get_all_from_index(self):
        """Test parallel_scan encoder with ColumnsToGet ALL_FROM_INDEX."""
        query = MatchAllQuery()
        scan_query = ScanQuery(query, limit=10, next_token=None, current_parallel_id=0, max_parallel=1)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX)
        args = ('test_table', 'test_index', scan_query, None, columns_to_get, None)
        python_body = self._encode_with_python('_encode_parallel_scan', *args)
        native_body = self._encode_with_native('_encode_parallel_scan', *args)
        self.assertEqual(python_body, native_body)

    def test_parallel_scan_with_timeout(self):
        """Test parallel_scan encoder with timeout_s."""
        query = MatchAllQuery()
        scan_query = ScanQuery(query, limit=10, next_token=None, current_parallel_id=0, max_parallel=1)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.NONE)
        args = ('test_table', 'test_index', scan_query, None, columns_to_get, 60)
        python_body = self._encode_with_python('_encode_parallel_scan', *args)
        native_body = self._encode_with_native('_encode_parallel_scan', *args)
        self.assertEqual(python_body, native_body)

    def _random_string(self, length=10):
        """Generate random string for testing."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def _random_bytes(self, length=10):
        """Generate random bytearray for testing."""
        return bytearray(random.getrandbits(8) for _ in range(length))

    def _random_pk_value(self, pk_type):
        """Generate random primary key value based on type."""
        if pk_type == 'string':
            return self._random_string()
        elif pk_type == 'integer':
            return random.randint(0, 1000000)
        elif pk_type == 'binary':
            return self._random_bytes()
        else:
            raise ValueError(f"Unsupported PK type: {pk_type}")

    def _random_column_value(self, col_type):
        """Generate random column value based on type."""
        if col_type == 'string':
            return self._random_string()
        elif col_type == 'integer':
            return random.randint(-1000000, 1000000)
        elif col_type == 'double':
            return random.uniform(-1000000.0, 1000000.0)
        elif col_type == 'boolean':
            return random.choice([True, False])
        elif col_type == 'binary':
            return self._random_bytes()
        else:
            raise ValueError(f"Unsupported column type: {col_type}")

    def test_put_row_all_pk_types(self):
        """Test put_row encoder with all PK types."""
        random.seed(42)
        for pk_type in ['string', 'integer', 'binary']:
            with self.subTest(pk_type=pk_type):
                pk_value = self._random_pk_value(pk_type)
                primary_key = [('pk', pk_value)]
                row = Row(primary_key, [])
                condition = Condition(RowExistenceExpectation.IGNORE)
                args = ('test_table', row, condition, None, None)
                python_body = self._encode_with_python('_encode_put_row', *args)
                native_body = self._encode_with_native('_encode_put_row', *args)
                self.assertEqual(python_body, native_body)

    def test_put_row_all_column_types(self):
        """Test put_row encoder with all column types."""
        random.seed(42)
        for col_type in ['string', 'integer', 'double', 'boolean', 'binary']:
            with self.subTest(col_type=col_type):
                col_value = self._random_column_value(col_type)
                primary_key = [('pk', 'test')]
                row = Row(primary_key, [('col', col_value)])
                condition = Condition(RowExistenceExpectation.IGNORE)
                args = ('test_table', row, condition, None, None)
                python_body = self._encode_with_python('_encode_put_row', *args)
                native_body = self._encode_with_native('_encode_put_row', *args)
                self.assertEqual(python_body, native_body)

    def test_put_row_pk_column_type_combinations(self):
        """Test put_row encoder with PK and column type combinations."""
        random.seed(42)
        for pk_type in ['string', 'integer', 'binary']:
            for col_type in ['string', 'integer', 'double', 'boolean', 'binary']:
                with self.subTest(pk_type=pk_type, col_type=col_type):
                    pk_value = self._random_pk_value(pk_type)
                    col_value = self._random_column_value(col_type)
                    primary_key = [('pk', pk_value)]
                    row = Row(primary_key, [('col', col_value)])
                    condition = Condition(RowExistenceExpectation.IGNORE)
                    args = ('test_table', row, condition, None, None)
                    python_body = self._encode_with_python('_encode_put_row', *args)
                    native_body = self._encode_with_native('_encode_put_row', *args)
                    self.assertEqual(python_body, native_body)

    def test_put_row_multi_columns_random(self):
        """Test put_row encoder with multiple columns of all types."""
        random.seed(42)
        for _ in range(5):
            primary_key = [('pk', self._random_string())]
            columns = [
                ('col_string', self._random_column_value('string')),
                ('col_integer', self._random_column_value('integer')),
                ('col_double', self._random_column_value('double')),
                ('col_boolean', self._random_column_value('boolean')),
                ('col_binary', self._random_column_value('binary')),
            ]
            row = Row(primary_key, columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            args = ('test_table', row, condition, None, None)
            python_body = self._encode_with_python('_encode_put_row', *args)
            native_body = self._encode_with_native('_encode_put_row', *args)
            self.assertEqual(python_body, native_body)

    def test_put_row_with_timestamp_random(self):
        """Test put_row encoder with random timestamps."""
        random.seed(42)
        for _ in range(5):
            primary_key = [('pk', self._random_string())]
            timestamp = random.randint(1000000000, 9999999999)
            columns = [('col', 'value', timestamp)]
            row = Row(primary_key, columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            args = ('test_table', row, condition, None, None)
            python_body = self._encode_with_python('_encode_put_row', *args)
            native_body = self._encode_with_native('_encode_put_row', *args)
            self.assertEqual(python_body, native_body)

    def test_update_row_all_column_types(self):
        """Test update_row encoder with PUT operations for all column types."""
        random.seed(42)
        for col_type in ['string', 'integer', 'double', 'boolean', 'binary']:
            with self.subTest(col_type=col_type):
                col_value = self._random_column_value(col_type)
                primary_key = [('pk', 'test')]
                update_of_attribute_columns = {
                    'PUT': [('col', col_value)],
                }
                row = Row(primary_key, update_of_attribute_columns)
                condition = Condition(RowExistenceExpectation.IGNORE)
                args = ('test_table', row, condition, None, None)
                python_body = self._encode_with_python('_encode_update_row', *args)
                native_body = self._encode_with_native('_encode_update_row', *args)
                self.assertEqual(python_body, native_body)

    def test_update_row_mixed_random(self):
        """Test update_row encoder with mixed PUT, DELETE, DELETE_ALL operations."""
        random.seed(42)
        for _ in range(5):
            primary_key = [('pk', self._random_string())]
            update_of_attribute_columns = {
                'PUT': [('col_put', self._random_column_value('string'))],
                'DELETE': [('col_delete', None, random.randint(1000000000, 9999999999))],
                'DELETE_ALL': ['col_delete_all1', 'col_delete_all2'],
            }
            row = Row(primary_key, update_of_attribute_columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            args = ('test_table', row, condition, None, None)
            python_body = self._encode_with_python('_encode_update_row', *args)
            native_body = self._encode_with_native('_encode_update_row', *args)
            self.assertEqual(python_body, native_body)

    def test_delete_row_all_pk_types(self):
        """Test delete_row encoder with all PK types."""
        random.seed(42)
        for pk_type in ['string', 'integer', 'binary']:
            with self.subTest(pk_type=pk_type):
                pk_value = self._random_pk_value(pk_type)
                primary_key = [('pk', pk_value)]
                condition = Condition(RowExistenceExpectation.IGNORE)
                args = ('test_table', primary_key, condition, None, None)
                python_body = self._encode_with_python('_encode_delete_row', *args)
                native_body = self._encode_with_native('_encode_delete_row', *args)
                self.assertEqual(python_body, native_body)

    def test_get_row_all_pk_types(self):
        """Test get_row encoder with all PK types."""
        random.seed(42)
        for pk_type in ['string', 'integer', 'binary']:
            with self.subTest(pk_type=pk_type):
                pk_value = self._random_pk_value(pk_type)
                primary_key = [('pk', pk_value)]
                args = ('test_table', primary_key, ['col1', 'col2'], None, 10, None, None, None, None, None)
                python_body = self._encode_with_python('_encode_get_row', *args)
                native_body = self._encode_with_native('_encode_get_row', *args)
                self.assertEqual(python_body, native_body)

    def test_batch_get_row_random_pk_types(self):
        """Test batch_get_row encoder with random PK types."""
        random.seed(42)
        for pk_type in ['string', 'integer', 'binary']:
            with self.subTest(pk_type=pk_type):
                request = BatchGetRowRequest()
                for i in range(3):
                    pk_value = self._random_pk_value(pk_type)
                    primary_key = [('pk', pk_value)]
                    item = TableInBatchGetRowItem('test_table', [primary_key], columns_to_get=['col1', 'col2'])
                    request.add(item)
                args = (request,)
                python_body = self._encode_with_python('_encode_batch_get_row', *args)
                native_body = self._encode_with_native('_encode_batch_get_row', *args)
                self.assertEqual(python_body, native_body)

    def test_batch_write_row_random_types(self):
        """Test batch_write_row encoder with random types."""
        random.seed(42)
        for pk_type in ['string', 'integer', 'binary']:
            with self.subTest(pk_type=pk_type):
                request = BatchWriteRowRequest()
                # PutRowItem
                pk_value = self._random_pk_value(pk_type)
                primary_key = [('pk', pk_value)]
                row = Row(primary_key, [('col', self._random_column_value('string'))])
                condition = Condition(RowExistenceExpectation.IGNORE)
                put_item = PutRowItem(row, condition)
                # UpdateRowItem
                pk_value = self._random_pk_value(pk_type)
                primary_key = [('pk', pk_value)]
                row = Row(primary_key, {'PUT': [('col', self._random_column_value('integer'))]})
                update_item = UpdateRowItem(row, condition)
                # DeleteRowItem
                pk_value = self._random_pk_value(pk_type)
                primary_key = [('pk', pk_value)]
                row = Row(primary_key, [])
                delete_item = DeleteRowItem(row, condition)
                request.add(TableInBatchWriteRowItem('test_table', [put_item, update_item, delete_item]))
                args = (request,)
                python_body = self._encode_with_python('_encode_batch_write_row', *args)
                native_body = self._encode_with_native('_encode_batch_write_row', *args)
                self.assertEqual(python_body, native_body)

    def test_get_range_all_pk_types(self):
        """Test get_range encoder with all PK types and boundary values."""
        random.seed(42)
        for pk_type in ['string', 'integer', 'binary']:
            with self.subTest(pk_type=pk_type):
                start_pk_value = self._random_pk_value(pk_type)
                end_pk_value = self._random_pk_value(pk_type)
                start_primary_key = [('pk', start_pk_value)]
                end_primary_key = [('pk', end_pk_value)]
                args = ('test_table', 'FORWARD', start_primary_key, end_primary_key, None, 10, None, 1, None, None, None, None, None)
                python_body = self._encode_with_python('_encode_get_range', *args)
                native_body = self._encode_with_native('_encode_get_range', *args)
                self.assertEqual(python_body, native_body)

        # Test with INF_MIN and INF_MAX boundaries
        start_primary_key = [('pk', INF_MIN)]
        end_primary_key = [('pk', INF_MAX)]
        args = ('test_table', 'FORWARD', start_primary_key, end_primary_key, None, 10, None, 1, None, None, None, None, None)
        python_body = self._encode_with_python('_encode_get_range', *args)
        native_body = self._encode_with_native('_encode_get_range', *args)
        self.assertEqual(python_body, native_body)

@unittest.skipUnless(NATIVE_DECODER_AVAILABLE, "Native C++ decoder not available")
class TestNativeDecoderConsistency(unittest.TestCase):
    """Test that native decoder produces output consistent with Python decoder.

    These tests only run when the native C++ module is available.
    """

    def _decode_with_python(self, method_name, body, request_id='test'):
        """Force Python decoding by temporarily disabling native decoder."""
        decoder = OTSProtoBufferDecoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)
        original_flag = decoder._use_native_decoder
        decoder._use_native_decoder = False
        try:
            self.assertFalse(decoder._use_native_decoder)
            method = getattr(decoder, method_name)
            return method(body, request_id)
        finally:
            decoder._use_native_decoder = original_flag

    def _decode_with_native(self, method_name, body, request_id='test'):
        """Force native decoding."""
        decoder = OTSProtoBufferDecoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)
        original_flag = decoder._use_native_decoder
        decoder._use_native_decoder = True
        try:
            self.assertTrue(decoder._use_native_decoder)
            method = getattr(decoder, method_name)
            return method(body, request_id)
        finally:
            decoder._use_native_decoder = original_flag

    def test_decode_get_row_empty_consistency(self):
        proto = pb2.GetRowResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 0
        proto.row = b''
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_get_row', body)
        native_result, _ = self._decode_with_native('_decode_get_row', body)

        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[0].write, native_result[0].write)
        self.assertEqual(python_result[1], native_result[1])  # both None

    def test_decode_put_row_empty_consistency(self):
        proto = pb2.PutRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_put_row', body)
        native_result, _ = self._decode_with_native('_decode_put_row', body)

        self.assertEqual(python_result[0].write, native_result[0].write)
        self.assertEqual(python_result[1], native_result[1])

    def test_decode_get_range_empty_consistency(self):
        proto = pb2.GetRangeResponse()
        proto.consumed.capacity_unit.read = 3
        proto.rows = b''
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_get_range', body)
        native_result, _ = self._decode_with_native('_decode_get_range', body)

        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[1], native_result[1])  # both None
        self.assertEqual(len(python_result[2]), len(native_result[2]))


    # === Additional decoder consistency tests ===

    def test_decode_get_row_with_consumed(self):
        """Test decode_get_row with different consumed values."""
        proto = pb2.GetRowResponse()
        proto.consumed.capacity_unit.read = 5
        proto.consumed.capacity_unit.write = 3
        proto.row = b''
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_get_row', body)
        native_result, _ = self._decode_with_native('_decode_get_row', body)

        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[0].write, native_result[0].write)
        self.assertEqual(python_result[1], native_result[1])

    def test_decode_put_row_with_consumed(self):
        """Test decode_put_row with different consumed values."""
        proto = pb2.PutRowResponse()
        proto.consumed.capacity_unit.read = 2
        proto.consumed.capacity_unit.write = 4
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_put_row', body)
        native_result, _ = self._decode_with_native('_decode_put_row', body)

        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[0].write, native_result[0].write)
        self.assertEqual(python_result[1], native_result[1])

    def test_decode_update_row_empty(self):
        """Test decode_update_row with empty response."""
        proto = pb2.UpdateRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_update_row', body)
        native_result, _ = self._decode_with_native('_decode_update_row', body)

        self.assertEqual(python_result[0].write, native_result[0].write)
        self.assertEqual(python_result[1], native_result[1])

    def test_decode_update_row_with_consumed(self):
        """Test decode_update_row with different consumed values."""
        proto = pb2.UpdateRowResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 2
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_update_row', body)
        native_result, _ = self._decode_with_native('_decode_update_row', body)

        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[0].write, native_result[0].write)
        self.assertEqual(python_result[1], native_result[1])

    def test_decode_delete_row_empty(self):
        """Test decode_delete_row with empty response."""
        proto = pb2.DeleteRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_delete_row', body)
        native_result, _ = self._decode_with_native('_decode_delete_row', body)

        self.assertEqual(python_result[0].write, native_result[0].write)
        self.assertEqual(python_result[1], native_result[1])

    def test_decode_delete_row_with_consumed(self):
        """Test decode_delete_row with different consumed values."""
        proto = pb2.DeleteRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 3
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_delete_row', body)
        native_result, _ = self._decode_with_native('_decode_delete_row', body)

        self.assertEqual(python_result[0].write, native_result[0].write)
        self.assertEqual(python_result[1], native_result[1])

    def test_decode_batch_get_row_empty(self):
        """Test decode_batch_get_row with empty response."""
        proto = pb2.BatchGetRowResponse()
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_batch_get_row', body)
        native_result, _ = self._decode_with_native('_decode_batch_get_row', body)

        self.assertEqual(len(python_result), len(native_result))

    def test_decode_batch_write_row_empty(self):
        """Test decode_batch_write_row with empty response."""
        proto = pb2.BatchWriteRowResponse()
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_batch_write_row', body)
        native_result, _ = self._decode_with_native('_decode_batch_write_row', body)

        self.assertEqual(len(python_result), len(native_result))

    def test_decode_get_range_with_next_token(self):
        """Test decode_get_range with next_token."""
        proto = pb2.GetRangeResponse()
        proto.consumed.capacity_unit.read = 3
        proto.rows = b''
        proto.next_token = b'next_token_bytes'
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_get_range', body)
        native_result, _ = self._decode_with_native('_decode_get_range', body)

        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[3], native_result[3])

    def test_decode_get_range_with_consumed(self):
        """Test decode_get_range with different consumed values."""
        proto = pb2.GetRangeResponse()
        proto.consumed.capacity_unit.read = 7
        proto.consumed.capacity_unit.write = 2
        proto.rows = b''
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_get_range', body)
        native_result, _ = self._decode_with_native('_decode_get_range', body)

        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[0].write, native_result[0].write)

    def test_decode_search_empty(self):
        """Test decode_search with empty response."""
        proto = search_pb2.SearchResponse()
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_search', body)
        native_result, _ = self._decode_with_native('_decode_search', body)

        self.assertEqual(python_result.next_token, native_result.next_token)
        self.assertEqual(python_result.is_all_succeed, native_result.is_all_succeed)

    def test_decode_search_with_total_count(self):
        """Test decode_search with total_hits and is_all_succeed."""
        proto = search_pb2.SearchResponse()
        proto.total_hits = 100
        proto.is_all_succeed = True
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_search', body)
        native_result, _ = self._decode_with_native('_decode_search', body)

        self.assertEqual(python_result.total_count, native_result.total_count)
        self.assertEqual(python_result.is_all_succeed, native_result.is_all_succeed)

    def test_decode_parallel_scan_empty(self):
        """Test decode_parallel_scan with empty response."""
        proto = search_pb2.ParallelScanResponse()
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_parallel_scan', body)
        native_result, _ = self._decode_with_native('_decode_parallel_scan', body)

        self.assertEqual(python_result.next_token, native_result.next_token)

    def test_decode_parallel_scan_with_next_token(self):
        """Test decode_parallel_scan with next_token."""
        proto = search_pb2.ParallelScanResponse()
        proto.next_token = b'scan_next_token'
        body = proto.SerializeToString()

        python_result, _ = self._decode_with_python('_decode_parallel_scan', body)
        native_result, _ = self._decode_with_native('_decode_parallel_scan', body)

        self.assertEqual(python_result.next_token, native_result.next_token)

    # -------------------------------------------------------------------------
    # Randomized decoder consistency test helpers
    # -------------------------------------------------------------------------

    def _random_string(self, length=10):
        """Generate random string of specified length."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def _random_bytes(self, length=10):
        """Generate random bytes of specified length."""
        return bytearray(random.getrandbits(8) for _ in range(length))

    def _random_pk_value(self, pk_type):
        """Generate random primary key value based on type."""
        if pk_type == 'INTEGER':
            return random.randint(-1000000, 1000000)
        elif pk_type == 'STRING':
            return self._random_string(10)
        elif pk_type == 'BINARY':
            return self._random_bytes(10)
        elif pk_type == 'INF_MIN':
            return INF_MIN
        elif pk_type == 'INF_MAX':
            return INF_MAX
        elif pk_type == 'AUTO_INCREMENT':
            return PK_AUTO_INCR
        else:
            raise ValueError(f"Unknown pk_type: {pk_type}")

    def _random_column_value(self, col_type):
        """Generate random column value based on type."""
        if col_type == 'INTEGER':
            return random.randint(-1000000, 1000000)
        elif col_type == 'STRING':
            return self._random_string(10)
        elif col_type == 'BINARY':
            return self._random_bytes(10)
        elif col_type == 'BOOLEAN':
            return random.choice([True, False])
        elif col_type == 'DOUBLE':
            return random.uniform(-1000.0, 1000.0)
        else:
            raise ValueError(f"Unknown col_type: {col_type}")

    def _build_row_bytes(self, primary_keys, columns):
        """Build row bytes using PlainBufferBuilder."""
        from tablestore.plainbuffer.plain_buffer_builder import PlainBufferBuilder
        return bytes(PlainBufferBuilder.serialize_for_put_row(primary_keys, columns))

    # -------------------------------------------------------------------------
    # Randomized decoder consistency tests
    # -------------------------------------------------------------------------

    def test_decode_get_row_with_row_data_all_types(self):
        """Test decode_get_row with row data of all PK and column types."""
        random.seed(42)
        pk_types = ['INTEGER', 'STRING', 'BINARY']
        col_types = ['INTEGER', 'STRING', 'BINARY', 'BOOLEAN', 'DOUBLE']
        
        for pk_type in pk_types:
            for col_type in col_types:
                with self.subTest(pk_type=pk_type, col_type=col_type):
                    # Construct GetRowResponse with row data
                    primary_keys = [('pk1', self._random_pk_value(pk_type))]
                    columns = [('col1', self._random_column_value(col_type), 1000)]
                    row_bytes = self._build_row_bytes(primary_keys, columns)
                    
                    proto = pb2.GetRowResponse()
                    proto.consumed.capacity_unit.read = 1
                    proto.consumed.capacity_unit.write = 0
                    proto.row = row_bytes
                    body = proto.SerializeToString()
                    
                    python_result, _ = self._decode_with_python('_decode_get_row', body)
                    native_result, _ = self._decode_with_native('_decode_get_row', body)
                    
                    self.assertEqual(python_result[0].read, native_result[0].read)
                    self.assertEqual(python_result[0].write, native_result[0].write)
                    
                    # Compare primary keys
                    python_row = python_result[1]
                    native_row = native_result[1]
                    self.assertEqual(len(python_row.primary_key), len(native_row.primary_key))
                    for pk_py, pk_na in zip(python_row.primary_key, native_row.primary_key):
                        self.assertEqual(pk_py[0], pk_na[0])  # name
                        self.assertEqual(pk_py[1], pk_na[1])  # value
                    
                    # Compare attribute columns
                    self.assertEqual(len(python_row.attribute_columns), len(native_row.attribute_columns))
                    for col_py, col_na in zip(python_row.attribute_columns, native_row.attribute_columns):
                        self.assertEqual(col_py[0], col_na[0])  # name
                        self.assertEqual(col_py[1], col_na[1])  # value
                        self.assertEqual(col_py[2], col_na[2])  # timestamp

    def test_decode_put_row_with_row_data(self):
        """Test decode_put_row with return_row."""
        random.seed(42)
        primary_keys = [('pk1', 'value1')]
        columns = [('col1', 'str_val', 1000)]
        row_bytes = self._build_row_bytes(primary_keys, columns)
        
        proto = pb2.PutRowResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 1
        proto.row = row_bytes
        body = proto.SerializeToString()
        
        python_result, _ = self._decode_with_python('_decode_put_row', body)
        native_result, _ = self._decode_with_native('_decode_put_row', body)
        
        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[0].write, native_result[0].write)
        
        # Compare primary keys
        python_row = python_result[1]
        native_row = native_result[1]
        self.assertEqual(len(python_row.primary_key), len(native_row.primary_key))
        for pk_py, pk_na in zip(python_row.primary_key, native_row.primary_key):
            self.assertEqual(pk_py[0], pk_na[0])
            self.assertEqual(pk_py[1], pk_na[1])
        
        # Compare attribute columns
        self.assertEqual(len(python_row.attribute_columns), len(native_row.attribute_columns))
        for col_py, col_na in zip(python_row.attribute_columns, native_row.attribute_columns):
            self.assertEqual(col_py[0], col_na[0])
            self.assertEqual(col_py[1], col_na[1])
            self.assertEqual(col_py[2], col_na[2])

    def test_decode_update_row_with_row_data(self):
        """Test decode_update_row with return_row."""
        random.seed(42)
        primary_keys = [('pk1', 'value1')]
        columns = [('col1', 'str_val', 1000)]
        row_bytes = self._build_row_bytes(primary_keys, columns)
        
        proto = pb2.UpdateRowResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 1
        proto.row = row_bytes
        body = proto.SerializeToString()
        
        python_result, _ = self._decode_with_python('_decode_update_row', body)
        native_result, _ = self._decode_with_native('_decode_update_row', body)
        
        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[0].write, native_result[0].write)
        
        # Compare primary keys
        python_row = python_result[1]
        native_row = native_result[1]
        self.assertEqual(len(python_row.primary_key), len(native_row.primary_key))
        for pk_py, pk_na in zip(python_row.primary_key, native_row.primary_key):
            self.assertEqual(pk_py[0], pk_na[0])
            self.assertEqual(pk_py[1], pk_na[1])
        
        # Compare attribute columns
        self.assertEqual(len(python_row.attribute_columns), len(native_row.attribute_columns))
        for col_py, col_na in zip(python_row.attribute_columns, native_row.attribute_columns):
            self.assertEqual(col_py[0], col_na[0])
            self.assertEqual(col_py[1], col_na[1])
            self.assertEqual(col_py[2], col_na[2])

    def test_decode_delete_row_with_row_data(self):
        """Test decode_delete_row with return_row."""
        random.seed(42)
        primary_keys = [('pk1', 'value1')]
        columns = [('col1', 'str_val', 1000)]
        row_bytes = self._build_row_bytes(primary_keys, columns)
        
        proto = pb2.DeleteRowResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 1
        proto.row = row_bytes
        body = proto.SerializeToString()
        
        python_result, _ = self._decode_with_python('_decode_delete_row', body)
        native_result, _ = self._decode_with_native('_decode_delete_row', body)
        
        self.assertEqual(python_result[0].read, native_result[0].read)
        self.assertEqual(python_result[0].write, native_result[0].write)
        
        # Compare primary keys
        python_row = python_result[1]
        native_row = native_result[1]
        self.assertEqual(len(python_row.primary_key), len(native_row.primary_key))
        for pk_py, pk_na in zip(python_row.primary_key, native_row.primary_key):
            self.assertEqual(pk_py[0], pk_na[0])
            self.assertEqual(pk_py[1], pk_na[1])
        
        # Compare attribute columns
        self.assertEqual(len(python_row.attribute_columns), len(native_row.attribute_columns))
        for col_py, col_na in zip(python_row.attribute_columns, native_row.attribute_columns):
            self.assertEqual(col_py[0], col_na[0])
            self.assertEqual(col_py[1], col_na[1])
            self.assertEqual(col_py[2], col_na[2])

    def test_decode_batch_get_row_with_row_data(self):
        """Test decode_batch_get_row with actual row data."""
        random.seed(42)
        primary_keys = [('pk1', 'value1')]
        columns = [('col1', 'str_val', 1000)]
        row_bytes = self._build_row_bytes(primary_keys, columns)
        
        proto = pb2.BatchGetRowResponse()
        table_item = proto.tables.add()
        table_item.table_name = 'test_table'
        
        row_item = table_item.rows.add()
        row_item.is_ok = True
        row_item.consumed.capacity_unit.read = 1
        row_item.consumed.capacity_unit.write = 0
        row_item.row = row_bytes
        
        body = proto.SerializeToString()
        
        python_result, _ = self._decode_with_python('_decode_batch_get_row', body)
        native_result, _ = self._decode_with_native('_decode_batch_get_row', body)
        
        # batch_get_row returns a list of (table_name, row_items_list) or similar structure
        # Just compare the overall length and structure
        self.assertEqual(len(python_result), len(native_result))

    def test_decode_batch_write_row_with_row_data(self):
        """Test decode_batch_write_row with actual row data."""
        random.seed(42)
        primary_keys = [('pk1', 'value1')]
        columns = [('col1', 'str_val', 1000)]
        row_bytes = self._build_row_bytes(primary_keys, columns)
        
        proto = pb2.BatchWriteRowResponse()
        table_item = proto.tables.add()
        table_item.table_name = 'test_table'
        
        row_item = table_item.rows.add()
        row_item.is_ok = True
        row_item.consumed.capacity_unit.read = 1
        row_item.consumed.capacity_unit.write = 1
        row_item.row = row_bytes
        
        body = proto.SerializeToString()
        
        python_result, _ = self._decode_with_python('_decode_batch_write_row', body)
        native_result, _ = self._decode_with_native('_decode_batch_write_row', body)
        
        # batch_write_row returns a list of (table_name, row_items_list) or similar structure
        # Just compare the overall length and structure
        self.assertEqual(len(python_result), len(native_result))

    def test_decode_get_range_with_row_data_all_types(self):
        """Test decode_get_range with row data of all PK and column types."""
        random.seed(42)
        pk_types = ['INTEGER', 'STRING', 'BINARY']
        col_types = ['INTEGER', 'STRING', 'BINARY', 'BOOLEAN', 'DOUBLE']
        
        for pk_type in pk_types:
            for col_type in col_types:
                with self.subTest(pk_type=pk_type, col_type=col_type):
                    # Construct GetRangeResponse with row data
                    primary_keys = [('pk1', self._random_pk_value(pk_type))]
                    columns = [('col1', self._random_column_value(col_type), 1000)]
                    row_bytes = self._build_row_bytes(primary_keys, columns)
                    
                    proto = pb2.GetRangeResponse()
                    proto.consumed.capacity_unit.read = 1
                    proto.consumed.capacity_unit.write = 0
                    proto.rows = row_bytes
                    body = proto.SerializeToString()
                    
                    python_result, _ = self._decode_with_python('_decode_get_range', body)
                    native_result, _ = self._decode_with_native('_decode_get_range', body)
                    
                    self.assertEqual(python_result[0].read, native_result[0].read)
                    self.assertEqual(python_result[0].write, native_result[0].write)
                    
                    # Compare rows
                    python_rows = python_result[2]
                    native_rows = native_result[2]
                    self.assertEqual(len(python_rows), len(native_rows))
                    for py_row, na_row in zip(python_rows, native_rows):
                        self.assertEqual(len(py_row.primary_key), len(na_row.primary_key))
                        for pk_py, pk_na in zip(py_row.primary_key, na_row.primary_key):
                            self.assertEqual(pk_py[0], pk_na[0])
                            self.assertEqual(pk_py[1], pk_na[1])
                        
                        self.assertEqual(len(py_row.attribute_columns), len(na_row.attribute_columns))
                        for col_py, col_na in zip(py_row.attribute_columns, na_row.attribute_columns):
                            self.assertEqual(col_py[0], col_na[0])
                            self.assertEqual(col_py[1], col_na[1])
                            self.assertEqual(col_py[2], col_na[2])

    def test_decode_get_row_random_consumed(self):
        """Test decode_get_row with random consumed values."""
        random.seed(42)
        for i in range(5):
            with self.subTest(iteration=i):
                read = random.randint(0, 100)
                write = random.randint(0, 100)
                
                primary_keys = [('pk1', 'value1')]
                columns = [('col1', 'str_val', 1000)]
                row_bytes = self._build_row_bytes(primary_keys, columns)
                
                proto = pb2.GetRowResponse()
                proto.consumed.capacity_unit.read = read
                proto.consumed.capacity_unit.write = write
                proto.row = bytes(row_bytes)
                body = proto.SerializeToString()
                
                python_result, _ = self._decode_with_python('_decode_get_row', body)
                native_result, _ = self._decode_with_native('_decode_get_row', body)
                
                self.assertEqual(python_result[0].read, native_result[0].read)
                self.assertEqual(python_result[0].write, native_result[0].write)

    def test_decode_get_range_random_consumed_and_token(self):
        """Test decode_get_range with random consumed and next_token."""
        random.seed(42)
        for i in range(5):
            with self.subTest(iteration=i):
                read = random.randint(0, 100)
                write = random.randint(0, 100)
                
                primary_keys = [('pk1', 'value1')]
                columns = [('col1', 'str_val', 1000)]
                row_bytes = self._build_row_bytes(primary_keys, columns)
                
                # Use valid PlainBuffer data for next_start_primary_key
                next_pk = [('pk1', self._random_string())]
                next_pk_bytes = self._build_row_bytes(next_pk, [])
                
                proto = pb2.GetRangeResponse()
                proto.consumed.capacity_unit.read = read
                proto.consumed.capacity_unit.write = write
                proto.rows = bytes(row_bytes)
                proto.next_start_primary_key = bytes(next_pk_bytes)
                body = proto.SerializeToString()
                
                python_result, _ = self._decode_with_python('_decode_get_range', body)
                native_result, _ = self._decode_with_native('_decode_get_range', body)
                
                self.assertEqual(python_result[0].read, native_result[0].read)
                self.assertEqual(python_result[0].write, native_result[0].write)

    def test_decode_search_random_total_hits(self):
        """Test decode_search with random total_hits and is_all_succeed."""
        random.seed(42)
        for i in range(5):
            with self.subTest(iteration=i):
                total_hits = random.randint(0, 10000)
                is_all_succeed = random.choice([True, False])
                
                proto = search_pb2.SearchResponse()
                proto.total_hits = total_hits
                proto.is_all_succeed = is_all_succeed
                body = proto.SerializeToString()
                
                python_result, _ = self._decode_with_python('_decode_search', body)
                native_result, _ = self._decode_with_native('_decode_search', body)
                
                self.assertEqual(python_result.total_count, native_result.total_count)
                self.assertEqual(python_result.is_all_succeed, native_result.is_all_succeed)

###############################################################################
# TestImportFallback — encoder.py / decoder.py import-failure branches
###############################################################################

class TestImportFallback(unittest.TestCase):
    """Test that encoder.py and decoder.py gracefully handle ImportError
    when the native C++ SDK is not available."""

    def test_encoder_import_error_sets_flag_false(self):
        """When tablestore.ots_sdk is not importable, NATIVE_ENCODER_AVAILABLE
        should be False and _native_sdk should be None."""
        import importlib
        import tablestore.encoder as encoder_mod

        # Save originals
        orig_available = encoder_mod.NATIVE_ENCODER_AVAILABLE
        orig_sdk = encoder_mod._native_sdk

        try:
            # Remove the ots_sdk sub-module from sys.modules so reload
            # will try to import it again.
            saved = {}
            for key in list(sys.modules.keys()):
                if 'tablestore.ots_sdk' in key or key == 'ots_sdk':
                    saved[key] = sys.modules.pop(key)

            # Inject a broken entry so "from tablestore.ots_sdk import ots_sdk"
            # raises ImportError.
            from unittest.mock import patch
            original_import = __import__

            def fail_ots_sdk(name, *args, **kwargs):
                if name == 'tablestore.ots_sdk' or 'tablestore.ots_sdk' in name:
                    raise ImportError("mocked: no ots_sdk")
                return original_import(name, *args, **kwargs)

            with patch('builtins.__import__', side_effect=fail_ots_sdk):
                importlib.reload(encoder_mod)

            self.assertFalse(encoder_mod.NATIVE_ENCODER_AVAILABLE)
            self.assertIsNone(encoder_mod._native_sdk)
        finally:
            # Restore original state
            encoder_mod.NATIVE_ENCODER_AVAILABLE = orig_available
            encoder_mod._native_sdk = orig_sdk
            # Clean up ots_sdk modules from sys.modules
            for key in list(sys.modules.keys()):
                if 'tablestore.ots_sdk' in key or key == 'ots_sdk':
                    sys.modules.pop(key, None)
            # Reload encoder module to fully restore original state
            importlib.reload(encoder_mod)
            # Also reload protocol.py which imports NativeEncodedBytes from encoder
            import tablestore.protocol as protocol_mod
            importlib.reload(protocol_mod)
            # Restore saved modules
            if saved:
                sys.modules.update(saved)

    def test_decoder_import_error_sets_flag_false(self):
        """When tablestore.ots_sdk is not importable, NATIVE_DECODER_AVAILABLE
        should be False and _native_sdk should be None."""
        import importlib
        import tablestore.decoder as decoder_mod

        orig_available = decoder_mod.NATIVE_DECODER_AVAILABLE
        orig_sdk = decoder_mod._native_sdk

        try:
            saved = {}
            for key in list(sys.modules.keys()):
                if 'tablestore.ots_sdk' in key or key == 'ots_sdk':
                    saved[key] = sys.modules.pop(key)

            from unittest.mock import patch
            original_import = __import__

            def fail_ots_sdk(name, *args, **kwargs):
                if name == 'tablestore.ots_sdk' or 'tablestore.ots_sdk' in name:
                    raise ImportError("mocked: no ots_sdk")
                return original_import(name, *args, **kwargs)

            with patch('builtins.__import__', side_effect=fail_ots_sdk):
                importlib.reload(decoder_mod)

            self.assertFalse(decoder_mod.NATIVE_DECODER_AVAILABLE)
            self.assertIsNone(decoder_mod._native_sdk)
        finally:
            # Restore original state
            decoder_mod.NATIVE_DECODER_AVAILABLE = orig_available
            decoder_mod._native_sdk = orig_sdk
            # Clean up ots_sdk modules from sys.modules
            for key in list(sys.modules.keys()):
                if 'tablestore.ots_sdk' in key or key == 'ots_sdk':
                    sys.modules.pop(key, None)
            # Reload decoder module to fully restore original state
            importlib.reload(decoder_mod)
            # Also reload protocol.py which imports from encoder/decoder
            import tablestore.protocol as protocol_mod
            importlib.reload(protocol_mod)
            # Restore saved modules
            if saved:
                sys.modules.update(saved)


###############################################################################
# TestEncoderFallback — native encoder exception → Python fallback
###############################################################################

class TestEncoderFallback2(unittest.TestCase):
    """Test that when using native encoder but the native encoder
    raises an exception, the encoder falls back to the Python implementation
    and returns a protobuf object (not NativeEncodedBytes)."""

    def setUp(self):
        # must set native_fallback to True to test fallback
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=True)
        # A mock _native_sdk whose encode_* methods all raise RuntimeError
        self.mock_sdk = unittest.mock.MagicMock()
        for method_name in [
            'encode_get_row', 'encode_put_row', 'encode_update_row',
            'encode_delete_row', 'encode_batch_get_row', 'encode_batch_write_row',
            'encode_get_range', 'encode_search', 'encode_parallel_scan',
        ]:
            getattr(self.mock_sdk, method_name).side_effect = RuntimeError(
                f"Simulated native {method_name} failure"
            )

    def _patch(self):
        """Return a context manager that patches encoder to use our mock."""
        import contextlib

        @contextlib.contextmanager
        def _ctx():
            import tablestore.encoder as enc
            orig_use_native = self.encoder._use_native_encoder
            orig_sdk = enc._native_sdk
            self.encoder._use_native_encoder = True
            enc._native_sdk = self.mock_sdk
            try:
                yield
            finally:
                self.encoder._use_native_encoder = orig_use_native
                enc._native_sdk = orig_sdk

        return _ctx()

    # --- get_row ---
    def test_fallback_encode_get_row(self):
        """Native encode_get_row fails → Python fallback returns protobuf."""
        with self._patch():
            result = self.encoder._encode_get_row(
                'test_table',
                [('pk', 'val')],
                columns_to_get=None, column_filter=None,
                max_version=1, time_range=None,
                start_column=None, end_column=None,
                token=None, transaction_id=None,
            )
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, pb2.GetRowRequest)
        self.assertEqual(result.table_name, 'test_table')

    # --- put_row ---
    def test_fallback_encode_put_row(self):
        """Native encode_put_row fails → Python fallback returns protobuf."""
        row = Row([('pk', 'val')], [('col', 'data')])
        cond = Condition(RowExistenceExpectation.IGNORE)
        with self._patch():
            result = self.encoder._encode_put_row(
                'test_table', row, cond, ReturnType.RT_NONE, None,
            )
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, pb2.PutRowRequest)
        self.assertEqual(result.table_name, 'test_table')

    # --- update_row ---
    def test_fallback_encode_update_row(self):
        """Native encode_update_row fails → Python fallback returns protobuf."""
        row = Row([('pk', 'val')], {'PUT': [('col', 'data')]})
        cond = Condition(RowExistenceExpectation.IGNORE)
        with self._patch():
            result = self.encoder._encode_update_row(
                'test_table', row, cond, ReturnType.RT_NONE, None,
            )
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, pb2.UpdateRowRequest)
        self.assertEqual(result.table_name, 'test_table')

    # --- delete_row ---
    def test_fallback_encode_delete_row(self):
        """Native encode_delete_row fails → Python fallback returns protobuf."""
        cond = Condition(RowExistenceExpectation.IGNORE)
        with self._patch():
            result = self.encoder._encode_delete_row(
                'test_table', [('pk', 'val')], cond, ReturnType.RT_NONE, None,
            )
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, pb2.DeleteRowRequest)
        self.assertEqual(result.table_name, 'test_table')

    # --- batch_get_row ---
    def test_fallback_encode_batch_get_row(self):
        """Native encode_batch_get_row fails → Python fallback returns protobuf."""
        request = BatchGetRowRequest()
        item = TableInBatchGetRowItem('test_table', [[('pk', 'v1')]], columns_to_get=['col1'])
        request.add(item)
        with self._patch():
            result = self.encoder._encode_batch_get_row(request)
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, pb2.BatchGetRowRequest)

    # --- batch_write_row ---
    def test_fallback_encode_batch_write_row(self):
        """Native encode_batch_write_row fails → Python fallback returns protobuf."""
        request = BatchWriteRowRequest()
        put_item = PutRowItem(
            Row([('pk', 'val')], [('col', 'data')]),
            Condition(RowExistenceExpectation.IGNORE),
        )
        request.add(TableInBatchWriteRowItem('test_table', [put_item]))
        with self._patch():
            result = self.encoder._encode_batch_write_row(request)
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, pb2.BatchWriteRowRequest)

    # --- get_range ---
    def test_fallback_encode_get_range(self):
        """Native encode_get_range fails → Python fallback returns protobuf."""
        with self._patch():
            result = self.encoder._encode_get_range(
                'test_table', Direction.FORWARD,
                [('pk', INF_MIN)], [('pk', INF_MAX)],
                columns_to_get=None, limit=100, column_filter=None,
                max_version=1, time_range=None,
                start_column=None, end_column=None,
                token=None, transaction_id=None,
            )
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, pb2.GetRangeRequest)
        self.assertEqual(result.table_name, 'test_table')

    # --- search ---
    def test_fallback_encode_search(self):
        """Native encode_search fails → Python fallback returns protobuf."""
        search_query = SearchQuery(MatchAllQuery(), limit=10, get_total_count=True)
        cols = ColumnsToGet(return_type=ColumnReturnType.ALL)
        with self._patch():
            result = self.encoder._encode_search(
                'test_table', 'test_index', search_query,
                columns_to_get=cols, routing_keys=None, timeout_s=None,
            )
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, search_pb2.SearchRequest)
        self.assertEqual(result.table_name, 'test_table')
        self.assertEqual(result.index_name, 'test_index')

    # --- parallel_scan ---
    def test_fallback_encode_parallel_scan(self):
        """Native encode_parallel_scan fails → Python fallback returns protobuf."""
        scan_query = ScanQuery(
            MatchAllQuery(), limit=10,
            next_token=None, current_parallel_id=0, max_parallel=1,
        )
        cols = ColumnsToGet(return_type=ColumnReturnType.ALL)
        with self._patch():
            result = self.encoder._encode_parallel_scan(
                'test_table', 'test_index', scan_query,
                session_id=None, columns_to_get=cols, timeout_s=None,
            )
        self.assertNotIsInstance(result, NativeEncodedBytes)
        self.assertIsInstance(result, search_pb2.ParallelScanRequest)
        self.assertEqual(result.table_name, 'test_table')
        self.assertEqual(result.index_name, 'test_index')


###############################################################################
# TestDecoderFallback — native decoder exception → Python fallback
###############################################################################

class TestDecoderFallback2(unittest.TestCase):
    """Test that when using native decoder but the native decoder
    raises an exception, the decoder falls back to the Python implementation
    and returns correct results."""

    def setUp(self):
        self.encoder = OTSProtoBufferEncoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)
        # must set native_fallback to True to test fallback
        self.decoder = OTSProtoBufferDecoder('utf-8', enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=True)
        # A mock _native_sdk whose decode_* methods all raise RuntimeError
        self.mock_sdk = unittest.mock.MagicMock()
        for method_name in [
            'decode_get_row', 'decode_put_row', 'decode_update_row',
            'decode_delete_row', 'decode_batch_get_row', 'decode_batch_write_row',
            'decode_get_range', 'decode_search', 'decode_parallel_scan',
        ]:
            getattr(self.mock_sdk, method_name).side_effect = RuntimeError(
                f"Simulated native {method_name} failure"
            )

    def _patch_decoder(self):
        """Return a context manager that patches decoder to use our mock."""
        import contextlib

        @contextlib.contextmanager
        def _ctx():
            import tablestore.decoder as dec
            orig_use_native = self.decoder._use_native_decoder
            orig_sdk = dec._native_sdk
            self.decoder._use_native_decoder = True
            dec._native_sdk = self.mock_sdk
            try:
                yield
            finally:
                self.decoder._use_native_decoder = orig_use_native
                dec._native_sdk = orig_sdk

        return _ctx()

    def _make_get_row_response_body(self):
        """Build a valid GetRowResponse protobuf body with no row."""
        from tablestore.protobuf import table_store_pb2 as pb
        proto = pb.GetRowResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 0
        proto.row = b''
        return proto.SerializeToString()

    def _make_put_row_response_body(self):
        """Build a valid PutRowResponse protobuf body."""
        from tablestore.protobuf import table_store_pb2 as pb
        proto = pb.PutRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        return proto.SerializeToString()

    def _make_update_row_response_body(self):
        """Build a valid UpdateRowResponse protobuf body."""
        from tablestore.protobuf import table_store_pb2 as pb
        proto = pb.UpdateRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        return proto.SerializeToString()

    def _make_delete_row_response_body(self):
        """Build a valid DeleteRowResponse protobuf body."""
        from tablestore.protobuf import table_store_pb2 as pb
        proto = pb.DeleteRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        return proto.SerializeToString()

    def _make_batch_get_row_response_body(self):
        """Build a valid BatchGetRowResponse protobuf body (empty tables)."""
        from tablestore.protobuf import table_store_pb2 as pb
        proto = pb.BatchGetRowResponse()
        return proto.SerializeToString()

    def _make_batch_write_row_response_body(self):
        """Build a valid BatchWriteRowResponse protobuf body (empty tables)."""
        from tablestore.protobuf import table_store_pb2 as pb
        proto = pb.BatchWriteRowResponse()
        return proto.SerializeToString()

    def _make_get_range_response_body(self):
        """Build a valid GetRangeResponse protobuf body (empty rows)."""
        from tablestore.protobuf import table_store_pb2 as pb
        proto = pb.GetRangeResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 0
        proto.rows = b''
        return proto.SerializeToString()

    def _make_search_response_body(self):
        """Build a valid SearchResponse protobuf body (no rows)."""
        proto = search_pb2.SearchResponse()
        proto.total_hits = 0
        proto.is_all_succeed = True
        return proto.SerializeToString()

    def _make_parallel_scan_response_body(self):
        """Build a valid ParallelScanResponse protobuf body (no rows)."""
        proto = search_pb2.ParallelScanResponse()
        return proto.SerializeToString()

    # --- get_row ---
    def test_fallback_decode_get_row(self):
        """Native decode_get_row fails → Python fallback works correctly."""
        body = self._make_get_row_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_get_row(body, 'req-001')
        consumed, return_row, next_token = result
        self.assertIsNotNone(consumed)
        self.assertIsNone(return_row)

    # --- put_row ---
    def test_fallback_decode_put_row(self):
        """Native decode_put_row fails → Python fallback works correctly."""
        body = self._make_put_row_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_put_row(body, 'req-002')
        consumed, return_row = result
        self.assertIsNotNone(consumed)
        self.assertIsNone(return_row)

    # --- update_row ---
    def test_fallback_decode_update_row(self):
        """Native decode_update_row fails → Python fallback works correctly."""
        body = self._make_update_row_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_update_row(body, 'req-003')
        consumed, return_row = result
        self.assertIsNotNone(consumed)
        self.assertIsNone(return_row)

    # --- delete_row ---
    def test_fallback_decode_delete_row(self):
        """Native decode_delete_row fails → Python fallback works correctly."""
        body = self._make_delete_row_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_delete_row(body, 'req-004')
        consumed, return_row = result
        self.assertIsNotNone(consumed)
        self.assertIsNone(return_row)

    # --- batch_get_row ---
    def test_fallback_decode_batch_get_row(self):
        """Native decode_batch_get_row fails → Python fallback works correctly."""
        body = self._make_batch_get_row_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_batch_get_row(body, 'req-005')
        self.assertIsNotNone(result)

    # --- batch_write_row ---
    def test_fallback_decode_batch_write_row(self):
        """Native decode_batch_write_row fails → Python fallback works correctly."""
        body = self._make_batch_write_row_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_batch_write_row(body, 'req-006')
        self.assertIsNotNone(result)

    # --- get_range ---
    def test_fallback_decode_get_range(self):
        """Native decode_get_range fails → Python fallback works correctly."""
        body = self._make_get_range_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_get_range(body, 'req-007')
        capacity_unit, next_start_pk, row_list, next_token = result
        self.assertIsNotNone(capacity_unit)
        self.assertIsNone(next_start_pk)
        self.assertEqual(len(row_list), 0)

    # --- search ---
    def test_fallback_decode_search(self):
        """Native decode_search fails → Python fallback works correctly."""
        body = self._make_search_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_search(body, 'req-008')
        self.assertIsNotNone(result)
        self.assertEqual(result.total_count, 0)
        self.assertTrue(result.is_all_succeed)

    # --- parallel_scan ---
    def test_fallback_decode_parallel_scan(self):
        """Native decode_parallel_scan fails → Python fallback works correctly."""
        body = self._make_parallel_scan_response_body()
        with self._patch_decoder():
            result, _ = self.decoder._decode_parallel_scan(body, 'req-009')
        self.assertIsNotNone(result)
        self.assertEqual(len(result.rows), 0)


if __name__ == '__main__':
    unittest.main()
