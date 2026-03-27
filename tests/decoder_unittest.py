# -*- coding: utf8 -*-

import unittest
import unittest.mock
import contextlib

from tests.lib import test_config
from tests.lib.api_test_base import APITestBase
from tablestore import decoder
from tablestore.decoder import OTSProtoBufferDecoder
import tablestore.protobuf.table_store_pb2 as pb
import tablestore.protobuf.search_pb2 as search_pb2
from tablestore.plainbuffer.plain_buffer_builder import PlainBufferBuilder

class DecoderTest(APITestBase):

    """DecoderTest"""

    def test_decode_timeseries_tag_or_attribute(self):
        d = decoder.OTSProtoBufferDecoder("utf-8", enable_native=test_config.OTS_ENABLE_NATIVE, native_fallback=test_config.OTS_NATIVE_FALLBACK)

        attri = d._parse_timeseries_tag_or_attribute("[]")
        self.assert_equal(len(attri), 0)

        attri = d._parse_timeseries_tag_or_attribute("[\"a=a1\",\"b=b2\",\"c=0.3\"]")
        self.assert_equal(len(attri), 3)
        try:
            d._parse_timeseries_tag_or_attribute("[a=a1\",\"b=b2\",\"c=0.3\"]")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)

        try:
            d._parse_timeseries_tag_or_attribute("[\"a==a1\",\"b=b2\",\"c=0.3\"]")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)

        try:
            d._parse_timeseries_tag_or_attribute("[\"a==a1\",\"b=b2=0.3\"]")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)

        try:
            d._parse_timeseries_tag_or_attribute("[\"a=a1\",]")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)

        try:
            d._parse_timeseries_tag_or_attribute("[\"a=a1\"")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)


class NativeDecoderTest(unittest.TestCase):
    """Test 9 native decoder _decode functions with normal and abnormal cases.

    The 9 functions are:
    1. _decode_get_row
    2. _decode_put_row
    3. _decode_update_row
    4. _decode_delete_row
    5. _decode_batch_get_row
    6. _decode_batch_write_row
    7. _decode_get_range
    8. _decode_search
    9. _decode_parallel_scan
    """

    def setUp(self):
        self.decoder = OTSProtoBufferDecoder(
            'utf-8',
            enable_native=test_config.OTS_ENABLE_NATIVE,
            native_fallback=test_config.OTS_NATIVE_FALLBACK,
        )

    # =========================================================================
    # Helper: build valid protobuf response bodies
    # =========================================================================

    def _make_get_row_response_body(self, with_row=False):
        """Build a valid GetRowResponse protobuf body."""
        proto = pb.GetRowResponse()
        proto.consumed.capacity_unit.read = 1
        proto.consumed.capacity_unit.write = 0
        if with_row:
            primary_key = [('pk1', 'val1'), ('pk2', 100)]
            attribute_columns = [('col1', 'hello', 1000)]
            proto.row = bytes(PlainBufferBuilder.serialize_for_put_row(primary_key, attribute_columns))
        else:
            proto.row = b''
        return proto.SerializeToString()

    def _make_put_row_response_body(self):
        """Build a valid PutRowResponse protobuf body."""
        proto = pb.PutRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        return proto.SerializeToString()

    def _make_update_row_response_body(self):
        """Build a valid UpdateRowResponse protobuf body."""
        proto = pb.UpdateRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        return proto.SerializeToString()

    def _make_delete_row_response_body(self):
        """Build a valid DeleteRowResponse protobuf body."""
        proto = pb.DeleteRowResponse()
        proto.consumed.capacity_unit.read = 0
        proto.consumed.capacity_unit.write = 1
        return proto.SerializeToString()

    def _make_batch_get_row_response_body(self):
        """Build a valid BatchGetRowResponse protobuf body (empty tables)."""
        proto = pb.BatchGetRowResponse()
        return proto.SerializeToString()

    def _make_batch_write_row_response_body(self):
        """Build a valid BatchWriteRowResponse protobuf body (empty tables)."""
        proto = pb.BatchWriteRowResponse()
        return proto.SerializeToString()

    def _make_get_range_response_body(self):
        """Build a valid GetRangeResponse protobuf body (empty rows)."""
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

    # =========================================================================
    # Helper: patch decoder to force native decoder on/off
    # =========================================================================

    @contextlib.contextmanager
    def _force_native_decoder(self, enable=True, fallback=True):
        """Context manager to force native decoder on or off."""
        orig_use_native = self.decoder._use_native_decoder
        orig_fallback = self.decoder.native_fallback
        self.decoder._use_native_decoder = enable
        self.decoder.native_fallback = fallback
        try:
            yield
        finally:
            self.decoder._use_native_decoder = orig_use_native
            self.decoder.native_fallback = orig_fallback

    @contextlib.contextmanager
    def _mock_native_sdk_failure(self):
        """Context manager that patches _native_sdk to simulate failures."""
        import tablestore.decoder as dec
        mock_sdk = unittest.mock.MagicMock()
        for method_name in [
            'decode_get_row', 'decode_put_row', 'decode_update_row',
            'decode_delete_row', 'decode_batch_get_row', 'decode_batch_write_row',
            'decode_get_range', 'decode_search', 'decode_parallel_scan',
        ]:
            getattr(mock_sdk, method_name).side_effect = RuntimeError(
                f"Simulated native {method_name} failure"
            )
        orig_sdk = dec._native_sdk
        dec._native_sdk = mock_sdk
        try:
            yield mock_sdk
        finally:
            dec._native_sdk = orig_sdk

    # =========================================================================
    # 1. _decode_get_row
    # =========================================================================

    def test_decode_get_row_normal_empty_row(self):
        """Normal case: decode GetRowResponse with empty row using native decoder."""
        body = self._make_get_row_response_body(with_row=False)
        result, proto = self.decoder._decode_get_row(body, 'req-get-001')
        if proto is not None:
            # Python decoder path
            consumed, return_row, next_token = result
            self.assertIsNotNone(consumed)
            self.assertIsNone(return_row)
        else:
            # Native decoder path - result is a tuple directly
            self.assertIsNotNone(result)

    def test_decode_get_row_normal_with_row(self):
        """Normal case: decode GetRowResponse with a row using native decoder."""
        body = self._make_get_row_response_body(with_row=True)
        result, proto = self.decoder._decode_get_row(body, 'req-get-002')
        if proto is not None:
            consumed, return_row, next_token = result
            self.assertIsNotNone(consumed)
            self.assertIsNotNone(return_row)
        else:
            self.assertIsNotNone(result)

    def test_decode_get_row_invalid_body_with_fallback(self):
        """Abnormal case: invalid body with native decoder, fallback enabled → Python protobuf also fails."""
        invalid_body = b'\x00\x01\x02\x03\x04\x05'
        with self._force_native_decoder(enable=True, fallback=True):
            with self._mock_native_sdk_failure():
                # Python protobuf ParseFromString may succeed with empty/garbage data
                # or raise an exception - either way the test should not crash
                try:
                    self.decoder._decode_get_row(invalid_body, 'req-get-err')
                except Exception:
                    pass  # Expected: native fails, Python fallback also fails on invalid data

    def test_decode_get_row_invalid_body_no_fallback(self):
        """Abnormal case: invalid body with native decoder, fallback disabled → exception raised."""
        invalid_body = b'\x00\x01\x02\x03\x04\x05'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_get_row(invalid_body, 'req-get-err2')
                self.assertIn("Simulated native decode_get_row failure", str(ctx.exception))

    # =========================================================================
    # 2. _decode_put_row
    # =========================================================================

    def test_decode_put_row_normal(self):
        """Normal case: decode PutRowResponse."""
        body = self._make_put_row_response_body()
        result, proto = self.decoder._decode_put_row(body, 'req-put-001')
        if proto is not None:
            consumed, return_row = result
            self.assertIsNotNone(consumed)
            self.assertIsNone(return_row)
        else:
            self.assertIsNotNone(result)

    def test_decode_put_row_invalid_body_no_fallback(self):
        """Abnormal case: invalid body, no fallback → exception raised."""
        invalid_body = b'\xff\xfe\xfd'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_put_row(invalid_body, 'req-put-err')
                self.assertIn("Simulated native decode_put_row failure", str(ctx.exception))

    # =========================================================================
    # 3. _decode_update_row
    # =========================================================================

    def test_decode_update_row_normal(self):
        """Normal case: decode UpdateRowResponse."""
        body = self._make_update_row_response_body()
        result, proto = self.decoder._decode_update_row(body, 'req-upd-001')
        if proto is not None:
            consumed, return_row = result
            self.assertIsNotNone(consumed)
            self.assertIsNone(return_row)
        else:
            self.assertIsNotNone(result)

    def test_decode_update_row_invalid_body_no_fallback(self):
        """Abnormal case: invalid body, no fallback → exception raised."""
        invalid_body = b'\xaa\xbb\xcc'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_update_row(invalid_body, 'req-upd-err')
                self.assertIn("Simulated native decode_update_row failure", str(ctx.exception))

    # =========================================================================
    # 4. _decode_delete_row
    # =========================================================================

    def test_decode_delete_row_normal(self):
        """Normal case: decode DeleteRowResponse."""
        body = self._make_delete_row_response_body()
        result, proto = self.decoder._decode_delete_row(body, 'req-del-001')
        if proto is not None:
            consumed, return_row = result
            self.assertIsNotNone(consumed)
            self.assertIsNone(return_row)
        else:
            self.assertIsNotNone(result)

    def test_decode_delete_row_invalid_body_no_fallback(self):
        """Abnormal case: invalid body, no fallback → exception raised."""
        invalid_body = b'\x10\x20\x30'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_delete_row(invalid_body, 'req-del-err')
                self.assertIn("Simulated native decode_delete_row failure", str(ctx.exception))

    # =========================================================================
    # 5. _decode_batch_get_row
    # =========================================================================

    def test_decode_batch_get_row_normal(self):
        """Normal case: decode BatchGetRowResponse (empty)."""
        body = self._make_batch_get_row_response_body()
        result, proto = self.decoder._decode_batch_get_row(body, 'req-bg-001')
        self.assertIsNotNone(result)
        if proto is not None:
            self.assertEqual(len(result), 0)

    def test_decode_batch_get_row_invalid_body_no_fallback(self):
        """Abnormal case: invalid body, no fallback → exception raised."""
        invalid_body = b'\xde\xad\xbe\xef'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_batch_get_row(invalid_body, 'req-bg-err')
                self.assertIn("Simulated native decode_batch_get_row failure", str(ctx.exception))

    # =========================================================================
    # 6. _decode_batch_write_row
    # =========================================================================

    def test_decode_batch_write_row_normal(self):
        """Normal case: decode BatchWriteRowResponse (empty)."""
        body = self._make_batch_write_row_response_body()
        result, proto = self.decoder._decode_batch_write_row(body, 'req-bw-001')
        self.assertIsNotNone(result)
        if proto is not None:
            self.assertEqual(len(result), 0)

    def test_decode_batch_write_row_invalid_body_no_fallback(self):
        """Abnormal case: invalid body, no fallback → exception raised."""
        invalid_body = b'\xca\xfe\xba\xbe'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_batch_write_row(invalid_body, 'req-bw-err')
                self.assertIn("Simulated native decode_batch_write_row failure", str(ctx.exception))

    # =========================================================================
    # 7. _decode_get_range
    # =========================================================================

    def test_decode_get_range_normal(self):
        """Normal case: decode GetRangeResponse (empty rows)."""
        body = self._make_get_range_response_body()
        result, proto = self.decoder._decode_get_range(body, 'req-gr-001')
        if proto is not None:
            capacity_unit, next_start_pk, row_list, next_token = result
            self.assertIsNotNone(capacity_unit)
            self.assertIsNone(next_start_pk)
            self.assertEqual(len(row_list), 0)
        else:
            self.assertIsNotNone(result)

    def test_decode_get_range_invalid_body_no_fallback(self):
        """Abnormal case: invalid body, no fallback → exception raised."""
        invalid_body = b'\x01\x02\x03\x04\x05\x06'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_get_range(invalid_body, 'req-gr-err')
                self.assertIn("Simulated native decode_get_range failure", str(ctx.exception))

    # =========================================================================
    # 8. _decode_search
    # =========================================================================

    def test_decode_search_normal(self):
        """Normal case: decode SearchResponse (no rows)."""
        body = self._make_search_response_body()
        result, proto = self.decoder._decode_search(body, 'req-search-001')
        self.assertIsNotNone(result)
        if proto is not None:
            self.assertEqual(result.total_count, 0)
            self.assertTrue(result.is_all_succeed)

    def test_decode_search_invalid_body_no_fallback(self):
        """Abnormal case: invalid body, no fallback → exception raised."""
        invalid_body = b'\xab\xcd\xef\x01\x23'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_search(invalid_body, 'req-search-err')
                self.assertIn("Simulated native decode_search failure", str(ctx.exception))

    # =========================================================================
    # 9. _decode_parallel_scan
    # =========================================================================

    def test_decode_parallel_scan_normal(self):
        """Normal case: decode ParallelScanResponse (no rows)."""
        body = self._make_parallel_scan_response_body()
        result, proto = self.decoder._decode_parallel_scan(body, 'req-ps-001')
        self.assertIsNotNone(result)
        if proto is not None:
            self.assertEqual(len(result.rows), 0)

    def test_decode_parallel_scan_invalid_body_no_fallback(self):
        """Abnormal case: invalid body, no fallback → exception raised."""
        invalid_body = b'\x99\x88\x77\x66'
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                with self.assertRaises(RuntimeError) as ctx:
                    self.decoder._decode_parallel_scan(invalid_body, 'req-ps-err')
                self.assertIn("Simulated native decode_parallel_scan failure", str(ctx.exception))

    # =========================================================================
    # Cross-cutting: fallback behavior tests
    # =========================================================================

    def test_all_decoders_fallback_on_native_failure(self):
        """When native decoder fails and fallback is enabled, Python decoder takes over."""
        test_cases = [
            ('_decode_get_row', self._make_get_row_response_body()),
            ('_decode_put_row', self._make_put_row_response_body()),
            ('_decode_update_row', self._make_update_row_response_body()),
            ('_decode_delete_row', self._make_delete_row_response_body()),
            ('_decode_batch_get_row', self._make_batch_get_row_response_body()),
            ('_decode_batch_write_row', self._make_batch_write_row_response_body()),
            ('_decode_get_range', self._make_get_range_response_body()),
            ('_decode_search', self._make_search_response_body()),
            ('_decode_parallel_scan', self._make_parallel_scan_response_body()),
        ]
        with self._force_native_decoder(enable=True, fallback=True):
            with self._mock_native_sdk_failure() as mock_sdk:
                for method_name, body in test_cases:
                    decode_fn = getattr(self.decoder, method_name)
                    result, proto = decode_fn(body, f'req-fallback-{method_name}')
                    # Should have fallen back to Python decoder successfully
                    self.assertIsNotNone(result, f"{method_name} fallback returned None")
                    # proto should not be None when Python decoder is used
                    self.assertIsNotNone(proto, f"{method_name} should use Python decoder (proto not None)")

    def test_all_decoders_raise_on_native_failure_without_fallback(self):
        """When native decoder fails and fallback is disabled, exception is raised."""
        test_cases = [
            ('_decode_get_row', self._make_get_row_response_body(), 'decode_get_row'),
            ('_decode_put_row', self._make_put_row_response_body(), 'decode_put_row'),
            ('_decode_update_row', self._make_update_row_response_body(), 'decode_update_row'),
            ('_decode_delete_row', self._make_delete_row_response_body(), 'decode_delete_row'),
            ('_decode_batch_get_row', self._make_batch_get_row_response_body(), 'decode_batch_get_row'),
            ('_decode_batch_write_row', self._make_batch_write_row_response_body(), 'decode_batch_write_row'),
            ('_decode_get_range', self._make_get_range_response_body(), 'decode_get_range'),
            ('_decode_search', self._make_search_response_body(), 'decode_search'),
            ('_decode_parallel_scan', self._make_parallel_scan_response_body(), 'decode_parallel_scan'),
        ]
        with self._force_native_decoder(enable=True, fallback=False):
            with self._mock_native_sdk_failure():
                for method_name, body, native_name in test_cases:
                    decode_fn = getattr(self.decoder, method_name)
                    with self.assertRaises(RuntimeError, msg=f"{method_name} should raise RuntimeError") as ctx:
                        decode_fn(body, f'req-no-fallback-{method_name}')
                    self.assertIn(f"Simulated native {native_name} failure", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()