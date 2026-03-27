# -*- coding: utf8 -*-

import unittest
import json

from tests.lib.api_test_base import APITestBase
import tablestore.native
from tablestore import *
import tests.lib.restriction as restriction
import copy
from tablestore.error import *
import math
import time
import sys
from tests.test_utils import make_table_name

if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')


class NativeExtensionTest(APITestBase):

    def setUp(self):
        APITestBase.setUp(self)
        self.table_name_1 = make_table_name('NativeExtensionTest')
        self.client_test.set_use_native_decoder(False)

    def tearDown(self):
        for t in [self.table_name_1]:
            try:
                self.client_test.delete_table(t)
            except:
                pass
        APITestBase.tearDown(self)

    def test_compare_decode_normal(self):
        # prepare table
        table_name = self.table_name_1

        table_meta = TableMeta(table_name, [("PK1", "STRING"), ("PK2", "INTEGER"), ("PK3", "BINARY")])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        # prepare data
        put_row_items = []
        for i in range(0, 20):
            primary_key = [('PK1', str(i)), ('PK2', i + 1), ('PK3', bytearray(str(i), encoding='utf8'))]
            attribute_columns = [('col0', str(i)), ('col1', i), ('col2', i * 0.0001), ('col3', bool(i)), ('col4', bytearray(str(i), encoding='utf8'))]
            row = Row(primary_key, attribute_columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            item = PutRowItem(row, condition)
            put_row_items.append(item)

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, put_row_items))

        try:
            result = self.client_test.batch_write_row(request)
            print('batch write row status: %s' % (result.is_all_succeed()))
        except OTSClientError as e:
            print("batch write row failed, http_status:%d, error_message:%s" % (e.get_http_status(), e.get_error_message()))
            self.assertTrue(False)
        except OTSServiceError as e:
            print("batch write row failed, http_status:%d, error_code:%s, error_message:%s, request_id:%s" % (
            e.get_http_status(), e.get_error_code(), e.get_error_message(), e.get_request_id()))
            self.assertTrue(False)

        # compare get row
        get_row_native_extension = []
        get_row_normal = []

        self.client_test.set_use_native_parser(True)
        for i in range(0, 20):
            primary_key = [('PK1', str(i)), ('PK2', i + 1), ('PK3', bytearray(str(i), encoding='utf8'))]
            columns_to_get = []

            try:
                consumed, return_row, next_token = self.client_test.get_row(table_name, primary_key, columns_to_get)
                get_row_native_extension.append([consumed, return_row, next_token])
            except OTSClientError as e:
                print('get row failed, http_status:%d, error_message:%s' % (e.get_http_status(), e.get_error_message()))
                self.assertTrue(False)

        self.client_test.set_use_native_parser(False)
        for i in range(0, 20):
            primary_key = [('PK1', str(i)), ('PK2', i + 1), ('PK3', bytearray(str(i), encoding='utf8'))]
            columns_to_get = []

            try:
                consumed, return_row, next_token = self.client_test.get_row(table_name, primary_key, columns_to_get)
                get_row_normal.append([consumed, return_row, next_token])
            except OTSClientError as e:
                print('get row failed, http_status:%d, error_message:%s' % (e.get_http_status(), e.get_error_message()))
                self.assertTrue(False)

        for i in range(0, 20):
            self.assertEqual(json.dumps(str(get_row_normal[i][0]), sort_keys=True), json.dumps(str(get_row_native_extension[i][0]), sort_keys=True))
            self.assertEqual(json.dumps(str(get_row_normal[i][1].primary_key), sort_keys=True), json.dumps(str(get_row_native_extension[i][1].primary_key), sort_keys=True))
            self.assertEqual(json.dumps(str(get_row_normal[i][1].attribute_columns), sort_keys=True), json.dumps(str(get_row_native_extension[i][1].attribute_columns), sort_keys=True))
            self.assertEqual(json.dumps(str(get_row_normal[i][2]), sort_keys=True), json.dumps(str(get_row_native_extension[i][2]), sort_keys=True))

        # compare get range

        inclusive_start_primary_key = [("PK1", INF_MIN), ("PK2", INF_MIN), ("PK3", INF_MIN)]
        exclusive_end_primary_key = [("PK1", INF_MAX), ("PK2", INF_MAX), ("PK3", INF_MAX)]
        columns_to_get = []
        limit = 20

        try:
            self.client_test.set_use_native_parser(True)

            consumed1, next_start_primary_key1, row_list1, next_token1 = self.client_test.get_range(
                table_name, Direction.FORWARD,
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get,
                limit,
                max_version=1
            )
            self.client_test.set_use_native_parser(False)

            consumed2, next_start_primary_key2, row_list2, next_token2 = self.client_test.get_range(
                table_name, Direction.FORWARD,
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get,
                limit,
                max_version=1
            )

            self.assertEqual(json.dumps(str(consumed1), sort_keys=True),
                             json.dumps(str(consumed2), sort_keys=True))
            self.assertEqual(json.dumps(str(next_start_primary_key1), sort_keys=True),
                             json.dumps(str(next_start_primary_key2), sort_keys=True))
            self.assertEqual(json.dumps(str(next_token1), sort_keys=True),
                             json.dumps(str(next_token2), sort_keys=True))

            for i in range(0, len(row_list1)):
                self.assertEqual(json.dumps(str(row_list1[i].primary_key), sort_keys=True),
                                 json.dumps(str(row_list2[i].primary_key), sort_keys=True))
                self.assertEqual(json.dumps(str(row_list1[i].attribute_columns), sort_keys=True),
                                 json.dumps(str(row_list2[i].attribute_columns), sort_keys=True))


        except OTSClientError as e:
            print('get row failed, http_status:%d, error_message:%s' % (e.get_http_status(), e.get_error_message()))
            self.assertTrue(False)
        except OTSServiceError as e:
            print('get row failed, http_status:%d, error_code:%s, error_message:%s, request_id:%s' % (
            e.get_http_status(), e.get_error_code(), e.get_error_message(), e.get_request_id()))
            self.assertTrue(False)

        columns_to_get = ['column_not_exist']
        try:
            self.client_test.set_use_native_parser(True)

            consumed1, next_start_primary_key1, row_list1, next_token1 = self.client_test.get_range(
                table_name, Direction.FORWARD,
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get,
                limit,
                max_version=1
            )
            self.client_test.set_use_native_parser(False)

            consumed2, next_start_primary_key2, row_list2, next_token2 = self.client_test.get_range(
                table_name, Direction.FORWARD,
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get,
                limit,
                max_version=1
            )

            self.assertEqual(json.dumps(str(consumed1), sort_keys=True),
                             json.dumps(str(consumed2), sort_keys=True))
            self.assertEqual(json.dumps(str(next_start_primary_key1), sort_keys=True),
                             json.dumps(str(next_start_primary_key2), sort_keys=True))
            self.assertEqual(json.dumps(str(next_token1), sort_keys=True),
                             json.dumps(str(next_token2), sort_keys=True))

            for i in range(0, len(row_list1)):
                self.assertEqual(json.dumps(str(row_list1[i].primary_key), sort_keys=True),
                                 json.dumps(str(row_list2[i].primary_key), sort_keys=True))
                self.assertEqual(json.dumps(str(row_list1[i].attribute_columns), sort_keys=True),
                                 json.dumps(str(row_list2[i].attribute_columns), sort_keys=True))
        except OTSClientError as e:
            print('get row failed, http_status:%d, error_message:%s' % (e.get_http_status(), e.get_error_message()))
            self.assertTrue(False)
        except OTSServiceError as e:
            print('get row failed, http_status:%d, error_code:%s, error_message:%s, request_id:%s' % (
            e.get_http_status(), e.get_error_code(), e.get_error_message(), e.get_request_id()))
            self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()
