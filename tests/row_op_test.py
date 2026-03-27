# -*- coding: utf8 -*-

import unittest
import time
import sys
import tests.lib.restriction as restriction
from tests.lib.api_test_base import APITestBase
from tests.test_utils import make_table_name
from tablestore import *
from tablestore.error import *


if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')


class RowOperationTest(APITestBase):
    """Row operation test"""

    def setUp(self):
        APITestBase.setUp(self)
        # Initialize all table names
        self.table_name = make_table_name('RowOperationTest')

    def tearDown(self):
        # Clean up all test tables
        for table_name in self.client_test.list_table():
            if table_name.find(self.table_name) == -1:
                continue
            try:
                self.client_test.delete_table(table_name)
            except:
                pass
        APITestBase.tearDown(self)


    # Row operation API: GetRow, PutRow, UpdateRow, BatchGetRow, BatchWriteRow, GetRange
    # Test each write operation, and verify the data operation meets expectations using GetRow or BatchGetRow.
    def _check_all_row_op_with_exception_meta_not_match(self, table_name, wrong_pk, error_msg):
        try:
            self.client_test.get_row(table_name, wrong_pk, max_version=1)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSInvalidPK", error_msg)

        try:
            self.client_test.put_row(table_name, Row(wrong_pk, [('col', 1)]), Condition(RowExistenceExpectation.IGNORE))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSInvalidPK", error_msg)

        try:
            self.client_test.update_row(table_name, Row(wrong_pk, {'put': [('C1', 'V')]}),
                                        Condition(RowExistenceExpectation.IGNORE))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSInvalidPK", error_msg)

        try:
            self.client_test.delete_row(table_name, wrong_pk, Condition(RowExistenceExpectation.IGNORE))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSInvalidPK", error_msg)

        request = BatchGetRowRequest()
        request.add(TableInBatchGetRowItem(table_name, [wrong_pk], [], None, 1))
        response = self.client_test.batch_get_row(request)

        succ, failed = response.get_result()
        self.assert_equal(0, len(succ))
        self.assert_equal(1, len(failed))
        self.assert_equal('OTSInvalidPK', failed[0].error_code)
        self.assert_equal(error_msg, failed[0].error_message)

        try:
            wrong_pk_item = PutRowItem(Row(wrong_pk, []), Condition(RowExistenceExpectation.IGNORE))
            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem(table_name, [wrong_pk_item]))
            response = self.client_test.batch_write_row(request)
        except OTSServiceError as e:
            self.assert_error(e, 400, 'OTSParameterInvalid', 'Attribute column is missing.')

        wrong_pk_item = UpdateRowItem(Row(wrong_pk, {'put': [('C1', 'V')]}), Condition(RowExistenceExpectation.IGNORE))
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [wrong_pk_item]))
        response = self.client_test.batch_write_row(request)

        put_succ, put_failed = response.get_update()
        self.assert_equal(0, len(put_succ))
        self.assert_equal(1, len(put_failed))
        self.assert_equal('OTSInvalidPK', put_failed[0].error_code)
        self.assert_equal(error_msg, put_failed[0].error_message)

        wrong_pk_item = DeleteRowItem(Row(wrong_pk), Condition(RowExistenceExpectation.IGNORE))
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [wrong_pk_item]))
        response = self.client_test.batch_write_row(request)

        put_succ, put_failed = response.get_delete()
        self.assert_equal(0, len(put_succ))
        self.assert_equal(1, len(put_failed))
        self.assert_equal('OTSInvalidPK', put_failed[0].error_code)
        self.assert_equal(error_msg, put_failed[0].error_message)

        get_range_end = []
        for k in wrong_pk:
            get_range_end.append((k[0], INF_MAX))

        try:
            self.client_test.get_range(table_name, 'FORWARD', wrong_pk, get_range_end, max_version=1)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSInvalidPK", error_msg)

    def _check_all_row_op_without_exception(self, table_name, pks, cols):
        cu, row, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(row, None)

        row = Row(pks, cols)
        cu, return_row = self.client_test.put_row(table_name, row, Condition(RowExistenceExpectation.IGNORE))
        cu, return_row, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(return_row.primary_key, pks)
        self.assert_columns(return_row.attribute_columns, cols)

        row = Row(pks, {'put': cols})
        cu, return_row = self.client_test.update_row(table_name, row, Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        cu, return_row = self.client_test.delete_row(table_name, pks, Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow, None)

        request = BatchGetRowRequest()
        request.add(TableInBatchGetRowItem(table_name, [pks], [], None, 1))
        response = self.client_test.batch_get_row(request)

        succ, failed = response.get_result()
        self.assert_equal(1, len(succ))
        self.assert_equal(0, len(failed))
        self.assert_equal(None, succ[0].error_code)
        self.assert_equal(None, succ[0].error_message)

        row = Row(pks, cols)
        pks_item = PutRowItem(row, Condition(RowExistenceExpectation.IGNORE))
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [pks_item]))
        response = self.client_test.batch_write_row(request)

        put_succ, put_failed = response.get_put()
        self.assert_equal(1, len(put_succ))
        self.assert_equal(0, len(put_failed))
        self.assert_equal(None, put_succ[0].error_code)
        self.assert_equal(None, put_succ[0].error_message)

        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        pks_item = UpdateRowItem(Row(pks, {'put': cols}), Condition(RowExistenceExpectation.IGNORE))
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [pks_item]))
        response = self.client_test.batch_write_row(request)

        update_succ, update_failed = response.get_update()
        self.assert_equal(1, len(update_succ))
        self.assert_equal(0, len(update_failed))
        self.assert_equal(None, update_succ[0].error_code)
        self.assert_equal(None, update_succ[0].error_message)

        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        pks_item = DeleteRowItem(Row(pks), Condition(RowExistenceExpectation.IGNORE))
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [pks_item]))
        response = self.client_test.batch_write_row(request)

        delete_succ, delete_failed = response.get_delete()
        self.assert_equal(1, len(delete_succ))
        self.assert_equal(0, len(delete_failed))
        self.assert_equal(None, delete_succ[0].error_code)
        self.assert_equal(None, delete_succ[0].error_message)

        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow, None)

        get_range_end = []
        for k in pks:
            get_range_end.append((k[0], INF_MAX))
        self.client_test.get_range(table_name, 'FORWARD', pks, get_range_end, max_version=1)

    def test_pk_name_not_match(self):
        """Create a table with PK `[('PK1', 'STRING')]`, test all row operation APIs. Use PK `[('PK2', 'blah')]`, expect to return OTSMetaNotMatch."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        wrong_pk = [('PK2', 'blah')]
        self._check_all_row_op_with_exception_meta_not_match(table_name, wrong_pk,
                                                             'Validate PK name fail. Input: PK2, Meta: PK1.')

    def test_pk_value_type_not_match(self):
        """Create a table with PK `[('PK1', 'STRING')]`, test all row operation APIs. Use PK `[('PK2', 123)]`, expect to return `OTSMetaNotMatch`."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        wrong_pk = [('PK2', 123)]
        self._check_all_row_op_with_exception_meta_not_match(table_name, wrong_pk,
                                                             'Validate PK name fail. Input: PK2, Meta: PK1.')

    def test_pk_num_not_match(self):
        """Create a table with PK being [('PK1', 'STRING'), ('PK2', 'INTEGER')], test all row operation APIs. PK is either {'PK1': 'blah'} or {'PK1': 'blah', 'PK2': 123, 'PK3': 'blah'}. Expected return is OTSMetaNotMatch."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING'), ('PK2', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        wrong_pk = [('PK1', 'blah')]
        self._check_all_row_op_with_exception_meta_not_match(table_name, wrong_pk,
                                                             'Validate PK size fail. Input: 1, Meta: 2.')

        wrong_pk = [('PK1', 'blah'), ('PK2', 123), ('PK3', 'blah')]
        self._check_all_row_op_with_exception_meta_not_match(table_name, wrong_pk,
                                                             'Validate PK size fail. Input: 3, Meta: 2.')

    def test_pk_order_insensitive(self):
        """Create a table with PK being [('PK1', 'STRING'), ('PK2', 'INTEGER')], test all row operation APIs. The PK is OrderedDict([('PK2', 123), ('PK1', 'blah')]), and the corresponding operations are expected to fail."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING'), ('PK2', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pks = [('PK2', 123), ('PK1', 'blah')]
        try:
            self._check_all_row_op_without_exception(table_name, pks, [('C1', 'V')])
            self.assertTrue(False)
        except:
            pass

    def test_all_the_types(self):
        """Create a table with PK being [('PK1', 'STRING'), ('PK2', 'INTEGER')], and let all row operation APIs operate on the row ({'PK1': 'blah', 'PK2': 123}, {'C1': 'blah', 'C2': 123, 'C3': True, 'C4': False, 'C5': 3.14, 'C6': bytearray(1)}), expecting the corresponding operations to succeed."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING'), ('PK2', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pks = [('PK1', 'blah'), ('PK2', 123)]
        cols = [('C1', 'blah'), ('C2', 123), ('C3', True), ('C4', False), ('C5', 3.14), ('C6', bytearray(1))]
        self._check_all_row_op_without_exception(table_name, pks, cols)

    def test_row_op_with_binary_type_pk(self):
        """Create a table with [('PK1', 'BINARY'), ('PK2', 'BINARY')], pre-insert some data into the table, test all read/write APIs for these data, and verify that all APIs are compatible after the PK type supports Binary."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'BINARY'), ('PK2', 'BINARY')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pks = [('PK1', bytearray(3)), ('PK2', bytearray(2))]
        cols = [('C1', 'blah'), ('C2', 123), ('C3', True), ('C4', False), ('C5', 3.14), ('C6', bytearray(1))]
        self._check_all_row_op_without_exception(table_name, pks, cols)

    def test_columns_to_get_semantics(self):
        """There are two tables, A and B, with four rows: A0, A1, B0, B1, distributed across these two tables. The data for all rows is the same: (`{'PK1': 'blah', 'PK2': 123}`, `{'C1': 'blah', 'C2': 123, 'C3': True, 'C4': False, 'C5': 3.14, 'C6': bytearray(1)}`). Use GetRow to read A0, BatchGetRow to read A0, A1, B0, B1, and GetRange to read A0, A1. The `columns_to_get` parameter is set respectively to empty, `['C1']`, `['PK1']`, and `['blah']`. It is expected that the returned results meet the expectations, and the CU (Capacity Unit) verification also meets the expectations."""
        table_name_a = self.table_name + 'A'
        table_name_b = self.table_name + 'B'
        table_meta_a = TableMeta(table_name_a, [('PK1', 'STRING'), ('PK2', 'INTEGER')])
        table_meta_b = TableMeta(table_name_b, [('PK1', 'STRING'), ('PK2', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta_a, table_options, reserved_throughput)
        self.client_test.create_table(table_meta_b, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name_a)
        self.wait_for_partition_load(table_name_b)

        pks0 = [('PK1', '0'), ('PK2', 123)]
        pks1 = [('PK1', '1'), ('PK2', 123)]
        cols = [('C1', 'blah'), ('C2', 123), ('C3', True), ('C4', False), ('C5', 3.14), ('C6', bytearray(1))]
        putrow_item0 = PutRowItem(Row(pks0, cols), Condition(RowExistenceExpectation.IGNORE))
        putrow_item1 = PutRowItem(Row(pks1, cols), Condition(RowExistenceExpectation.IGNORE))
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name_a, [putrow_item0]))
        request.add(TableInBatchWriteRowItem(table_name_b, [putrow_item1]))

        self.client_test.batch_write_row(request)

        cu, rrow, token = self.client_test.get_row(table_name_a, pks0, max_version=1)
        self.assert_equal(rrow.primary_key, pks0)
        self.assert_columns(rrow.attribute_columns, cols)

        cu, rrow, token = self.client_test.get_row(table_name_a, pks0, ['C1'], max_version=1)
        self.assert_equal(rrow.primary_key, pks0)
        self.assert_columns(rrow.attribute_columns, [('C1', 'blah')])

        cu, rrow, token = self.client_test.get_row(table_name_a, pks0, ['blah'], max_version=1)
        self.assert_equal(rrow, None)

        request = BatchGetRowRequest()
        request.add(TableInBatchGetRowItem(table_name_a, [pks0, pks1], [], None, 1))
        request.add(TableInBatchGetRowItem(table_name_b, [pks0, pks1], [], None, 1))
        response = self.client_test.batch_get_row(request)

        self.assertTrue(response.is_all_succeed())
        table_result_0 = response.get_result_by_table(table_name_a)
        self.assert_equal(2, len(table_result_0))
        self.assert_equal(True, table_result_0[0].is_ok)
        self.assert_equal(pks0, table_result_0[0].row.primary_key)
        self.assert_columns(cols, table_result_0[0].row.attribute_columns)
        self.assert_equal(True, table_result_0[1].is_ok)
        self.assert_equal(None, table_result_0[1].row)

        table_result_1 = response.get_result_by_table(table_name_b)
        self.assert_equal(2, len(table_result_1))
        self.assert_equal(True, table_result_1[0].is_ok)
        self.assert_equal(None, table_result_1[0].row)
        self.assert_equal(True, table_result_1[1].is_ok)
        self.assert_equal(pks1, table_result_1[1].row.primary_key)
        self.assert_columns(cols, table_result_1[1].row.attribute_columns)

        request = BatchGetRowRequest()
        request.add(TableInBatchGetRowItem(table_name_a, [pks0, pks1], ['C1'], None, 1))
        request.add(TableInBatchGetRowItem(table_name_b, [pks0, pks1], ['C1'], None, 1))
        response = self.client_test.batch_get_row(request)

        self.assertTrue(response.is_all_succeed())
        table_result_0 = response.get_result_by_table(table_name_a)
        self.assert_equal(2, len(table_result_0))
        self.assert_equal(True, table_result_0[0].is_ok)
        self.assert_equal(pks0, table_result_0[0].row.primary_key)
        self.assert_columns([('C1', 'blah')], table_result_0[0].row.attribute_columns)
        self.assert_equal(True, table_result_0[1].is_ok)
        self.assert_equal(None, table_result_0[1].row)

        table_result_1 = response.get_result_by_table(table_name_b)
        self.assert_equal(2, len(table_result_1))
        self.assert_equal(True, table_result_1[0].is_ok)
        self.assert_equal(None, table_result_1[0].row)
        self.assert_equal(True, table_result_1[1].is_ok)
        self.assert_equal(pks1, table_result_1[1].row.primary_key)
        self.assert_columns([('C1', 'blah')], table_result_1[1].row.attribute_columns)

        request = BatchGetRowRequest()
        request.add(TableInBatchGetRowItem(table_name_a, [pks0, pks1], ['blah'], None, 1))
        request.add(TableInBatchGetRowItem(table_name_b, [pks0, pks1], ['blah'], None, 1))

        response = self.client_test.batch_get_row(request)
        self.assertTrue(response.is_all_succeed())
        table_result_0 = response.get_result_by_table(table_name_a)
        self.assert_equal(2, len(table_result_0))
        self.assert_equal(True, table_result_0[0].is_ok)
        self.assert_equal(None, table_result_0[0].row)
        self.assert_equal(True, table_result_0[1].is_ok)
        self.assert_equal(None, table_result_0[1].row)

        cu, next_pks, rows, token = self.client_test.get_range(table_name_a, 'FORWARD',
                                                               [('PK1', INF_MIN), ('PK2', INF_MIN)],
                                                               [('PK1', INF_MAX), ('PK2', INF_MAX)],
                                                               [], max_version=2
                                                               )
        row = rows[0]
        self.assert_equal(next_pks, None)
        self.assert_equal(pks0, row.primary_key)
        self.assert_columns(cols, row.attribute_columns)

        cu, next_pks, rows, token = self.client_test.get_range(table_name_a, 'FORWARD',
                                                               [('PK1', INF_MIN), ('PK2', INF_MIN)],
                                                               [('PK1', INF_MAX), ('PK2', INF_MAX)],
                                                               ['C1'],
                                                               max_version=2
                                                               )
        row = rows[0]
        self.assert_equal(next_pks, None)
        self.assert_equal(pks0, row.primary_key)
        self.assert_columns([('C1', 'blah')], row.attribute_columns)

        cu, next_pks, rows, token = self.client_test.get_range(table_name_a, 'FORWARD',
                                                               [('PK1', INF_MIN), ('PK2', INF_MIN)],
                                                               [('PK1', INF_MAX), ('PK2', INF_MAX)],
                                                               ['blah'],
                                                               max_version=1
                                                               )
        self.assert_equal(rows, [])

    def test_CU_consumed_for_whole_row(self):
        """There is a row with data (`{'PK0' : 'blah'}`, `{'C1' : 500B, 'C2' : 500B}`), reading the entire row or just PK0, C1, C2, the expected consumed CU should be (2, 0) or (1, 0)."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK0', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pks = [('PK0', 'blah')]
        cols = [('C1', 'V' * 512), ('C2', 'X' * 512)]
        row = Row(pks, cols)
        self.client_test.put_row(table_name, row, Condition(RowExistenceExpectation.IGNORE))

        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        try:
            cu, rrow, token = self.client_test.get_row(table_name, pks, ['PK0'], max_version=1)
            self.assertTrue(False)
        except:
            pass

        cu, rrow, token = self.client_test.get_row(table_name, pks, ['C1'], max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, [('C1', 'V' * 512)])

        cu, rrow, token = self.client_test.get_row(table_name, pks, ['C2'], max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, [('C2', 'X' * 512)])

    def test_get_row_miss(self):
        """Get a non-existent row using GetRow, expect an empty return, verify CU consumption (1, 0)"""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        cu_expect = CapacityUnit(0, 0)
        cu, rrow, token = self.client_test.get_row(table_name, [('PK1', 'b')], max_version=1)
        self.assert_equal(rrow, None)

    def test_update_row_when_row_exist(self):
        """The original row contains columns `{'C0' : 2k, 'C1' : 2k}` (2k refers to 2k unique STRING values), with data size less than 8K and greater than 4K. Test UpdateRow separately when row existence expectation is EXIST and IGNORE, with column values set as follows: `{'C0' : 2k, 'C1' : 2k}` (overwrite), `{'C2' : 2k, 'C3' : 2k}` (addition), `{'C0' : Delete, 'C1' : Delete}` (deletion), and `{'C0' : 2k, 'C1' : Delete, 'C2' : 2k}` (interleaved). Each time, use GetRow to check if the data matches the expected results and verify if CU consumption is correct."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pks = [('PK1', '0' * 20)]
        cols = [('C0', 'V' * 2048), ('C1', 'B' * 2048)]
        row = Row(pks, cols)
        self.client_test.put_row(table_name, row, Condition(RowExistenceExpectation.IGNORE))

        # EXIST+COVER
        cu, return_row = self.client_test.update_row(table_name, Row(pks, {'put': cols}),
                                                     Condition(RowExistenceExpectation.EXPECT_EXIST))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        # EXIST+ADD
        cols = [('C2', 'V' * 2048), ('C3', 'B' * 2048)]
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'put': cols}),
                                               Condition(RowExistenceExpectation.EXPECT_EXIST))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns,
                            [('C0', 'V' * 2048), ('C1', 'B' * 2048), ('C2', 'V' * 2048), ('C3', 'B' * 2048)])

        # EXIST+DELETE
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'delete_all': [('C0'), ('C1')]}),
                                               Condition(RowExistenceExpectation.EXPECT_EXIST))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, [('C2', 'V' * 2048), ('C3', 'B' * 2048)])

        # EXIST+MIX
        cols = [('C0', 'V' * 2048), ('C1', None), ('C2', 'V' * 2048)]
        cu = self.client_test.update_row(table_name, Row(pks, {'put': [('C0', 'V' * 2048), ('C2', 'V' * 2048)],
                                                               'delete_all': [('C1')]}),
                                         Condition(RowExistenceExpectation.EXPECT_EXIST))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, [('C0', 'V' * 2048), ('C2', 'V' * 2048), ('C3', 'B' * 2048)])

        pks = [('PK1', '0' * 20)]
        cols = [('C0', 'V' * 2048), ('C1', 'B' * 2048)]
        row = Row(pks, cols)
        self.client_test.put_row(table_name, row, Condition(RowExistenceExpectation.IGNORE))

        # IGNORE+COVER
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'put': cols}),
                                               Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        # IGNORE+ADD
        cols = [('C2', 'V' * 2048), ('C3', 'B' * 2048)]
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'put': cols}),
                                               Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns,
                            [('C0', 'V' * 2048), ('C1', 'B' * 2048), ('C2', 'V' * 2048), ('C3', 'B' * 2048)])

        # IGNORE+DELETE
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'delete_all': [('C0'), ('C1')]}),
                                               Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, [('C2', 'V' * 2048), ('C3', 'B' * 2048)])

        # IGNORE+MIX
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'put': [('C0', 'V' * 2048), ('C2', 'V' * 2048)],
                                                                     'delete_all': [('C1')]}),
                                               Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, [('C0', 'V' * 2048), ('C2', 'V' * 2048), ('C3', 'B' * 2048)])

        # IGNORE+INCREMENT
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'increment': [('counter', 10)]}),
                                               Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns,
                            [('C0', 'V' * 2048), ('C2', 'V' * 2048), ('C3', 'B' * 2048), ('counter', 10)])
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'increment': [('counter', 10)]}),
                                               Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns,
                            [('C0', 'V' * 2048), ('C2', 'V' * 2048), ('C3', 'B' * 2048), ('counter', 20)])
        cu, rrow = self.client_test.update_row(table_name, Row(pks, {'increment': [('counter', -30)]}),
                                               Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns,
                            [('C0', 'V' * 2048), ('C2', 'V' * 2048), ('C3', 'B' * 2048), ('counter', -10)])

    def test_update_row_when_value_type_changed(self):
        """The original row contains max columns, with values being INTEGER, DOUBLE, STRING(8 byte), BOOLEAN, BINARY(8 byte) respectively. Test PutRow containing max columns, with values being INTEGER, DOUBLE, STRING(8 byte), BOOLEAN, BINARY(8 byte) respectively. Check the data each time GetRow is performed and verify that CU consumption is normal."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pks = [('PK1', '0')]
        cols = []
        for i in range(0, restriction.MaxColumnCountForRow):
            cols.append(('C' + str(1000 + i), 1))
        cu, rrow = self.client_test.put_row(table_name, Row(pks, cols), Condition(RowExistenceExpectation.IGNORE))

        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        old_cols = cols
        cols = []
        for k in old_cols:
            cols.append((k[0], 1.0))
        cu, rrow = self.client_test.put_row(table_name, Row(pks, cols), Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        old_cols = cols
        cols = []
        for k in old_cols:
            cols.append((k[0], 'V' * 8))
        cu, rrow = self.client_test.put_row(table_name, Row(pks, cols), Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        old_cols = cols
        cols = []
        for k in old_cols:
            cols.append((k[0], True))
        cu, rrow = self.client_test.put_row(table_name, Row(pks, cols), Condition(RowExistenceExpectation.IGNORE))

        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

        old_cols = cols
        cols = []
        for k in old_cols:
            value = bytearray('VVV'.encode('utf-8'))
            cols.append((k[0], value))
        cu, rrow = self.client_test.put_row(table_name, Row(pks, cols), Condition(RowExistenceExpectation.IGNORE))
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=1)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_columns(rrow.attribute_columns, cols)

    def test_update_row_but_expect_row_not_exist(self):
        """The row existence expectation for UpdateRow is set to EXIST, expecting anOTSParameterInvalid return."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        try:
            self.client_test.update_row(table_name, Row([('PK1', '0')], {'put': [('Col0', 'XXXX')]}),
                                        Condition(RowExistenceExpectation.EXPECT_EXIST))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Row does not exist which is expected existent")

    def test_delete_row_but_expect_row_not_exist(self):
        """The row existence expectation for DeleteRow is NOT_EXIST, expecting return OTSInvalidPK."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))

        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        try:
            self.client_test.delete_row(table_name, [('PK1', '0')], Condition(RowExistenceExpectation.EXPECT_EXIST))
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Row does not exist which is expected existent")

    def test_get_range_less_than_4K(self):
        """GetRange includes 10 rows, data does not exceed 4K, or is greater than 4K but less than 8K, expected to consume CU(1, 0) or (2, 0)"""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK1', 'STRING')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        rowitems = []
        for i in range(0, 9):
            pk = [('PK1', str(i))]
            col = [('C', 'V' * 400)]
            row = Row(pk, col)
            putrow_item = PutRowItem(row, Condition(RowExistenceExpectation.IGNORE))
            rowitems.append(putrow_item)

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, rowitems))
        self.client_test.batch_write_row(request)

        cu_sum = CapacityUnit(0, 0)
        i = 0
        for row in self.client_test.xget_range(table_name, 'FORWARD', [('PK1', INF_MIN)], [('PK1', INF_MAX)], cu_sum,
                                               max_version=1):
            epk = [('PK1', str(i))]
            ecol = [('C', 'V' * 400)]
            self.assert_equal(row.primary_key, epk)
            self.assert_columns(row.attribute_columns, ecol)
            i += 1

        rowitems = []
        for i in range(0, 9):
            pk = [('PK1', str(i))]
            col = [('C', 'V' * 800)]
            row = Row(pk, col)
            putrow_item = PutRowItem(row, Condition(RowExistenceExpectation.IGNORE))
            rowitems.append(putrow_item)

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, rowitems))
        self.client_test.batch_write_row(request)

        cu_sum = CapacityUnit(0, 0)
        i = 0
        for row in self.client_test.xget_range(table_name, 'FORWARD', [('PK1', INF_MIN)], [('PK1', INF_MAX)], cu_sum,
                                               max_version=1):
            epk = [('PK1', str(i))]
            ecol = [('C', 'V' * 800)]
            self.assert_equal(row.primary_key, epk)
            self.assert_columns(row.attribute_columns, ecol)
            i += 1

    def _valid_column_name_test(self, table_name, pk, columns):
        # put_row
        consumed, rrow = self.client_test.put_row(table_name, Row(pk, columns),
                                                  Condition(RowExistenceExpectation.IGNORE))
        # get_row
        consumed, rrow, next_token = self.client_test.get_row(table_name, pk, max_version=1)
        self.assert_equal(rrow.primary_key, pk)
        self.assert_columns(rrow.attribute_columns, columns)
        # batch_get_row
        batches = [(table_name, [pk], [])]

        request = BatchGetRowRequest()
        request.add(TableInBatchGetRowItem(table_name, [pk], [], None, 1))
        response = self.client_test.batch_get_row(request)

        self.assertTrue(response.is_all_succeed())
        table_result_0 = response.get_result_by_table(table_name)
        self.assert_equal(1, len(table_result_0))
        self.assert_equal(True, table_result_0[0].is_ok)
        self.assert_equal(pk, table_result_0[0].row.primary_key)
        self.assert_columns(columns, table_result_0[0].row.attribute_columns)

        # get_range
        inclusive_key = []
        exclusive_key = []
        for k in pk:
            inclusive_key.append((k[0], INF_MIN))
            exclusive_key.append((k[0], INF_MAX))
        consumed, next_start_primary_keys, rows, token = self.client_test.get_range(table_name, 'FORWARD',
                                                                                    inclusive_key, exclusive_key, [],
                                                                                    None, max_version=1)
        self.assert_equal(next_start_primary_keys, None)
        self.assert_equal(1, len(rows))
        self.assert_equal(rows[0].primary_key, pk)
        self.assert_columns(rows[0].attribute_columns, columns)

        # update_row
        consumed, rrow = self.client_test.update_row(table_name, Row(pk, {'put': columns}),
                                                     Condition(RowExistenceExpectation.IGNORE))
        self.assert_equal(None, rrow)

        # delete_row
        consumed, rrow = self.client_test.delete_row(table_name, pk, Condition(RowExistenceExpectation.IGNORE))
        self.assert_equal(None, rrow)

        # batch_write_row
        put_row_item = PutRowItem(Row(pk, columns), Condition(RowExistenceExpectation.IGNORE))
        update_row_item = UpdateRowItem(Row(pk, {'put': columns}), Condition(RowExistenceExpectation.IGNORE))
        delete_row_item = DeleteRowItem(Row(pk), Condition(RowExistenceExpectation.IGNORE))
        expect_write_data_item = BatchWriteRowResponseItem(True, "", "", "", CapacityUnit(0, 0))

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [put_row_item]))
        response = self.client_test.batch_write_row(request)

        self.assertTrue(response.is_all_succeed())
        put_succ, put_failed = response.get_put()
        self.assert_equal(1, len(put_succ))
        self.assert_equal(0, len(put_failed))
        self.assert_equal(None, put_succ[0].error_code)
        self.assert_equal(None, put_succ[0].error_message)

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [update_row_item]))
        response = self.client_test.batch_write_row(request)

        self.assertTrue(response.is_all_succeed())
        put_succ, put_failed = response.get_update()
        self.assert_equal(1, len(put_succ))
        self.assert_equal(0, len(put_failed))
        self.assert_equal(None, put_succ[0].error_code)
        self.assert_equal(None, put_succ[0].error_message)

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [delete_row_item]))
        response = self.client_test.batch_write_row(request)

        self.assertTrue(response.is_all_succeed())
        put_succ, put_failed = response.get_delete()
        self.assert_equal(1, len(put_succ))
        self.assert_equal(0, len(put_failed))
        self.assert_equal(None, put_succ[0].error_code)
        self.assert_equal(None, put_succ[0].error_message)

    def test_max_data_size_row(self):
        """For each row operation API, test a row with the maximum data volume: the primary key is the max number, each primary key name has the max length, value is a string of max length, column data is max bytes, and types are INTEGER, BOOLEAN, BINARY, STRING, DOUBLE. Determine if the consumed CU meets expectations."""
        # Here assumes MaxPKColumnNum <= 10, otherwise the length of PKname cannot meet the requirements.
        pk_schema, pk = self.get_primary_keys(restriction.MaxPKColumnNum, "STRING",
                                              pk_name="P" * (restriction.MaxColumnNameLength - 1),
                                              pk_value="x" * (restriction.MaxPKStringValueLength))

        table_name = self.table_name
        table_meta = TableMeta(table_name, pk_schema)
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(
            CapacityUnit(restriction.MinReadWriteCapacityUnit, restriction.MinReadWriteCapacityUnit))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        remained_size = restriction.MaxColumnDataSizeForRow - restriction.MaxPKColumnNum * (
                    restriction.MaxColumnNameLength + restriction.MaxPKStringValueLength)
        remained_column_cnt = restriction.MaxColumnCountForRow - restriction.MaxPKColumnNum

        integer_columns = []
        string_columns = []
        bool_columns = []
        binary_columns = []
        double_columns = []
        # INTEGER and DOUBLE
        col_key_tmp = "a%0" + str(restriction.MaxColumnNameLength - 1) + "d"
        if (remained_size / (restriction.MaxColumnNameLength + 8)) > remained_column_cnt:
            for i in range(remained_column_cnt):
                integer_columns.append((col_key_tmp % i, i))
                double_columns.append((col_key_tmp % i, i + 0.1))
        else:
            for i in range(remained_size / (restriction.MaxColumnNameLength + 8)):
                integer_columns.append((col_key_tmp % i, i))
                double_columns.append((col_key_tmp % i, i + 0.1))
        # BOOL
        if (remained_size / (restriction.MaxColumnNameLength + 1)) > remained_column_cnt:
            for i in range(remained_column_cnt):
                bool_columns.append((col_key_tmp % i, True))
        else:
            for i in range(remained_size / (restriction.MaxColumnNameLength + 1)):
                bool_columns.append((col_key_tmp % i, False))
        # string
        for i in range(int(remained_size / (restriction.MaxColumnNameLength + restriction.MaxNonPKStringValueLength))):
            string_columns.append((col_key_tmp % i, "X" * restriction.MaxNonPKStringValueLength))
        # binary
        for i in range(int(remained_size / (restriction.MaxColumnNameLength + restriction.MaxBinaryValueLength))):
            binary_columns.append((col_key_tmp % i, bytearray(restriction.MaxBinaryValueLength)))

        for col in [integer_columns, string_columns, bool_columns, binary_columns, double_columns]:
            self._valid_column_name_test(table_name, pk, col)

    def test_batch_get_on_the_same_row(self):
        """Create a table T, a row R, with data size < 1KB, BatchGetRow([(T, [R, R])]), duplicate rows, expect to return OTSInvalidPK. Then BatchGetRow([(T, [R]), (T, [R])]) again, same named tables in different groups, expect to return OTSInvalidPK."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        pk_dict = [('PK0', 'a'), ('PK1', 1)]
        column_dict = [('col1', 'M' * 500)]
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()

        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load('table_test_batch_get_on_the_same_row')

        consumed, rrow = self.client_test.put_row(table_name, Row(pk_dict, column_dict),
                                                  Condition(RowExistenceExpectation.IGNORE))
        consumed_expect = CapacityUnit(0, 0)

        try:
            request = BatchGetRowRequest()
            request.add(TableInBatchGetRowItem(table_name, [pk_dict, pk_dict], [], None, 1))
            response = self.client_test.batch_get_row(request)
        except OTSServiceError as e:
            self.assert_false()

        try:
            request = BatchGetRowRequest()
            request.add(TableInBatchGetRowItem(table_name, [pk_dict], [], None, 1))
            request.add(TableInBatchGetRowItem(table_name, [pk_dict], [], None, 1))
            response = self.client_test.batch_get_row(request)
        except OTSServiceError as e:
            self.assert_false()

    def test_batch_write_on_the_same_row(self):
        """BatchWriteRow, for put, delete, update operations on the same row (under the same table name or two repeated table names), expects to return OTSInvalidPK."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        pk_dict = [('PK0', 'a'), ('PK1', 1)]
        pk_dict_2 = [('PK0', 'a'), ('PK1', 2)]
        column_dict = [('col1', 'M')]
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()

        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load('table_test_batch_write_on_the_same_row')

        put_row_item = PutRowItem(Row(pk_dict, [('col1', 150)]), Condition(RowExistenceExpectation.IGNORE))
        update_row_item = UpdateRowItem(Row(pk_dict, {'put': [('col1', 200)]}),
                                        Condition(RowExistenceExpectation.IGNORE))
        delete_row_item = DeleteRowItem(Row(pk_dict), Condition(RowExistenceExpectation.IGNORE))
        test_batch_write_row_list = [[put_row_item, update_row_item], [put_row_item, delete_row_item],
                                     [update_row_item, delete_row_item]]

        for items in test_batch_write_row_list:
            try:
                request = BatchWriteRowRequest()
                request.add(TableInBatchWriteRowItem(table_name, items))
                write_response = self.client_test.batch_write_row(request)
            except OTSServiceError as e:
                self.assert_false()

    def test_no_item_in_batch_ops(self):
        """BatchGetRow and BatchWriteRow do not contain any rows, expect to return OTSInvalidPK."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        try:
            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem(table_name, []))

            write_response = self.client_test.batch_write_row(request)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid",
                              "No operation is specified for table: '" + table_name + "'.")

        try:
            request = BatchGetRowRequest()
            request.add(TableInBatchGetRowItem(table_name, [], [], None, 1))
            response = self.client_test.batch_get_row(request)
        except OTSServiceError as e:
            self.assert_false()

    def test_no_table_in_batch_ops(self):
        """BatchGetRow and BatchWriteRow do not contain any rows, expect to return OTSInvalidPK."""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        try:
            request = BatchWriteRowRequest()
            write_response = self.client_test.batch_write_row(request)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "No row is specified in BatchWriteRow.")

        try:
            request = BatchGetRowRequest()
            response = self.client_test.batch_get_row(request)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "No row specified in the request of BatchGetRow.")

    def test_get_range_when_direction_is_wrong_for_1_PK(self):
        """A table has 1 PK, the test direction is FORWARD/FORWARD, the first begin (greater than or equal to)/(less than or equal to) end, and the PK types are STRING, INTEGER respectively. The expected return is OTSInvalidPK."""
        table_name_string = self.table_name + "_string"
        table_meta_string = TableMeta(table_name_string, [('PK0', 'STRING')])
        table_name_integer = self.table_name + "_integer"
        table_meta_integer = TableMeta(table_name_integer, [('PK0', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()

        self.client_test.create_table(table_meta_string, table_options, reserved_throughput)
        self.client_test.create_table(table_meta_integer, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name_string)
        self.wait_for_partition_load(table_name_integer)

        pk_dict_small = [('PK0', 'AAA')]
        pk_dict_big = [('PK0', 'DDDD')]
        self._check_get_range_expect_exception(table_name_string, pk_dict_big, pk_dict_small, 'FORWARD',
                                               "Begin key must less than end key in FORWARD")
        self._check_get_range_expect_exception(table_name_string, pk_dict_small, pk_dict_big, 'BACKWARD',
                                               "Begin key must more than end key in BACKWARD")

        pk_dict_small = [('PK0', 10)]
        pk_dict_big = [('PK0', 90)]
        self._check_get_range_expect_exception(table_name_integer, pk_dict_big, pk_dict_small, 'FORWARD',
                                               "Begin key must less than end key in FORWARD")
        self._check_get_range_expect_exception(table_name_integer, pk_dict_small, pk_dict_big, 'BACKWARD',
                                               "Begin key must more than end key in BACKWARD")

    def _check_get_range_expect_exception(self, table_name, inclusive_start_primary_keys, exclusive_end_primary_keys,
                                          direction, error_msg, error_code="OTSParameterInvalid", limit=None,
                                          columns_to_get=[]):
        try:
            consumed, next_start_primary_keys, rows = self.client_test.get_range(table_name, direction,
                                                                                 inclusive_start_primary_keys,
                                                                                 exclusive_end_primary_keys,
                                                                                 columns_to_get, limit, max_version=1)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, error_code, error_msg)

    def test_all_the_ranges_for_2_PK(self):
        """A table has 2 primary keys (PKs), the partition key is a < b < c < d, which can be STRING, INTEGER (test separately). Each partition key has 2 rows: ax, ay, bx, by, cx, cy, where x and y are the values of the second PK, being STRING, INTEGER, BOOLEAN respectively. Test `get_range` in both forward and reverse directions (for reverse direction, swap begin and end): (a MIN, b MAX), (b MAX, a MIN) (error), (b MIN, a MAX) (error), (a MIN, a MAX), (a MAX, a MIN) (error), (a MAX, b MIN), (a MAX, c MIN), (b x, a x) (error), (a x, a y), (a MIN, a y), (a x, a MAX), (a x, c x), (a x, c y), (a y, c x), (a x, a x) (error). For each successful operation, verify that the returned data matches expectations and CU consumption is as expected."""
        raw_table_name = self.table_name
        for first_pk_type in ('STRING', 'INTEGER'):
            for second_pk_type in ('STRING', 'INTEGER'):
                table_name = raw_table_name + first_pk_type + second_pk_type
                table_meta = TableMeta(table_name, [('PK0', first_pk_type), ('PK1', second_pk_type)])
                table_options = TableOptions()
                self.client_test.create_table(table_meta, table_options, ReservedThroughput(CapacityUnit(0, 0)))
                self.wait_for_partition_load(table_name)

                if first_pk_type == 'STRING':
                    a, b, c = 'A', 'B', 'C'
                else:
                    a, b, c = 1, 2, 3

                if second_pk_type == 'STRING':
                    x, y = 'A', 'B'
                else:
                    x, y = 1, 2

                # pk_list = [{'PK0': 'a', 'PK1': 1}, {'PK0': 'a', 'PK1': 2}, {'PK0': 'b', 'PK1': 1},
                #           {'PK0': 'b', 'PK1': 2}, {'PK0': 'c', 'PK1': 1}, {'PK0': 'c', 'PK1': 2}]
                #
                row_list = []
                for first_pk_value in [a, b, c]:
                    for second_pk_value in [x, y]:
                        row_list.append(([('PK0', first_pk_value), ('PK1', second_pk_value)], [('col1', 10)]))
                        row = Row([('PK0', first_pk_value), ('PK1', second_pk_value)], [('col1', 10)])
                        self.client_test.put_row(table_name, row, Condition(RowExistenceExpectation.IGNORE))

                def get_range(begin_pk0, begin_pk1, end_pk0, end_pk1):
                    return [[('PK0', begin_pk0), ('PK1', begin_pk1)], [('PK0', end_pk0), ('PK1', end_pk1)]]

                range_list = [
                    # range                             Whether normal Expect rows
                    (get_range(b, x, a, x), False, row_list[0:1]),
                    (get_range(a, x, a, y), True, row_list[0:2]),
                    (get_range(a, x, c, x), True, row_list[0:5]),
                    (get_range(a, x, c, y), True, row_list[0:6]),
                    (get_range(a, y, c, x), True, row_list[1:5]),
                    (get_range(a, x, a, x), False, row_list[0:1]),
                ]

                range_list_1 = [
                    (get_range(a, INF_MAX, b, INF_MIN), True, row_list[0:0]),
                    (get_range(b, INF_MAX, a, INF_MIN), False, row_list[0:0]),
                    (get_range(b, INF_MAX, a, INF_MAX), False, row_list[0:0]),
                    (get_range(a, INF_MIN, a, INF_MAX), True, row_list[0:2]),
                    (get_range(a, INF_MAX, a, INF_MIN), False, row_list[0:0]),
                    (get_range(a, INF_MAX, b, INF_MIN), True, row_list[0:0]),
                    (get_range(a, INF_MAX, c, INF_MIN), True, row_list[2:4]),
                ]

                range_list_2 = [
                    (get_range(a, INF_MIN, a, y), True, row_list[0:2]),
                    (get_range(a, x, a, INF_MAX), True, row_list[0:3]),
                ]
                begin, end = get_range(a, INF_MIN, a, y)
                self._check_xget_range(table_name, begin, end, 'FORWARD', row_list[0:1])
                row_list_temp = row_list[0:2]
                row_list_temp.reverse()
                self._check_xget_range(table_name, end, begin, 'BACKWARD', row_list_temp)
                begin, end = get_range(a, x, a, INF_MAX)
                self._check_xget_range(table_name, begin, end, 'FORWARD', row_list[0:2])
                row_list_temp = row_list[1:2]
                row_list_temp.reverse()
                self._check_xget_range(table_name, end, begin, 'BACKWARD', row_list_temp)

    def test_4_PK_range(self):
        """A table has 4 primary keys (PK), with types STRING, STRING, INTEGER. Testing ranges: 
(`'A'`, `'A'`, `10`, `False`, `'A'`, `'A'`, `10`, `True`), 
(`'A'`, `'A'`, `10`, `False`, `'A'`, `'A'`, `11`, `True`), 
(`'A'`, `'A'`, `10`, `False`, `'A'`, `'A'`, `9`, `True`) (error), 
(`'A'`, `'A'`, `10`, `MAX`, `'A'`, `'B'`, `10`, `MIN`), 
(`'A'`, `MIN`, `10`, `False`, `'B'`, `MAX`, `2`, `True`). Construct data so that each interval has a value."""
        table_name = self.table_name
        table_meta = TableMeta(table_name,
                               [('PK0', 'STRING'), ('PK1', 'STRING'), ('PK2', 'INTEGER'), ('PK3', 'INTEGER')])
        table_options = TableOptions()
        pk_dict_list = [[('PK0', 'A'), ('PK1', 'A'), ('PK2', 9), ('PK3', 9)],
                        [('PK0', 'A'), ('PK1', 'A'), ('PK2', 10), ('PK3', 1)],
                        [('PK0', 'A'), ('PK1', 'A'), ('PK2', 10), ('PK3', 9)],
                        [('PK0', 'A'), ('PK1', 'A'), ('PK2', 11), ('PK3', 9)]]
        row_list = []
        putrow_list = []
        for pk_dict in pk_dict_list:
            row = Row(pk_dict, [('col', 5)])
            putrow_list.append(PutRowItem(row, Condition(RowExistenceExpectation.IGNORE)))
            row_list.append((pk_dict, [('col', 5)]))
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load('table_test_4_PK_range')

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, putrow_list))

        self.client_test.batch_write_row(request)

        pk_dict_min = [('PK0', 'A'), ('PK1', INF_MIN), ('PK2', 10), ('PK3', 1)]
        pk_dict3 = [('PK0', 'A'), ('PK1', 'A'), ('PK2', 10), ('PK3', INF_MAX)]
        pk_dict5 = [('PK0', 'A'), ('PK1', 'B'), ('PK2', 10), ('PK3', INF_MIN)]
        pk_dict_max = [('PK0', 'B'), ('PK1', INF_MAX), ('PK2', 2), ('PK3', 9)]

        self._check_get_range_expect_exception(table_name, pk_dict_list[1], pk_dict_list[0], 'FORWARD',
                                               "Begin key must less than end key in FORWARD")

        self._check_xget_range(table_name, pk_dict_list[1], pk_dict_list[2], 'FORWARD', row_list[1:2])
        self._check_xget_range(table_name, pk_dict_list[1], pk_dict_list[3], 'FORWARD', row_list[1:3])
        self._check_xget_range(table_name, pk_dict3, pk_dict5, 'FORWARD', row_list[3:4])
        self._check_xget_range(table_name, pk_dict_min, pk_dict_max, 'FORWARD', row_list[0:4])

    def test_empty_range(self):
        """Test four tables with the number of PKs being 1, 2, 3, and 4 respectively, where the number of rows included in the range is 0. The expected return is empty, and the CU consumption is (1, 0)."""
        table_name_list = [self.table_name + '_1',
                           self.table_name + '_2',
                           self.table_name + '_3',
                           self.table_name + '_4']
        pk_schema0, pk_dict0_exclusive = self.get_primary_keys(1, 'STRING', 'PK', INF_MAX)
        pk_schema1, pk_dict1_exclusive = self.get_primary_keys(2, 'STRING', 'PK', INF_MAX)
        pk_schema2, pk_dict2_exclusive = self.get_primary_keys(3, 'STRING', 'PK', INF_MAX)
        pk_schema3, pk_dict3_exclusive = self.get_primary_keys(4, 'STRING', 'PK', INF_MAX)
        pk_schema0, pk_dict0_inclusive = self.get_primary_keys(1, 'STRING', 'PK', INF_MIN)
        pk_schema1, pk_dict1_inclusive = self.get_primary_keys(2, 'STRING', 'PK', INF_MIN)
        pk_schema2, pk_dict2_inclusive = self.get_primary_keys(3, 'STRING', 'PK', INF_MIN)
        pk_schema3, pk_dict3_inclusive = self.get_primary_keys(4, 'STRING', 'PK', INF_MIN)
        pk_schema_list = [pk_schema0, pk_schema1, pk_schema2, pk_schema3]
        pk_dict_inclusive_list = [pk_dict0_inclusive, pk_dict1_inclusive, pk_dict2_inclusive, pk_dict3_inclusive]
        pk_dict_exclusive_list = [pk_dict0_exclusive, pk_dict1_exclusive, pk_dict2_exclusive, pk_dict3_exclusive]
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        for i in range(len(table_name_list)):
            table_meta = TableMeta(table_name_list[i], pk_schema_list[i])
            table_options = TableOptions()
            self.client_test.create_table(table_meta, table_options, reserved_throughput)
            self.wait_for_partition_load(table_name_list[i])
            consumed, next_start_primary_keys, rows, next_token = self.client_test.get_range(table_name_list[i],
                                                                                             'FORWARD',
                                                                                             pk_dict_inclusive_list[i],
                                                                                             pk_dict_exclusive_list[i],
                                                                                             max_version=1)
            self.assert_equal(next_start_primary_keys, None)
            self.assert_columns(rows, [])

    def test_get_range_limit_invalid(self):
        """Test get_range with limit set to 0 or -1, expecting to return error OTSInvalidPK"""
        table_name = self.table_name
        table_meta = TableMeta(table_name, [('PK0', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load('table_test_get_range_limit_invalid')

        pk_dict_small = [('PK0', 'AAA')]
        pk_dict_big = [('PK0', 'DDDD')]
        self._check_get_range_expect_exception(table_name, pk_dict_small, pk_dict_big, 'FORWARD',
                                               "The limit must be greater than 0.", "OTSParameterInvalid", 0)
        self._check_get_range_expect_exception(table_name, pk_dict_small, pk_dict_big, 'FORWARD',
                                               "The limit must be greater than 0.", "OTSParameterInvalid", -1)

    def _check_xget_range(self, table_name, inclusive_start_primary_keys, exclusive_end_primary_keys, direction,
                          expect_rows, limit=10, columns=[]):
        consumed_count = CapacityUnit(0, 0)
        row_size_sum = 0
        count = 0
        for row in self.client_test.xget_range(table_name, direction, inclusive_start_primary_keys,
                                               exclusive_end_primary_keys, consumed_count, columns, limit,
                                               max_version=1):
            expect_row = expect_rows[count]
            self.assert_equal(expect_row[0], row.primary_key)
            self.assert_columns(expect_row[1], row.attribute_columns)

            row_size_sum = row_size_sum + self.get_row_size(row.primary_key, row.attribute_columns)
            count = count + 1
        self.assert_equal(count, len(expect_rows))
        consumed_expect = CapacityUnit(0, 0)

    def test_all_item_in_batch_write_row_failed(self):
        """When all items in the batch write row fail completely on the backend, it is expected that each item returns a specific error, while the entire operation returns normally."""

        table1 = self.table_name
        table_meta1 = TableMeta(table1, [("PK", "INTEGER")])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()
        self.client_test.create_table(table_meta1, table_options, reserved_throughput)

        self.wait_for_partition_load(table1)
        put_table1 = PutRowItem(Row([("PK", 11)], [("COL", "table1_11")]),
                                Condition(RowExistenceExpectation.EXPECT_EXIST))
        update_table1 = UpdateRowItem(Row([("PK", 12)], {"put": [("COL", "table1_12")]}),
                                      Condition(RowExistenceExpectation.EXPECT_EXIST))
        delete_table1 = DeleteRowItem(Row([("PK", 13)]), Condition(RowExistenceExpectation.EXPECT_EXIST))

        row_items = [put_table1, update_table1, delete_table1]
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table1, row_items))
        response = self.client_test.batch_write_row(request)

        self.assertFalse(response.is_all_succeed())
        put_succ, put_failed = response.get_put()
        self.assert_equal(0, len(put_succ))
        self.assert_equal(1, len(put_failed))
        self.assert_equal('OTSConditionCheckFail', put_failed[0].error_code)
        self.assert_equal('Row does not exist which is expected existent', put_failed[0].error_message)

        update_succ, update_failed = response.get_update()
        self.assert_equal(0, len(update_succ))
        self.assert_equal(1, len(update_failed))
        self.assert_equal('OTSConditionCheckFail', update_failed[0].error_code)
        self.assert_equal('Row does not exist which is expected existent', update_failed[0].error_message)

        delete_succ, delete_failed = response.get_delete()
        self.assert_equal(0, len(delete_succ))
        self.assert_equal(1, len(delete_failed))
        self.assert_equal('OTSConditionCheckFail', delete_failed[0].error_code)
        self.assert_equal('Row does not exist which is expected existent', delete_failed[0].error_message)

    def test_one_delete_in_update(self):
        """When a row is empty, perform an UpdateRow, and it only contains a Delete operation."""

        table_name = self.table_name

        table_meta = TableMeta(table_name, [("PK", "INTEGER")])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        cu, rrow = self.client_test.update_row(table_name, Row([("PK", 11)], {"delete_all": [("Col0")]}),
                                               Condition(RowExistenceExpectation.IGNORE))

    def test_delete_with_timestamp_in_update(self):
        """Test UpdateRow with delete operation that specifies a timestamp to delete a specific column version."""

        table_name = self.table_name

        table_meta = TableMeta(table_name, [("PK", "INTEGER")])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions(max_version=2)
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        pks = [("PK", 1)]

        # Step 1: Write two versions of Col0 with different timestamps using put
        # get current timestamp in ms
        ts1 = int(round(time.time() * 1000)) + 1000
        ts2 = int(round(time.time() * 1000)) + 2000
        cu, rrow = self.client_test.update_row(
            table_name,
            Row(pks, {'put': [('Col0', 'value_v1', ts1), ('Col0', 'value_v2', ts2)]}),
            Condition(RowExistenceExpectation.IGNORE)
        )

        # Verify both versions exist
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=2)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_equal(len(rrow.attribute_columns), 2)

        # Step 2: Delete only the version with ts2 using delete (not delete_all)
        cu, rrow = self.client_test.update_row(
            table_name,
            Row(pks, {'delete': [('Col0', None, ts2)]}),
            Condition(RowExistenceExpectation.IGNORE)
        )

        # Step 3: Verify only the ts1 version remains
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=2)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_equal(len(rrow.attribute_columns), 1)
        self.assert_equal(rrow.attribute_columns[0][0], 'Col0')
        self.assert_equal(rrow.attribute_columns[0][1], 'value_v1')
        self.assert_equal(rrow.attribute_columns[0][2], ts1)

        # Step 4: Delete the remaining version with ts1
        cu, rrow = self.client_test.update_row(
            table_name,
            Row(pks, {'delete': [('Col0', None, ts1)]}),
            Condition(RowExistenceExpectation.IGNORE)
        )

        # Step 5: Verify the row has no attribute columns
        cu, rrow, token = self.client_test.get_row(table_name, pks, max_version=2)
        self.assert_equal(rrow.primary_key, pks)
        self.assert_equal(len(rrow.attribute_columns), 0)

    def test_all_delete_in_update(self):
        """When a row is empty, perform an UpdateRow with 128 Delete operations."""

        table_name = self.table_name

        table_meta = TableMeta(table_name, [("PK", "INTEGER")])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        columns_to_delete = []
        for i in range(restriction.MaxColumnCountForRow):
            columns_to_delete.append(('Col' + str(i)))

            cu, rrow = self.client_test.update_row(table_name, Row([("PK", 11)], {"delete_all": columns_to_delete}),
                                                   Condition(RowExistenceExpectation.IGNORE))

        cu, rrow, token = self.client_test.get_row(table_name, [("PK", 11)], max_version=1)
        self.assert_equal(rrow, None)

    def test_one_delete_in_update_of_batch_write(self):
        """When a row is empty, perform a BatchWriteRow UpdateRow with only one Delete operation."""

        table_name = self.table_name

        table_meta = TableMeta(table_name, [("PK", "INTEGER")])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        update_item = UpdateRowItem(Row([("PK", 12)], {"delete_all": ["Col0"]}),
                                    Condition(RowExistenceExpectation.IGNORE))
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [update_item]))

        response = self.client_test.batch_write_row(request)

        self.assertTrue(response.is_all_succeed())
        put_succ, put_failed = response.get_update()
        self.assert_equal(1, len(put_succ))
        self.assert_equal(0, len(put_failed))
        self.assert_equal(None, put_succ[0].error_code)
        self.assert_equal(None, put_succ[0].error_message)

        cu, rrow, token = self.client_test.get_row(table_name, [("PK", 12)], max_version=1)
        self.assert_equal(rrow, None)

    def test_all_delete_in_update_of_batch_write(self):
        """When a row is empty, perform a BatchWriteRow UpdateRow with 128 Delete operations included."""

        table_name = self.table_name

        table_meta = TableMeta(table_name, [("PK", "INTEGER")])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        columns_to_delete = []
        for i in range(restriction.MaxColumnCountForRow):
            columns_to_delete.append('Col' + str(i))

        update_item = UpdateRowItem(Row([("PK", 12)], {"delete_all": columns_to_delete}),
                                    Condition(RowExistenceExpectation.IGNORE))
        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, [update_item]))

        response = self.client_test.batch_write_row(request)
        self.assertTrue(response.is_all_succeed())

        cu, rrow, token = self.client_test.get_row(table_name, [("PK", 12)], max_version=1)
        self.assert_equal(rrow, None)

    def test_bytearray(self):
        '''Use the two constructors of bytearray to construct row value, ensuring successful read and write operations.'''
        table_name = self.table_name

        table_meta = TableMeta(table_name, [("PK1", "BINARY"), ("PK2", "BINARY")])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        # Put
        primary_key = [('PK1', bytearray([1, 23, 45, 101, 33])), ('PK2', bytearray("武汉", 'utf-8'))]
        attribute_columns = [('address', bytearray([238, 180, 225, 242, 32])), ('age', 29.7),
                             ('female', False), ('mobile', 15100000000), ('name', '萧峰')]
        row = Row(primary_key, attribute_columns)

        consumed, return_row = self.client_test.put_row(table_name, row, None)

        # Read
        consumed, return_row, next_token = self.client_test.get_row(table_name, primary_key, [], None, 1)
        self.assert_equal(return_row.primary_key, primary_key)
        self.assert_columns(return_row.attribute_columns, attribute_columns)

        # Update
        update_of_attribute_columns = {
            'PUT': [('address', bytearray([2, 9, 0, 199, 12]))],
        }
        row = Row(primary_key, update_of_attribute_columns)
        consumed, return_row = self.client_test.update_row(table_name, row, None)

        # Read
        attribute_columns = [('address', bytearray([2, 9, 0, 199, 12])), ('age', 29.7),
                             ('female', False), ('mobile', 15100000000), ('name', '萧峰')]
        consumed, return_row, next_token = self.client_test.get_row(table_name, primary_key, [], None, 1)
        self.assert_equal(return_row.primary_key, primary_key)
        self.assert_columns(return_row.attribute_columns, attribute_columns)

        # Batch Write
        put_row_items = []
        primary_key1 = [('PK1', bytearray([1, 1, 1])), ('PK2', bytearray("西安", 'utf-8'))]
        attribute_columns1 = [("address", bytearray([10, 10])), ('age', 1)]
        row1 = Row(primary_key1, attribute_columns1)
        item1 = PutRowItem(row1, None)
        put_row_items.append(item1)

        primary_key2 = [('PK1', bytearray([2, 2, 2])), ('PK2', bytearray("西安", 'utf-8'))]
        attribute_columns2 = {'put': [('address', bytearray([20, 20]))]}
        row2 = Row(primary_key2, attribute_columns2)
        item2 = UpdateRowItem(row2, None)
        put_row_items.append(item2)

        item3 = DeleteRowItem(Row(primary_key), None)
        put_row_items.append(item3)

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, put_row_items))
        result = self.client_test.batch_write_row(request)

        self.assert_equal(True, result.is_all_succeed())

        # Batch Read
        rows_to_get = []
        rows_to_get.append([('PK1', bytearray([1, 1, 1])), ('PK2', bytearray("西安", 'utf-8'))])
        rows_to_get.append([('PK1', bytearray([2, 2, 2])), ('PK2', bytearray("西安", 'utf-8'))])

        request = BatchGetRowRequest()
        request.add(TableInBatchGetRowItem(table_name, rows_to_get, [], None, 1))

        result = self.client_test.batch_get_row(request)

        table_result = result.get_result_by_table(table_name)
        self.assert_equal(2, len(table_result))
        self.assert_equal([('PK1', bytearray([1, 1, 1])), ('PK2', bytearray("西安", 'utf-8'))],
                          table_result[0].row.primary_key)
        self.assert_columns(attribute_columns1, table_result[0].row.attribute_columns)

        self.assert_equal([('PK1', bytearray([2, 2, 2])), ('PK2', bytearray("西安", 'utf-8'))],
                          table_result[1].row.primary_key)
        self.assert_columns(attribute_columns2['put'], table_result[1].row.attribute_columns)

        # Delete
        primary_key3 = [('PK1', bytearray([1, 1, 1])), ('PK2', bytearray("西安", 'utf-8'))]
        self.client_test.delete_row(table_name, primary_key3, None)

        # Read
        consumed, return_row, next_token = self.client_test.get_row(table_name, primary_key3, [], None, 1)
        self.assert_equal(return_row, None)


    def test_bytes_pk_type(self):
        '''Write with bytes (not bytearray) as PK value and verify the actual type returned by get_row.

        In the current SDK implementation, bytes (six.binary_type) is encoded as VT_STRING
        rather than VT_BLOB, so get_row returns str instead of bytearray.
        This test documents and verifies that behavior.
        '''
        table_name = self.table_name

        table_meta = TableMeta(table_name, [("PK1", "STRING"), ("PK2", "INTEGER")])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        # Write with bytes PK value (not bytearray)
        bytes_value = b'hello_bytes'
        primary_key = [('PK1', bytes_value), ('PK2', 1)]
        attribute_columns = [('col1', 'value1'), ('col2', 100)]
        row = Row(primary_key, attribute_columns)

        consumed, return_row = self.client_test.put_row(table_name, row, None)

        # Read back and inspect the actual types
        consumed, return_row, next_token = self.client_test.get_row(table_name, primary_key, [], None, 1)
        self.assertIsNotNone(return_row, "get_row should return a non-None row")

        returned_pk_value = return_row.primary_key[0][1]
        print(f"\n[test_bytes_pk_type] Input type: {type(bytes_value).__name__}, "
              f"Returned type: {type(returned_pk_value).__name__}, "
              f"Returned value: {returned_pk_value!r}")

        # bytes (binary_type) is encoded as VT_STRING by PlainBuffer, so the returned value is str
        self.assertIsInstance(returned_pk_value, str,
                              f"Expected str but got {type(returned_pk_value).__name__}. "
                              f"bytes PK is encoded as VT_STRING, not VT_BLOB.")
        self.assertEqual(returned_pk_value, bytes_value.decode('utf-8'),
                         "Returned string value should match the decoded bytes input")

        # Also verify attribute columns are correct
        self.assert_columns(return_row.attribute_columns, attribute_columns)

        # Contrast: write with bytearray PK on a BINARY table to show the difference
        table_name_binary = self.table_name + "_binary"
        table_meta_binary = TableMeta(table_name_binary, [("PK1", "BINARY"), ("PK2", "INTEGER")])
        self.client_test.create_table(table_meta_binary, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name_binary)

        bytearray_value = bytearray(b'hello_bytearray')
        primary_key_binary = [('PK1', bytearray_value), ('PK2', 1)]
        row_binary = Row(primary_key_binary, attribute_columns)
        consumed, return_row = self.client_test.put_row(table_name_binary, row_binary, None)

        consumed, return_row_binary, next_token = self.client_test.get_row(
            table_name_binary, primary_key_binary, [], None, 1)
        self.assertIsNotNone(return_row_binary, "get_row should return a non-None row for BINARY table")

        returned_binary_pk_value = return_row_binary.primary_key[0][1]
        print(f"[test_bytes_pk_type] bytearray input type: {type(bytearray_value).__name__}, "
              f"Returned type: {type(returned_binary_pk_value).__name__}, "
              f"Returned value: {returned_binary_pk_value!r}")

        # bytearray is encoded as VT_BLOB, so the returned value is bytearray
        self.assertIsInstance(returned_binary_pk_value, bytearray,
                              f"Expected bytearray but got {type(returned_binary_pk_value).__name__}. "
                              f"bytearray PK is encoded as VT_BLOB.")
        self.assertEqual(returned_binary_pk_value, bytearray_value,
                         "Returned bytearray value should match the input")

if __name__ == '__main__':
    unittest.main()
