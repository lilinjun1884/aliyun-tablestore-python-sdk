#!/bin/python
# -*- coding: utf8 -*-

import logging
import unittest
from builtins import range

from tablestore.client import *
from tablestore.metadata import *
from tablestore.error import *
from tests.lib.mock_connection import MockConnection
from tests.lib.test_config import *
from tests.test_utils import make_table_name

class SDKParamTest(unittest.TestCase):

    def setUp(self):
        logger = logging.getLogger('test')
        handler = logging.FileHandler("test.log")
        formatter = logging.Formatter("[%(asctime)s]    [%(process)d]   [%(levelname)s] " \
                    "[%(filename)s:%(lineno)s]   %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        self.client = OTSClient(OTS_ENDPOINT, OTS_ACCESS_KEY_ID, OTS_ACCESS_KEY_SECRET, OTS_INSTANCE, enable_native=OTS_ENABLE_NATIVE, native_fallback=OTS_NATIVE_FALLBACK)
        self.table_name = make_table_name('SDKParamTest')

        if self.table_name in self.client.list_table():
            self.client.delete_table(self.table_name)
        schema_of_primary_key = [('gid', 'INTEGER')]
        table_meta = TableMeta(self.table_name, schema_of_primary_key)
        table_option = TableOptions(-1, 2)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client.create_table(table_meta, table_option, reserved_throughput)

    def tearDown(self):
        try:
            self.client.delete_table(self.table_name)
        except Exception:
            pass

    def test_list_table(self):
        with self.assertRaises(TypeError):
            self.client.list_table('one')

    def test_create_table(self):
        with self.assertRaises(OTSClientError):
            self.client.create_table('one', 'two', 'three')

        with self.assertRaises(OTSClientError):
            table_meta = TableMeta('test_table', ['PK1', 'STRING'])
            capacity_unit = CapacityUnit(10, 10)
            self.client.create_table(table_meta, TableOptions(), capacity_unit)

        with self.assertRaises(OTSClientError):
            table_meta = TableMeta('test_table', [('PK1', 'STRING'), ('PK2', 'INTEGER')])
            capacity_unit = CapacityUnit(10, None)
            self.client.create_table(table_meta, TableOptions(), capacity_unit)

        with self.assertRaises(OTSClientError):
            capacity_unit = CapacityUnit(10, 10)
            self.client.create_table('test_table', TableOptions(), capacity_unit)

        with self.assertRaises(OTSClientError):
            table_meta = TableMeta('test_table', [('PK1', 'STRING'), ('PK2', 'INTEGER')])
            self.client.create_table(table_meta, TableOptions(), [1, 2])

    def test_delete_table(self):
        with self.assertRaises(TypeError):
            self.client.delete_table('one', 'two')

        with self.assertRaises(OTSClientError):
            capacity_unit = CapacityUnit(10, 10)
            self.client.delete_table(capacity_unit)

    def test_update_table(self):
        with self.assertRaises(OTSClientError):
            self.client.update_table('one', 'two', 'three')

        with self.assertRaises(OTSClientError):
            self.client.update_table('test_table', TableOptions(), (10, 10))

        with self.assertRaises(OTSClientError):
            capacity_unit = CapacityUnit(None, None)
            self.client.update_table('test_table', TableOptions(), capacity_unit)

    def test_describe_table(self):
        with self.assertRaises(TypeError):
            self.client.describe_table('one', 'two')

        with self.assertRaises(OTSClientError):
            self.client.describe_table(['test_table'])

    def test_put_row(self):
        with self.assertRaises(AttributeError):
            self.client.put_row('one', 'two')

        with self.assertRaises(OTSClientError):
            primary_key = [('PK1','hello'), ('PK2',100)]
            attribute_columns = [('COL1','world'), ('COL2',1000)]
            condition = Condition('InvalidCondition')
            self.client.put_row('test_table', condition, primary_key, attribute_columns)

        try:
            primary_key = [('PK1','hello'), ('PK2',100)]
            attribute_columns = [('COL1','world'), ('COL2',1000)]
            self.client.put_row('test_table', [RowExistenceExpectation.IGNORE], primary_key, attribute_columns)
            self.fail("put_row should raise an error for invalid condition type")
        except (OTSClientError, AttributeError):
            pass

        try:
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client.put_row('test_table', condition, 'primary_key', 'attribute_columns')
            self.fail("put_row should raise an error for invalid primary_key type")
        except (OTSClientError, AttributeError):
            pass

    def test_get_row(self):
        with self.assertRaises(TypeError):
            self.client.get_row('one', 'two')

        try:
            self.client.get_row('test_table', 'primary_key', 'columns_to_get')
            self.fail("get_row should raise an error for invalid primary_key type")
        except (OTSClientError, TypeError):
            pass

    def test_update_row(self):
        with self.assertRaises(AttributeError):
            self.client.update_row('one', 'two', 'three')

        try:
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client.update_row('test_table', condition, [('PK1', 'STRING'), ('PK2', 'INTEGER')], 'update_of_attribute_columns')
            self.fail("update_row should raise an error for invalid condition type")
        except (OTSClientError, AttributeError):
            pass

        try:
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client.update_row('test_table', condition, [('PK1', 'STRING'), ('PK2', 'INTEGER')], [('ncv', 1)])
            self.fail("update_row should raise an error for invalid condition type")
        except (OTSClientError, AttributeError):
            pass

        try:
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client.update_row('test_table', condition, [('PK1', 'STRING'), ('PK2', 'INTEGER')], {'put' : []})
            self.fail("update_row should raise an error for invalid condition type")
        except (OTSClientError, AttributeError):
            pass

        try:
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client.update_row('test_table', condition, [('PK1', 'STRING'), ('PK2', 'INTEGER')], {'delete' : []})
            self.fail("update_row should raise an error for invalid condition type")
        except (OTSClientError, AttributeError):
            pass

    def test_delete_row(self):
        with self.assertRaises(TypeError):
            self.client.delete_row('one', 'two', 'three', 'four')

        try:
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client.delete_row('test_table', condition, 'primary_key')
            self.fail("delete_row should raise an error for invalid primary_key type")
        except (OTSClientError, TypeError):
            pass
    
    def test_delete_row_compatible(self):
        table_name = self.table_name
        primary_key = [('gid',1)]
        row = Row(primary_key)
        condition = Condition(RowExistenceExpectation.IGNORE)
        return_type = ReturnType.RT_PK

        if table_name in self.client.list_table():
            self.client.delete_table(table_name)
        schema_of_primary_key = [('gid', 'INTEGER')]
        table_meta = TableMeta(table_name, schema_of_primary_key)
        table_option = TableOptions(-1, 2)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client.create_table(table_meta, table_option, reserved_throughput)

        self.client.delete_row(table_name, row, condition)
        self.client.delete_row(table_name, row, condition, return_type)
        # self.client.delete_row(table_name, row, condition, return_type, transaction_id)
        self.client.delete_row(table_name, row=row, condition=condition)
        self.client.delete_row(table_name, row=row, condition=condition, return_type=return_type)
        # self.client.delete_row(table_name, row=row, condition=condition, return_type=return_type, transaction_id=transaction_id)
        self.client.delete_row(table_name, primary_key, condition)
        self.client.delete_row(table_name, primary_key, condition, return_type)
        # self.client.delete_row(table_name, primary_key, condition, return_type, transaction_id)
        self.client.delete_row(table_name, primary_key=primary_key, condition=condition)
        self.client.delete_row(table_name, primary_key=primary_key, condition=condition, return_type=return_type)
        # self.client.delete_row(table_name, primary_key=primary_key, condition=condition, return_type=return_type, transaction_id=transaction_id)

        self.client.delete_table(table_name)

    def test_batch_get_row(self):
        with self.assertRaises(TypeError):
            self.client.batch_get_row('one', 'two')

        try:
            self.client.batch_get_row('batches')
            self.fail("batch_get_row should raise an error for invalid input")
        except (OTSClientError, AttributeError):
            pass

    def test_batch_write_row(self):
        with self.assertRaises(TypeError):
            self.client.batch_write_row('one', 'two')

        try:
            self.client.batch_write_row('batches')
            self.fail("batch_write_row should raise an error for invalid input")
        except (OTSClientError, AttributeError):
            pass

        try:
            self.client.batch_write_row([('test_table')])
            self.fail("batch_write_row should raise an error for invalid input")
        except (OTSClientError, AttributeError):
            pass

        try:
            self.client.batch_write_row([{'table_name':None}])
            self.fail("batch_write_row should raise an error for invalid input")
        except (OTSClientError, AttributeError):
            pass

        try:
            self.client.batch_write_row([{'table_name':'abc', 'put':None}])
            self.fail("batch_write_row should raise an error for invalid input")
        except (OTSClientError, AttributeError):
            pass

        try:
            self.client.batch_write_row([{'table_name':'abc', 'put':['xxx']}])
            self.fail("batch_write_row should raise an error for invalid input")
        except (OTSClientError, AttributeError):
            pass

        try:
            self.client.batch_write_row([{'table_name':'abc', 'Put':[]}])
            self.fail("batch_write_row should raise an error for invalid input")
        except (OTSClientError, AttributeError):
            pass

        try:
            self.client.batch_write_row([{'table_name':'abc', 'Any':[]}])
            self.fail("batch_write_row should raise an error for invalid input")
        except (OTSClientError, AttributeError):
            pass

    def test_get_range(self):
        with self.assertRaises(TypeError):
            self.client.get_range('one', 'two')

        with self.assertRaises(OTSClientError):
            start_primary_key = [('PK1','hello'),('PK2',100)]
            end_primary_key = [('PK1',INF_MAX),('PK2',INF_MIN)]
            columns_to_get = ['COL1','COL2']
            self.client.get_range('table_name', 'InvalidDirection',
                        start_primary_key, end_primary_key,
                        columns_to_get, limit=100, max_version=1)

        try:
            start_primary_key = ['PK1','hello','PK2',100]
            end_primary_key = [('PK1',INF_MAX), ('PK2',INF_MIN)]
            columns_to_get = ['COL1', 'COL2']
            self.client.get_range('table_name', 'FORWARD',
                        start_primary_key, end_primary_key,
                        columns_to_get, limit=100, max_version=1)
            self.fail("get_range should raise an error for invalid start_primary_key format")
        except (OTSClientError, TypeError):
            pass

        try:
            start_primary_key = [('PK1','hello'),('PK2',100)]
            end_primary_key = [('PK1',INF_MAX), ('PK2',INF_MIN)]
            columns_to_get = ['COL1', 'COL2']
            self.client.get_range('table_name', 'FORWARD',
                        start_primary_key, end_primary_key,
                        columns_to_get, limit=100, max_version=-1)
            self.fail("get_range should raise an error for table not exist")
        except OTSServiceError:
            pass

        try:
            self.client.get_range('table_name', 'FORWARD',
                        'primary_key', 'primary_key', 'columns_to_get', 100)
            self.fail("get_range should raise an error for invalid primary_key type")
        except (OTSClientError, TypeError):
            pass

    def test_xget_range(self):
        with self.assertRaises(TypeError):
            self.client.xget_range('one', 'two')

        with self.assertRaises(OTSClientError):
            iter = self.client.xget_range('one', 'two', 'three', 'four', 'five', 'six', 'seven')
            next(iter)

    def assert_client_error(self, error, message):
        self.assertEqual(str(error), message)

    def test_condition(self):
        Condition(RowExistenceExpectation.IGNORE)
        Condition(RowExistenceExpectation.EXPECT_EXIST)
        Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)

        with self.assertRaisesRegex(OTSClientError, "Expect input row_existence_expectation should be one of"):
            Condition('errr')

        with self.assertRaisesRegex(OTSClientError, "The input column_condition should be an instance of ColumnCondition, not str"):
            Condition(RowExistenceExpectation.IGNORE, "")

        with self.assertRaisesRegex(OTSClientError, "Expect input comparator of SingleColumnCondition should be one of"):
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("", "", ""))

        with self.assertRaisesRegex(OTSClientError, "Expect input comparator of SingleColumnRegexCondition should be one of"):
            Condition(RowExistenceExpectation.IGNORE, SingleColumnRegexCondition("", "", ""))

    def test_column_condition(self):
        cond = SingleColumnCondition("uid", 100, ComparatorType.EQUAL)
        self.assertEqual(ColumnConditionType.SINGLE_COLUMN_CONDITION, cond.get_type())

        cond = SingleColumnRegexCondition("uid", ComparatorType.EXIST)
        self.assertEqual(ColumnConditionType.SINGLE_COLUMN_REGEX_CONDITION, cond.get_type())

        cond = CompositeColumnCondition(LogicalOperator.AND)
        self.assertEqual(ColumnConditionType.COMPOSITE_COLUMN_CONDITION, cond.get_type())
    
    def test_regex_rule(self):
        with self.assertRaisesRegex(OTSClientError, "regex_input should be an instance of str, not int"):
            regex_rule = RegexRule(1, "")
        with self.assertRaisesRegex(OTSClientError, "input cast_type should be an instance of CastType, not str"):
            regex_rule = RegexRule("", "")
        
        regex_rule = RegexRule("123", CastType.VT_INTEGER)
        self.assertEqual(regex_rule.get_regex(), "123")
        self.assertEqual(regex_rule.get_cast_type(), CastType.VT_INTEGER)

    def test_relation_condition(self):
        SingleColumnCondition("uid", 100, ComparatorType.EQUAL)
        SingleColumnCondition("uid", 100, ComparatorType.NOT_EQUAL)
        SingleColumnCondition("uid", 100, ComparatorType.GREATER_THAN)
        SingleColumnCondition("uid", 100, ComparatorType.GREATER_EQUAL)
        SingleColumnCondition("uid", 100, ComparatorType.LESS_THAN)
        SingleColumnCondition("uid", 100, ComparatorType.LESS_EQUAL)

        with self.assertRaisesRegex(OTSClientError, "Expect input comparator of SingleColumnCondition should be one of"):
            SingleColumnCondition("uid", 100, "")

        with self.assertRaisesRegex(OTSClientError, "The input pass_if_missing of SingleColumnCondition should be an instance of Bool, not str"):
            SingleColumnCondition("uid", 100, ComparatorType.LESS_EQUAL, "True")
        
        with self.assertRaisesRegex(OTSClientError, "The input column_name of SingleColumnRegexCondition should be an instance of str, not int"):
            cond = SingleColumnRegexCondition(1, ComparatorType.EXIST)
        with self.assertRaisesRegex(OTSClientError, "Expect input comparator of SingleColumnRegexCondition should be one of"):
            cond = SingleColumnRegexCondition("abc", None)
        with self.assertRaisesRegex(OTSClientError, "when column_value is not set, comparator should be EXIST or NOT_EXIST"):
            cond = SingleColumnRegexCondition("abc", ComparatorType.LESS_EQUAL)
        with self.assertRaisesRegex(OTSClientError, "when column_value is set, comparator should not be EXIST or NOT_EXIST"):
            cond = SingleColumnRegexCondition("abc", ComparatorType.EXIST, 0)
        with self.assertRaisesRegex(OTSClientError, "The input column_value of SingleColumnRegexCondition.set_column_value should not be None"):
            cond = SingleColumnRegexCondition("abc", ComparatorType.EXIST)
            cond.set_column_value(None)
        with self.assertRaisesRegex(OTSClientError, "input regex_rule should be an instance of RegexRule or None, not str"):
            cond = SingleColumnRegexCondition("abc", ComparatorType.EXIST, None, "")
        with self.assertRaisesRegex(OTSClientError, "The input latest_version_only of SingleColumnRegexCondition should be an instance of Bool, not NoneType"):
            cond = SingleColumnRegexCondition("abc", ComparatorType.EXIST, None, None, None)
        
        cond = SingleColumnRegexCondition("abc", ComparatorType.EXIST, None, None, False)
        self.assertEqual(cond.get_column_name(), "abc")
        self.assertEqual(cond.get_comparator(), ComparatorType.EXIST)
        self.assertEqual(cond.get_column_value(), 0)
        self.assertEqual(cond.get_regex_rule(), None)
        self.assertEqual(cond.get_latest_version_only(), False)
        self.assertEqual(cond.get_pass_if_missing(), False)

    def test_composite_condition(self):
        CompositeColumnCondition(LogicalOperator.NOT)
        CompositeColumnCondition(LogicalOperator.AND)
        CompositeColumnCondition(LogicalOperator.OR)

        with self.assertRaisesRegex(OTSClientError, "Expect input combinator should be one of"):
            CompositeColumnCondition("")


    def test_search_timeout_s_param(self):
        """Test timeout_s parameter validation for search method"""
        from tablestore.metadata import SearchQuery, MatchAllQuery, ColumnsToGet, ColumnReturnType
        
        query = MatchAllQuery()
        search_query = SearchQuery(query, limit=10)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL)
        
        # Test timeout_s with string type
        with self.assertRaisesRegex(OTSClientError, "timeout_s must be an integer or float"):
            self.client.search('test_table', 'test_index', search_query, columns_to_get, timeout_s="invalid")
        
        # Test timeout_s with negative value
        with self.assertRaisesRegex(OTSClientError, "timeout_s must be a non-negative integer"):
            self.client.search('test_table', 'test_index', search_query, columns_to_get, timeout_s=-1)
        
        # Test timeout_s with negative float value
        with self.assertRaisesRegex(OTSClientError, "timeout_s must be a non-negative integer"):
            self.client.search('test_table', 'test_index', search_query, columns_to_get, timeout_s=-0.5)

    def test_parallel_scan_timeout_s_param(self):
        """Test timeout_s parameter validation for parallel_scan method"""
        from tablestore.metadata import ScanQuery, MatchAllQuery, ColumnsToGet, ColumnReturnType
        
        query = MatchAllQuery()
        scan_query = ScanQuery(query, next_token=None, limit=10, current_parallel_id=0, max_parallel=1)
        columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL)
        session_id = "test_session_id"
        
        # Test timeout_s with string type
        with self.assertRaisesRegex(OTSClientError, "timeout_s must be an integer or float"):
            self.client.parallel_scan('test_table', 'test_index', scan_query, session_id, columns_to_get, timeout_s="invalid")
        
        # Test timeout_s with negative value
        with self.assertRaisesRegex(OTSClientError, "timeout_s must be a non-negative integer"):
            self.client.parallel_scan('test_table', 'test_index', scan_query, session_id, columns_to_get, timeout_s=-1)
        
        # Test timeout_s with negative float value
        with self.assertRaisesRegex(OTSClientError, "timeout_s must be a non-negative integer"):
            self.client.parallel_scan('test_table', 'test_index', scan_query, session_id, columns_to_get, timeout_s=-0.5)

    def test_make_batch_write_row_with_invalid_request(self):
        """Test _make_batch_write_row raises OTSClientError when request is not BatchWriteRowRequest"""
        from tablestore.encoder import OTSProtoBufferEncoder
        from tests.lib.test_config import OTS_ENABLE_NATIVE, OTS_NATIVE_FALLBACK
        import tablestore.protobuf.table_store_pb2 as pb2

        encoder = OTSProtoBufferEncoder("utf-8", enable_native=OTS_ENABLE_NATIVE, native_fallback=OTS_NATIVE_FALLBACK)
        proto = pb2.BatchWriteRowRequest()

        # Test with a string
        with self.assertRaisesRegex(OTSClientError, "The request should be a instance of BatchWriteRowRequest, not str"):
            encoder._make_batch_write_row(proto, "invalid_request")

        # Test with a dict
        with self.assertRaisesRegex(OTSClientError, "The request should be a instance of BatchWriteRowRequest, not dict"):
            encoder._make_batch_write_row(proto, {"table": "test"})

        # Test with a list
        with self.assertRaisesRegex(OTSClientError, "The request should be a instance of BatchWriteRowRequest, not list"):
            encoder._make_batch_write_row(proto, [1, 2, 3])

        # Test with an integer
        with self.assertRaisesRegex(OTSClientError, "The request should be a instance of BatchWriteRowRequest, not int"):
            encoder._make_batch_write_row(proto, 12345)

        # Test with None
        with self.assertRaisesRegex(OTSClientError, "The request should be a instance of BatchWriteRowRequest, not NoneType"):
            encoder._make_batch_write_row(proto, None)

if __name__ == '__main__':
    unittest.main()
