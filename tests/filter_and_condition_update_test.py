# -*- coding: utf8 -*-

import unittest
from tests.lib.api_test_base import APITestBase
import tests.lib.restriction as restriction
from tablestore import *
from tablestore.error import *
import time
import logging
from tests.test_utils import make_table_name

class FilterAndConditionUpdateTest(APITestBase):
    TABLE_NAME = "test_filter_and_condition_update"

    """ConditionUpdate"""

    def setUp(self):
        APITestBase.setUp(self)

        self.table_name_update_row = make_table_name(FilterAndConditionUpdateTest.TABLE_NAME)
        self.table_name_put_row = make_table_name(FilterAndConditionUpdateTest.TABLE_NAME)
        self.table_name_get_row = make_table_name(FilterAndConditionUpdateTest.TABLE_NAME)
        self.table_name_delete_row = make_table_name(FilterAndConditionUpdateTest.TABLE_NAME)
        self.table_name_batch_write_0 = make_table_name('myTable0_')
        self.table_name_batch_write_1 = make_table_name('myTable1_')
        self.table_name_batch_get_0 = make_table_name('myTable0_')
        self.table_name_batch_get_1 = make_table_name('myTable1_')
        self.table_name_get_range = make_table_name(FilterAndConditionUpdateTest.TABLE_NAME)
        self.table_name_get_range2 = make_table_name(FilterAndConditionUpdateTest.TABLE_NAME)

    def tearDown(self):
        for table_name in [self.table_name_update_row, self.table_name_put_row,
                          self.table_name_get_row, self.table_name_delete_row,
                          self.table_name_batch_write_0, 
                          self.table_name_batch_write_1, self.table_name_batch_get_0,
                          self.table_name_batch_get_1, self.table_name_get_range,
                          self.table_name_get_range2]:
            try:
                self.client_test.delete_table(table_name)
            except:
                pass
        APITestBase.tearDown(self)

    def test_update_row(self):
        """Call the UpdateRow API, construct different Conditions"""
        table_name = self.table_name_update_row
        table_meta = TableMeta(table_name, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        time.sleep(5)

    
        # Inject a row at index = 0
        primary_key = [('gid',0), ('uid',0)]
        attribute_columns = [('index',0)]
        row = Row(primary_key, attribute_columns)
        self.client_test.put_row(table_name, row)

        attribute_columns = {
            'put': [('index' , 0)]
        }
        # Inject a row, the condition is when index = 1, expect to write failure
        row.attribute_columns = attribute_columns
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.EQUAL))
            self.assertIsInstance(condition.get_column_condition(), SingleColumnCondition)
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is when index = 0, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        self.client_test.update_row(table_name, row, condition)

        # Inject a row, the condition is addr = china, since this column does not exist, expect the write to fail
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("addr", "china", ComparatorType.EQUAL, False))
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Inject a row again, with the condition that addr = china, and set to not check if the column does not exist, expecting the write to fail.
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("addr", "china", ComparatorType.EQUAL, True))
        self.client_test.update_row(table_name, row, condition)

        ## NOT_EQUAL

        # Insert a row, with the condition that when index != 0, the expected write failure occurs.
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Inject a row, the condition is when index != 1, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.NOT_EQUAL))
        self.client_test.update_row(table_name, row, condition)

        ## GREATER_THAN

        # Inject a row, the condition is when index > 0, expect the write to fail
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.GREATER_THAN))
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is that when index > -1, it is expected to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", -1, ComparatorType.GREATER_THAN))
        self.client_test.update_row(table_name, row, condition)

        ## GREATER_EQUAL

        # Insert a row, the condition is that when index >= 1, it is expected to fail to write
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.GREATER_EQUAL))
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is that when index >= 0, it is expected to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.GREATER_EQUAL))
        self.client_test.update_row(table_name, row, condition)

        ## LESS_THAN

        # Inject a row, the condition is when index < 0, expect to fail to write
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.LESS_THAN))
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is when index < 1, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.LESS_THAN))
        self.client_test.update_row(table_name, row, condition)

        ## LESS_EQUAL

        # Inject a row, the condition is when index <= -1, expect write failure
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", -1, ComparatorType.LESS_EQUAL))
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is when index <= 0, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.LESS_EQUAL))
        self.client_test.update_row(table_name, row, condition)

        ## COMPOSITE_CONDITION
        row.attribute_columns = {
            'put': [('index',0), ('addr','china')]
        }
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.update_row(table_name, row, condition)

        ## AND

        # Inject a row, the condition is index == 0 & addr != china, expecting write failure
        try:
            cond = CompositeColumnCondition(LogicalOperator.AND)
            cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
            cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, with the condition that when index == 0 & addr == china, expect the write to succeed
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))
        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.update_row(table_name, row, condition)

        ## NOT

        # Inject a row with the condition !(index == 0 & addr == china), expecting write failure
        try:
            cond = CompositeColumnCondition(LogicalOperator.NOT)
            sub_cond = CompositeColumnCondition(LogicalOperator.AND)
            sub_cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
            sub_cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))
            cond.add_sub_condition(sub_cond)

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Inject a row, the condition is !(index != 0 & addr == china), expecting a successful write.
        cond = CompositeColumnCondition(LogicalOperator.NOT)
        
        sub_cond = CompositeColumnCondition(LogicalOperator.AND)
        sub_cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
        sub_cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))
        cond.add_sub_condition(sub_cond)

        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.update_row(table_name, row, condition)

        ## OR

        # Inject a row, the condition is index != 0 or addr != china, expecting write failure
        try:
            cond = CompositeColumnCondition(LogicalOperator.OR)
            cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
            cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.update_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is index == 0 or addr != china, expecting successful write
        cond = CompositeColumnCondition(LogicalOperator.OR)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))
        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.update_row(table_name, row, condition)

    def test_put_row(self):
        """Call the PutRow API to construct different Conditions"""
        table_name = self.table_name_put_row
        table_meta = TableMeta(table_name, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        time.sleep(5)
    
        ## SingleColumnCondition
        ## EQUAL
         
        # Inject a row at index = 0
        primary_key = [('gid',0), ('uid',0)]
        attribute_columns = [('index',0)]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # Inject a row, with the condition that index = 1, expecting a write failure.
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.EQUAL))
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is when index = 0, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        self.client_test.put_row(table_name, row, condition)

        # Inject a row, the condition is addr = china, since this column does not exist, expect the write to fail
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("addr", "china", ComparatorType.EQUAL, False))
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Inject a row again, with the condition that addr = china, and set to not check if the column does not exist, expecting the write to fail.
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("addr", "china", ComparatorType.EQUAL, True))
        self.client_test.put_row(table_name, row, condition)

        ## NOT_EQUAL

        # Inject a row, the condition is when index != 0, expect write failure
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is index != 1, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.NOT_EQUAL))
        self.client_test.put_row(table_name, row, condition)

        ## GREATER_THAN

        # Inject a row, the condition is when index > 0, expect to write failure
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.GREATER_THAN))
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is when index > -1, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", -1, ComparatorType.GREATER_THAN))
        self.client_test.put_row(table_name, row, condition)

        ## GREATER_EQUAL

        # Inject a row, the condition is when index >= 1, expect to write failure
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.GREATER_EQUAL))
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is that when index >= 0, it is expected to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.GREATER_EQUAL))
        self.client_test.put_row(table_name, row, condition)

        ## LESS_THAN

        # Inject a row, the condition is when index < 0, expect to fail to write
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.LESS_THAN))
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, condition is when index < 1, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.LESS_THAN))
        self.client_test.put_row(table_name, row, condition)

        ## LESS_EQUAL

        # Inject a row, the condition is that the write is expected to fail when index <= -1
        try:
            condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", -1, ComparatorType.LESS_EQUAL))
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is when index <= 0, expect to write successfully
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.LESS_EQUAL))
        self.client_test.put_row(table_name, row, condition)

        ## COMPOSITE_CONDITION
        row.attribute_columns = [('index',0), ('addr','china')]
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        ## AND

        # Inject a row, the condition is index == 0 & addr != china, expecting write failure
        try:
            cond = CompositeColumnCondition(LogicalOperator.AND)
            cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
            cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is when index == 0 & addr == china, expect to write successfully
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))
        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.put_row(table_name, row, condition)

        ## NOT

        # Inject a row with the condition !(index == 0 & addr == china), expecting write failure
        try:
            cond = CompositeColumnCondition(LogicalOperator.NOT)
            sub_cond = CompositeColumnCondition(LogicalOperator.AND)
            sub_cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
            sub_cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))
            cond.add_sub_condition(sub_cond)

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is !(index != 0 & addr == china), expecting successful write.
        cond = CompositeColumnCondition(LogicalOperator.NOT)
        
        sub_cond = CompositeColumnCondition(LogicalOperator.AND)
        sub_cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
        sub_cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))
        cond.add_sub_condition(sub_cond)

        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.put_row(table_name, row, condition)

        ## OR

        # Inject a row, the condition is index != 0 or addr != china, expecting write failure
        try:
            cond = CompositeColumnCondition(LogicalOperator.OR)
            cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
            cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))

            condition = Condition(RowExistenceExpectation.IGNORE, cond)
            self.client_test.put_row(table_name, row, condition)
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        # Insert a row, the condition is index == 0 or addr != china, expecting successful write
        cond = CompositeColumnCondition(LogicalOperator.OR)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))
        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.put_row(table_name, row, condition)

        # Insert a row with the condition that age equals 99, and when the age column does not exist, pass_if_missing = True allows the condition to pass, thus inserting successfully.
        cond = Condition(RowExistenceExpectation.IGNORE, 
                         SingleColumnCondition("age", 99, ComparatorType.EQUAL, pass_if_missing = True))
        self.client_test.put_row(table_name, row, cond)

        # Insert a row with the condition that age equals 99. If the age column does not exist and pass_if_missing = False, the condition fails and the insertion fails.
        cond = Condition(RowExistenceExpectation.IGNORE,
                         SingleColumnCondition("age", 99, ComparatorType.EQUAL, pass_if_missing = False))
        try:
            self.client_test.put_row(table_name, row, cond)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.") 


    def test_get_row(self):
        """Call the GetRow API, construct different Conditions"""
        table_name = self.table_name_get_row
        table_meta = TableMeta(table_name, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        time.sleep(5)
 
        primary_key = [('gid',0), ('uid',0)]
        attribute_columns = [('index',0), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        ## COMPOSITE_CONDITION
        ## AND

        # Read a row of data, (index != 0 & addr == china), expect to fail to read
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))

        cu, return_row,token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(None, return_row)

        # Read a row of data, (index == 0 & addr == china), expect to read successfully
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))

        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(primary_key, return_row.primary_key)

        ## OR

        # Read a row of data, (index != 0 or addr != china), expect to fail to read
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))

        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(None, return_row)

        # Read a row of data, (index != 0 or addr == china), expect to read successfully
        cond = CompositeColumnCondition(LogicalOperator.OR)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))

        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(primary_key, return_row.primary_key)

        ## NOT

        # Read a row of data, !(index == 0 or addr == china), expect to fail to read
        cond = CompositeColumnCondition(LogicalOperator.NOT)
        sub_cond = CompositeColumnCondition(LogicalOperator.AND)
        sub_cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        sub_cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))
        cond.add_sub_condition(sub_cond)

        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(None, return_row)

        # Read a row of data, !(index != 0 & addr != china), expect to read successfully
        cond = CompositeColumnCondition(LogicalOperator.NOT)
        sub_cond = CompositeColumnCondition(LogicalOperator.AND)
        sub_cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL))
        sub_cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))
        cond.add_sub_condition(sub_cond)

        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(primary_key, return_row.primary_key)

        ## RELATION_CONDITION

        # Read a row of data, index != 0, expect to read failure
        cond = SingleColumnCondition("index", 0, ComparatorType.NOT_EQUAL)
        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(None, return_row)

        # Read a row of data, index == 0, expect to read successfully
        cond = SingleColumnCondition("index", 0, ComparatorType.EQUAL)
        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(primary_key, return_row.primary_key)

        # Read a row of data, index >= 0, expect to read successfully
        cond = SingleColumnCondition("index", 0, ComparatorType.GREATER_EQUAL)
        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(primary_key, return_row.primary_key)

        # Read a row of data, index <= 0, expect to read successfully
        cond = SingleColumnCondition("index", 0, ComparatorType.LESS_EQUAL)
        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(primary_key, return_row.primary_key)

        # Read a row of data, index > 0, expect to read failure
        cond = SingleColumnCondition("index", 0, ComparatorType.GREATER_THAN)
        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(None, return_row)

        # Read a row of data, index < 0, expect to fail to read
        cond = SingleColumnCondition("index", 0, ComparatorType.LESS_THAN)
        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(None, return_row)

        # Read a row of data, the filter condition is age > 5, age does not exist, set pass_if_missing to True
        cond = SingleColumnCondition("age", 5, ComparatorType.GREATER_THAN, pass_if_missing = True)
        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(primary_key, return_row.primary_key)

        # Read a row of data, the filter condition is age > 5, age does not exist, set pass_if_missing to False
        cond = SingleColumnCondition("age", 5, ComparatorType.GREATER_THAN, pass_if_missing = False)
        cu, return_row, token = self.client_test.get_row(table_name, primary_key, column_filter=cond, max_version=1)
        self.assertEqual(None, return_row)

    def test_delete_row(self):
        """Call the DeleteRow API, construct different Conditions"""
        table_name = self.table_name_delete_row
        table_meta = TableMeta(table_name, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        time.sleep(5)

        # Inject a row with index = 0
        primary_key = [('gid',0), ('uid',0)]
        attribute_columns = [('index',0)]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        ## RELACTION_CONDITION

        # Read a row of data, index < 0, expect to fail to read
        condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.LESS_THAN))

        try:
            self.client_test.delete_row(table_name, primary_key, condition)
        except OTSServiceError as e:
            self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")


        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))

        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        try:
             self.client_test.delete_row(table_name, primary_key, condition)
        except OTSServiceError as e:
             self.assert_error(e, 403, "OTSConditionCheckFail", "Condition check failed.")

        cond = CompositeColumnCondition(LogicalOperator.OR)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.NOT_EQUAL))

        condition = Condition(RowExistenceExpectation.IGNORE, cond)
        self.client_test.delete_row(table_name, primary_key, condition)


    def test_batch_write_row(self):
        """Call the BatchWriteRow API, construct different Conditions"""
        myTable0 = self.table_name_batch_write_0
        myTable1 = self.table_name_batch_write_1

        table_meta = TableMeta(myTable0, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        table_meta = TableMeta(myTable1, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        time.sleep(5)

        primary_key = [('gid',0), ('uid',0)]
        attribute_columns = [('index',0), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable0, row, condition)

        primary_key = [('gid',0), ('uid',1)]
        attribute_columns = [('index',1), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable0, row, condition)

        primary_key = [('gid',0), ('uid',2)]
        attribute_columns = [('index',2), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable0, row, condition)

        primary_key = [('gid',0), ('uid',3)]
        attribute_columns = [('index',3), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable1, row, condition)

        primary_key = [('gid',0), ('uid',4)]
        attribute_columns = [('index',4), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable1, row, condition)

        primary_key = [('gid',0), ('uid',5)]
        attribute_columns = [('index',5), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable1, row, condition)


        # put
        put_row_items = []
        put_row_items.append(PutRowItem(                
                Row([('gid',0), ('uid',0)], 
                    [('index',6), ('addr','china')]),
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))))

        put_row_items.append(PutRowItem(
            Row([('gid',0), ('uid',1)], 
                [('index',7), ('addr','china')]),
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.EQUAL))))

        put_row_items.append(PutRowItem(                
            Row([('gid',0), ('uid',2)], 
                [('index',8), ('addr','china')]),
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 2, ComparatorType.EQUAL))))

        batch_list = BatchWriteRowRequest()
        batch_list.add(TableInBatchWriteRowItem(myTable0, put_row_items))
        batch_list.add(TableInBatchWriteRowItem(myTable1, put_row_items))

        result = self.client_test.batch_write_row(batch_list)

        self.assertEqual(True, result.is_all_succeed())

        r0 = result.get_put_by_table(myTable0)
        r1 = result.get_put_by_table(myTable1)


        self.assertEqual(3, len(r0))
        self.assertEqual(3, len(r1))

        for i in r0:
            self.assertTrue(i.is_ok)
            self.assertEqual(1, i.consumed.write)
            self.assertEqual(1, i.consumed.read)

        for i in r1:
            self.assertTrue(i.is_ok)
            self.assertEqual(1, i.consumed.write)
            self.assertEqual(1, i.consumed.read)

        self.assertEqual(6, len(result.get_succeed_of_put()))
        self.assertEqual(0, len(result.get_failed_of_put()))

        # update
        update_row_items = []
        update_row_items.append(UpdateRowItem(                
            Row([('gid',0), ('uid',0)], 
                {
                        'put': [('index',9), ('addr','china')]
                }),
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))))

        update_row_items.append(UpdateRowItem(                
            Row([('gid',0), ('uid',1)], 
                {
                        'put': [('index',10), ('addr','china')]
                        }),
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 1, ComparatorType.EQUAL))))


        update_row_items.append(UpdateRowItem(
            Row([('gid',0), ('uid',2)], 
            {
                'put': [('index',11), ('addr','china')]
            }),
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 2, ComparatorType.EQUAL))))


        batch_list = BatchWriteRowRequest()
        batch_list.add(TableInBatchWriteRowItem(myTable0, update_row_items))
        batch_list.add(TableInBatchWriteRowItem(myTable1, update_row_items))

        result = self.client_test.batch_write_row(batch_list)

        self.assertEqual(False, result.is_all_succeed())

        r0 = result.get_update_by_table(myTable0)
        r1 = result.get_update_by_table(myTable1)

        self.assertEqual(3, len(r0))
        self.assertEqual(3, len(r1))

        for i in r0:
            self.assertFalse(i.is_ok)
            self.assertEqual("OTSConditionCheckFail", i.error_code)
            self.assertEqual("Condition check failed.", i.error_message)

        for i in r1:
            self.assertFalse(i.is_ok)
            self.assertEqual("OTSConditionCheckFail", i.error_code)
            self.assertEqual("Condition check failed.", i.error_message)


        self.assertEqual(0, len(result.get_succeed_of_update()))
        self.assertEqual(6, len(result.get_failed_of_update()))

        # delete
        delete_row_items = []
        delete_row_items.append(DeleteRowItem(               
            Row([('gid',0), ('uid',0)]),
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 3, ComparatorType.EQUAL, False))))

        delete_row_items.append(DeleteRowItem(                
            Row([('gid',0), ('uid',1)]),
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 4, ComparatorType.EQUAL, False))))

        delete_row_items.append(DeleteRowItem(                
            Row([('gid',0), ('uid',2)]),
            Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 5, ComparatorType.EQUAL, False))))

        batch_list = BatchWriteRowRequest()
        batch_list.add(TableInBatchWriteRowItem(myTable0, delete_row_items))
        batch_list.add(TableInBatchWriteRowItem(myTable1, delete_row_items))

        result = self.client_test.batch_write_row(batch_list)

        self.assertEqual(False, result.is_all_succeed())

        r0 = result.get_delete_by_table(myTable0)
        r1 = result.get_delete_by_table(myTable1)

        self.assertEqual(3, len(r0))
        self.assertEqual(3, len(r1))

        for i in r0:
            self.assertFalse(i.is_ok)
            self.assertEqual("OTSConditionCheckFail", i.error_code)
            self.assertEqual("Condition check failed.", i.error_message)

        for i in r1:
            self.assertFalse(i.is_ok)
            self.assertEqual("OTSConditionCheckFail", i.error_code)
            self.assertEqual("Condition check failed.", i.error_message)

        self.assertEqual(0, len(result.get_succeed_of_delete()))
        self.assertEqual(6, len(result.get_failed_of_delete()))
       
    def test_batch_get_row(self):
        """Call the BatchGetRow API, construct different Conditions"""
        myTable0 = self.table_name_batch_get_0
        myTable1 = self.table_name_batch_get_1
        table_meta = TableMeta(myTable0, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        table_meta = TableMeta(myTable1, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        time.sleep(5)
 
        primary_key = [('gid',0), ('uid',0)]
        attribute_columns = [('index',0), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable0, row, condition)

        primary_key = [('gid',0), ('uid',1)]
        attribute_columns = [('index',1), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable0, row, condition)

        primary_key = [('gid',0), ('uid',2)]
        attribute_columns = [('index',2), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable0, row, condition)

        primary_key = [('gid',0), ('uid',0)]
        attribute_columns = [('index',0), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable1, row, condition)

        primary_key = [('gid',1), ('uid',0)]
        attribute_columns = [('index',1), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable1, row, condition)

        primary_key = [('gid',2), ('uid',0)]
        attribute_columns = [('index',2), ('addr','china')]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(myTable1, row, condition)


        ## COMPOSITE_CONDITION

        # Read a row of data, (index != 0 & addr == china), expect to fail to read
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))

        column_to_get = ['index']
        
        batch_list = BatchGetRowRequest()

        primary_keys = []
        primary_keys.append([('gid',0), ('uid',0)])
        primary_keys.append([('gid',0), ('uid',1)])
        primary_keys.append([('gid',0), ('uid',2)])
        batch_list.add(TableInBatchGetRowItem(myTable0, primary_keys, column_to_get, cond, 1))

        primary_keys = []
        primary_keys.append([('gid',0), ('uid',0)])
        primary_keys.append([('gid',1), ('uid',0)])
        primary_keys.append([('gid',2), ('uid',0)])
        batch_list.add(TableInBatchGetRowItem(myTable1, primary_keys, column_to_get, cond, 1))

        result = self.client_test.batch_get_row(batch_list)
        table0 = result.get_result_by_table(myTable0)
        table1 = result.get_result_by_table(myTable1)

        self.assertEqual(6, len(result.get_succeed_rows()))
        self.assertEqual(0, len(result.get_failed_rows()))
        self.assertEqual(True, result.is_all_succeed())

        self.assertEqual(3, len(table0))
        self.assertEqual(3, len(table1))

        # myTable0
        # row 0
        self.assertEqual([('gid',0), ('uid',0)], table0[0].row.primary_key)
        self.assert_columns([('index',0)], table0[0].row.attribute_columns)

        # row 1
        self.assertEqual(None, table0[1].row)

        # row 2
        self.assertEqual(None, table0[2].row)

        # myTable1
        # row 0
        self.assertEqual([('gid',0), ('uid',0)], table1[0].row.primary_key)
        self.assert_columns([('index',0)], table0[0].row.attribute_columns)

        # row 1
        self.assertEqual(None, table1[1].row)

        # row 2
        self.assertEqual(None, table1[2].row)

        ## RELATION_CONDITION
        cond = SingleColumnCondition('index', 0, ComparatorType.GREATER_THAN)
        column_to_get = ['index']
        
        batch_list = BatchGetRowRequest()

        primary_keys = []
        primary_keys.append([('gid',0), ('uid',0)])
        primary_keys.append([('gid',0), ('uid',1)])
        primary_keys.append([('gid',0), ('uid',2)])
        batch_list.add(TableInBatchGetRowItem(myTable0, primary_keys, column_to_get, cond, 1))

        primary_keys = []
        primary_keys.append([('gid',0), ('uid',0)])
        primary_keys.append([('gid',1), ('uid',0)])
        primary_keys.append([('gid',2), ('uid',0)])
        batch_list.add(TableInBatchGetRowItem(myTable1, primary_keys, column_to_get, cond, 1))

        result = self.client_test.batch_get_row(batch_list)

        self.assertEqual(6, len(result.get_succeed_rows()))
        self.assertEqual(0, len(result.get_failed_rows()))

        self.assertEqual(True, result.is_all_succeed())

        table0 = result.get_result_by_table(myTable0)
        table1 = result.get_result_by_table(myTable1)

        self.assertEqual(3, len(table0))
        self.assertEqual(3, len(table1))

        # myTable0
        # row 
        self.assertEqual(None, table0[0].row)

        # row 1
        self.assertEqual([('gid',0), ('uid',1)], table0[1].row.primary_key)
        self.assert_columns([('index', 1)], table0[1].row.attribute_columns)

        # row 2
        self.assertEqual([('gid',0), ('uid',2)], table0[2].row.primary_key)
        self.assert_columns([('index', 2)], table0[2].row.attribute_columns)

        # myTable1
        # row 0
        self.assertEqual(None, table1[0].row)

        # row 1
        self.assertEqual([('gid',1), ('uid',0)], table1[1].row.primary_key)
        self.assert_columns([('index', 1)], table1[1].row.attribute_columns)

        # row 2
        self.assertEqual([('gid',2), ('uid',0)], table1[2].row.primary_key)
        self.assert_columns([('index', 2)], table1[2].row.attribute_columns)

    def _get_xrange_helper(self, table_name, cond):
        inclusive_start_primary_key = [('gid',INF_MIN), ('uid',INF_MIN)]
        exclusive_end_primary_key = [('gid',INF_MAX), ('uid',INF_MAX)]
        consumed_counter = CapacityUnit()

        rows_result = []
        range_iterator = self.client_test.xget_range(
                table_name,
                Direction.FORWARD,
                inclusive_start_primary_key,
                exclusive_end_primary_key,
                consumed_counter,
                column_filter=cond,
                max_version=1)
        for r in range_iterator:
            rows_result.append(r)
        return rows_result

    def test_get_range(self):
        """Call the GetRange API, construct different Conditions"""
        table_name = self.table_name_get_range
        table_meta = TableMeta(table_name, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        time.sleep(5)
 
        for i in range(0, 100):
            primary_key = [('gid',0), ('uid',i)]
            attribute_columns = [('index',i), ('addr','china')]
            row = Row(primary_key, attribute_columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client_test.put_row(table_name, row, condition)

        ## COMPOSITE_CONDITION
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("index", 50, ComparatorType.LESS_THAN))
        cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))

        inclusive_start_primary_key = [('gid',INF_MIN), ('uid',INF_MIN)]
        exclusive_end_primary_key = [('gid',INF_MAX), ('uid',INF_MAX)]

        rows = []
        next_pk = inclusive_start_primary_key
        while next_pk != None:
            cu, next_token, row_list, token = self.client_test.get_range(
                table_name, 
                Direction.FORWARD, 
                next_pk, 
                exclusive_end_primary_key, 
                column_filter=cond,
                max_version=1)

            next_pk = next_token
            rows.extend(row_list)

        self.assertEqual(50, len(rows))
        for i in range(0, 50):
            r  = rows[i]
            self.assertEqual([('gid',0), ('uid',i)], r.primary_key)
            self.assert_columns([('addr','china'),('index',i)], r.attribute_columns)

        ## RELATION_CONDITION SingleColumnCondition
        cond = SingleColumnCondition("index", 50, ComparatorType.GREATER_EQUAL)
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(50, len(rows))
        for i in range(50, 100):
            r = rows[i - 50]
            self.assertEqual([('gid',0), ('uid',i)], r.primary_key)
            self.assert_columns([('addr','china'),('index',i)], r.attribute_columns)
        
        ## RELATION_CONDITION SingleColumnRegexCondition
        cond = SingleColumnRegexCondition("index", ComparatorType.EXIST)
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(100, len(rows))
    
    def test_get_range2(self):
        """Call the GetRange API, construct different SingleColumnRegexConditions"""
        table_name = self.table_name_get_range2
        table_meta = TableMeta(table_name, [('gid', ColumnType.INTEGER), ('uid', ColumnType.INTEGER)])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        time.sleep(5)
 
        for i in range(0, 100):
            primary_key = [('gid',0), ('uid',i)]
            if i % 2 == 0:
                col_i_value = 'x' * (i+1)
            else:
                col_i_value = 'y' * (i+1)
            attribute_columns = [('index',i), ('addr','china'), ('col'+str(i),col_i_value), ('intCol',str(i))]
            row = Row(primary_key, attribute_columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client_test.put_row(table_name, row, condition)
        
        ## SingleColumnRegexCondition, EXIST match all
        cond = SingleColumnRegexCondition("index", ComparatorType.EXIST)
        consumed_counter = CapacityUnit()
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(100, len(rows))

        ## SingleColumnRegexCondition, EXIST match nothing
        cond = SingleColumnRegexCondition("index_not_exist", ComparatorType.EXIST)
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(0, len(rows))

        ## SingleColumnRegexCondition, EXIST match 1 row
        cond = SingleColumnRegexCondition("col0", ComparatorType.EXIST)
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(1, len(rows))

        ## SingleColumnRegexCondition, NOT_EXIST match 99 row
        cond = SingleColumnRegexCondition("col0", ComparatorType.NOT_EXIST)
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(99, len(rows))

        ## SingleColumnRegexCondition, EQUAL match 1 row
        cond = SingleColumnRegexCondition("index", ComparatorType.EQUAL, 0)
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(1, len(rows))

        ## SingleColumnRegexCondition, EXIST match all
        regex_rule = RegexRule(".*", CastType.VT_INTEGER)
        cond = SingleColumnRegexCondition("intCol", ComparatorType.EXIST, column_value=None, regex_rule=regex_rule)
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(100, len(rows))

        ## SingleColumnRegexCondition, EXIST match 1 row
        regex_rule = RegexRule("xx[x]+xx", CastType.VT_STRING)
        cond = SingleColumnRegexCondition("col10", ComparatorType.EXIST, column_value=None, regex_rule=regex_rule)
        rows = self._get_xrange_helper(table_name, cond)
        self.assertEqual(1, len(rows))
        self.assertEqual([('gid',0), ('uid',10)], rows[0].primary_key)
        self.assert_columns([
            ('addr', 'china'),
            ('col10', 'x' * (10+1)),
            ('index', 10),
            ('intCol', '10')
        ], rows[0].attribute_columns)

if __name__ == '__main__':
    unittest.main()
