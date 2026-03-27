# -*- coding: utf8 -*-

import unittest
import time
from tests.lib.api_test_base import APITestBase
from tests.lib.api_test_base import get_no_retry_client
from tablestore import *
from tablestore.error import *
from tests.test_utils import make_table_name


class TableOperationTest(APITestBase):
    """Table-level operation test"""

    def setUp(self):
        super(TableOperationTest, self).setUp()
        self.table_name_delete_existing = make_table_name('table_test_delete_existing')
        self.table_name_already_exist = make_table_name('table_test_already_exist')
        self.table_name_sequence = make_table_name('table_test_sequence')
        self.table_name_duplicate_PK = make_table_name('table_test_duplicate_PK')
        self.table_name_PK_option = make_table_name('table_PK_option')
        self.table_name_PK_type = make_table_name('table_PK_type')
        self.table_name_create_again = make_table_name('table_create_again')
        self.table_name_1_CU_mess_up = make_table_name('table1_CU_mess_up_test')
        self.table_name_2_CU_mess_up = make_table_name('table2_CU_mess_up_test')
        self.table_name_with_index = make_table_name('table_with_index')

    def tearDown(self):
        tables_to_delete = [
            self.table_name_delete_existing,
            self.table_name_already_exist,
            self.table_name_sequence,
            self.table_name_duplicate_PK,
            self.table_name_PK_option,
            self.table_name_PK_type,
            self.table_name_create_again,
            self.table_name_1_CU_mess_up,
            self.table_name_2_CU_mess_up,
            self.table_name_with_index,
        ]
        for t in tables_to_delete:
            try:
                self.client_test.delete_table(t)
            except:
                pass
        APITestBase.tearDown(self)

    def test_delete_existing_table(self):
        """Delete an existing table, expect success, use list_table() to confirm the table has been deleted, and describe_table() returns an exception: OTSObjectNotExist."""
        table_name = self.table_name_delete_existing
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.client_test.delete_table(table_name)
        self.assert_equal(False, table_name in self.client_test.list_table())

        try:
            self.client_test.describe_table(table_name)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 404, "OTSObjectNotExist", "Requested table does not exist.")

    def test_create_table_already_exist(self):
        """Create a table with a name that duplicates an existing table, expect to return ErrorCode: OTSObjectAlreadyExist, confirm there are no two tables with the same name using list_table()"""
        table_name = self.table_name_already_exist
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        table_meta_new = TableMeta(table_name, [('PK2', 'STRING'), ('PK3', 'STRING')])
        try:
            self.client_test.create_table(table_meta_new, table_options, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 409, "OTSObjectAlreadyExist", "Requested table already exists.")

        table_list = self.client_test.list_table()
        self.assert_equal(1, table_list.count(table_name))

    def test_create_table_with_sequence(self):
        """Create a table, the impact of PK order"""
        table_name = self.table_name_sequence
        table_meta = TableMeta(table_name, [('PK9', 'STRING'), ('PK1', 'INTEGER'), ('PK3', 'BINARY')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        table_list = self.client_test.list_table()
        self.assert_equal(1, table_list.count(table_name))

    def test_duplicate_PK_name_in_table_meta(self):
        """When creating a table, there are 2 PK columns in TableMeta with duplicate column names. Expected to return OTSParameterInvalid, and confirm that this table does not exist using list_table()"""
        table_name = self.table_name_duplicate_PK
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK0', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        try:
            self.client_test.create_table(table_meta, table_options, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "Duplicated primary key name: 'PK0'.")

        self.assert_equal(False, table_name in self.client_test.list_table())

    def test_PK_option(self):
        """Test the default value, custom value, and value update of table_option"""
        table_name = self.table_name_PK_option
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'INTEGER'), ('PK2', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        table_options_default = TableOptions()  # Use default option
        self.client_test.create_table(table_meta, table_options_default, reserved_throughput)
        describe_response = self.client_test.describe_table(table_name)
        expect_options = TableOptions(-1, 1, 86400)  # Default option is (-1, 1, 86400)
        self.assert_DescribeTableResponse(
            describe_response, reserved_throughput.capacity_unit, table_meta, expect_options)

        # Delete table
        self.client_test.delete_table(table_name)
        time.sleep(0.5)

        table_options_special = TableOptions(1200000, 2, 86401)  # Use special option
        self.assert_CreateTableResult(table_name, table_meta, table_options_special, reserved_throughput)
        time.sleep(0.5)

        table_options_update = TableOptions(-1, 3, 86402)  # Test update option
        self.client_test.update_table(table_name, table_options_update)
        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta,
                                          table_options_update)
        
    def test_PK_type(self):
        """Test the types of PK columns, including STRING type, INTEGER type, BINARY type, and invalid types."""
        table_name = self.table_name_PK_type
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        # Valid types
        valid_type = ['STRING', 'INTEGER', 'BINARY']
        for vt in valid_type:
            table_meta = TableMeta(table_name,
                                   [('PK0', vt), ('PK1', vt), ('PK2', vt), ('PK3', vt)])
            self.assert_CreateTableResult(table_name, table_meta, table_options, reserved_throughput)
            # Delete table
            self.client_test.delete_table(table_name)
            time.sleep(0.5)

        # Invalid type
        invalid_type = ['DOUBLE', 'BOOLEAN']
        for vt in invalid_type:
            table_meta = TableMeta(table_name,
                                   [('PK0', vt), ('PK1', vt), ('PK2', vt), ('PK3', vt)])
            try:
                self.client_test.create_table(table_meta, table_options, reserved_throughput)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", vt + " is an invalid type for the primary key.")
            except OTSClientError as e:
                self.assert_equal("primary_key_type should be one of [BINARY, INTEGER, STRING], not " + vt, str(e))
            self.assert_equal(False, table_name in self.client_test.list_table())
            time.sleep(0.5)

    def test_create_table_again(self):
        """
        Create a table, set CU (1, 1), delete it, then create a table with the same name but different PK, set CU to (2, 2), and verify the operation of CU.
        """
        table_name = self.table_name_create_again
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(1, 1))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.client_test.delete_table(table_name)

        table_meta_new = TableMeta(table_name, [('PK0_new', 'INTEGER'), ('PK1', 'STRING')])
        reserved_throughput_new = ReservedThroughput(CapacityUnit(2, 2))
        self.client_test.create_table(table_meta_new, table_options, reserved_throughput_new)
        self.wait_for_partition_load(table_name)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(
            describe_response, reserved_throughput_new.capacity_unit, table_meta_new, table_options)

        pk_dict_exist = [('PK0_new', 3), ('PK1', '1')]
        pk_dict_not_exist = [('PK0_new', 5), ('PK1', '2')]
        self.check_CU_by_consuming(
            table_name, pk_dict_exist, pk_dict_not_exist, reserved_throughput_new.capacity_unit)
        
    def test_CU_not_messed_up_with_two_tables(self):
        """Create two tables, set CU to (1, 2) and (2, 1) respectively, operate to verify CU, use describe_table() to confirm the settings are successful."""
        table_name_1 = self.table_name_1_CU_mess_up
        table_meta_1 = TableMeta(table_name_1, [('PK0', 'STRING'), ('PK1', 'STRING')])
        reserved_throughput_1 = ReservedThroughput(CapacityUnit(1, 2))
        table_name_2 = self.table_name_2_CU_mess_up
        table_meta_2 = TableMeta(table_name_2, [('PK0', 'STRING'), ('PK1', 'STRING')])
        reserved_throughput_2 = ReservedThroughput(CapacityUnit(2, 1))
        pk_dict_exist = [('PK0', 'a'), ('PK1', '1')]
        pk_dict_not_exist = [('PK0', 'B'), ('PK1', '2')]
        table_options = TableOptions()
        self.client_test.create_table(table_meta_1, table_options, reserved_throughput_1)
        self.client_test.create_table(table_meta_2, table_options, reserved_throughput_2)
        self.wait_for_partition_load(table_name_1)
        self.wait_for_partition_load(table_name_2)

        describe_response_1 = self.client_test.describe_table(table_name_1)
        self.assert_DescribeTableResponse(
            describe_response_1, reserved_throughput_1.capacity_unit, table_meta_1, table_options)
        self.check_CU_by_consuming(
            table_name_1, pk_dict_exist, pk_dict_not_exist, reserved_throughput_1.capacity_unit)
        describe_response_2 = self.client_test.describe_table(table_name_2)
        self.assert_DescribeTableResponse(
            describe_response_2, reserved_throughput_2.capacity_unit, table_meta_2, table_options)
        self.check_CU_by_consuming(
            table_name_2, pk_dict_exist, pk_dict_not_exist, reserved_throughput_2.capacity_unit)
        
    def test_create_table_with_CU(self):
        """Create a table, CU is from (0, 0) to (1, 1), and confirm the settings are successful with describe_table()"""
        for (i, j) in [(0, 0), (0, 1), (1, 0), (1, 1)]:
            table_name = make_table_name('table_cu_' + str(i) + '_' + str(j))
            table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING')])
            table_options = TableOptions()
            reserved_throughput = ReservedThroughput(CapacityUnit(i, j))
            self.client_test.create_table(table_meta, table_options, reserved_throughput)
            self.wait_for_partition_load(table_name)

            describe_response = self.client_test.describe_table(table_name)
            self.assert_DescribeTableResponse(
                describe_response, reserved_throughput.capacity_unit, table_meta, table_options)
            time.sleep(0.5)
            self.client_test.delete_table(table_name)

    def _assert_index_meta(self, expect_index, actual_index):
        self.assert_equal(expect_index.index_name, actual_index.index_name)
        self.assert_equal(expect_index.primary_key_names, actual_index.primary_key_names)
        self.assert_equal(expect_index.defined_column_names, actual_index.defined_column_names)
        self.assert_equal(expect_index.index_type, actual_index.index_type)

    def test_create_table_with_secondary_index(self):
        """Test creating a table with secondary indexes"""
        table_name = self.table_name_with_index
        schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
        defined_columns = [('i', 'INTEGER'), ('bool', 'BOOLEAN'), ('d', 'DOUBLE'), ('s', 'STRING'), ('b', 'BINARY')]
        table_meta = TableMeta(table_name, schema_of_primary_key, defined_columns)
        table_option = TableOptions(-1, 1)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        secondary_indexes = [
            SecondaryIndexMeta('index_1', ['i', 's'], ['bool', 'b', 'd']),
        ]

        # Create a secondary index when creating a table
        self.client_test.create_table(table_meta, table_option, reserved_throughput, secondary_indexes)

        dtr = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(dtr, reserved_throughput.capacity_unit, table_meta, table_option)
        self.assert_equal(table_meta.defined_columns, dtr.table_meta.defined_columns)
        self.assert_equal(1, len(dtr.secondary_indexes))
        self._assert_index_meta(secondary_indexes[0], dtr.secondary_indexes[0])

    def test_create_secondary_index(self):
        """Test creating a secondary index"""
        table_name = self.table_name_with_index
        schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
        defined_columns = [('i', 'INTEGER'), ('bool', 'BOOLEAN'), ('d', 'DOUBLE'), ('s', 'STRING'), ('b', 'BINARY')]
        table_meta = TableMeta(table_name, schema_of_primary_key, defined_columns)
        table_option = TableOptions(-1, 1)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        # Create table
        self.client_test.create_table(table_meta, table_option, reserved_throughput)

        # Create a secondary index, set include_base_data to False
        secondary_index_meta = SecondaryIndexMeta('index_1', ['s', 'b'], ['i'])
        self.client_test.create_secondary_index(table_name, secondary_index_meta, False)

        dtr = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(dtr, reserved_throughput.capacity_unit, table_meta, table_option)
        self.assert_equal(table_meta.defined_columns, dtr.table_meta.defined_columns)
        self.assert_equal(1, len(dtr.secondary_indexes))
        self._assert_index_meta(secondary_index_meta, dtr.secondary_indexes[0])

        # Delete index
        self.client_test.delete_secondary_index(table_name, 'index_1')
        dtr = self.client_test.describe_table(table_name)
        self.assert_equal(0, len(dtr.secondary_indexes))

        def put_row(pk, cols):
            row = Row(pk, cols)
            condition = Condition('EXPECT_NOT_EXIST')
            _, _ = self.client_test.put_row(table_name, row, condition)
            _, rows, _ = self.client_test.get_row(table_name, pk, max_version=1)
            self.assert_equal(rows.primary_key, pk)
            self.assert_columns(rows.attribute_columns, cols)

        # Insert data
        primary_key = [('gid', 0), ('uid', '0')]
        attribute_columns = [('i', 0), ('bool', True), ('d', 123.0), ('s', 'test1'), ('b', bytearray(1))]
        put_row(primary_key, attribute_columns)
        primary_key = [('gid', 0), ('uid', '1')]
        attribute_columns = [('i', 1), ('bool', True), ('d', 321.0), ('s', 'test2'), ('b', bytearray(2))]
        put_row(primary_key, attribute_columns)

        # Create a secondary index, test setting include_base_data to True
        current_time = int(time.time() * 1000)
        index_name = 'index_' + str(current_time)
        secondary_index_meta = SecondaryIndexMeta(
            index_name, ['gid', 's'], ['bool', 'd'])
        self.client_test.create_secondary_index(table_name, secondary_index_meta, True)

        time.sleep(30)  # Wait for index creation
        dtr = self.client_test.describe_table(table_name)
        self.assert_equal(1, len(dtr.secondary_indexes))
        self._assert_index_meta(secondary_index_meta, dtr.secondary_indexes[0])

        # Wait for the synchronization of existing data, which will take a relatively long time.
        columns_to_get = ['bool', 'd']
        primary_key = [('gid', 0), ('s', 'test2'), ('uid', '1')]
        no_retry_client = get_no_retry_client()
        retry_times = 0
        max_retry_times = 20
        while retry_times < max_retry_times:
            try:
                _, return_row, _ = no_retry_client.get_row(
                    index_name, primary_key, columns_to_get=columns_to_get, max_version=1)
            except OTSServiceError as e:
                # This error message indicates that the existing data is still being synchronized, so ignore it.
                # If it is not the error message, then throw an exception
                if e.message != 'Disallow read index table in building base state':
                    raise e
                retry_times += 1
                self.assertLess(retry_times, max_retry_times, 'exceed retry times -- ' + str(e))
                time.sleep(10)
                continue
            self.assert_equal(return_row.primary_key, primary_key)
            expect_cols = [('bool', True), ('d', 321.0)]
            self.assert_columns(return_row.attribute_columns, expect_cols)
            break


if __name__ == '__main__':
    unittest.main()
