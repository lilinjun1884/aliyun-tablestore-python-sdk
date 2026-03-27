# -*- coding: utf8 -*-

import json
import unittest

from tablestore import *
from tablestore.error import *
from tests.lib.api_test_base import APITestBase
from tests.test_utils import make_table_name


class SearchIndexTest(APITestBase):

    def setUp(self):
        APITestBase.setUp(self)

        self.table_name_create_index = make_table_name('SearchIndexTest_')
        self.table_name_query_with_nested = make_table_name('SearchIndexQueryTest_')
        self.table_name_describe_sync_stat = make_table_name('SearchIndexQueryTest_')
        self.table_name_text_similarity = make_table_name('TextSimilarityTest_')

    def tearDown(self):
        for table_name in [self.table_name_create_index, self.table_name_query_with_nested,
                          self.table_name_describe_sync_stat, self.table_name_text_similarity]:
            try:
                indexes = self.client_test.list_search_index(table_name)
                for idx in indexes:
                    self.client_test.delete_search_index(table_name, idx[1])
            except:
                pass
            try:
                self.client_test.delete_table(table_name)
            except:
                pass
        APITestBase.tearDown(self)

    def _check_field_schema(self, expect_field_schema, actual_field_schema):
        self.assert_equal(expect_field_schema.field_name, actual_field_schema.field_name)
        self.assert_equal(expect_field_schema.field_type, actual_field_schema.field_type)
        if actual_field_schema.field_type != FieldType.NESTED:
            self.assert_equal(expect_field_schema.index if expect_field_schema.index else True,
                              actual_field_schema.index)
            self.assert_equal(expect_field_schema.is_array if expect_field_schema.is_array else False,
                              actual_field_schema.is_array)
            self.assert_equal(
                expect_field_schema.enable_sort_and_agg if expect_field_schema.enable_sort_and_agg else False,
                actual_field_schema.enable_sort_and_agg)

        if actual_field_schema.field_type == FieldType.TEXT:
            self.assert_equal(
                expect_field_schema.analyzer if expect_field_schema.analyzer is not None else AnalyzerType.SINGLEWORD,
                actual_field_schema.analyzer)

        if expect_field_schema.text_similarity is not None:
            self.assert_equal(expect_field_schema.text_similarity, actual_field_schema.text_similarity)

        if actual_field_schema.sub_field_schemas:
            for i in range(len(actual_field_schema.sub_field_schemas)):
                self._check_field_schema(expect_field_schema.sub_field_schemas[i],
                                         actual_field_schema.sub_field_schemas[i])

    def _check_sorter(self, expect_sorter, actual_sorter):
        if isinstance(expect_sorter, FieldSort):
            self.assertTrue(isinstance(actual_sorter, FieldSort))
        elif isinstance(expect_sorter, PrimaryKeySort):
            self.assertTrue(isinstance(actual_sorter, PrimaryKeySort))
        else:
            self.assertTrue(False)

    def _check_index_meta(self, expect_index_meta, actual_index_meta, with_nested=False):
        self.assert_equal(len(expect_index_meta.fields), len(actual_index_meta.fields))
        for i in range(len(actual_index_meta.fields)):
            self._check_field_schema(expect_index_meta.fields[i], actual_index_meta.fields[i])

        # check index setting
        if expect_index_meta.index_setting is None:
            self.assert_equal(actual_index_meta.index_setting.routing_fields, [])
        else:
            self.assert_equal(actual_index_meta.index_setting.routing_fields,
                              expect_index_meta.index_setting.routing_fields)

        # check index sort
        if expect_index_meta.index_sort is None:
            if with_nested:
                self.assert_equal(len(actual_index_meta.index_sort.sorters), 0)
            else:
                self.assert_equal(len(actual_index_meta.index_sort.sorters), 1)
                self._check_sorter(actual_index_meta.index_sort.sorters[0], PrimaryKeySort(SortOrder.ASC))
        else:
            for i in range(len(expect_index_meta.index_sort.sorters)):
                self._check_sorter(expect_index_meta.index_sort.sorters[i], actual_index_meta.index_sort.sorters[i])

    def test_create_search_index(self):
        table_name = self.table_name_create_index
        table_meta = TableMeta(table_name, [('PK1', 'STRING'), ('PK2', 'INTEGER')])

        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
        field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
        field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
        field_t = FieldSchema('time', FieldType.DATE, index=True, store=True, date_formats=["yyyy-MM-dd"])
        field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
        nested_field = FieldSchema('n', FieldType.NESTED, sub_field_schemas=
        [
            FieldSchema('nk', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True),
            FieldSchema('nt', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD),
            FieldSchema('ng', FieldType.GEOPOINT, index=True, store=True, enable_sort_and_agg=True)
        ])
        # search index 1: simple schema
        fields = [field_a, field_b, field_c, field_d, field_t]
        index_name_1 = 'search_index_1'
        index_meta_1 = SearchIndexMeta(fields, index_setting=None, index_sort=None)
        self.client_test.create_search_index(table_name, index_name_1, index_meta_1)

        # search index 2: with sort
        index_sort = Sort([PrimaryKeySort(sort_order=SortOrder.DESC)])
        index_meta_2 = SearchIndexMeta(fields, index_setting=None, index_sort=index_sort)
        index_name_2 = 'search_index_2'
        self.client_test.create_search_index(table_name, index_name_2, index_meta_2)

        # search index 3: with nested
        fields = [field_a, field_b, field_c, field_d, nested_field]
        index_name_3 = 'search_index_3'
        index_meta_3 = SearchIndexMeta(fields, index_setting=None, index_sort=None)
        self.client_test.create_search_index(table_name, index_name_3, index_meta_3)

        # search index 4: with routing keys
        index_setting = IndexSetting(routing_fields=['PK2'])
        fields = [field_a, field_b, field_c, field_d]
        index_name_4 = 'search_index_4'
        index_meta_4 = SearchIndexMeta(fields, index_setting=index_setting, index_sort=None)
        self.client_test.create_search_index(table_name, index_name_4, index_meta_4)

        search_indexes = self.client_test.list_search_index(table_name)
        self.assert_equal(4, len(search_indexes))
        search_indexes.sort()
        self.assert_equal((table_name, index_name_1), search_indexes[0])
        self.assert_equal((table_name, index_name_2), search_indexes[1])
        self.assert_equal((table_name, index_name_3), search_indexes[2])
        self.assert_equal((table_name, index_name_4), search_indexes[3])

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name_1)
        self._check_index_meta(index_meta_1, index_meta)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name_2)
        self._check_index_meta(index_meta_2, index_meta)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name_3)
        self._check_index_meta(index_meta_3, index_meta, with_nested=True)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name_4)
        self._check_index_meta(index_meta_4, index_meta)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

    def _test_match_all_query(self, table_name, index_name):
        # simple queries: match all query and scan to get all data with next token
        query = MatchAllQuery()
        all_rows = []
        next_token = None

        while not all_rows or next_token:
            search_response = self.client_test.search(table_name, index_name,
                                                      SearchQuery(query, next_token=next_token, limit=100,
                                                                  get_total_count=True),
                                                      columns_to_get=ColumnsToGet(['k', 't', 'g', 'ka', 'la'],
                                                                                  ColumnReturnType.SPECIFIED))
            if search_response.next_token:
                self.assert_equal(len(search_response.rows), 100)
            self.assert_equal(search_response.total_count, 1000)
            self.assertTrue(search_response.is_all_succeed)
            all_rows.extend(search_response.rows)

        self.assert_equal(len(all_rows), 100)
        for i in range(len(all_rows)):
            row = all_rows[i]
            pk = row[0]
            cols = row[1]
            self.assert_equal('pk_' + str(i % 10), pk[1][1])
            self.assert_equal(i, pk[0][1])
            self.assert_equal(len(cols), 5)
            self.assert_equal(cols[1][:2], ('k', 'key%03d' % i))
            self.assert_equal(cols[2][:2], ('ka', '["a", "b", "%d"]' % i))
            self.assert_equal(cols[3][:2], ('la', '[-1, %d]' % i))
            self.assert_equal(cols[4][:2], ('t', 'this is %d' % i))

    def _test_match_query(self, table_name, index_name):
        query = MatchQuery('t', 'this')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchQuery('json_object.jot', 'this')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchQuery('t', 'not')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

        query = MatchQuery('t', 'this is 0')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchQuery('t', 'this is 0', minimum_should_match=3)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchQuery('t', 'this is 0', minimum_should_match="3")
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchQuery('t', 'this is 0', minimum_should_match="50%")
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchQuery('t', 'this is 0', minimum_should_match=3, operator=QueryOperator.OR)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchQuery('t', 'this is 0', operator=QueryOperator.AND)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchQuery('t', 'is this 0', minimum_should_match=0, operator=QueryOperator.AND)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        # zero should queries
        query = MatchQuery('t', 'this is 0', minimum_should_match=1, operator=QueryOperator.AND)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

    def _test_match_phrase_query(self, table_name, index_name):
        query = MatchPhraseQuery('t', 'this is')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchPhraseQuery('t', 'this is 1')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchPhraseQuery('k', 'key999')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchPhraseQuery('json_object.jot', 'this is')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        # test slop parameter
        query = MatchPhraseQuery('t', 'this is', slop=0)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchPhraseQuery('t', 'this is', slop=1)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchPhraseQuery('t', 'this is', slop=2, weight=2.0)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

    def _test_fuzzy_query(self, table_name, index_name):
        query = MatchPhraseQuery('name', '苏')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 190)

    def _test_term_query(self, table_name, index_name):
        query = TermQuery('k', 'key000')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('t', '100')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('l', '900')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('la', '900')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('ka', 'a')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = TermQuery('b', True)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 500)

        query = TermQuery('d', 0.1)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = TermQuery('json_object.jok', 'key000')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('json_object.jot', '100')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('json_object.jol', '900')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

    def _test_range_query(self, table_name, index_name):
        query = RangeQuery('k', 'key100', 'key200', include_lower=False, include_upper=False)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 99, False, 99)

        query = RangeQuery('json_object.jok', 'key100', 'key200', include_lower=False, include_upper=False)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 99, False, 99)

        query = RangeQuery('k', 'key100', 'key200', include_lower=True, include_upper=True)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 101)

        query = RangeQuery('json_object.jok', 'key100', 'key200', include_lower=True, include_upper=True)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 101)

        query = RangeQuery('la', '100', '300', include_lower=True, include_upper=True)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 201)

        query = RangeQuery('time', '2022-05-06', '2022-05-12', include_lower=True, include_upper=True)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 96, False, 96)

    def _check_query_result(self, search_response, rows_count, has_next_token, expect_total_count):
        self.assert_equal(len(search_response.rows), rows_count, "rows_count")
        self.assert_equal(search_response.is_all_succeed, True, "is_all_succeed")
        self.assert_equal(len(search_response.next_token) > 0, has_next_token, "has_next_token")
        self.assert_equal(search_response.total_count, expect_total_count, "expect_total_count")

        self.assert_equal(len(search_response.v1_response()[0]), rows_count, "v1_response rows_count")
        self.assert_equal(search_response.v1_response()[3], True, "v1_response is_all_succeed")
        self.assert_equal(len(search_response.v1_response()[1]) > 0, has_next_token, "v1_response has_next_token")
        self.assert_equal(search_response.v1_response()[2], expect_total_count, "v1_response expect_total_count")

        pos = 0
        for item in search_response:
            if pos == 0:
                self.assert_equal(len(item), rows_count)
            elif pos == 1:
                self.assert_equal(len(item) > 0, has_next_token)
            elif pos == 2:
                self.assert_equal(item, expect_total_count)
            elif pos == 3:
                self.assert_equal(item, True)
            pos += 1

        return search_response.rows

    def _test_prefix_query(self, table_name, index_name):
        query = PrefixQuery('k', 'key')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = PrefixQuery('json_object.jok', 'key')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = PrefixQuery('k', 'key00')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 10, False, 10)

        query = PrefixQuery('json_object.jok', 'key00')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 10, False, 10)

        query = PrefixQuery('t', 'this')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = PrefixQuery('json_object.jot', 'this')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = PrefixQuery('t', 'this is')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

        query = PrefixQuery('json_object.jot', 'this is')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

    def _test_wildcard_query(self, table_name, index_name):
        query = WildcardQuery('t', 't*')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = WildcardQuery('json_object.jot', 't*')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = WildcardQuery('k', 'key00*')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 10, False, 10)

        query = WildcardQuery('json_object.jok', 'key00*')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 10, False, 10)

    def _test_terms_query(self, table_name, index_name):
        query = TermsQuery('k', ['key000', 'key100', 'key888', 'key999', 'key908', 'key1000'])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 5, False, 5)

        query = TermsQuery('json_object.jok', ['key000', 'key100', 'key888', 'key999', 'key908', 'key1000'])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 5, False, 5)

        query = TermsQuery('t', ['this', 'is'])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = TermsQuery('json_object.jot', ['this', 'is'])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = TermsQuery('l', [0, 999, 1000, 1002])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 2, False, 2)

        query = TermsQuery('json_object.jol', [0, 999, 1000, 1002])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 2, False, 2)

    def _test_bool_query(self, table_name, index_name):
        # k > 'key100' and not (150 <= l < 200) and l < 250
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', include_lower=False)
            ],
            must_not_queries=[
                RangeQuery('l', 150, 200, include_lower=True, include_upper=False)
            ],
            filter_queries=[
                RangeQuery('l', range_to=250, include_upper=False)
            ],
            should_queries=[]
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.DESC)]), limit=100,
                        get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 99, False, 99)

        values = [row[1][7][1] for row in rows]
        expect_values = []
        expect_values.extend(range(101, 150))
        expect_values.extend(range(200, 250))
        expect_values.reverse()

        self.assert_equal(values, expect_values)

        # k > 'key100' and (l > 110 and l < 200) and not (k = 'key121')
        # and should_queries(k > 'key120' or l < 300, minimum_should_match="2")
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', include_lower=False),
                BoolQuery(
                    must_queries=[
                        RangeQuery('l', range_from=110, include_lower=False),
                        RangeQuery('l', range_to=200, include_upper=False)
                    ],
                )
            ],
            must_not_queries=[
                TermQuery('k', 'key121')
            ],
            should_queries=[
                RangeQuery('k', range_from='key120', include_lower=False),
                RangeQuery('l', range_to=300, include_upper=130)
            ],
            minimum_should_match=2
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.ASC)]), limit=100,
                        get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 78, False, 78)

        values = [row[1][7][1] for row in rows]
        expect_values = []
        expect_values.extend(range(122, 200))
        self.assert_equal(values, expect_values)

        # k > 'key100' and (l > 110 and l < 200) and not (k = 'key121')
        # and should_queries(k > 'key120' or l < 300, minimum_should_match="50%")
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', include_lower=False),
                BoolQuery(
                    must_queries=[
                        RangeQuery('l', range_from=110, include_lower=False),
                        RangeQuery('l', range_to=200, include_upper=False)
                    ],
                )
            ],
            must_not_queries=[
                TermQuery('k', 'key121')
            ],
            should_queries=[
                RangeQuery('k', range_from='key120', include_lower=False),
                RangeQuery('l', range_to=300, include_upper=130)
            ],
            minimum_should_match="100%"
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.ASC)]), limit=100,
                        get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 78, False, 78)

        values = [row[1][7][1] for row in rows]
        expect_values = []
        expect_values.extend(range(122, 200))
        self.assert_equal(values, expect_values)

    def _test_geo_distance_query(self, table_name, index_name):
        query = GeoDistanceQuery('g', '32.5,116.5', 300000)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 668)

    def _test_geo_bounding_box_query(self, table_name, index_name):
        query = GeoBoundingBoxQuery('g', '30.9,112.0', '30.2,119.0')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 500)

    def _test_geo_polygon_query(self, table_name, index_name):
        query = GeoPolygonQuery('g', ['30.9,112.0', '30.5,115.0', '30.3, 117.0', '30.2,119.0'])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 90, False, 90)

    def _test_nested_query(self, table_name, index_name):
        nested_query = TermQuery('n.nk', 'key199')
        query = NestedQuery('n', nested_query)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        nested_query = RangeQuery('n.nl', range_from=100, range_to=300, include_lower=True, include_upper=True)
        query = NestedQuery('n', nested_query)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, False, 201)

        nested_query = TermQuery('json_nested.jnk', 'key199')
        query = NestedQuery('json_nested', nested_query)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        nested_query = RangeQuery('json_nested.jnl', range_from=100, range_to=300, include_lower=True,
                                  include_upper=True)
        query = NestedQuery('json_nested', nested_query)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, False, 201)

    def _test_sort(self, table_name, index_name):
        query = MatchQuery('t', 'this is')
        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(
                query, limit=100, get_total_count=True,
                sort=Sort(
                    sorters=[FieldSort('l', SortOrder.ASC)]
                )
            ),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        values = [row[1][7][1] for row in rows]
        expect_values = []
        expect_values.extend(range(0, 100))
        self.assert_equal(values, expect_values)

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(
                query, limit=100, get_total_count=True,
                sort=Sort(
                    sorters=[FieldSort('l', SortOrder.DESC)]
                )
            ),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        values = [row[1][7][1] for row in rows]
        expect_values = []
        expect_values.extend(range(900, 1000))
        expect_values.reverse()
        self.assert_equal(values, expect_values)

    def _test_search_with_weight(self, table_name, index_name):
        query_clause = WildcardQuery('k', 'key*')
        sort = Sort(sorters=[ScoreSort(sort_order=SortOrder.DESC)])
        search_query = SearchQuery(query_clause, sort, limit=2, get_total_count=True)
        columns = ColumnsToGet(return_type=ColumnReturnType.ALL)
        result = self.client_test.search(table_name, index_name, search_query, columns)
        self.assert_equal(len(result.rows), 2)
        self.assert_equal(2, len(result.search_hits))
        self.assert_equal(1.0, result.search_hits[0].score)
        self.assert_equal(1.0, result.search_hits[1].score)

        query_clause = WildcardQuery('k', 'key*', weight=0.5)
        search_query = SearchQuery(query_clause, sort, limit=2, get_total_count=True)
        result = self.client_test.search(table_name, index_name, search_query, columns)
        self.assert_equal(len(result.rows), 2)
        self.assert_equal(2, len(result.search_hits))
        self.assert_equal(0.5, result.search_hits[0].score)
        self.assert_equal(0.5, result.search_hits[1].score)

        query_clause_1 = WildcardQuery('k', 'key*', weight=0.6)
        query_clause_2 = MatchQuery('t', 'this is 1', weight=0.8)
        bool_query = BoolQuery(must_queries=[query_clause_1, query_clause_2])

        search_query = SearchQuery(bool_query, sort, limit=2, get_total_count=True)
        result = self.client_test.search(table_name, index_name, search_query, columns)
        self.assert_equal(len(result.rows), 2)
        self.assert_equal(2, len(result.search_hits))
        self.assertAlmostEqual(first=5.803, second=result.search_hits[0].score, delta=2)
        self.assertAlmostEqual(first=0.600, second=result.search_hits[1].score, delta=2)

        query_clause = BoolQuery(must_queries=[WildcardQuery('k', 'key*', weight=0.5)],weight=2.0)
        search_query = SearchQuery(query_clause, sort, limit=2, get_total_count=True)
        result = self.client_test.search(table_name, index_name, search_query, columns)
        self.assert_equal(len(result.rows), 2)
        self.assert_equal(2, len(result.search_hits))
        self.assert_equal(1.0, result.search_hits[0].score)
        self.assert_equal(1.0, result.search_hits[1].score)


    def _test_search_timeout(self, table_name, index_name):
        query_clause = WildcardQuery('k', 'ke*')
        search_query = SearchQuery(query_clause, limit=2, get_total_count=True)
        columns = ColumnsToGet(return_type=ColumnReturnType.ALL)

        try:
            result = self.client_test.search(table_name, index_name, search_query, columns, timeout_s=3)
            self.assert_equal(True, result.is_all_succeed)
        except OTSServiceError as e:
            self.assertTrue(False)

        try:
            result = self.client_test.search(table_name, index_name, search_query, columns, timeout_s=0.001)
        except OTSServiceError as e:
            self.assert_equal('OTSRequestTimeout', e.get_error_code())
            self.assert_equal('request timeout', e.get_error_message())

    def _test_exists_query(self, table_name, index_name):
        # 'key100' < k <= 'key200' and b is not null and not (150 <= l < 200)
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', range_to='key200', include_lower=False, include_upper=True),
                ExistsQuery('b')
            ],
            must_not_queries=[
                RangeQuery('l', 150, 200, include_lower=True, include_upper=False)
            ]
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.DESC)]), limit=100,
                        get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 50, False, 50)

        # 'key100' < k <= 'key200' and b is null and not (150 <= l < 200)
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', range_to='key200', include_lower=False, include_upper=True)
            ],
            must_not_queries=[
                RangeQuery('l', 150, 200, include_lower=True, include_upper=False),
                ExistsQuery('b')
            ]
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.DESC)]), limit=100,
                        get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

    def _test_knn_vector_query(self, table_name, index_name):
        knn_vector_query = KnnVectorQuery(field_name='vector', top_k=100, float32_query_vector=[1, -1, 5, -5])
        sort = Sort(sorters=[ScoreSort(sort_order=SortOrder.DESC)])

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query=knn_vector_query, get_total_count=False, limit=100, sort=sort),
            columns_to_get=ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX)), 100, True, -1)

        # min_score set to a large number: no rows returned
        knn_vector_query = KnnVectorQuery(field_name='vector', top_k=100, float32_query_vector=[1, -1, 5, -5],min_score=10)
        rows = self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query=knn_vector_query, get_total_count=False, limit=100, sort=sort),
            columns_to_get=ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX)), 0, False, -1)

    def _test_dis_max_query(self, table_name, index_name):
        dis_max_query = DisMaxQuery(queries=[
            MatchQuery('t', 'this is 1'),
            MatchQuery('t', 'this is 2'),
            MatchQuery('t', 'this is 3')
        ], tie_breaker=0.3,weight=2)
        sort = Sort(sorters=[ScoreSort(sort_order=SortOrder.DESC)])

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query=dis_max_query, get_total_count=False, limit=100, sort=sort),
            columns_to_get=ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX)), 100, True, -1)

    def test_queries(self):
        table_name = self.table_name_query_with_nested
        index_name = 'search_index'
        nested_index_name = 'search_index_nested'

        self._prepare_table(table_name)
        self._prepare_index(table_name, index_name, with_nested=False)
        self._prepare_index(table_name, nested_index_name, with_nested=True)
        self._prepare_data(table_name, 1000)
        self._wait_search_index_ready(table_name, index_name, 1000)

        self._test_match_all_query(table_name, index_name)
        self._test_match_query(table_name, index_name)
        self._test_match_phrase_query(table_name, index_name)
        self._test_fuzzy_query(table_name, index_name)
        self._test_term_query(table_name, index_name)
        self._test_range_query(table_name, index_name)
        self._test_prefix_query(table_name, index_name)
        self._test_wildcard_query(table_name, index_name)
        self._test_terms_query(table_name, index_name)
        self._test_bool_query(table_name, index_name)
        self._test_geo_distance_query(table_name, index_name)
        self._test_geo_bounding_box_query(table_name, index_name)
        self._test_geo_polygon_query(table_name, index_name)
        self._test_nested_query(table_name, nested_index_name)
        self._test_exists_query(table_name, index_name)
        self._test_knn_vector_query(table_name, index_name)
        self._test_dis_max_query(table_name, index_name)
        self._test_sort(table_name, index_name)
        self._test_search_with_weight(table_name, index_name)
        self._test_search_timeout(table_name, index_name)

    def _prepare_data(self, table_name, rows_count):
        name_list = u'马尔代夫巴厘岛苏梅岛'
        for i in range(rows_count):
            pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
            lj = int(i / 100)
            li = i % 100
            cols = [('k', 'key%03d' % i), ('t', 'this is ' + str(i)),
                    ('g', '%f,%f' % (30.0 + 0.05 * lj, 114.0 + 0.05 * li)), ('ka', '["a", "b", "%d"]' % i),
                    ('la', '[-1, %d]' % i), ('l', i),
                    ('name', name_list[int(i / 100)] + name_list[i % 10]),
                    ('b', i % 2 == 0), ('d', 0.1), ('time', '2022-05-%d' % (i % 31 + 1)),
                    ('n', json.dumps([{'nk': 'key%03d' % i, 'nl': i, 'nt': 'this is in nested ' + str(i)}])),
                    ('vector', '[%d, %d, %d, %d]' % (i + 1, i - 1, i + 5, i - 5)),
                    ('json_object',
                     json.dumps({'jok': 'key%03d' % i, 'jol': i, 'jot': 'this is in json object ' + str(i)})),
                    ('json_nested',
                     json.dumps({'jnk': 'key%03d' % i, 'jnl': i, 'jnt': 'this is in json nested ' + str(i)}))]

            self.client_test.put_row(table_name, Row(pk, cols))

    def _prepare_table(self, table_name):
        table_meta = TableMeta(table_name, [('PK1', 'INTEGER'), ('PK2', 'STRING')])

        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

    def _prepare_index(self, table_name, index_name, with_nested=False, text_similarity=None):
        field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
        field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD,
                              text_similarity=text_similarity)
        field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
        field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
        field_e = FieldSchema('la', FieldType.LONG, index=True, is_array=True, store=True)
        field_f = FieldSchema('l', FieldType.LONG, index=True, store=True)
        field_g = FieldSchema('b', FieldType.BOOLEAN, index=True, store=True)
        field_h = FieldSchema('d', FieldType.DOUBLE, index=True, store=True)
        field_t = FieldSchema('time', FieldType.DATE, index=True, store=True, date_formats=['yyyy-MM-dd'])
        field_j = FieldSchema('name', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.FUZZY,
                              analyzer_parameter=FuzzyAnalyzerParameter())
        field_v = FieldSchema('vector', FieldType.VECTOR,
                              vector_options=VectorOptions(data_type=VectorDataType.VD_FLOAT_32,
                                                           metric_type=VectorMetricType.VM_COSINE,
                                                           dimension=4))
        field_json_object = FieldSchema('json_object', FieldType.JSON, json_type=JsonType.OBJECT_JSON,
                                        sub_field_schemas=[
                                            FieldSchema('jok', FieldType.KEYWORD, index=True, store=True),
                                            FieldSchema('jol', FieldType.LONG, index=True, store=True),
                                            FieldSchema('jot', FieldType.TEXT, index=True, store=True),
                                        ])

        fields = [field_a, field_b, field_c, field_d, field_e, field_f, field_g, field_h, field_j, field_t, field_v,
                  field_json_object]

        if with_nested:
            field_n = FieldSchema('n', FieldType.NESTED, sub_field_schemas=[
                FieldSchema('nk', FieldType.KEYWORD, index=True, store=True),
                FieldSchema('nl', FieldType.LONG, index=True, store=True),
                FieldSchema('nt', FieldType.TEXT, index=True, store=True),
            ])

            field_json_nested = FieldSchema('json_nested', FieldType.JSON, json_type=JsonType.NESTED_JSON,
                                            sub_field_schemas=[
                                                FieldSchema('jnk', FieldType.KEYWORD, index=True, store=True),
                                                FieldSchema('jnl', FieldType.LONG, index=True, store=True),
                                                FieldSchema('jnt', FieldType.TEXT, index=True, store=True),
                                            ])

            fields.extend([field_n, field_json_nested])

        index_setting = IndexSetting(routing_fields=['PK1'])
        index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)]) if not with_nested else None
        index_meta = SearchIndexMeta(fields, index_setting=index_setting,
                                     index_sort=index_sort)  # default with index sort
        self.client_test.create_search_index(table_name, index_name, index_meta)

    def test_describe_search_index_include_sync_stat(self):
        table_name = self.table_name_describe_sync_stat
        index_name = 'search_index'
        self._prepare_table(table_name)
        self._prepare_index(table_name, index_name)
        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name, include_sync_stat=True)
        self.assertTrue(sync_stat is not None)

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name, include_sync_stat=False)
        self.assertTrue(sync_stat is None)

    def test_text_similarity(self):
        table_name = self.table_name_text_similarity
        index_name = 'search_index_text_similarity'
        rows_count = 100

        # create table
        table_meta = TableMeta(table_name, [('PK1', 'INTEGER'), ('PK2', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        # define fields: 3 TEXT fields with different text_similarity, plus a KEYWORD and LONG for sorting
        field_k = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
        field_l = FieldSchema('l', FieldType.LONG, index=True, enable_sort_and_agg=True, store=True)
        field_t_default = FieldSchema('t_default', FieldType.TEXT, index=True, store=True,
                                      analyzer=AnalyzerType.SINGLEWORD)
        field_t_bm25 = FieldSchema('t_bm25', FieldType.TEXT, index=True, store=True,
                                   analyzer=AnalyzerType.SINGLEWORD,
                                   text_similarity=TextSimilarity.BM25)
        field_t_short = FieldSchema('t_short', FieldType.TEXT, index=True, store=True,
                                    analyzer=AnalyzerType.SINGLEWORD,
                                    text_similarity=TextSimilarity.SHORT_TEXT)

        # create search index with all fields
        index_setting = IndexSetting(routing_fields=['PK1'])
        index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)])
        index_meta = SearchIndexMeta([field_k, field_l, field_t_default, field_t_bm25, field_t_short],
                                     index_setting=index_setting, index_sort=index_sort)
        self.client_test.create_search_index(table_name, index_name, index_meta)

        # write data: same text content to all 3 TEXT fields
        for i in range(rows_count):
            pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
            text_value = 'this is ' + str(i)
            cols = [('k', 'key%03d' % i), ('l', i),
                    ('t_default', text_value), ('t_bm25', text_value), ('t_short', text_value)]
            self.client_test.put_row(table_name, Row(pk, cols))

        self._wait_search_index_ready(table_name, index_name, rows_count)

        # 1. describe and verify field schemas
        described_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name)
        self._check_index_meta(index_meta, described_meta)

        # 2. MatchQuery: verify default and BM25 return the same results
        query_default = MatchQuery('t_default', 'this')
        result_default = self.client_test.search(
            table_name, index_name, SearchQuery(query_default, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))

        query_bm25 = MatchQuery('t_bm25', 'this')
        result_bm25 = self.client_test.search(
            table_name, index_name, SearchQuery(query_bm25, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))

        self.assert_equal(result_default.total_count, rows_count, "default total_count")
        self.assert_equal(result_bm25.total_count, rows_count, "bm25 total_count")
        self.assert_equal(result_default.total_count, result_bm25.total_count,
                          "default and bm25 total_count should be equal")

        # 3. MatchQuery with specific term: verify default and BM25 return the same count
        query_default_specific = MatchQuery('t_default', 'this is 0', operator=QueryOperator.AND)
        result_default_specific = self.client_test.search(
            table_name, index_name, SearchQuery(query_default_specific, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))

        query_bm25_specific = MatchQuery('t_bm25', 'this is 0', operator=QueryOperator.AND)
        result_bm25_specific = self.client_test.search(
            table_name, index_name, SearchQuery(query_bm25_specific, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))

        self.assert_equal(result_default_specific.total_count, 1, "default specific total_count")
        self.assert_equal(result_bm25_specific.total_count, 1, "bm25 specific total_count")
        self.assert_equal(result_default_specific.total_count, result_bm25_specific.total_count,
                          "default and bm25 specific total_count should be equal")

        # 4. TermQuery on default and BM25 fields
        query_default_term = TermQuery('t_default', '0')
        result_default_term = self.client_test.search(
            table_name, index_name, SearchQuery(query_default_term, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))

        query_bm25_term = TermQuery('t_bm25', '0')
        result_bm25_term = self.client_test.search(
            table_name, index_name, SearchQuery(query_bm25_term, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))

        self.assert_equal(result_default_term.total_count, result_bm25_term.total_count,
                          "default and bm25 term query total_count should be equal")

        # 5. SHORT_TEXT field queries
        query_short = MatchQuery('t_short', 'this')
        result_short = self.client_test.search(
            table_name, index_name, SearchQuery(query_short, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))
        self.assert_equal(result_short.total_count, rows_count, "short_text total_count")

        query_short_specific = MatchQuery('t_short', 'this is 0', operator=QueryOperator.AND)
        result_short_specific = self.client_test.search(
            table_name, index_name, SearchQuery(query_short_specific, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))
        self.assert_equal(result_short_specific.total_count, 1, "short_text specific total_count")

        # 6. MatchPhraseQuery on SHORT_TEXT field
        query_short_phrase = MatchPhraseQuery('t_short', 'this is 1')
        result_short_phrase = self.client_test.search(
            table_name, index_name, SearchQuery(query_short_phrase, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))
        self.assert_equal(result_short_phrase.total_count, 1, "short_text phrase total_count")

        # 7. MatchQuery with no match on all fields
        query_default_no_match = MatchQuery('t_default', 'nonexistent')
        result_default_no_match = self.client_test.search(
            table_name, index_name, SearchQuery(query_default_no_match, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))
        self.assert_equal(result_default_no_match.total_count, 0, "default no match total_count")

        query_bm25_no_match = MatchQuery('t_bm25', 'nonexistent')
        result_bm25_no_match = self.client_test.search(
            table_name, index_name, SearchQuery(query_bm25_no_match, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))
        self.assert_equal(result_bm25_no_match.total_count, 0, "bm25 no match total_count")

        query_short_no_match = MatchQuery('t_short', 'nonexistent')
        result_short_no_match = self.client_test.search(
            table_name, index_name, SearchQuery(query_short_no_match, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL))
        self.assert_equal(result_short_no_match.total_count, 0, "short_text no match total_count")


if __name__ == '__main__':
    unittest.main()
