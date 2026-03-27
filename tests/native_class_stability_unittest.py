# -*- coding: utf8 -*-

"""
Native API Class Stability Tests

This module tests that the structure (instance members, class attributes) of all classes
used by native C++ encoder/decoder has not been modified. If a class gains or loses a member,
these tests will fail, alerting developers that the native C++ side may need a corresponding update.

Covered APIs: GetRow, PutRow, UpdateRow, DeleteRow, BatchGetRow, BatchWriteRow, GetRange, Search, ParallelScan
"""

import unittest

# --- metadata.py: Row operation classes ---
from tablestore.metadata import (
    Row,
    Condition,
    RowExistenceExpectation,
    ReturnType,
    Direction,
    ColumnCondition,
    SingleColumnCondition,
    CompositeColumnCondition,
    LogicalOperator,
    ComparatorType,
    BatchGetRowRequest,
    TableInBatchGetRowItem,
    BatchWriteRowRequest,
    TableInBatchWriteRowItem,
    RowItem,
    PutRowItem,
    UpdateRowItem,
    DeleteRowItem,
    BatchWriteRowType,
    SearchQuery,
    ScanQuery,
    ColumnsToGet,
    ColumnReturnType,
    MatchAllQuery,
)

# --- metadata.py: Search Query classes ---
from tablestore.metadata import (
    QueryType,
    QueryOperator,
    ScoreMode,
    MatchQuery,
    MatchPhraseQuery,
    TermQuery,
    TermsQuery,
    PrefixQuery,
    RangeQuery,
    WildcardQuery,
    BoolQuery,
    NestedQuery,
    InnerHits,
    ExistsQuery,
    GeoBoundingBoxQuery,
    GeoDistanceQuery,
    GeoPolygonQuery,
    FunctionScoreQuery,
    FieldValueFactor,
    KnnVectorQuery,
    DisMaxQuery,
)

# --- metadata.py: Sort classes ---
from tablestore.metadata import (
    SortOrder,
    SortMode,
    GeoDistanceType,
    Sort,
    NestedFilter,
    Sorter,
    FieldSort,
    ScoreSort,
    PrimaryKeySort,
    DocSort,
    GeoDistanceSort,
)

# --- metadata.py: Highlight classes ---
from tablestore.metadata import (
    HighlightFragmentOrder,
    HighlightEncoder,
    HighlightParameter,
    Highlight,
)

# --- metadata.py: Collapse ---
from tablestore.metadata import Collapse

# --- aggregation.py ---
from tablestore.aggregation import (
    Agg,
    Max as AggMax,
    Min as AggMin,
    Avg,
    Sum as AggSum,
    Count,
    DistinctCount,
    Percentiles,
    TopRows,
)

# --- group_by.py ---
from tablestore.group_by import (
    BaseGroupBy,
    GroupKeySort,
    RowCountSort,
    SubAggSort,
    GroupByField,
    GroupByRange,
    GroupByFilter,
    GeoPoint,
    GroupByGeoDistance,
    FieldRange,
    GroupByHistogram,
)

# --- error.py ---
from tablestore.error import OTSError, OTSClientError, OTSServiceError

# --- timeseries_condition.py ---
from tablestore.timeseries_condition import (
    MetaQueryCompositeOperator, MetaQuerySingleOperator,
    MeasurementMetaQueryCondition, DataSourceMetaQueryCondition,
    TagMetaQueryCondition, UpdateTimeMetaQueryCondition,
    AttributeMetaQueryCondition, CompositeMetaQueryCondition,
)

# --- types.py ---
from tablestore.types import PrimaryKey, PrimaryKeyColumn, PrimaryKeyValue

# --- aggregation.py: Result classes ---
from tablestore.aggregation import AggResult, PercentilesResultItem

# --- group_by.py: Result classes ---
from tablestore.group_by import (
    GroupByResult, BaseGroupByResultItem, GroupByFieldResultItem,
    GroupByRangeResultItem, GroupByFilterResultItem,
    GroupByGeoDistanceResultItem, GroupByHistogramResultItem,
)

# --- metadata.py: Additional classes ---
from tablestore.metadata import (
    TableMeta, TableOptions, SSEKeyType, SSESpecification, SSEDetails,
    CapacityUnit, ReservedThroughput, ReservedThroughputDetails,
    FieldType, AnalyzerType, SingleWordAnalyzerParameter, SplitAnalyzerParameter,
    FuzzyAnalyzerParameter, Sorter, IndexSetting,
    VectorDataType, VectorMetricType, VectorOptions,
    JsonType, TextSimilarity,
    FieldSchema, SyncPhase, SyncStat, SearchIndexMeta,
    DefinedColumnSchema, SecondaryIndexType, SyncType, SecondaryIndexMeta,
    ColumnType, Column, UpdateType,
    CommonResponse, UpdateTableResponse, DescribeTableResponse,
    RowDataItem, CastType, ColumnConditionType, ColumnCondition,
    RegexRule, SingleColumnRegexCondition,
    BatchGetRowResponse, BatchWriteRowResponse, BatchWriteRowResponseItem,
    INF_MIN, INF_MAX, PK_AUTO_INCR, Query,
    IterableResponse, SearchResponse, ComputeSplitsResponse, ParallelScanResponse,
    SearchHit, SearchInnerHit, HighlightResult, HighlightField,
    TimeseriesKey, TimeseriesRow, TimeseriesTableOptions, TimeseriesMetaOptions,
    TimeseriesTableMeta, TimeseriesAnalyticalStore, LastpointIndexMeta,
    CreateTimeseriesTableRequest, DescribeTimeseriesTableResponse,
    UpdateTimeseriesMetaRequest, DeleteTimeseriesMetaRequest,
    TimeseriesMeta, Error, FailedRowResult,
    UpdateTimeseriesMetaResponse, PutTimeseriesDataResponse, DeleteTimeseriesMetaResponse,
    QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse,
    GetTimeseriesDataRequest, GetTimeseriesDataResponse,
)

# --- encoder.py / decoder.py: Encoder and Decoder classes ---
from tablestore.encoder import OTSProtoBufferEncoder
from tablestore.decoder import OTSProtoBufferDecoder


def _get_class_attributes(cls):
    """Get non-dunder, non-callable class attributes (public data attributes only)."""
    attrs = set()
    for name, value in vars(cls).items():
        if name.startswith('__') and name.endswith('__'):
            if name in ('__dict__', '__weakref__', '__doc__', '__module__', '__qualname__'):
                continue
            if callable(value) and not isinstance(value, (list, tuple, set, dict)):
                continue
        elif callable(value) and not isinstance(value, (staticmethod, classmethod)):
            if not isinstance(value, property):
                continue
        attrs.add(name)
    return attrs


def _get_instance_members(instance):
    """Get the set of instance member names from __dict__."""
    return set(instance.__dict__.keys())


def _assert_structure(test_case, expected, actual, class_name, check_type="instance members"):
    """Helper to assert structure equality with descriptive error message."""
    test_case.assertEqual(expected, actual,
                          f"{class_name} {check_type} changed! Expected {expected}, got {actual}. "
                          f"Added: {actual - expected}, Removed: {expected - actual}")

def _get_method_params(cls, method_name):
    """Get parameter names of a method, excluding 'self'."""
    import inspect
    method = getattr(cls, method_name)
    sig = inspect.signature(method)
    return tuple(p for p in sig.parameters.keys() if p != 'self')

def _get_public_methods(cls):
    """Get set of public method names (not starting with _)."""
    import inspect
    return {name for name, _ in inspect.getmembers(cls, predicate=inspect.isfunction)
            if not name.startswith('_')}


# =============================================================================
# Existing tests (22 tests) - Row operations, Batch, Search direct classes
# =============================================================================

class TestRowOperationClassStructure(unittest.TestCase):
    """Test structure stability of classes used by GetRow/PutRow/UpdateRow/DeleteRow/GetRange APIs."""

    def test_row_structure(self):
        row = Row(primary_key=[('pk', 'val')], attribute_columns=[('col', 'val')])
        expected = {'primary_key', 'attribute_columns'}
        _assert_structure(self, expected, _get_instance_members(row), 'Row')

    def test_condition_structure(self):
        cond = Condition(RowExistenceExpectation.IGNORE)
        expected = {'row_existence_expectation', 'column_condition'}
        _assert_structure(self, expected, _get_instance_members(cond), 'Condition')

    def test_row_existence_expectation_structure(self):
        expected = {'IGNORE', 'EXPECT_EXIST', 'EXPECT_NOT_EXIST', '__values__', '__members__'}
        _assert_structure(self, expected, _get_class_attributes(RowExistenceExpectation),
                          'RowExistenceExpectation', 'class attributes')

    def test_return_type_structure(self):
        expected = {'RT_NONE', 'RT_PK'}
        _assert_structure(self, expected, _get_class_attributes(ReturnType),
                          'ReturnType', 'class attributes')

    def test_direction_structure(self):
        expected = {'FORWARD', 'BACKWARD'}
        _assert_structure(self, expected, _get_class_attributes(Direction),
                          'Direction', 'class attributes')


class TestColumnConditionClassStructure(unittest.TestCase):
    """Test structure stability of ColumnCondition-related classes."""

    def test_single_column_condition_structure(self):
        cond = SingleColumnCondition('col', 'val', ComparatorType.EQUAL)
        expected = {'column_name', 'column_value', 'comparator', 'pass_if_missing', 'latest_version_only'}
        _assert_structure(self, expected, _get_instance_members(cond), 'SingleColumnCondition')

    def test_composite_column_condition_structure(self):
        cond = CompositeColumnCondition(LogicalOperator.AND)
        expected = {'sub_conditions', 'combinator'}
        _assert_structure(self, expected, _get_instance_members(cond), 'CompositeColumnCondition')

    def test_logical_operator_structure(self):
        expected = {'NOT', 'AND', 'OR', '__values__', '__members__'}
        _assert_structure(self, expected, _get_class_attributes(LogicalOperator),
                          'LogicalOperator', 'class attributes')

    def test_comparator_type_structure(self):
        expected = {
            'EQUAL', 'NOT_EQUAL', 'GREATER_THAN', 'GREATER_EQUAL',
            'LESS_THAN', 'LESS_EQUAL', 'EXIST', 'NOT_EXIST',
            '__values__', '__single_condition_values__',
            '__members__', '__single_condition_members__',
        }
        _assert_structure(self, expected, _get_class_attributes(ComparatorType),
                          'ComparatorType', 'class attributes')


class TestBatchGetRowClassStructure(unittest.TestCase):
    """Test structure stability of BatchGetRow-related classes."""

    def test_batch_get_row_request_structure(self):
        req = BatchGetRowRequest()
        expected = {'items'}
        _assert_structure(self, expected, _get_instance_members(req), 'BatchGetRowRequest')

    def test_table_in_batch_get_row_item_structure(self):
        item = TableInBatchGetRowItem('table', [('pk', 'val')])
        expected = {
            'table_name', 'primary_keys', 'columns_to_get', 'column_filter',
            'max_version', 'time_range', 'start_column', 'end_column', 'token',
        }
        _assert_structure(self, expected, _get_instance_members(item), 'TableInBatchGetRowItem')


class TestBatchWriteRowClassStructure(unittest.TestCase):
    """Test structure stability of BatchWriteRow-related classes."""

    def test_batch_write_row_request_structure(self):
        req = BatchWriteRowRequest()
        expected = {'items', 'transaction_id'}
        _assert_structure(self, expected, _get_instance_members(req), 'BatchWriteRowRequest')

    def test_table_in_batch_write_row_item_structure(self):
        item = TableInBatchWriteRowItem('table', [])
        expected = {'table_name', 'row_items'}
        _assert_structure(self, expected, _get_instance_members(item), 'TableInBatchWriteRowItem')

    def test_row_item_structure(self):
        row = Row([('pk', 'val')])
        cond = Condition(RowExistenceExpectation.IGNORE)
        item = RowItem('put', row, cond)
        expected = {'type', 'condition', 'row', 'return_type'}
        _assert_structure(self, expected, _get_instance_members(item), 'RowItem')

    def test_put_row_item_structure(self):
        row = Row([('pk', 'val')])
        cond = Condition(RowExistenceExpectation.IGNORE)
        item = PutRowItem(row, cond)
        expected = {'type', 'condition', 'row', 'return_type'}
        _assert_structure(self, expected, _get_instance_members(item), 'PutRowItem')

    def test_update_row_item_structure(self):
        row = Row([('pk', 'val')])
        cond = Condition(RowExistenceExpectation.IGNORE)
        item = UpdateRowItem(row, cond)
        expected = {'type', 'condition', 'row', 'return_type'}
        _assert_structure(self, expected, _get_instance_members(item), 'UpdateRowItem')

    def test_delete_row_item_structure(self):
        row = Row([('pk', 'val')])
        cond = Condition(RowExistenceExpectation.IGNORE)
        item = DeleteRowItem(row, cond)
        expected = {'type', 'condition', 'row', 'return_type'}
        _assert_structure(self, expected, _get_instance_members(item), 'DeleteRowItem')

    def test_batch_write_row_type_structure(self):
        expected = {'PUT', 'UPDATE', 'DELETE'}
        _assert_structure(self, expected, _get_class_attributes(BatchWriteRowType),
                          'BatchWriteRowType', 'class attributes')


class TestSearchParallelScanClassStructure(unittest.TestCase):
    """Test structure stability of Search/ParallelScan direct classes."""

    def test_search_query_structure(self):
        query = SearchQuery(MatchAllQuery())
        expected = {
            'query', 'sort', 'get_total_count', 'next_token',
            'offset', 'limit', 'aggs', 'group_bys', 'collapse', 'highlight',
        }
        _assert_structure(self, expected, _get_instance_members(query), 'SearchQuery')

    def test_scan_query_structure(self):
        query = ScanQuery(MatchAllQuery(), limit=10, next_token=None,
                          current_parallel_id=0, max_parallel=1)
        expected = {'query', 'limit', 'next_token', 'current_parallel_id', 'max_parallel', 'alive_time'}
        _assert_structure(self, expected, _get_instance_members(query), 'ScanQuery')

    def test_columns_to_get_structure(self):
        ctg = ColumnsToGet()
        expected = {'column_names', 'return_type'}
        _assert_structure(self, expected, _get_instance_members(ctg), 'ColumnsToGet')

    def test_column_return_type_structure(self):
        expected = {'ALL', 'SPECIFIED', 'NONE', 'ALL_FROM_INDEX'}
        actual = set(ColumnReturnType.__members__.keys())
        _assert_structure(self, expected, actual, 'ColumnReturnType', 'enum members')


# =============================================================================
# New tests - Search Query classes (22 tests)
# =============================================================================

class TestSearchQueryClassStructure(unittest.TestCase):
    """Test structure stability of all Search Query classes (indirect dependencies of SearchQuery.query)."""

    def test_query_type_structure(self):
        expected = {
            'MATCH_QUERY', 'MATCH_PHRASE_QUERY', 'TERM_QUERY', 'RANGE_QUERY',
            'PREFIX_QUERY', 'BOOL_QUERY', 'CONST_SCORE_QUERY', 'FUNCTION_SCORE_QUERY',
            'NESTED_QUERY', 'WILDCARD_QUERY', 'MATCH_ALL_QUERY',
            'GEO_BOUNDING_BOX_QUERY', 'GEO_DISTANCE_QUERY', 'GEO_POLYGON_QUERY',
            'TERMS_QUERY', 'KNN_VECTOR_QUERY', 'DIS_MAX_QUERY',
        }
        actual = set(QueryType.__members__.keys())
        _assert_structure(self, expected, actual, 'QueryType', 'enum members')

    def test_query_operator_structure(self):
        expected = {'OR', 'AND'}
        actual = set(QueryOperator.__members__.keys())
        _assert_structure(self, expected, actual, 'QueryOperator', 'enum members')

    def test_score_mode_structure(self):
        expected = {'NONE', 'AVG', 'MAX', 'TOTAL', 'MIN'}
        actual = set(ScoreMode.__members__.keys())
        _assert_structure(self, expected, actual, 'ScoreMode', 'enum members')

    def test_match_all_query_structure(self):
        q = MatchAllQuery()
        expected = set()
        _assert_structure(self, expected, _get_instance_members(q), 'MatchAllQuery')

    def test_match_query_structure(self):
        q = MatchQuery('field', 'text')
        expected = {'field_name', 'text', 'minimum_should_match', 'operator', 'weight'}
        _assert_structure(self, expected, _get_instance_members(q), 'MatchQuery')

    def test_match_phrase_query_structure(self):
        q = MatchPhraseQuery('field', 'text')
        expected = {'field_name', 'text', 'weight', 'slop'}
        _assert_structure(self, expected, _get_instance_members(q), 'MatchPhraseQuery')

    def test_term_query_structure(self):
        q = TermQuery('field', 'value')
        expected = {'field_name', 'column_value', 'weight'}
        _assert_structure(self, expected, _get_instance_members(q), 'TermQuery')

    def test_terms_query_structure(self):
        q = TermsQuery('field', ['v1', 'v2'])
        expected = {'field_name', 'column_values', 'weight'}
        _assert_structure(self, expected, _get_instance_members(q), 'TermsQuery')

    def test_prefix_query_structure(self):
        q = PrefixQuery('field', 'prefix')
        expected = {'field_name', 'prefix', 'weight'}
        _assert_structure(self, expected, _get_instance_members(q), 'PrefixQuery')

    def test_range_query_structure(self):
        q = RangeQuery('field')
        expected = {'field_name', 'range_from', 'range_to', 'include_lower', 'include_upper'}
        _assert_structure(self, expected, _get_instance_members(q), 'RangeQuery')

    def test_wildcard_query_structure(self):
        q = WildcardQuery('field', 'val*')
        expected = {'field_name', 'value', 'weight'}
        _assert_structure(self, expected, _get_instance_members(q), 'WildcardQuery')

    def test_bool_query_structure(self):
        q = BoolQuery()
        expected = {'must_queries', 'must_not_queries', 'filter_queries', 'should_queries',
                    'minimum_should_match', 'weight'}
        _assert_structure(self, expected, _get_instance_members(q), 'BoolQuery')

    def test_nested_query_structure(self):
        q = NestedQuery('path', MatchAllQuery())
        expected = {'path', 'query', 'score_mode', 'inner_hits', 'weight'}
        _assert_structure(self, expected, _get_instance_members(q), 'NestedQuery')

    def test_inner_hits_structure(self):
        ih = InnerHits(sort=None, offset=0, limit=10, highlight=None)
        expected = {'sort', 'offset', 'limit', 'highlight'}
        _assert_structure(self, expected, _get_instance_members(ih), 'InnerHits')

    def test_exists_query_structure(self):
        q = ExistsQuery('field')
        expected = {'field_name'}
        _assert_structure(self, expected, _get_instance_members(q), 'ExistsQuery')

    def test_geo_bounding_box_query_structure(self):
        q = GeoBoundingBoxQuery('field', '0,0', '1,1')
        expected = {'field_name', 'top_left', 'bottom_right'}
        _assert_structure(self, expected, _get_instance_members(q), 'GeoBoundingBoxQuery')

    def test_geo_distance_query_structure(self):
        q = GeoDistanceQuery('field', '0,0', 100)
        expected = {'field_name', 'center_point', 'distance'}
        _assert_structure(self, expected, _get_instance_members(q), 'GeoDistanceQuery')

    def test_geo_polygon_query_structure(self):
        q = GeoPolygonQuery('field', ['0,0', '1,1', '2,2'])
        expected = {'field_name', 'points'}
        _assert_structure(self, expected, _get_instance_members(q), 'GeoPolygonQuery')

    def test_function_score_query_structure(self):
        q = FunctionScoreQuery(MatchAllQuery(), FieldValueFactor('field'))
        expected = {'query', 'field_value_factor'}
        _assert_structure(self, expected, _get_instance_members(q), 'FunctionScoreQuery')

    def test_field_value_factor_structure(self):
        f = FieldValueFactor('field')
        expected = {'field_name'}
        _assert_structure(self, expected, _get_instance_members(f), 'FieldValueFactor')

    def test_knn_vector_query_structure(self):
        q = KnnVectorQuery('field')
        expected = {'field_name', 'top_k', 'float32_query_vector', 'filter', 'weight',
                    'min_score', 'num_candidates'}
        _assert_structure(self, expected, _get_instance_members(q), 'KnnVectorQuery')

    def test_dis_max_query_structure(self):
        q = DisMaxQuery()
        expected = {'queries', 'tie_breaker', 'weight'}
        _assert_structure(self, expected, _get_instance_members(q), 'DisMaxQuery')


# =============================================================================
# New tests - Sort classes (10 tests)
# =============================================================================

class TestSortClassStructure(unittest.TestCase):
    """Test structure stability of Sort-related classes (indirect dependencies of SearchQuery.sort)."""

    def test_sort_order_structure(self):
        expected = {'ASC', 'DESC'}
        actual = set(SortOrder.__members__.keys())
        _assert_structure(self, expected, actual, 'SortOrder', 'enum members')

    def test_sort_mode_structure(self):
        expected = {'MIN', 'MAX', 'AVG'}
        actual = set(SortMode.__members__.keys())
        _assert_structure(self, expected, actual, 'SortMode', 'enum members')

    def test_geo_distance_type_structure(self):
        expected = {'ARC', 'PLANE'}
        actual = set(GeoDistanceType.__members__.keys())
        _assert_structure(self, expected, actual, 'GeoDistanceType', 'enum members')

    def test_sort_structure(self):
        s = Sort(sorters=[])
        expected = {'sorters'}
        _assert_structure(self, expected, _get_instance_members(s), 'Sort')

    def test_nested_filter_structure(self):
        nf = NestedFilter('path', MatchAllQuery())
        expected = {'path', 'query_filter'}
        _assert_structure(self, expected, _get_instance_members(nf), 'NestedFilter')

    def test_field_sort_structure(self):
        fs = FieldSort('field')
        expected = {'field_name', 'sort_order', 'sort_mode', 'nested_filter'}
        _assert_structure(self, expected, _get_instance_members(fs), 'FieldSort')

    def test_score_sort_structure(self):
        ss = ScoreSort()
        expected = {'sort_order'}
        _assert_structure(self, expected, _get_instance_members(ss), 'ScoreSort')

    def test_primary_key_sort_structure(self):
        pks = PrimaryKeySort()
        expected = {'sort_order'}
        _assert_structure(self, expected, _get_instance_members(pks), 'PrimaryKeySort')

    def test_doc_sort_structure(self):
        ds = DocSort()
        expected = {'sort_order'}
        _assert_structure(self, expected, _get_instance_members(ds), 'DocSort')

    def test_geo_distance_sort_structure(self):
        gds = GeoDistanceSort('field', ['0,0'])
        expected = {'field_name', 'points', 'geo_distance_type', 'sort_order', 'sort_mode', 'nested_filter'}
        _assert_structure(self, expected, _get_instance_members(gds), 'GeoDistanceSort')


# =============================================================================
# New tests - Highlight classes (4 tests)
# =============================================================================

class TestHighlightClassStructure(unittest.TestCase):
    """Test structure stability of Highlight-related classes."""

    def test_highlight_fragment_order_structure(self):
        expected = {'TEXT_SEQUENCE', 'SCORE'}
        actual = set(HighlightFragmentOrder.__members__.keys())
        _assert_structure(self, expected, actual, 'HighlightFragmentOrder', 'enum members')

    def test_highlight_encoder_structure(self):
        expected = {'PLAIN_MODE', 'HTML_MODE'}
        actual = set(HighlightEncoder.__members__.keys())
        _assert_structure(self, expected, actual, 'HighlightEncoder', 'enum members')

    def test_highlight_parameter_structure(self):
        hp = HighlightParameter('field')
        expected = {'field_name', 'number_of_fragments', 'fragment_size', 'pre_tag', 'post_tag', 'fragments_order'}
        _assert_structure(self, expected, _get_instance_members(hp), 'HighlightParameter')

    def test_highlight_structure(self):
        h = Highlight(highlight_parameters=[])
        expected = {'highlight_parameters', 'highlight_encoder'}
        _assert_structure(self, expected, _get_instance_members(h), 'Highlight')


# =============================================================================
# New tests - Collapse class (1 test)
# =============================================================================

class TestCollapseClassStructure(unittest.TestCase):
    """Test structure stability of Collapse class."""

    def test_collapse_structure(self):
        c = Collapse('field')
        expected = {'field_name'}
        _assert_structure(self, expected, _get_instance_members(c), 'Collapse')


# =============================================================================
# New tests - Aggregation classes (9 tests)
# =============================================================================

class TestAggregationClassStructure(unittest.TestCase):
    """Test structure stability of Aggregation classes (indirect dependencies of SearchQuery.aggs)."""

    def test_agg_base_structure(self):
        a = Agg('field', None, 'test', 'agg_type')
        expected = {'field', 'missing', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(a), 'Agg')

    def test_max_structure(self):
        a = AggMax('field')
        expected = {'field', 'missing', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(a), 'Max')

    def test_min_structure(self):
        a = AggMin('field')
        expected = {'field', 'missing', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(a), 'Min')

    def test_avg_structure(self):
        a = Avg('field')
        expected = {'field', 'missing', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(a), 'Avg')

    def test_sum_structure(self):
        a = AggSum('field')
        expected = {'field', 'missing', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(a), 'Sum')

    def test_count_structure(self):
        a = Count('field')
        expected = {'field', 'missing', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(a), 'Count')

    def test_distinct_count_structure(self):
        a = DistinctCount('field')
        expected = {'field', 'missing', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(a), 'DistinctCount')

    def test_percentiles_structure(self):
        a = Percentiles('field', [50, 90, 99])
        expected = {'field', 'missing', 'name', 'type', 'percentiles_list'}
        _assert_structure(self, expected, _get_instance_members(a), 'Percentiles')

    def test_top_rows_structure(self):
        a = TopRows(limit=10, sort=None)
        expected = {'limit', 'sort', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(a), 'TopRows')


# =============================================================================
# New tests - GroupBy classes (11 tests)
# =============================================================================

class TestGroupByClassStructure(unittest.TestCase):
    """Test structure stability of GroupBy classes (indirect dependencies of SearchQuery.group_bys)."""

    def test_base_group_by_structure(self):
        g = BaseGroupBy('field', [], [], 'test', 'type')
        expected = {'field_name', 'sub_aggs', 'sub_group_bys', 'name', 'type'}
        _assert_structure(self, expected, _get_instance_members(g), 'BaseGroupBy')

    def test_group_key_sort_structure(self):
        s = GroupKeySort(SortOrder.ASC)
        expected = {'sort_order'}
        _assert_structure(self, expected, _get_instance_members(s), 'GroupKeySort')

    def test_row_count_sort_structure(self):
        s = RowCountSort(SortOrder.ASC)
        expected = {'sort_order'}
        _assert_structure(self, expected, _get_instance_members(s), 'RowCountSort')

    def test_sub_agg_sort_structure(self):
        s = SubAggSort(SortOrder.ASC, 'agg_name')
        expected = {'sort_order', 'sub_agg_name'}
        _assert_structure(self, expected, _get_instance_members(s), 'SubAggSort')

    def test_group_by_field_structure(self):
        g = GroupByField('field')
        expected = {'field_name', 'sub_aggs', 'sub_group_bys', 'name', 'type', 'size', 'group_by_sort'}
        _assert_structure(self, expected, _get_instance_members(g), 'GroupByField')

    def test_group_by_range_structure(self):
        g = GroupByRange('field', ranges=[(0, 10)])
        expected = {'field_name', 'sub_aggs', 'sub_group_bys', 'name', 'type', 'ranges'}
        _assert_structure(self, expected, _get_instance_members(g), 'GroupByRange')

    def test_group_by_filter_structure(self):
        g = GroupByFilter(filters=[MatchAllQuery()])
        expected = {'field_name', 'sub_aggs', 'sub_group_bys', 'name', 'type', 'filters'}
        _assert_structure(self, expected, _get_instance_members(g), 'GroupByFilter')

    def test_geo_point_structure(self):
        p = GeoPoint(lat=0.0, lon=0.0)
        expected = {'lat', 'lon'}
        _assert_structure(self, expected, _get_instance_members(p), 'GeoPoint')

    def test_group_by_geo_distance_structure(self):
        g = GroupByGeoDistance('field', origin=GeoPoint(0, 0), ranges=[(0, 100)])
        expected = {'field_name', 'sub_aggs', 'sub_group_bys', 'name', 'type', 'origin', 'ranges'}
        _assert_structure(self, expected, _get_instance_members(g), 'GroupByGeoDistance')

    def test_field_range_structure(self):
        r = FieldRange(min=0, max=100)
        expected = {'min', 'max'}
        _assert_structure(self, expected, _get_instance_members(r), 'FieldRange')

    def test_group_by_histogram_structure(self):
        g = GroupByHistogram('field', interval=10, field_range=FieldRange(0, 100))
        expected = {'field_name', 'sub_aggs', 'sub_group_bys', 'name', 'type',
                    'interval', 'field_range', 'missing_value', 'min_doc_count', 'group_by_sort'}
        _assert_structure(self, expected, _get_instance_members(g), 'GroupByHistogram')


# =============================================================================
# New tests - Error classes (3 tests)
# =============================================================================

class TestErrorClassStructure(unittest.TestCase):
    """Test structure stability of Error classes from tablestore/error.py."""

    def test_ots_error_structure(self):
        e = OTSError()
        expected = set()
        _assert_structure(self, expected, _get_instance_members(e), 'OTSError')

    def test_ots_client_error_structure(self):
        e = OTSClientError('test message', http_status=400)
        expected = {'message', 'http_status'}
        _assert_structure(self, expected, _get_instance_members(e), 'OTSClientError')

    def test_ots_service_error_structure(self):
        e = OTSServiceError(500, 'OTSInternalError', 'Internal error', 'req-001')
        expected = {'http_status', 'code', 'message', 'request_id'}
        _assert_structure(self, expected, _get_instance_members(e), 'OTSServiceError')

# =============================================================================
# New tests - Timeseries Condition classes (8 tests)
# =============================================================================

class TestTimeseriesConditionClassStructure(unittest.TestCase):
    """Test structure stability of Timeseries Condition classes from tablestore/timeseries_condition.py."""

    def test_meta_query_composite_operator_structure(self):
        expected = {'OP_AND', 'OP_OR', 'OP_NOT'}
        actual = set(MetaQueryCompositeOperator.__members__.keys())
        _assert_structure(self, expected, actual, 'MetaQueryCompositeOperator', 'enum members')

    def test_meta_query_single_operator_structure(self):
        expected = {'OP_EQUAL', 'OP_NOT_EQUAL', 'OP_GREATER_THAN', 'OP_GREATER_EQUAL',
                    'OP_LESS_THAN', 'OP_LESS_EQUAL', 'OP_PREFIX'}
        actual = set(MetaQuerySingleOperator.__members__.keys())
        _assert_structure(self, expected, actual, 'MetaQuerySingleOperator', 'enum members')

    def test_measurement_meta_query_condition_structure(self):
        c = MeasurementMetaQueryCondition(MetaQuerySingleOperator.OP_EQUAL, 'cpu')
        expected = {'operator', 'value'}
        _assert_structure(self, expected, _get_instance_members(c), 'MeasurementMetaQueryCondition')

    def test_data_source_meta_query_condition_structure(self):
        c = DataSourceMetaQueryCondition(MetaQuerySingleOperator.OP_EQUAL, 'source1')
        expected = {'operator', 'value'}
        _assert_structure(self, expected, _get_instance_members(c), 'DataSourceMetaQueryCondition')

    def test_tag_meta_query_condition_structure(self):
        c = TagMetaQueryCondition(MetaQuerySingleOperator.OP_EQUAL, 'host', 'server1')
        expected = {'operator', 'tag_name', 'value'}
        _assert_structure(self, expected, _get_instance_members(c), 'TagMetaQueryCondition')

    def test_update_time_meta_query_condition_structure(self):
        c = UpdateTimeMetaQueryCondition(MetaQuerySingleOperator.OP_GREATER_THAN, 1000000)
        expected = {'operator', 'time_in_us'}
        _assert_structure(self, expected, _get_instance_members(c), 'UpdateTimeMetaQueryCondition')

    def test_attribute_meta_query_condition_structure(self):
        c = AttributeMetaQueryCondition(MetaQuerySingleOperator.OP_EQUAL, 'attr1', 'val1')
        expected = {'operator', 'attribute_name', 'value'}
        _assert_structure(self, expected, _get_instance_members(c), 'AttributeMetaQueryCondition')

    def test_composite_meta_query_condition_structure(self):
        c = CompositeMetaQueryCondition(MetaQueryCompositeOperator.OP_AND, [])
        expected = {'operator', 'subConditions'}
        _assert_structure(self, expected, _get_instance_members(c), 'CompositeMetaQueryCondition')

# =============================================================================
# New tests - Types classes (3 tests)
# =============================================================================

class TestTypesClassStructure(unittest.TestCase):
    """Test structure stability of Types classes from tablestore/types.py."""

    def test_primary_key_structure(self):
        pk = PrimaryKey()
        expected = {'pks'}
        _assert_structure(self, expected, _get_instance_members(pk), 'PrimaryKey')

    def test_primary_key_column_structure(self):
        pkc = PrimaryKeyColumn('col_name', 'col_value')
        expected = {'name', 'value'}
        _assert_structure(self, expected, _get_instance_members(pkc), 'PrimaryKeyColumn')

    def test_primary_key_value_structure(self):
        pkv = PrimaryKeyValue('STRING', 'test_value')
        expected = {'type', 'value'}
        _assert_structure(self, expected, _get_instance_members(pkv), 'PrimaryKeyValue')

# =============================================================================
# New tests - Aggregation Result classes (2 tests)
# =============================================================================

class TestAggregationResultClassStructure(unittest.TestCase):
    """Test structure stability of Aggregation Result classes from tablestore/aggregation.py."""

    def test_agg_result_structure(self):
        r = AggResult('max_agg', 100.0)
        expected = {'name', 'value'}
        _assert_structure(self, expected, _get_instance_members(r), 'AggResult')

    def test_percentiles_result_item_structure(self):
        r = PercentilesResultItem(50.0, 99.5)
        expected = {'key', 'value'}
        _assert_structure(self, expected, _get_instance_members(r), 'PercentilesResultItem')

# =============================================================================
# New tests - GroupBy Result classes (7 tests)
# =============================================================================

class TestGroupByResultClassStructure(unittest.TestCase):
    """Test structure stability of GroupBy Result classes from tablestore/group_by.py."""

    def test_group_by_result_structure(self):
        r = GroupByResult('gb_field', [])
        expected = {'name', 'items'}
        _assert_structure(self, expected, _get_instance_members(r), 'GroupByResult')

    def test_base_group_by_result_item_structure(self):
        r = BaseGroupByResultItem([], [])
        expected = {'sub_aggs', 'sub_group_bys'}
        _assert_structure(self, expected, _get_instance_members(r), 'BaseGroupByResultItem')

    def test_group_by_field_result_item_structure(self):
        r = GroupByFieldResultItem('key1', 10, [], [])
        expected = {'key', 'row_count', 'sub_aggs', 'sub_group_bys'}
        _assert_structure(self, expected, _get_instance_members(r), 'GroupByFieldResultItem')

    def test_group_by_range_result_item_structure(self):
        r = GroupByRangeResultItem(0, 100, 5, [], [])
        expected = {'range_from', 'range_to', 'row_count', 'sub_aggs', 'sub_group_bys'}
        _assert_structure(self, expected, _get_instance_members(r), 'GroupByRangeResultItem')

    def test_group_by_filter_result_item_structure(self):
        r = GroupByFilterResultItem(8, [], [])
        expected = {'row_count', 'sub_aggs', 'sub_group_bys'}
        _assert_structure(self, expected, _get_instance_members(r), 'GroupByFilterResultItem')

    def test_group_by_geo_distance_result_item_structure(self):
        r = GroupByGeoDistanceResultItem(0, 1000, 3, [], [])
        expected = {'range_from', 'range_to', 'row_count', 'sub_aggs', 'sub_group_bys'}
        _assert_structure(self, expected, _get_instance_members(r), 'GroupByGeoDistanceResultItem')

    def test_group_by_histogram_result_item_structure(self):
        r = GroupByHistogramResultItem('bucket1', 7, [], [])
        expected = {'key', 'value', 'sub_aggs', 'sub_group_bys'}
        _assert_structure(self, expected, _get_instance_members(r), 'GroupByHistogramResultItem')

# =============================================================================
# Metadata tests - Table related classes (7 tests)
# =============================================================================

class TestMetadataTableClassStructure(unittest.TestCase):
    """Test structure stability of table-related classes from tablestore/metadata.py."""

    def test_table_meta_structure(self):
        t = TableMeta('test_table', [('pk', 'STRING')])
        expected = {'table_name', 'schema_of_primary_key', 'defined_columns'}
        _assert_structure(self, expected, _get_instance_members(t), 'TableMeta')

    def test_table_options_structure(self):
        t = TableOptions()
        expected = {'time_to_live', 'max_version', 'max_time_deviation', 'allow_update'}
        _assert_structure(self, expected, _get_instance_members(t), 'TableOptions')

    def test_capacity_unit_structure(self):
        c = CapacityUnit()
        expected = {'read', 'write'}
        _assert_structure(self, expected, _get_instance_members(c), 'CapacityUnit')

    def test_reserved_throughput_structure(self):
        r = ReservedThroughput(CapacityUnit())
        expected = {'capacity_unit'}
        _assert_structure(self, expected, _get_instance_members(r), 'ReservedThroughput')

    def test_reserved_throughput_details_structure(self):
        r = ReservedThroughputDetails(CapacityUnit(), 0, 0)
        expected = {'capacity_unit', 'last_increase_time', 'last_decrease_time'}
        _assert_structure(self, expected, _get_instance_members(r), 'ReservedThroughputDetails')

    def test_defined_column_schema_structure(self):
        d = DefinedColumnSchema('col1', 'STRING')
        expected = {'name', 'column_type'}
        _assert_structure(self, expected, _get_instance_members(d), 'DefinedColumnSchema')

    def test_column_structure(self):
        c = Column('col1', 'value1')
        expected = {'name', 'value', 'timestamp'}
        _assert_structure(self, expected, _get_instance_members(c), 'Column')

# =============================================================================
# Metadata tests - SSE classes (3 tests)
# =============================================================================

class TestMetadataSSEClassStructure(unittest.TestCase):
    """Test structure stability of SSE-related classes from tablestore/metadata.py."""

    def test_sse_key_type_structure(self):
        expected = {'SSE_KMS_SERVICE', 'SSE_BYOK'}
        _assert_structure(self, expected, _get_class_attributes(SSEKeyType),
                          'SSEKeyType', 'class attributes')

    def test_sse_specification_structure(self):
        s = SSESpecification()
        expected = {'enable', 'key_type', 'key_id', 'role_arn'}
        _assert_structure(self, expected, _get_instance_members(s), 'SSESpecification')

    def test_sse_details_structure(self):
        s = SSEDetails()
        expected = {'enable', 'key_type', 'key_id', 'role_arn'}
        _assert_structure(self, expected, _get_instance_members(s), 'SSEDetails')

# =============================================================================
# Metadata tests - Enum and constant classes (14 tests)
# =============================================================================

class TestMetadataEnumClassStructure(unittest.TestCase):
    """Test structure stability of IntEnum and constant classes from tablestore/metadata.py."""

    def test_field_type_structure(self):
        expected = {'LONG', 'DOUBLE', 'BOOLEAN', 'KEYWORD', 'TEXT',
                    'NESTED', 'GEOPOINT', 'DATE', 'VECTOR', 'JSON'}
        actual = set(FieldType.__members__.keys())
        _assert_structure(self, expected, actual, 'FieldType', 'enum members')

    def test_vector_data_type_structure(self):
        expected = {'VD_FLOAT_32'}
        actual = set(VectorDataType.__members__.keys())
        _assert_structure(self, expected, actual, 'VectorDataType', 'enum members')

    def test_vector_metric_type_structure(self):
        expected = {'VM_EUCLIDEAN', 'VM_COSINE', 'VM_DOT_PRODUCT'}
        actual = set(VectorMetricType.__members__.keys())
        _assert_structure(self, expected, actual, 'VectorMetricType', 'enum members')

    def test_json_type_structure(self):
        expected = {'OBJECT_JSON', 'NESTED_JSON'}
        actual = set(JsonType.__members__.keys())
        _assert_structure(self, expected, actual, 'JsonType', 'enum members')

    def test_text_similarity_structure(self):
        expected = {'BM25', 'SHORT_TEXT'}
        actual = set(TextSimilarity.__members__.keys())
        _assert_structure(self, expected, actual, 'TextSimilarity', 'enum members')

    def test_sync_phase_structure(self):
        expected = {'FULL', 'INCR'}
        actual = set(SyncPhase.__members__.keys())
        _assert_structure(self, expected, actual, 'SyncPhase', 'enum members')

    def test_secondary_index_type_structure(self):
        expected = {'GLOBAL_INDEX', 'LOCAL_INDEX'}
        actual = set(SecondaryIndexType.__members__.keys())
        _assert_structure(self, expected, actual, 'SecondaryIndexType', 'enum members')

    def test_sync_type_structure(self):
        expected = {'SYNC_TYPE_FULL', 'SYNC_TYPE_INCR'}
        actual = set(SyncType.__members__.keys())
        _assert_structure(self, expected, actual, 'SyncType', 'enum members')

    def test_cast_type_structure(self):
        expected = {'VT_INTEGER', 'VT_DOUBLE', 'VT_STRING'}
        actual = set(CastType.__members__.keys())
        _assert_structure(self, expected, actual, 'CastType', 'enum members')

    def test_analyzer_type_structure(self):
        expected = {'SINGLEWORD', 'MAXWORD', 'MINWORD', 'FUZZY', 'SPLIT'}
        _assert_structure(self, expected, _get_class_attributes(AnalyzerType),
                          'AnalyzerType', 'class attributes')

    def test_column_type_structure(self):
        expected = {'STRING', 'INTEGER', 'BOOLEAN', 'DOUBLE', 'BINARY', 'INF_MIN', 'INF_MAX'}
        _assert_structure(self, expected, _get_class_attributes(ColumnType),
                          'ColumnType', 'class attributes')

    def test_update_type_structure(self):
        expected = {'PUT', 'DELETE', 'DELETE_ALL', 'INCREMENT'}
        _assert_structure(self, expected, _get_class_attributes(UpdateType),
                          'UpdateType', 'class attributes')

    def test_column_condition_type_structure(self):
        expected = {'COMPOSITE_COLUMN_CONDITION', 'SINGLE_COLUMN_CONDITION', 'SINGLE_COLUMN_REGEX_CONDITION'}
        _assert_structure(self, expected, _get_class_attributes(ColumnConditionType),
                          'ColumnConditionType', 'class attributes')

# =============================================================================
# Metadata tests - Analyzer parameter classes (3 tests)
# =============================================================================

class TestMetadataAnalyzerClassStructure(unittest.TestCase):
    """Test structure stability of analyzer parameter classes from tablestore/metadata.py."""

    def test_single_word_analyzer_parameter_structure(self):
        p = SingleWordAnalyzerParameter()
        expected = {'case_sensitive', 'delimit_word'}
        _assert_structure(self, expected, _get_instance_members(p), 'SingleWordAnalyzerParameter')

    def test_split_analyzer_parameter_structure(self):
        p = SplitAnalyzerParameter()
        expected = {'delimiter'}
        _assert_structure(self, expected, _get_instance_members(p), 'SplitAnalyzerParameter')

    def test_fuzzy_analyzer_parameter_structure(self):
        p = FuzzyAnalyzerParameter()
        expected = {'min_chars', 'max_chars'}
        _assert_structure(self, expected, _get_instance_members(p), 'FuzzyAnalyzerParameter')

# =============================================================================
# Metadata tests - Search index classes (7 tests)
# =============================================================================

class TestMetadataSearchIndexClassStructure(unittest.TestCase):
    """Test structure stability of search index classes from tablestore/metadata.py."""

    def test_sorter_structure(self):
        s = Sorter()
        expected = set()
        _assert_structure(self, expected, _get_instance_members(s), 'Sorter')

    def test_index_setting_structure(self):
        s = IndexSetting()
        expected = {'routing_fields'}
        _assert_structure(self, expected, _get_instance_members(s), 'IndexSetting')

    def test_vector_options_structure(self):
        v = VectorOptions(VectorDataType.VD_FLOAT_32, VectorMetricType.VM_EUCLIDEAN, 128)
        expected = {'data_type', 'metric_type', 'dimension'}
        _assert_structure(self, expected, _get_instance_members(v), 'VectorOptions')

    def test_field_schema_structure(self):
        f = FieldSchema('field1', FieldType.KEYWORD)
        expected = {
            'field_name', 'field_type', 'index', 'store', 'is_array',
            'enable_sort_and_agg', 'analyzer', 'analyzer_parameter',
            'sub_field_schemas', 'date_formats', 'is_virtual_field',
            'source_fields', 'vector_options', 'enable_highlighting',
            'json_type', 'text_similarity',
        }
        _assert_structure(self, expected, _get_instance_members(f), 'FieldSchema')

    def test_sync_stat_structure(self):
        s = SyncStat(SyncPhase.FULL, 0)
        expected = {'sync_phase', 'current_sync_timestamp'}
        _assert_structure(self, expected, _get_instance_members(s), 'SyncStat')

    def test_search_index_meta_structure(self):
        m = SearchIndexMeta([FieldSchema('f1', FieldType.KEYWORD)])
        expected = {'fields', 'index_setting', 'index_sort', 'time_to_live'}
        _assert_structure(self, expected, _get_instance_members(m), 'SearchIndexMeta')

    def test_secondary_index_meta_structure(self):
        m = SecondaryIndexMeta('idx1', ['pk1'], ['col1'])
        expected = {'index_name', 'primary_key_names', 'defined_column_names', 'index_type'}
        _assert_structure(self, expected, _get_instance_members(m), 'SecondaryIndexMeta')

# =============================================================================
# Metadata tests - Response classes (8 tests)
# =============================================================================

class TestMetadataResponseClassStructure(unittest.TestCase):
    """Test structure stability of response classes from tablestore/metadata.py."""

    def test_common_response_structure(self):
        r = CommonResponse()
        expected = {'request_id'}
        _assert_structure(self, expected, _get_instance_members(r), 'CommonResponse')

    def test_update_table_response_structure(self):
        r = UpdateTableResponse(None, None)
        expected = {'reserved_throughput_details', 'table_options'}
        _assert_structure(self, expected, _get_instance_members(r), 'UpdateTableResponse')

    def test_describe_table_response_structure(self):
        r = DescribeTableResponse(None, None, None)
        expected = {'table_meta', 'table_options', 'reserved_throughput_details', 'sse_details', 'secondary_indexes'}
        _assert_structure(self, expected, _get_instance_members(r), 'DescribeTableResponse')

    def test_row_data_item_structure(self):
        r = RowDataItem(True, None, None, 'table', None, None, None)
        expected = {'is_ok', 'error_code', 'error_message', 'table_name', 'consumed', 'row'}
        _assert_structure(self, expected, _get_instance_members(r), 'RowDataItem')

    def test_batch_get_row_response_structure(self):
        r = BatchGetRowResponse({})
        expected = {'items'}
        _assert_structure(self, expected, _get_instance_members(r), 'BatchGetRowResponse')

    def test_batch_write_row_response_item_structure(self):
        r = BatchWriteRowResponseItem(True, None, None, None, None)
        expected = {'is_ok', 'error_code', 'error_message', 'consumed', 'row'}
        _assert_structure(self, expected, _get_instance_members(r), 'BatchWriteRowResponseItem')

    def test_column_condition_structure(self):
        c = ColumnCondition()
        expected = set()
        _assert_structure(self, expected, _get_instance_members(c), 'ColumnCondition')

    def test_query_structure(self):
        q = Query()
        expected = set()
        _assert_structure(self, expected, _get_instance_members(q), 'Query')

    def test_batch_write_row_response_structure(self):
        # Create a simple mock request
        request = BatchWriteRowRequest()
        request.items = {}
        
        # Create a simple mock response (empty dict means no tables)
        response = {}
        
        r = BatchWriteRowResponse(request, response)
        expected = {'table_of_put', 'table_of_update', 'table_of_delete'}
        _assert_structure(self, expected, _get_instance_members(r), 'BatchWriteRowResponse')

# =============================================================================
# Metadata tests - Regex condition classes (2 tests)
# =============================================================================

class TestMetadataRegexClassStructure(unittest.TestCase):
    """Test structure stability of regex condition classes from tablestore/metadata.py."""

    def test_regex_rule_structure(self):
        r = RegexRule('pattern', CastType.VT_STRING)
        expected = {'regex', 'cast_type'}
        _assert_structure(self, expected, _get_instance_members(r), 'RegexRule')

    def test_single_column_regex_condition_structure(self):
        r = SingleColumnRegexCondition('col', ComparatorType.EQUAL, column_value='val')
        expected = {'column_name', 'comparator', 'column_value', 'latest_version_only', 'regex_rule'}
        _assert_structure(self, expected, _get_instance_members(r), 'SingleColumnRegexCondition')

# =============================================================================
# Metadata tests - Marker classes (3 tests)
# =============================================================================

class TestMetadataMarkerClassStructure(unittest.TestCase):
    """Test structure stability of marker classes from tablestore/metadata.py."""

    def test_inf_min_structure(self):
        m = INF_MIN()
        expected = set()
        _assert_structure(self, expected, _get_instance_members(m), 'INF_MIN')

    def test_inf_max_structure(self):
        m = INF_MAX()
        expected = set()
        _assert_structure(self, expected, _get_instance_members(m), 'INF_MAX')

    def test_pk_auto_incr_structure(self):
        m = PK_AUTO_INCR()
        expected = set()
        _assert_structure(self, expected, _get_instance_members(m), 'PK_AUTO_INCR')

# =============================================================================
# Metadata tests - Iterable response classes (8 tests)
# =============================================================================

class TestMetadataIterableResponseClassStructure(unittest.TestCase):
    """Test structure stability of iterable response classes from tablestore/metadata.py."""

    def test_iterable_response_structure(self):
        r = IterableResponse()
        expected = {'request_id', 'index', 'response'}
        _assert_structure(self, expected, _get_instance_members(r), 'IterableResponse')

    def test_search_response_structure(self):
        r = SearchResponse([], [], [], None, True, 0, [])
        expected = {'request_id', 'index', 'response', 'rows', 'agg_results',
                    'group_by_results', 'next_token', 'is_all_succeed', 'total_count', 'search_hits'}
        _assert_structure(self, expected, _get_instance_members(r), 'SearchResponse')

    def test_compute_splits_response_structure(self):
        r = ComputeSplitsResponse('session-123', 10)
        expected = {'request_id', 'index', 'response', 'session_id', 'splits_size'}
        _assert_structure(self, expected, _get_instance_members(r), 'ComputeSplitsResponse')

    def test_parallel_scan_response_structure(self):
        r = ParallelScanResponse([], None)
        expected = {'request_id', 'index', 'response', 'rows', 'next_token'}
        _assert_structure(self, expected, _get_instance_members(r), 'ParallelScanResponse')

    def test_search_hit_structure(self):
        h = SearchHit(None, 0.0, None, None, None)
        expected = {'row', 'score', 'highlight_result', 'search_inner_hits', 'nested_doc_offset'}
        _assert_structure(self, expected, _get_instance_members(h), 'SearchHit')

    def test_search_inner_hit_structure(self):
        h = SearchInnerHit('path', [])
        expected = {'path', 'search_hits'}
        _assert_structure(self, expected, _get_instance_members(h), 'SearchInnerHit')

    def test_highlight_result_structure(self):
        h = HighlightResult({})
        expected = {'highlight_fields'}
        _assert_structure(self, expected, _get_instance_members(h), 'HighlightResult')

    def test_highlight_field_structure(self):
        h = HighlightField('field', [])
        expected = {'field_name', 'field_fragments'}
        _assert_structure(self, expected, _get_instance_members(h), 'HighlightField')

# =============================================================================
# Metadata tests - Timeseries classes (21 tests)
# =============================================================================

class TestMetadataTimeseriesClassStructure(unittest.TestCase):
    """Test structure stability of timeseries classes from tablestore/metadata.py."""

    def test_timeseries_key_structure(self):
        k = TimeseriesKey()
        expected = {'measurement_name', 'data_source', 'tags'}
        _assert_structure(self, expected, _get_instance_members(k), 'TimeseriesKey')

    def test_timeseries_row_structure(self):
        r = TimeseriesRow(TimeseriesKey(), {}, 0)
        expected = {'timeseries_key', 'fields', 'time_in_us'}
        _assert_structure(self, expected, _get_instance_members(r), 'TimeseriesRow')

    def test_timeseries_table_options_structure(self):
        o = TimeseriesTableOptions(86400)
        expected = {'time_to_live'}
        _assert_structure(self, expected, _get_instance_members(o), 'TimeseriesTableOptions')

    def test_timeseries_meta_options_structure(self):
        o = TimeseriesMetaOptions()
        expected = {'meta_time_to_live', 'allow_update_attributes'}
        _assert_structure(self, expected, _get_instance_members(o), 'TimeseriesMetaOptions')

    def test_timeseries_table_meta_structure(self):
        m = TimeseriesTableMeta('table_name')
        expected = {'timeseries_table_name', 'timeseries_table_options', 'timeseries_meta_options',
                    'status', 'timeseries_keys', 'field_primary_keys'}
        _assert_structure(self, expected, _get_instance_members(m), 'TimeseriesTableMeta')

    def test_timeseries_analytical_store_structure(self):
        s = TimeseriesAnalyticalStore('store_name')
        expected = {'analytical_store_name', 'time_to_live', 'sync_option'}
        _assert_structure(self, expected, _get_instance_members(s), 'TimeseriesAnalyticalStore')

    def test_lastpoint_index_meta_structure(self):
        m = LastpointIndexMeta('index_table')
        expected = {'index_table_name'}
        _assert_structure(self, expected, _get_instance_members(m), 'LastpointIndexMeta')

    def test_create_timeseries_table_request_structure(self):
        r = CreateTimeseriesTableRequest(TimeseriesTableMeta('table'))
        expected = {'table_meta', 'analytical_stores', 'lastpoint_index_metas'}
        _assert_structure(self, expected, _get_instance_members(r), 'CreateTimeseriesTableRequest')

    def test_describe_timeseries_table_response_structure(self):
        r = DescribeTimeseriesTableResponse(TimeseriesTableMeta('table'))
        expected = {'table_meta'}
        _assert_structure(self, expected, _get_instance_members(r), 'DescribeTimeseriesTableResponse')

    def test_update_timeseries_meta_request_structure(self):
        r = UpdateTimeseriesMetaRequest('table', [])
        expected = {'timeseries_tablename', 'metas'}
        _assert_structure(self, expected, _get_instance_members(r), 'UpdateTimeseriesMetaRequest')

    def test_delete_timeseries_meta_request_structure(self):
        r = DeleteTimeseriesMetaRequest('table', [])
        expected = {'timeseries_tablename', 'timeseries_keys'}
        _assert_structure(self, expected, _get_instance_members(r), 'DeleteTimeseriesMetaRequest')

    def test_timeseries_meta_structure(self):
        m = TimeseriesMeta(TimeseriesKey(), {})
        expected = {'timeseries_key', 'attributes', 'update_time_in_us'}
        _assert_structure(self, expected, _get_instance_members(m), 'TimeseriesMeta')

    def test_error_structure(self):
        e = Error('code', 'message')
        expected = {'code', 'message'}
        _assert_structure(self, expected, _get_instance_members(e), 'Error')

    def test_failed_row_result_structure(self):
        r = FailedRowResult(0, Error('code', 'msg'))
        expected = {'index', 'error'}
        _assert_structure(self, expected, _get_instance_members(r), 'FailedRowResult')

    def test_update_timeseries_meta_response_structure(self):
        r = UpdateTimeseriesMetaResponse([])
        expected = {'failedRows'}
        _assert_structure(self, expected, _get_instance_members(r), 'UpdateTimeseriesMetaResponse')

    def test_put_timeseries_data_response_structure(self):
        r = PutTimeseriesDataResponse()
        expected = {'failedRows'}
        _assert_structure(self, expected, _get_instance_members(r), 'PutTimeseriesDataResponse')

    def test_delete_timeseries_meta_response_structure(self):
        r = DeleteTimeseriesMetaResponse([])
        expected = {'failedRows'}
        _assert_structure(self, expected, _get_instance_members(r), 'DeleteTimeseriesMetaResponse')

    def test_query_timeseries_meta_request_structure(self):
        r = QueryTimeseriesMetaRequest('table')
        expected = {'timeseriesTableName', 'condition', 'getTotalHits', 'limit', 'nextToken'}
        _assert_structure(self, expected, _get_instance_members(r), 'QueryTimeseriesMetaRequest')

    def test_query_timeseries_meta_response_structure(self):
        r = QueryTimeseriesMetaResponse()
        expected = {'timeseriesMetas', 'totalHits', 'nextToken'}
        _assert_structure(self, expected, _get_instance_members(r), 'QueryTimeseriesMetaResponse')

    def test_get_timeseries_data_request_structure(self):
        r = GetTimeseriesDataRequest('table')
        expected = {'timeseriesTableName', 'timeseriesKey', 'beginTimeInUs', 'endTimeInUs',
                    'limit', 'nextToken', 'backward', 'fieldsToGet'}
        _assert_structure(self, expected, _get_instance_members(r), 'GetTimeseriesDataRequest')

    def test_get_timeseries_data_response_structure(self):
        r = GetTimeseriesDataResponse()
        expected = {'rows', 'nextToken'}
        _assert_structure(self, expected, _get_instance_members(r), 'GetTimeseriesDataResponse')

# =============================================================================
# Metadata module completeness check
# =============================================================================

class TestMetadataClassCompleteness(unittest.TestCase):
    """Ensure all classes in metadata.py have stability tests.

    If this test fails, it means a new class was added to metadata.py
    without a corresponding stability test. Please add a test for the
    new class in the appropriate test class above.
    """

    # Complete set of all classes in metadata.py that have stability tests.
    # When adding a new class to metadata.py, add its name here AND write
    # a stability test for it.
    KNOWN_CLASSES = frozenset({
        'AnalyzerType', 'BatchGetRowRequest', 'BatchGetRowResponse',
        'BatchWriteRowRequest', 'BatchWriteRowResponse', 'BatchWriteRowResponseItem',
        'BatchWriteRowType', 'BoolQuery', 'CapacityUnit', 'CastType',
        'Collapse', 'Column', 'ColumnCondition', 'ColumnConditionType',
        'ColumnReturnType', 'ColumnsToGet', 'ColumnType', 'CommonResponse',
        'ComparatorType', 'CompositeColumnCondition', 'ComputeSplitsResponse',
        'Condition', 'CreateTimeseriesTableRequest', 'DefinedColumnSchema',
        'DeleteRowItem', 'DeleteTimeseriesMetaRequest', 'DeleteTimeseriesMetaResponse',
        'DescribeTableResponse', 'DescribeTimeseriesTableResponse', 'Direction',
        'DisMaxQuery', 'DocSort', 'Error', 'ExistsQuery', 'FailedRowResult',
        'FieldSchema', 'FieldSort', 'FieldType', 'FieldValueFactor',
        'FunctionScoreQuery', 'FuzzyAnalyzerParameter', 'GeoBoundingBoxQuery',
        'GeoDistanceQuery', 'GeoDistanceSort', 'GeoDistanceType', 'GeoPolygonQuery',
        'GetTimeseriesDataRequest', 'GetTimeseriesDataResponse', 'Highlight',
        'HighlightEncoder', 'HighlightField', 'HighlightFragmentOrder',
        'HighlightParameter', 'HighlightResult', 'INF_MAX', 'INF_MIN',
        'IndexSetting', 'InnerHits', 'IterableResponse', 'JsonType',
        'KnnVectorQuery', 'LastpointIndexMeta', 'LogicalOperator',
        'MatchAllQuery', 'MatchPhraseQuery', 'MatchQuery', 'NestedFilter',
        'NestedQuery', 'PK_AUTO_INCR', 'ParallelScanResponse', 'PrefixQuery',
        'PrimaryKeySort', 'PutRowItem', 'PutTimeseriesDataResponse', 'Query',
        'QueryOperator', 'QueryTimeseriesMetaRequest', 'QueryTimeseriesMetaResponse',
        'QueryType', 'RangeQuery', 'RegexRule', 'ReservedThroughput',
        'ReservedThroughputDetails', 'ReturnType', 'Row', 'RowDataItem',
        'RowExistenceExpectation', 'RowItem', 'ScanQuery', 'ScoreMode',
        'ScoreSort', 'SearchHit', 'SearchIndexMeta', 'SearchInnerHit',
        'SearchQuery', 'SearchResponse', 'SecondaryIndexMeta', 'SecondaryIndexType',
        'SingleColumnCondition', 'SingleColumnRegexCondition',
        'SingleWordAnalyzerParameter', 'Sort', 'SortMode', 'SortOrder',
        'Sorter', 'SplitAnalyzerParameter', 'SSEDetails', 'SSEKeyType',
        'SSESpecification', 'SyncPhase', 'SyncStat', 'SyncType',
        'TableInBatchGetRowItem', 'TableInBatchWriteRowItem', 'TableMeta',
        'TableOptions', 'TermQuery', 'TermsQuery', 'TextSimilarity',
        'TimeseriesAnalyticalStore', 'TimeseriesKey', 'TimeseriesMeta',
        'TimeseriesMetaOptions', 'TimeseriesRow', 'TimeseriesTableMeta',
        'TimeseriesTableOptions', 'UpdateRowItem', 'UpdateTableResponse',
        'UpdateTimeseriesMetaRequest', 'UpdateTimeseriesMetaResponse',
        'UpdateType', 'VectorDataType', 'VectorMetricType', 'VectorOptions',
        'WildcardQuery',
    })

    def test_no_new_classes_in_metadata(self):
        """Detect new classes added to metadata.py that lack stability tests."""
        import re
        import os

        metadata_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'tablestore', 'metadata.py',
        )
        with open(metadata_path, 'r') as f:
            content = f.read()

        actual_classes = set(re.findall(r'^class\s+([A-Za-z0-9_]+)', content, re.MULTILINE))
        new_classes = actual_classes - self.KNOWN_CLASSES
        removed_classes = self.KNOWN_CLASSES - actual_classes

        self.assertEqual(
            set(), new_classes,
            f"New class(es) found in metadata.py without stability tests: {sorted(new_classes)}. "
            f"Please add stability test(s) and update KNOWN_CLASSES in TestMetadataClassCompleteness.",
        )
        self.assertEqual(
            set(), removed_classes,
            f"Class(es) removed from metadata.py but still in KNOWN_CLASSES: {sorted(removed_classes)}. "
            f"Please remove them from KNOWN_CLASSES in TestMetadataClassCompleteness.",
        )

# =============================================================================
# Encoder / Decoder class definition stability tests
# =============================================================================

class TestEncoderDecoderClassStructure(unittest.TestCase):
    """Test structure stability of OTSProtoBufferEncoder and OTSProtoBufferDecoder classes."""

    def test_encoder_init_members(self):
        encoder = OTSProtoBufferEncoder('utf-8', enable_native=False)
        expected = {'encoding', 'enable_native', 'native_fallback',
                    '_use_native_encoder', 'api_encode_map',
                    'timeseries_meta_condition_encode_map'}
        _assert_structure(self, expected, _get_instance_members(encoder),
                          'OTSProtoBufferEncoder')

    def test_encoder_public_methods(self):
        expected = {'encode_request', 'unsigned_to_signed'}
        actual = _get_public_methods(OTSProtoBufferEncoder)
        _assert_structure(self, expected, actual,
                          'OTSProtoBufferEncoder', 'public methods')

    def test_encoder_api_encode_map_keys(self):
        encoder = OTSProtoBufferEncoder('utf-8', enable_native=False)
        expected = frozenset({
            'AbortTransaction', 'BatchGetRow', 'BatchWriteRow',
            'CommitTransaction', 'ComputeSplits', 'CreateIndex',
            'CreateSearchIndex', 'CreateTable', 'CreateTimeseriesTable',
            'DeleteRow', 'DeleteSearchIndex', 'DeleteTable',
            'DeleteTimeseriesMeta', 'DeleteTimeseriesTable',
            'DescribeSearchIndex', 'DescribeTable', 'DescribeTimeseriesTable',
            'DropIndex', 'GetRange', 'GetRow', 'GetTimeseriesData',
            'ListSearchIndex', 'ListTable', 'ListTimeseriesTable',
            'ParallelScan', 'PutRow', 'PutTimeseriesData',
            'QueryTimeseriesMeta', 'SQLQuery', 'Search',
            'StartLocalTransaction', 'UpdateRow', 'UpdateSearchIndex',
            'UpdateTable', 'UpdateTimeseriesMeta', 'UpdateTimeseriesTable',
        })
        actual = frozenset(encoder.api_encode_map.keys())
        self.assertEqual(expected, actual,
                         f"OTSProtoBufferEncoder api_encode_map keys changed! "
                         f"Added: {actual - expected}, Removed: {expected - actual}")

    def test_decoder_init_members(self):
        decoder = OTSProtoBufferDecoder('utf-8', enable_native=False)
        expected = {'encoding', 'enable_native', 'native_fallback',
                    '_use_native_decoder', '_use_native_parser', 'api_decode_map'}
        _assert_structure(self, expected, _get_instance_members(decoder),
                          'OTSProtoBufferDecoder')

    def test_decoder_public_methods(self):
        expected = {'decode_response'}
        actual = _get_public_methods(OTSProtoBufferDecoder)
        _assert_structure(self, expected, actual,
                          'OTSProtoBufferDecoder', 'public methods')

    def test_decoder_api_decode_map_keys(self):
        decoder = OTSProtoBufferDecoder('utf-8', enable_native=False)
        expected = frozenset({
            'AbortTransaction', 'BatchGetRow', 'BatchWriteRow',
            'CommitTransaction', 'ComputeSplits', 'CreateIndex',
            'CreateSearchIndex', 'CreateTable', 'CreateTimeseriesTable',
            'DeleteRow', 'DeleteSearchIndex', 'DeleteTable',
            'DeleteTimeseriesMeta', 'DeleteTimeseriesTable',
            'DescribeSearchIndex', 'DescribeTable', 'DescribeTimeseriesTable',
            'DropIndex', 'GetRange', 'GetRow', 'GetTimeseriesData',
            'ListSearchIndex', 'ListTable', 'ListTimeseriesTable',
            'ParallelScan', 'PutRow', 'PutTimeseriesData',
            'QueryTimeseriesMeta', 'SQLQuery', 'Search',
            'StartLocalTransaction', 'UpdateRow', 'UpdateSearchIndex',
            'UpdateTable', 'UpdateTimeseriesMeta', 'UpdateTimeseriesTable',
        })
        actual = frozenset(decoder.api_decode_map.keys())
        self.assertEqual(expected, actual,
                         f"OTSProtoBufferDecoder api_decode_map keys changed! "
                         f"Added: {actual - expected}, Removed: {expected - actual}")

# =============================================================================
# Native-accelerated method signature stability tests
# =============================================================================

class TestNativeAcceleratedMethodSignatures(unittest.TestCase):
    """Test that method signatures of native C++ accelerated methods have not changed.

    These methods have corresponding native C++ implementations. If their Python
    signatures change, the native C++ side must be updated accordingly.
    """

    # --- Encoder native-accelerated methods (7 methods) ---

    def test_encode_put_row_signature(self):
        expected = ('table_name', 'row', 'condition', 'return_type', 'transaction_id')
        actual = _get_method_params(OTSProtoBufferEncoder, '_encode_put_row')
        self.assertEqual(expected, actual,
                         f"_encode_put_row signature changed! Expected {expected}, got {actual}")

    def test_encode_update_row_signature(self):
        expected = ('table_name', 'row', 'condition', 'return_type', 'transaction_id')
        actual = _get_method_params(OTSProtoBufferEncoder, '_encode_update_row')
        self.assertEqual(expected, actual,
                         f"_encode_update_row signature changed! Expected {expected}, got {actual}")

    def test_encode_delete_row_signature(self):
        expected = ('table_name', 'primary_key', 'condition', 'return_type', 'transaction_id')
        actual = _get_method_params(OTSProtoBufferEncoder, '_encode_delete_row')
        self.assertEqual(expected, actual,
                         f"_encode_delete_row signature changed! Expected {expected}, got {actual}")

    def test_encode_batch_get_row_signature(self):
        expected = ('request',)
        actual = _get_method_params(OTSProtoBufferEncoder, '_encode_batch_get_row')
        self.assertEqual(expected, actual,
                         f"_encode_batch_get_row signature changed! Expected {expected}, got {actual}")

    def test_encode_batch_write_row_signature(self):
        expected = ('request',)
        actual = _get_method_params(OTSProtoBufferEncoder, '_encode_batch_write_row')
        self.assertEqual(expected, actual,
                         f"_encode_batch_write_row signature changed! Expected {expected}, got {actual}")

    def test_encode_search_signature(self):
        expected = ('table_name', 'index_name', 'search_query', 'columns_to_get',
                    'routing_keys', 'timeout_s')
        actual = _get_method_params(OTSProtoBufferEncoder, '_encode_search')
        self.assertEqual(expected, actual,
                         f"_encode_search signature changed! Expected {expected}, got {actual}")

    def test_encode_parallel_scan_signature(self):
        expected = ('table_name', 'index_name', 'scan_query', 'session_id',
                    'columns_to_get', 'timeout_s')
        actual = _get_method_params(OTSProtoBufferEncoder, '_encode_parallel_scan')
        self.assertEqual(expected, actual,
                         f"_encode_parallel_scan signature changed! Expected {expected}, got {actual}")

    # --- Decoder native-accelerated methods (9 methods) ---

    def test_decode_get_row_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_get_row')
        self.assertEqual(expected, actual,
                         f"_decode_get_row signature changed! Expected {expected}, got {actual}")

    def test_decode_put_row_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_put_row')
        self.assertEqual(expected, actual,
                         f"_decode_put_row signature changed! Expected {expected}, got {actual}")

    def test_decode_update_row_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_update_row')
        self.assertEqual(expected, actual,
                         f"_decode_update_row signature changed! Expected {expected}, got {actual}")

    def test_decode_delete_row_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_delete_row')
        self.assertEqual(expected, actual,
                         f"_decode_delete_row signature changed! Expected {expected}, got {actual}")

    def test_decode_batch_get_row_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_batch_get_row')
        self.assertEqual(expected, actual,
                         f"_decode_batch_get_row signature changed! Expected {expected}, got {actual}")

    def test_decode_batch_write_row_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_batch_write_row')
        self.assertEqual(expected, actual,
                         f"_decode_batch_write_row signature changed! Expected {expected}, got {actual}")

    def test_decode_get_range_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_get_range')
        self.assertEqual(expected, actual,
                         f"_decode_get_range signature changed! Expected {expected}, got {actual}")

    def test_decode_search_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_search')
        self.assertEqual(expected, actual,
                         f"_decode_search signature changed! Expected {expected}, got {actual}")

    def test_decode_parallel_scan_signature(self):
        expected = ('body', 'request_id')
        actual = _get_method_params(OTSProtoBufferDecoder, '_decode_parallel_scan')
        self.assertEqual(expected, actual,
                         f"_decode_parallel_scan signature changed! Expected {expected}, got {actual}")

if __name__ == '__main__':
    unittest.main()
