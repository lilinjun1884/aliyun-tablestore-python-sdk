"""
OTS Python SDK - 表格存储 Python SDK

基于 C++ SDK 的 Python 绑定，提供高性能的表格存储访问能力。
支持导入失败时的优雅降级，不会导致程序异常退出。
"""

import sys
import warnings
import platform

# ---------------------------------------------------------------------------
#  导入状态标志
# ---------------------------------------------------------------------------
OTS_SDK_AVAILABLE = False

# 所有从 C++ 扩展导入的符号，初始化为 None
OTSClient = None
ClientConfiguration = None
Credential = None

_OTSException = None
_OTSClientException = None

CreateTableRequest = None
CreateTableResult = None
ListTableResult = None
DescribeTableRequest = None
DescribeTableResult = None
DeleteTableRequest = None
DeleteTableResult = None
UpdateTableRequest = None
UpdateTableResult = None

GetRowRequest = None
GetRowResult = None
PutRowRequest = None
PutRowResult = None
UpdateRowRequest = None
UpdateRowResult = None
DeleteRowRequest = None
DeleteRowResult = None
SingleRowQueryCriteria = None
RowPutChange = None
RowUpdateChange = None
RowDeleteChange = None

BatchGetRowRequest = None
BatchGetRowResult = None
BatchWriteRowRequest = None
BatchWriteRowResult = None
MultiRowQueryCriteria = None

GetRangeRequest = None
GetRangeResult = None
RangeRowQueryCriteria = None
RowRangeIterator = None

SearchRequest = None
SearchResult = None
CreateSearchIndexRequest = None
CreateSearchIndexResult = None
DeleteSearchIndexRequest = None
DeleteSearchIndexResult = None
ListSearchIndexRequest = None
ListSearchIndexResult = None
DescribeSearchIndexRequest = None
DescribeSearchIndexResult = None

SQLQueryRequest = None
SQLQueryResult = None
ComputeSplitsRequest = None
ComputeSplitsResult = None
ParallelScanRequest = None
ParallelScanResult = None

OTSResult = None
TableMeta = None
PrimaryKey = None
PrimaryKeyValue = None
PrimaryKeyColumn = None
Column = None
ColumnValue = None
CapacityUnit = None
ConsumedCapacity = None
ReservedThroughput = None
TableOptions = None

# ---------------------------------------------------------------------------
#  尝试导入 C++ 扩展
# ---------------------------------------------------------------------------
try:
    from .ots_sdk import (
        OTSClient,
        ClientConfiguration,
        Credential,
        OTSException as _OTSException,
        OTSClientException as _OTSClientException,
        CreateTableRequest,
        CreateTableResult,
        ListTableResult,
        DescribeTableRequest,
        DescribeTableResult,
        DeleteTableRequest,
        DeleteTableResult,
        UpdateTableRequest,
        UpdateTableResult,
        GetRowRequest,
        GetRowResult,
        PutRowRequest,
        PutRowResult,
        UpdateRowRequest,
        UpdateRowResult,
        DeleteRowRequest,
        DeleteRowResult,
        SingleRowQueryCriteria,
        RowPutChange,
        RowUpdateChange,
        RowDeleteChange,
        BatchGetRowRequest,
        BatchGetRowResult,
        BatchWriteRowRequest,
        BatchWriteRowResult,
        MultiRowQueryCriteria,
        GetRangeRequest,
        GetRangeResult,
        RangeRowQueryCriteria,
        RowRangeIterator,
        SearchRequest,
        SearchResult,
        CreateSearchIndexRequest,
        CreateSearchIndexResult,
        DeleteSearchIndexRequest,
        DeleteSearchIndexResult,
        ListSearchIndexRequest,
        ListSearchIndexResult,
        DescribeSearchIndexRequest,
        DescribeSearchIndexResult,
        SQLQueryRequest,
        SQLQueryResult,
        ComputeSplitsRequest,
        ComputeSplitsResult,
        ParallelScanRequest,
        ParallelScanResult,
        OTSResult,
        TableMeta,
        PrimaryKey,
        PrimaryKeyValue,
        PrimaryKeyColumn,
        Column,
        ColumnValue,
        CapacityUnit,
        ConsumedCapacity,
        ReservedThroughput,
        TableOptions,
    )
    OTS_SDK_AVAILABLE = True

    # pybind11 注册异常类时使用的模块名为 'ots_sdk'，抛异常时会通过
    # sys.modules['ots_sdk'] 查找 OTSException / OTSClientException。
    # 需要将 .so 模块注册到该路径，否则会出现
    # "module 'ots_sdk' has no attribute 'OTSException'" 错误。
    from . import ots_sdk as _so_module
    if 'ots_sdk' not in sys.modules:
        sys.modules['ots_sdk'] = _so_module

except ImportError as exc:
    OTS_SDK_AVAILABLE = False
    warnings.warn(
        f"OTS C++ SDK extension 'ots_sdk' could not be imported: {exc}. "
        f"Platform: {platform.platform()}, Python: {sys.version}. "
        "The C++ SDK based client will not be available.",
        ImportWarning,
        stacklevel=2,
    )

except Exception as exc:
    OTS_SDK_AVAILABLE = False
    warnings.warn(
        f"Unexpected error while importing OTS C++ SDK extension: {exc}. "
        f"Platform: {platform.platform()}, Python: {sys.version}. "
        "The C++ SDK based client will not be available.",
        RuntimeWarning,
        stacklevel=2,
    )

# ---------------------------------------------------------------------------
#  扩展异常类（仅在导入成功时定义）
# ---------------------------------------------------------------------------
if OTS_SDK_AVAILABLE and _OTSException is not None:
    class OTSException(_OTSException):
        """OTS 服务异常，继承自 C++ 绑定的基础异常类"""

        def GetErrorCode(self):
            """获取错误码"""
            return self.error_code

        def GetMessage(self):
            """获取错误消息"""
            return self.message

        def GetRequestId(self):
            """获取请求 ID"""
            return self.request_id

        def GetTraceId(self):
            """获取追踪 ID"""
            return self.trace_id

        def GetHttpStatus(self):
            """获取 HTTP 状态码"""
            return self.http_status
else:
    OTSException = None

if OTS_SDK_AVAILABLE and _OTSClientException is not None:
    class OTSClientException(_OTSClientException):
        """OTS 客户端异常，继承自 C++ 绑定的基础异常类"""

        def GetMessage(self):
            """获取错误消息"""
            return self.message

        def GetTraceId(self):
            """获取追踪 ID"""
            return self.trace_id
else:
    OTSClientException = None

__all__ = [
    'OTS_SDK_AVAILABLE',

    # 核心客户端
    'OTSClient',

    # 配置类
    'ClientConfiguration',
    'Credential',

    # 异常类
    'OTSException',
    'OTSClientException',

    # 表操作
    'CreateTableRequest',
    'CreateTableResult',
    'ListTableResult',
    'DescribeTableRequest',
    'DescribeTableResult',
    'DeleteTableRequest',
    'DeleteTableResult',
    'UpdateTableRequest',
    'UpdateTableResult',

    # 单行操作
    'GetRowRequest',
    'GetRowResult',
    'PutRowRequest',
    'PutRowResult',
    'UpdateRowRequest',
    'UpdateRowResult',
    'DeleteRowRequest',
    'DeleteRowResult',
    'SingleRowQueryCriteria',
    'RowPutChange',
    'RowUpdateChange',
    'RowDeleteChange',

    # 批量操作
    'BatchGetRowRequest',
    'BatchGetRowResult',
    'BatchWriteRowRequest',
    'BatchWriteRowResult',
    'MultiRowQueryCriteria',

    # 范围查询
    'GetRangeRequest',
    'GetRangeResult',
    'RangeRowQueryCriteria',
    'RowRangeIterator',

    # 多元索引
    'SearchRequest',
    'SearchResult',
    'CreateSearchIndexRequest',
    'CreateSearchIndexResult',
    'DeleteSearchIndexRequest',
    'DeleteSearchIndexResult',
    'ListSearchIndexRequest',
    'ListSearchIndexResult',
    'DescribeSearchIndexRequest',
    'DescribeSearchIndexResult',

    # 其他操作
    'SQLQueryRequest',
    'SQLQueryResult',
    'ComputeSplitsRequest',
    'ComputeSplitsResult',
    'ParallelScanRequest',
    'ParallelScanResult',

    # 基础类型
    'OTSResult',
    'TableMeta',
]
