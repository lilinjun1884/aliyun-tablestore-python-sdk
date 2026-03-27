# -*- coding: utf8 -*-
"""
OTS C++ SDK Python 绑定的导入、基本功能和压力测试

测试内容：
1. 模块导入和 OTS_SDK_AVAILABLE 标志
2. 导入成功时的类型可用性验证
3. 客户端创建和基本请求访问（需要 OTS 测试环境）
4. 分阶段压力测试（基础 / 中等 / 高压）
"""

import gc
import math
import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List

from tests.lib import test_config
from tests.test_utils import make_table_name

# 检查测试环境是否配置
_CONFIG_READY = all([
    test_config.OTS_ENDPOINT,
    test_config.OTS_ACCESS_KEY_ID,
    test_config.OTS_ACCESS_KEY_SECRET,
    test_config.OTS_INSTANCE,
])
_SKIP_REASON = '缺少 OTS 测试环境变量 (OTS_TEST_ENDPOINT 等)'


class OtsSdkImportTest(unittest.TestCase):
    """OTS C++ SDK 导入测试"""

    def test_import_flag_exists(self):
        """OTS_SDK_AVAILABLE 标志应始终可访问，不论导入是否成功"""
        from tablestore.ots_sdk import OTS_SDK_AVAILABLE
        self.assertIsInstance(OTS_SDK_AVAILABLE, bool)

    def test_import_does_not_crash(self):
        """导入 ots_sdk 模块不应导致程序崩溃"""
        try:
            import tablestore.ots_sdk
        except SystemExit:
            self.fail("导入 tablestore.ots_sdk 导致程序退出")

    def test_all_exports_defined(self):
        """__all__ 中声明的所有符号都应存在（值可能为 None）"""
        import tablestore.ots_sdk as sdk
        for name in sdk.__all__:
            self.assertTrue(
                hasattr(sdk, name),
                f"__all__ 中声明的 '{name}' 在模块中不存在"
            )


class OtsSdkAvailabilityTest(unittest.TestCase):
    """OTS C++ SDK 可用性测试（仅在扩展加载成功时执行）"""

    @classmethod
    def setUpClass(cls):
        from tablestore.ots_sdk import OTS_SDK_AVAILABLE
        if not OTS_SDK_AVAILABLE:
            raise unittest.SkipTest(
                'OTS C++ SDK 扩展不可用（可能是平台不兼容或 .so 缺失）'
            )

    def test_core_classes_not_none(self):
        """导入成功时，核心类不应为 None"""
        from tablestore.ots_sdk import (
            OTSClient, ClientConfiguration, Credential,
        )
        self.assertIsNotNone(OTSClient)
        self.assertIsNotNone(ClientConfiguration)
        self.assertIsNotNone(Credential)

    def test_exception_classes_not_none(self):
        """导入成功时，异常类不应为 None"""
        from tablestore.ots_sdk import OTSException, OTSClientException
        self.assertIsNotNone(OTSException)
        self.assertIsNotNone(OTSClientException)

    def test_table_operation_classes_not_none(self):
        """导入成功时，表操作类不应为 None"""
        from tablestore.ots_sdk import (
            CreateTableRequest, CreateTableResult,
            ListTableResult,
            DescribeTableRequest, DescribeTableResult,
            DeleteTableRequest, DeleteTableResult,
            UpdateTableRequest, UpdateTableResult,
        )
        for cls in [
            CreateTableRequest, CreateTableResult,
            ListTableResult,
            DescribeTableRequest, DescribeTableResult,
            DeleteTableRequest, DeleteTableResult,
            UpdateTableRequest, UpdateTableResult,
        ]:
            self.assertIsNotNone(cls)

    def test_row_operation_classes_not_none(self):
        """导入成功时，行操作类不应为 None"""
        from tablestore.ots_sdk import (
            GetRowRequest, GetRowResult,
            PutRowRequest, PutRowResult,
            UpdateRowRequest, UpdateRowResult,
            DeleteRowRequest, DeleteRowResult,
            SingleRowQueryCriteria,
            RowPutChange, RowUpdateChange, RowDeleteChange,
        )
        for cls in [
            GetRowRequest, GetRowResult,
            PutRowRequest, PutRowResult,
            UpdateRowRequest, UpdateRowResult,
            DeleteRowRequest, DeleteRowResult,
            SingleRowQueryCriteria,
            RowPutChange, RowUpdateChange, RowDeleteChange,
        ]:
            self.assertIsNotNone(cls)

    def test_batch_operation_classes_not_none(self):
        """导入成功时，批量操作类不应为 None"""
        from tablestore.ots_sdk import (
            BatchGetRowRequest, BatchGetRowResult,
            BatchWriteRowRequest, BatchWriteRowResult,
            MultiRowQueryCriteria,
        )
        for cls in [
            BatchGetRowRequest, BatchGetRowResult,
            BatchWriteRowRequest, BatchWriteRowResult,
            MultiRowQueryCriteria,
        ]:
            self.assertIsNotNone(cls)

    def test_search_index_classes_not_none(self):
        """导入成功时，多元索引类不应为 None"""
        from tablestore.ots_sdk import (
            SearchRequest, SearchResult,
            CreateSearchIndexRequest, CreateSearchIndexResult,
            DeleteSearchIndexRequest, DeleteSearchIndexResult,
            ListSearchIndexRequest, ListSearchIndexResult,
            DescribeSearchIndexRequest, DescribeSearchIndexResult,
        )
        for cls in [
            SearchRequest, SearchResult,
            CreateSearchIndexRequest, CreateSearchIndexResult,
            DeleteSearchIndexRequest, DeleteSearchIndexResult,
            ListSearchIndexRequest, ListSearchIndexResult,
            DescribeSearchIndexRequest, DescribeSearchIndexResult,
        ]:
            self.assertIsNotNone(cls)

    def test_basic_type_classes_not_none(self):
        """导入成功时，基础类型类不应为 None"""
        from tablestore.ots_sdk import (
            OTSResult, TableMeta,
            PrimaryKey, PrimaryKeyValue, PrimaryKeyColumn,
            Column, ColumnValue,
            CapacityUnit, ConsumedCapacity,
            ReservedThroughput, TableOptions,
        )
        for cls in [
            OTSResult, TableMeta,
            PrimaryKey, PrimaryKeyValue, PrimaryKeyColumn,
            Column, ColumnValue,
            CapacityUnit, ConsumedCapacity,
            ReservedThroughput, TableOptions,
        ]:
            self.assertIsNotNone(cls)

    def test_credential_creation(self):
        """应能成功创建 Credential 对象"""
        from tablestore.ots_sdk import Credential
        credential = Credential('test_ak_id', 'test_ak_secret')
        self.assertIsNotNone(credential)

    def test_client_configuration_creation(self):
        """应能成功创建 ClientConfiguration 对象"""
        from tablestore.ots_sdk import ClientConfiguration
        config = ClientConfiguration()
        self.assertIsNotNone(config)


class OtsSdkRequestTest(unittest.TestCase):
    """OTS C++ SDK 请求访问测试（需要 OTS 测试环境和 SDK 可用）"""

    TABLE_NAME = 'OtsSdkTestTable'

    @classmethod
    def setUpClass(cls):
        from tablestore.ots_sdk import OTS_SDK_AVAILABLE
        if not OTS_SDK_AVAILABLE:
            raise unittest.SkipTest(
                'OTS C++ SDK 扩展不可用'
            )
        if not _CONFIG_READY:
            raise unittest.SkipTest(_SKIP_REASON)

        from tablestore.ots_sdk import (
            OTSClient, Credential, ClientConfiguration,
        )
        credential = Credential(
            test_config.OTS_ACCESS_KEY_ID,
            test_config.OTS_ACCESS_KEY_SECRET,
        )
        config = ClientConfiguration()
        cls.client = OTSClient(
            test_config.OTS_ENDPOINT,
            test_config.OTS_INSTANCE,
            credential,
            config,
        )
        cls.table_name_cd = make_table_name(cls.TABLE_NAME + 'CD')
        cls.table_name_row = make_table_name(cls.TABLE_NAME + 'Row')

    @classmethod
    def tearDownClass(cls):
        from tablestore.ots_sdk import DeleteTableRequest
        for t in [cls.table_name_cd, cls.table_name_row]:
            try:
                cls.client.deleteTable(DeleteTableRequest(t))
            except Exception:
                pass

    def _create_test_table(self, table_name):
        """创建测试表（包含单个 INTEGER 主键 'pk'）"""
        from tablestore.ots_sdk.ots_sdk import PrimaryKeyType
        from tablestore.ots_sdk import (
            CreateTableRequest, TableMeta,
            ReservedThroughput, TableOptions,
        )
        table_meta = TableMeta(table_name)
        table_meta.addPrimaryKeySchema('pk', PrimaryKeyType.PKT_INTEGER)
        reserved_throughput = ReservedThroughput(0, 0)
        table_options = TableOptions()
        table_options.setTimeToLive(-1)
        table_options.setMaxVersions(1)
        create_req = CreateTableRequest(table_meta, reserved_throughput, table_options)
        return self.client.createTable(create_req)

    def _delete_test_table(self, table_name):
        """删除测试表（忽略不存在的情况）"""
        from tablestore.ots_sdk import DeleteTableRequest
        try:
            self.client.deleteTable(DeleteTableRequest(table_name))
        except Exception:
            pass

    def test_list_table(self):
        """应能成功调用 listTable"""
        result = self.client.listTable()
        self.assertIsNotNone(result)
        table_names = result.tableNames()
        self.assertIsInstance(table_names, list)

    def test_create_and_delete_table(self):
        """应能成功创建和删除表"""
        table_name = self.table_name_cd

        # 创建表
        create_result = self._create_test_table(table_name)
        self.assertIsNotNone(create_result)
        time.sleep(2)

        # 验证表存在
        list_result = self.client.listTable()
        table_names = list_result.tableNames()
        self.assertIn(table_name, table_names)

        # 删除表
        from tablestore.ots_sdk import DeleteTableRequest
        delete_result = self.client.deleteTable(DeleteTableRequest(table_name))
        self.assertIsNotNone(delete_result)

    def test_put_and_get_row(self):
        """应能成功写入和读取单行数据"""
        from tablestore.ots_sdk import (
            PutRowRequest, GetRowRequest,
            RowPutChange, SingleRowQueryCriteria,
            PrimaryKey, PrimaryKeyValue,
            ColumnValue,
        )

        table_name = self.table_name_row

        self._create_test_table(table_name)
        time.sleep(2)

        # 构建主键
        primary_key = PrimaryKey()
        primary_key.addPrimaryKeyColumn('pk', PrimaryKeyValue(1))

        # 写入一行
        put_change = RowPutChange(table_name, primary_key)
        put_change.addColumn('name', ColumnValue('test_value'))
        put_change.addColumn('age', ColumnValue(25))
        put_result = self.client.putRow(PutRowRequest(put_change))
        self.assertIsNotNone(put_result)

        # 读取该行
        query = SingleRowQueryCriteria(table_name, primary_key)
        query.setMaxVersions(1)
        get_result = self.client.getRow(GetRowRequest(query))
        self.assertIsNotNone(get_result)

# ---------------------------------------------------------------------------
#  导入 C++ SDK 所需的压力测试类
# ---------------------------------------------------------------------------
# 以下类用于压力测试
from tablestore.ots_sdk import (
    PutRowRequest,
    GetRowRequest,
    GetRangeRequest,
    BatchGetRowRequest,
    SingleRowQueryCriteria,
    MultiRowQueryCriteria,
    RangeRowQueryCriteria,
    RowPutChange,
    PrimaryKey,
    PrimaryKeyValue,
    ColumnValue,
)


# ---------------------------------------------------------------------------
#  压力测试辅助工具
# ---------------------------------------------------------------------------
BATCH_SIZE = 100  # 单次批量操作最大行数
WARMUP_ROUNDS = 1000  # 预热轮次：正式测试前发送的预热请求数


@dataclass
class OtsSdkStressConfig:
    """OTS SDK 压力测试配置"""
    name: str
    total_rows: int
    concurrent_workers: int
    rows_per_batch: int = BATCH_SIZE
    row_size_kb: int = 4
    columns_count: int = 50
    total_requests: int = None


OTS_SDK_STAGE_CONFIGS = {
    'small': OtsSdkStressConfig(
        name='[C++ SDK] 小压力测试',
        total_rows=500,
        concurrent_workers=5,
        rows_per_batch=10,
        columns_count=3,
        row_size_kb=1,
    ),
    'basic': OtsSdkStressConfig(
        name='[C++ SDK] 基础压力测试',
        total_rows=5000,
        concurrent_workers=30,
    ),
    'basic_small_batch': OtsSdkStressConfig(
        name='[C++ SDK] 基础压力测试(小批次)',
        total_rows=5000,
        concurrent_workers=30,
        rows_per_batch=20,
    ),
    'basic_large_row': OtsSdkStressConfig(
        name='[C++ SDK] 基础压力测试(大行)',
        total_rows=2000,
        concurrent_workers=20,
        columns_count=100,
        row_size_kb=8,
    ),
    'medium': OtsSdkStressConfig(
        name='[C++ SDK] 中等压力测试',
        total_rows=50000,
        concurrent_workers=50,
    ),
    'medium_high_concurrency': OtsSdkStressConfig(
        name='[C++ SDK] 中等压力测试(高并发)',
        total_rows=50000,
        concurrent_workers=100,
        rows_per_batch=50,
    ),
    'high': OtsSdkStressConfig(
        name='[C++ SDK] 高压力测试',
        total_rows=500000,
        concurrent_workers=100,
    ),
    'very_high': OtsSdkStressConfig(
        name='[C++ SDK] 超高压力测试',
        total_rows=500000,
        concurrent_workers=500,
    ),
}


class OtsSdkLatencyStats:
    """线程安全的延时统计收集器，支持百分位数计算。"""

    def __init__(self):
        self._latencies: List[float] = []
        self._lock = threading.Lock()

    def record(self, latency_ms: float):
        """记录一次请求延时（毫秒）。"""
        with self._lock:
            self._latencies.append(latency_ms)

    @property
    def count(self) -> int:
        return len(self._latencies)

    def percentile(self, pct: float) -> float:
        """计算指定百分位数（0-100）的延时值。"""
        if not self._latencies:
            return 0.0
        sorted_data = sorted(self._latencies)
        rank = pct / 100.0 * (len(sorted_data) - 1)
        lower_index = int(math.floor(rank))
        upper_index = int(math.ceil(rank))
        if lower_index == upper_index:
            return sorted_data[lower_index]
        fraction = rank - lower_index
        return sorted_data[lower_index] * (1 - fraction) + sorted_data[upper_index] * fraction

    def summary(self) -> dict:
        """返回完整的延时统计摘要。"""
        if not self._latencies:
            return {}
        sorted_data = sorted(self._latencies)
        total = sum(sorted_data)
        count = len(sorted_data)
        avg = total / count
        return {
            'count': count,
            'min_ms': sorted_data[0],
            'max_ms': sorted_data[-1],
            'avg_ms': avg,
            'p50_ms': self.percentile(50),
            'p75_ms': self.percentile(75),
            'p90_ms': self.percentile(90),
            'p95_ms': self.percentile(95),
            'p99_ms': self.percentile(99),
            'std_dev_ms': (
                sum((v - avg) ** 2 for v in sorted_data) / count
            ) ** 0.5,
        }

    def report(self, title: str = '延时统计'):
        """打印格式化的延时报告。"""
        stats = self.summary()
        if not stats:
            _stress_log(f"{title}: 无数据", 'WARN')
            return
        _stress_log(f"┌─── {title} ({stats['count']} 次请求) ───")
        _stress_log(f"│  Min    = {stats['min_ms']:>10.2f} ms")
        _stress_log(f"│  Avg    = {stats['avg_ms']:>10.2f} ms")
        _stress_log(f"│  StdDev = {stats['std_dev_ms']:>10.2f} ms")
        _stress_log(f"│  P50    = {stats['p50_ms']:>10.2f} ms")
        _stress_log(f"│  P75    = {stats['p75_ms']:>10.2f} ms")
        _stress_log(f"│  P90    = {stats['p90_ms']:>10.2f} ms")
        _stress_log(f"│  P95    = {stats['p95_ms']:>10.2f} ms")
        _stress_log(f"│  P99    = {stats['p99_ms']:>10.2f} ms")
        _stress_log(f"│  Max    = {stats['max_ms']:>10.2f} ms")
        _stress_log(f"└{'─' * 40}")


def _stress_log(message: str, level: str = 'INFO'):
    """打印带时间戳的进度日志"""
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}")


def _new_ots_sdk_client():
    """创建一个 C++ SDK 的 OTSClient。"""
    from tablestore.ots_sdk import OTS_SDK_AVAILABLE
    if not OTS_SDK_AVAILABLE:
        raise unittest.SkipTest('OTS C++ SDK 扩展不可用')
    if not _CONFIG_READY:
        raise unittest.SkipTest(_SKIP_REASON)

    from tablestore.ots_sdk import OTSClient, Credential, ClientConfiguration
    credential = Credential(
        test_config.OTS_ACCESS_KEY_ID,
        test_config.OTS_ACCESS_KEY_SECRET,
    )
    config = ClientConfiguration()
    return OTSClient(
        test_config.OTS_ENDPOINT,
        test_config.OTS_INSTANCE,
        credential,
        config,
    )


def _ensure_ots_sdk_table(client, table_name):
    """确保测试表存在（先删后建），使用 C++ SDK API。"""
    from tablestore.ots_sdk import (
        CreateTableRequest, DeleteTableRequest,
        TableMeta, ReservedThroughput, TableOptions,
    )
    from tablestore.ots_sdk.ots_sdk import PrimaryKeyType

    # 先尝试删除
    try:
        table_names = client.listTable().tableNames()
        if table_name in table_names:
            client.deleteTable(DeleteTableRequest(table_name))
            time.sleep(1)
    except Exception:
        pass

    # 创建表：双主键 (gid INTEGER, uid INTEGER)
    table_meta = TableMeta(table_name)
    table_meta.addPrimaryKeySchema('gid', PrimaryKeyType.PKT_INTEGER)
    table_meta.addPrimaryKeySchema('uid', PrimaryKeyType.PKT_INTEGER)
    reserved_throughput = ReservedThroughput(0, 0)
    table_options = TableOptions()
    table_options.setTimeToLive(-1)
    table_options.setMaxVersions(1)
    client.createTable(CreateTableRequest(table_meta, reserved_throughput, table_options))
    time.sleep(1)


def _warmup_ots_sdk_connection(client, table_name, rounds=WARMUP_ROUNDS):
    """
    预热 C++ SDK 连接池和编解码路径。

    通过发送少量写入+读取请求，预先建立底层连接（TCP 握手 + TLS 协商），
    同时预热 C++ SDK 的序列化/反序列化路径，避免首批正式请求承受冷启动开销。
    """
    _stress_log(f"连接池预热开始 ({rounds} 轮)...")
    for i in range(rounds):
        warmup_gid = 999_000_000 + i
        primary_key = PrimaryKey()
        primary_key.addPrimaryKeyColumn('gid', PrimaryKeyValue(warmup_gid))
        primary_key.addPrimaryKeyColumn('uid', PrimaryKeyValue(warmup_gid))

        put_change = RowPutChange(table_name, primary_key)
        put_change.addColumn('warmup', ColumnValue(f'warmup_{i}'))
        try:
            client.putRow(PutRowRequest(put_change))
        except Exception:
            pass

        query = SingleRowQueryCriteria(table_name, primary_key)
        query.setMaxVersions(1)
        try:
            client.getRow(GetRowRequest(query))
        except Exception:
            pass
    _stress_log(f"连接池预热完成")


def _make_ots_sdk_batch_write(table_name, start_gid, count, columns_count=0):
    """
    创建一个 C++ SDK 的 BatchWriteRowRequest，最多写 BATCH_SIZE 行。

    columns_count > 0 时生成 columns_count 列，每列约 80 字节，
    50 列 ≈ 4 KB；columns_count == 0 时使用轻量 3 列。
    """
    from tablestore.ots_sdk import (
        BatchWriteRowRequest, RowPutChange,
        PrimaryKey, PrimaryKeyValue, ColumnValue,
    )

    count = min(count, BATCH_SIZE)
    request = BatchWriteRowRequest()

    for i in range(count):
        gid = start_gid + i
        primary_key = PrimaryKey()
        primary_key.addPrimaryKeyColumn('gid', PrimaryKeyValue(gid))
        primary_key.addPrimaryKeyColumn('uid', PrimaryKeyValue(gid))

        put_change = RowPutChange(table_name, primary_key)

        if columns_count > 0:
            for col_idx in range(columns_count):
                put_change.addColumn(
                    f'col_{col_idx:03d}',
                    ColumnValue(f'd_{gid}_{gid}_{col_idx}_' + 'x' * 60),
                )
        else:
            put_change.addColumn('name', ColumnValue(f'user_{gid}_{gid}'))
            put_change.addColumn('age', ColumnValue(gid % 100))
            put_change.addColumn('timestamp', ColumnValue(int(time.time())))

        request.addRowPutChange(put_change)

    return request

def _write_seed_data(client, table_name, total_rows, columns_count=0):
    """
    用 batchWriteRow 批量写入种子数据，供读操作测试使用。

    Args:
        client: C++ SDK 客户端
        table_name: 表名
        total_rows: 要写入的总行数
        columns_count: 每行的列数，0 表示使用轻量 3 列
    """
    from tablestore.ots_sdk import BatchWriteRowRequest

    _stress_log(f"开始写入种子数据: {total_rows} 行...")
    written = 0
    start_gid = 0

    while written < total_rows:
        batch_size = min(BATCH_SIZE, total_rows - written)
        request = _make_ots_sdk_batch_write(table_name, start_gid, batch_size, columns_count)
        client.batchWriteRow(request)
        written += batch_size
        start_gid += batch_size

        if written % 1000 < batch_size:
            _stress_log(f"种子数据写入进度: {written}/{total_rows}")

    _stress_log(f"种子数据写入完成: {written} 行")


# ---------------------------------------------------------------------------
#  OTS C++ SDK 分阶段压力测试
# ---------------------------------------------------------------------------
class OtsSdkStageStressTest(unittest.TestCase):
    """OTS C++ SDK 分阶段压力测试：基础 / 中等 / 高压"""

    TABLE_NAME = make_table_name('OtsSdkStageStressTable')

    @classmethod
    def setUpClass(cls):
        cls.client = _new_ots_sdk_client()
        _ensure_ots_sdk_table(cls.client, cls.TABLE_NAME)
        _warmup_ots_sdk_connection(cls.client, cls.TABLE_NAME)

    @classmethod
    def tearDownClass(cls):
        from tablestore.ots_sdk import DeleteTableRequest
        try:
            cls.client.deleteTable(DeleteTableRequest(cls.TABLE_NAME))
        except Exception:
            pass

    def _run_stage(self, config: OtsSdkStressConfig, requests, send_fn, operation_name='batch_write'):
        """
        执行一个阶段的压力测试。

        通用的压力测试引擎，接受预先构建好的 requests 列表和发送函数。
        requests: list 类型，每个元素是 (request_data, expected_rows) 元组
        send_fn: callable，签名为 send_fn(request_data)，负责发送请求

        预热策略：正式计时前用少量请求预热线程池和编解码路径，
        预热数据不计入正式统计。正式测试期间禁用 GC 以避免暂停干扰延时数据。
        """
        _stress_log(f"===== {config.name} 开始 =====")
        _stress_log(f"操作类型={operation_name}, 总行数={config.total_rows}, 并发={config.concurrent_workers}, "
                     f"每行≈{config.row_size_kb}KB ({config.columns_count}列)")

        total_requests = len(requests)
        _stress_log(f"总请求数={total_requests}")

        # ---- 引擎预热：用前几个请求预热线程池和编解码路径 ----
        warmup_count = min(WARMUP_ROUNDS, total_requests)
        if warmup_count > 0:
            _stress_log(f"引擎预热: 发送 {warmup_count} 个请求...")
            with ThreadPoolExecutor(max_workers=min(config.concurrent_workers, warmup_count)) as warmup_pool:
                warmup_futures = []
                for i in range(warmup_count):
                    request_data, _ = requests[i]
                    warmup_futures.append(warmup_pool.submit(send_fn, request_data))
                for future in as_completed(warmup_futures, timeout=1200):
                    try:
                        future.result()
                    except Exception:
                        pass
            _stress_log(f"引擎预热完成")

        # ---- GC 控制：正式测试期间禁用 GC ----
        gc.collect()
        gc_was_enabled = gc.isenabled()
        gc.disable()

        stats = {'processed': 0, 'failed': 0, 'errors': []}
        latency_stats = OtsSdkLatencyStats()
        lock = threading.Lock()
        start_time = time.time()
        log_interval = max(config.total_rows // 10, 1)

        try:
            def _send_request(request_index):
                """发送一次请求，从预构建的 requests 列表中获取。"""
                try:
                    request_start = time.monotonic()
                    request_data, expected_rows = requests[request_index]
                    send_fn(request_data)
                    request_elapsed_ms = (time.monotonic() - request_start) * 1000
                    latency_stats.record(request_elapsed_ms)
                    with lock:
                        stats['processed'] += expected_rows
                        current = stats['processed']
                        if current % log_interval < expected_rows:
                            elapsed = time.time() - start_time
                            speed = current / elapsed if elapsed > 0 else 0
                            _stress_log(f"进度 {current}/{config.total_rows} "
                                         f"({current * 100 / config.total_rows:.1f}%), "
                                         f"速度 {speed:.0f} 行/秒")
                except Exception as exc:
                    with lock:
                        stats['failed'] += 1
                        stats['errors'].append(str(exc))
                        _stress_log(f"{operation_name} 失败 request={request_index}: {exc}", 'ERROR')

            with ThreadPoolExecutor(max_workers=config.concurrent_workers) as pool:
                futures = [
                    pool.submit(_send_request, i)
                    for i in range(total_requests)
                ]
                for future in as_completed(futures, timeout=1200):
                    try:
                        future.result()
                    except Exception as exc:
                        _stress_log(f"Worker 异常: {exc}", 'ERROR')
        finally:
            if gc_was_enabled:
                gc.enable()
            gc.collect()

        elapsed = time.time() - start_time
        total = stats['processed'] + stats['failed']
        success_rate = stats['processed'] / config.total_rows * 100 if config.total_rows else 0

        _stress_log(f"===== {config.name} 完成 =====", 'SUCCESS')
        _stress_log(f"操作={operation_name}, 成功={stats['processed']}, 失败={stats['failed']}, "
                     f"成功率={success_rate:.2f}%", 'SUCCESS')
        _stress_log(f"耗时={elapsed:.2f}s, "
                     f"平均速度={total / elapsed:.0f} 行/秒", 'SUCCESS')

        latency_stats.report(f"{config.name} {operation_name} 请求延时")

        if stats['errors']:
            _stress_log(f"前 5 条错误: {stats['errors'][:5]}", 'ERROR')

        self.assertGreaterEqual(
            success_rate, 95,
            f"{config.name} {operation_name} 成功率 {success_rate:.2f}% 低于 95%"
        )

    # ---- 辅助方法：预构建 batch_write 请求 ----
    def _build_batch_write_requests(self, config):
        """预构建所有 batch_write 请求"""
        total_requests = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
        requests = []
        for i in range(total_requests):
            start_gid = i * config.rows_per_batch
            actual_batch = min(config.rows_per_batch, config.total_rows - start_gid)
            if actual_batch <= 0:
                break
            req = _make_ots_sdk_batch_write(self.TABLE_NAME, start_gid, actual_batch, config.columns_count)
            requests.append((req, actual_batch))
        return requests

    def test_stage_small(self):
        """[C++ SDK] 小压力：500 行 × 1KB，5 并发，batch=10"""
        config = OTS_SDK_STAGE_CONFIGS['small']
        requests = self._build_batch_write_requests(config)
        _stress_log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batchWriteRow, operation_name='batch_write')

    def test_stage_basic(self):
        """[C++ SDK] 基础压力：5000 行 × 4KB，30 并发"""
        config = OTS_SDK_STAGE_CONFIGS['basic']
        requests = self._build_batch_write_requests(config)
        _stress_log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batchWriteRow, operation_name='batch_write')

    def test_stage_basic_small_batch(self):
        """[C++ SDK] 基础压力(小批次)：5000 行 × 4KB，30 并发，batch=20"""
        config = OTS_SDK_STAGE_CONFIGS['basic_small_batch']
        requests = self._build_batch_write_requests(config)
        _stress_log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batchWriteRow, operation_name='batch_write')

    def test_stage_basic_large_row(self):
        """[C++ SDK] 基础压力(大行)：2000 行 × 8KB，20 并发，100 列"""
        config = OTS_SDK_STAGE_CONFIGS['basic_large_row']
        requests = self._build_batch_write_requests(config)
        _stress_log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batchWriteRow, operation_name='batch_write')

    def test_stage_medium(self):
        """[C++ SDK] 中等压力：5 万行 × 4KB，50 并发"""
        config = OTS_SDK_STAGE_CONFIGS['medium']
        requests = self._build_batch_write_requests(config)
        _stress_log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batchWriteRow, operation_name='batch_write')

    def test_stage_medium_high_concurrency(self):
        """[C++ SDK] 中等压力(高并发)：5 万行 × 4KB，100 并发，batch=50"""
        config = OTS_SDK_STAGE_CONFIGS['medium_high_concurrency']
        requests = self._build_batch_write_requests(config)
        _stress_log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batchWriteRow, operation_name='batch_write')

    def test_stage_high(self):
        """[C++ SDK] 高压力：50 万行 × 4KB，100 并发"""
        config = OTS_SDK_STAGE_CONFIGS['high']
        requests = self._build_batch_write_requests(config)
        _stress_log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batchWriteRow, operation_name='batch_write')

    def test_stage_very_high(self):
        """[C++ SDK] 超高压力：50 万行 × 4KB，500 并发"""
        config = OTS_SDK_STAGE_CONFIGS['very_high']
        requests = self._build_batch_write_requests(config)
        _stress_log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batchWriteRow, operation_name='batch_write')

    def test_put_row_basic(self):
        """[C++ SDK] put_row 基础压力测试"""
        config = OtsSdkStressConfig(
            name='[C++ SDK] put_row 基础压力',
            total_rows=20000,
            concurrent_workers=300,
            rows_per_batch=1,
            columns_count=50,
            row_size_kb=4,
        )
        
        # 预构建所有 put_row 请求
        requests = []
        for gid in range(config.total_rows):
            primary_key = PrimaryKey()
            primary_key.addPrimaryKeyColumn('gid', PrimaryKeyValue(gid))
            primary_key.addPrimaryKeyColumn('uid', PrimaryKeyValue(gid))
            put_change = RowPutChange(self.TABLE_NAME, primary_key)
            for col_idx in range(config.columns_count):
                put_change.addColumn(f'col_{col_idx:03d}', ColumnValue(f'd_{gid}_{gid}_{col_idx}_' + 'x' * 60))
            requests.append((PutRowRequest(put_change), 1))
        
        _stress_log(f"预构建 {len(requests)} 个 put_row 请求...")
        self._run_stage(config, requests, self.client.putRow, operation_name='put_row')

    def test_get_row_basic(self):
        """[C++ SDK] get_row 基础压力测试"""
        # 先写入种子数据
        _write_seed_data(self.client, self.TABLE_NAME, 5000, columns_count=50)
        
        config = OtsSdkStressConfig(
            name='[C++ SDK] get_row 基础压力',
            total_rows=5000,
            concurrent_workers=30,
            rows_per_batch=1,
        )
        
        # 预构建所有 get_row 请求
        requests = []
        for gid in range(config.total_rows):
            primary_key = PrimaryKey()
            primary_key.addPrimaryKeyColumn('gid', PrimaryKeyValue(gid))
            primary_key.addPrimaryKeyColumn('uid', PrimaryKeyValue(gid))
            query = SingleRowQueryCriteria(self.TABLE_NAME, primary_key)
            query.setMaxVersions(1)
            requests.append((GetRowRequest(query), 1))
        
        _stress_log(f"预构建 {len(requests)} 个 get_row 请求...")
        self._run_stage(config, requests, self.client.getRow, operation_name='get_row')

    @unittest.skipUnless(
        hasattr(MultiRowQueryCriteria, 'setMaxVersions'),
        'C++ SDK MultiRowQueryCriteria 尚未绑定 setMaxVersions 方法'
    )
    def test_batch_get_row_basic(self):
        """[C++ SDK] batch_get_row 基础压力测试"""
        # 先写入种子数据
        _write_seed_data(self.client, self.TABLE_NAME, 5000, columns_count=50)

        config = OtsSdkStressConfig(
            name='[C++ SDK] batch_get_row 基础压力',
            total_rows=50000,
            concurrent_workers=100,
            rows_per_batch=100,
        )

        # 预构建所有 batch_get_row 请求
        total_requests = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
        requests = []
        for i in range(total_requests):
            start_gid = i * config.rows_per_batch
            actual_batch = min(config.rows_per_batch, config.total_rows - start_gid)
            if actual_batch <= 0:
                break
            request = BatchGetRowRequest()
            criteria = MultiRowQueryCriteria(self.TABLE_NAME)
            criteria.setMaxVersions(1)
            for j in range(actual_batch):
                gid = start_gid + j
                primary_key = PrimaryKey()
                primary_key.addPrimaryKeyColumn('gid', PrimaryKeyValue(gid))
                primary_key.addPrimaryKeyColumn('uid', PrimaryKeyValue(gid))
                criteria.addPrimaryKey(primary_key)
            request.addCriteria(criteria)
            requests.append((request, actual_batch))
        
        _stress_log(f"预构建 {len(requests)} 个 batch_get_row 请求...")
        self._run_stage(config, requests, self.client.batchGetRow, operation_name='batch_get_row')

    @unittest.skipUnless(
        hasattr(RangeRowQueryCriteria, 'setDirection'),
        'C++ SDK RangeRowQueryCriteria 尚未绑定 setDirection 方法'
    )
    def test_get_range_basic(self):
        """[C++ SDK] get_range 基础压力测试"""
        # 先写入种子数据
        _write_seed_data(self.client, self.TABLE_NAME, 5000, columns_count=50)

        config = OtsSdkStressConfig(
            name='[C++ SDK] get_range 基础压力',
            total_rows=5000,
            concurrent_workers=30,
            rows_per_batch=100,
        )

        # 预构建所有 get_range 请求
        total_requests = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
        requests = []
        for i in range(total_requests):
            start_gid = i * config.rows_per_batch
            end_gid = start_gid + config.rows_per_batch
            if start_gid >= config.total_rows:
                break
            start_pk = PrimaryKey()
            start_pk.addPrimaryKeyColumn('gid', PrimaryKeyValue(start_gid))
            start_pk.addPrimaryKeyColumn('uid', PrimaryKeyValue(start_gid))
            end_pk = PrimaryKey()
            end_pk.addPrimaryKeyColumn('gid', PrimaryKeyValue(min(end_gid, config.total_rows)))
            end_pk.addPrimaryKeyColumn('uid', PrimaryKeyValue(min(end_gid, config.total_rows)))
            criteria = RangeRowQueryCriteria(self.TABLE_NAME)
            criteria.setDirection('FORWARD')
            criteria.setInclusiveStartPrimaryKey(start_pk)
            criteria.setExclusiveEndPrimaryKey(end_pk)
            criteria.setLimit(config.rows_per_batch)
            actual_rows = min(config.rows_per_batch, config.total_rows - start_gid)
            requests.append((GetRangeRequest(criteria), actual_rows))
        
        _stress_log(f"预构建 {len(requests)} 个 get_range 请求...")
        self._run_stage(config, requests, self.client.getRange, operation_name='get_range')


if __name__ == '__main__':
    unittest.main()
