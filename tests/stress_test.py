# -*- coding: utf8 -*-
"""
压力测试模块

包含并发场景和异步场景的压力测试用例
支持分阶段压力测试：
- 基础压力：5000 行 × 4KB，30 并发
- 中等压力：5 万行 × 4KB，50 并发
- 高压力：50 万行 × 4KB，100 并发
"""

import asyncio
import gc
import math
import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List

import pytest

import tablestore
from tablestore import (
    OTSClient, AsyncOTSClient,
    TableMeta, TableOptions, ReservedThroughput, CapacityUnit,
    Row, Condition, RowExistenceExpectation, PutRowItem,
    BatchWriteRowRequest, TableInBatchWriteRowItem,
    BatchGetRowRequest, TableInBatchGetRowItem,
    INF_MIN, INF_MAX,
    SearchQuery, MatchAllQuery, TermQuery,
    FieldSchema, FieldType, SearchIndexMeta, IndexSetting,
    Sort, PrimaryKeySort, SortOrder,
    ColumnsToGet, ColumnReturnType,
    ScanQuery,
)
from tests.lib import test_config
from tests.test_utils import make_table_name

# 直接引用原始 OTSClient，避免被 tests/__init__.py 中的 RandomOTSClient 影响
_OriginalOTSClient = tablestore.client.OTSClient
_OriginalAsyncOTSClient = tablestore.client.AsyncOTSClient

BATCH_SIZE = 100  # 单次批量操作最大行数
WARMUP_ROUNDS = 1000  # 预热轮次：正式测试前发送的预热请求数

# 检查测试环境是否配置
_CONFIG_READY = all([
    test_config.OTS_ENDPOINT,
    test_config.OTS_ACCESS_KEY_ID,
    test_config.OTS_ACCESS_KEY_SECRET,
    test_config.OTS_INSTANCE,
])
_SKIP_REASON = '缺少 OTS 测试环境变量 (OTS_TEST_ENDPOINT 等)'


@dataclass
class StressTestConfig:
    """压力测试配置"""
    name: str
    total_rows: int
    concurrent_workers: int
    rows_per_batch: int = BATCH_SIZE
    row_size_kb: int = 4
    columns_count: int = 50
    total_requests: int = None


STAGE_CONFIGS = {
    'small': StressTestConfig(
        name='小压力测试',
        total_rows=500,
        concurrent_workers=5,
        rows_per_batch=10,
        columns_count=3,
        row_size_kb=1,
    ),
    'basic': StressTestConfig(
        name='基础压力测试',
        total_rows=5000,
        concurrent_workers=30,
    ),
    'basic_small_batch': StressTestConfig(
        name='基础压力测试(小批次)',
        total_rows=5000,
        concurrent_workers=30,
        rows_per_batch=20,
    ),
    'basic_large_row': StressTestConfig(
        name='基础压力测试(大行)',
        total_rows=2000,
        concurrent_workers=20,
        columns_count=100,
        row_size_kb=8,
    ),
    'medium': StressTestConfig(
        name='中等压力测试',
        total_rows=50000,
        concurrent_workers=50,
    ),
    'medium_high_concurrency': StressTestConfig(
        name='中等压力测试(高并发)',
        total_rows=50000,
        concurrent_workers=100,
        rows_per_batch=50,
    ),
    'high': StressTestConfig(
        name='高压力测试',
        total_rows=50000,
        concurrent_workers=100,
    ),
    'very_high': StressTestConfig(
        name='超高压力测试',
        total_rows=50000,
        concurrent_workers=500,
    ),
}


class LatencyStats:
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
        return {
            'count': len(sorted_data),
            'min_ms': sorted_data[0],
            'max_ms': sorted_data[-1],
            'avg_ms': total / len(sorted_data),
            'p50_ms': self.percentile(50),
            'p75_ms': self.percentile(75),
            'p90_ms': self.percentile(90),
            'p95_ms': self.percentile(95),
            'p99_ms': self.percentile(99),
            'std_dev_ms': (
                sum((v - total / len(sorted_data)) ** 2 for v in sorted_data)
                / len(sorted_data)
            ) ** 0.5,
        }

    def report(self, title: str = '延时统计'):
        """打印格式化的延时报告。"""
        stats = self.summary()
        if not stats:
            _log(f"{title}: 无数据", 'WARN')
            return
        _log(f"┌─── {title} ({stats['count']} 次请求) ───")
        _log(f"│  Min    = {stats['min_ms']:>10.2f} ms")
        _log(f"│  Avg    = {stats['avg_ms']:>10.2f} ms")
        _log(f"│  StdDev = {stats['std_dev_ms']:>10.2f} ms")
        _log(f"│  P50    = {stats['p50_ms']:>10.2f} ms")
        _log(f"│  P75    = {stats['p75_ms']:>10.2f} ms")
        _log(f"│  P90    = {stats['p90_ms']:>10.2f} ms")
        _log(f"│  P95    = {stats['p95_ms']:>10.2f} ms")
        _log(f"│  P99    = {stats['p99_ms']:>10.2f} ms")
        _log(f"│  Max    = {stats['max_ms']:>10.2f} ms")
        _log(f"└{'─' * 40}")


def _log(message: str, level: str = 'INFO'):
    """打印带时间戳的进度日志"""
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}")


def _make_row_item(gid, uid, columns_count=0):
    """
    创建单行写入数据。

    columns_count > 0 时生成 columns_count 列，每列约 80 字节，
    50 列 ≈ 4 KB；columns_count == 0 时使用轻量 3 列。
    """
    primary_key = [('gid', gid), ('uid', uid)]

    if columns_count > 0:
        attribute_columns = [
            (f'col_{i:03d}', f'd_{gid}_{uid}_{i}_' + 'x' * 60)
            for i in range(columns_count)
        ]
    else:
        attribute_columns = [
            ('name', f'user_{gid}_{uid}'),
            ('age', gid % 100),
            ('timestamp', int(time.time())),
        ]

    row = Row(primary_key, attribute_columns)
    condition = Condition(RowExistenceExpectation.IGNORE)
    return PutRowItem(row, condition)


def _make_batch_write(table_name, start_gid, count, columns_count=0):
    """
    创建一个 BatchWriteRowRequest，最多写 BATCH_SIZE 行。
    """
    count = min(count, BATCH_SIZE)
    items = [
        _make_row_item(start_gid + i, start_gid + i, columns_count)
        for i in range(count)
    ]
    request = BatchWriteRowRequest()
    request.add(TableInBatchWriteRowItem(table_name, items))
    return request


def _make_batch_get(table_name, start_gid, count):
    """
    创建一个 BatchGetRowRequest，最多读 BATCH_SIZE 行。
    """
    count = min(count, BATCH_SIZE)
    rows_to_get = [
        [('gid', start_gid + i), ('uid', start_gid + i)]
        for i in range(count)
    ]
    request = BatchGetRowRequest()
    request.add(TableInBatchGetRowItem(table_name, rows_to_get, max_version=1))
    return request


def _new_client():
    """创建一个原始 OTSClient（绕过 RandomOTSClient）。"""
    if not _CONFIG_READY:
        raise unittest.SkipTest(_SKIP_REASON)
    max_conn = int(os.environ.get('OTS_MAX_CONNECTION', '500'))
    _log(f"创建 OTSClient: max_connection={max_conn}")
    return _OriginalOTSClient(
        test_config.OTS_ENDPOINT,
        test_config.OTS_ACCESS_KEY_ID,
        test_config.OTS_ACCESS_KEY_SECRET,
        test_config.OTS_INSTANCE,
        region=test_config.OTS_REGION,
        max_connection=max_conn,
        enable_native=test_config.OTS_ENABLE_NATIVE,
        native_fallback=test_config.OTS_NATIVE_FALLBACK,
    )


def _ensure_table(client, table_name):
    """确保测试表存在（先删除表上所有索引，再删表，最后重建）。"""
    try:
        if table_name in client.list_table():
            # 先删除表上所有 search index
            try:
                for _, index_name in client.list_search_index(table_name):
                    try:
                        client.delete_search_index(table_name, index_name)
                    except Exception:
                        pass
            except Exception:
                pass
            client.delete_table(table_name)
    except Exception:
        pass

    schema = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
    meta = TableMeta(table_name, schema)
    options = TableOptions()
    throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(meta, options, throughput)
    time.sleep(1)


def _warmup_connection_pool(client, table_name, rounds=WARMUP_ROUNDS):
    """
    预热连接池和编解码路径。

    通过发送少量写入+读取请求，预先建立 HTTP 连接（TCP 握手 + TLS 协商），
    同时预热 Python 解释器的编解码执行路径，避免首批正式请求承受冷启动开销。
    """
    _log(f"连接池预热开始 ({rounds} 轮)...")
    for i in range(rounds):
        warmup_gid = 999_000_000 + i
        item = _make_row_item(warmup_gid, warmup_gid, 3)
        try:
            client.put_row(table_name, item.row, item.condition)
        except Exception:
            pass
        try:
            client.get_row(table_name, [('gid', warmup_gid), ('uid', warmup_gid)], max_version=1)
        except Exception:
            pass
    _log(f"连接池预热完成")


# ---------------------------------------------------------------------------
#  分阶段压力测试
# ---------------------------------------------------------------------------
class StageStressTest(unittest.TestCase):
    """分阶段压力测试：基础 / 中等 / 高压"""

    TABLE_NAME = make_table_name('StageStressTest')

    @classmethod
    def setUpClass(cls):
        cls.client = _new_client()
        _ensure_table(cls.client, cls.TABLE_NAME)
        _warmup_connection_pool(cls.client, cls.TABLE_NAME)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.delete_table(cls.TABLE_NAME)
        except Exception:
            pass

    # ---- 核心执行引擎 ----
    def _run_stage(self, config: StressTestConfig, requests, send_fn, operation_name='batch_write'):
        """
        执行一个阶段的压力测试（通用引擎）。

        requests: list，每个元素是 (request_data, expected_rows) 元组
            request_data: 预构建好的请求数据
            expected_rows: 该请求预期处理的行数
        send_fn: callable，签名为 send_fn(request_data)，负责发送请求
        operation_name: 操作名称，用于日志显示

        预热策略：正式计时前用少量请求预热线程池、编解码路径和服务端缓存，
        预热数据不计入正式统计。正式测试期间禁用 GC 以避免暂停干扰延时数据。
        """
        total_requests = len(requests)

        _log(f"===== {config.name} ({operation_name}) 开始 =====")
        _log(f"总行数={config.total_rows}, 请求数={total_requests}, 并发={config.concurrent_workers}, "
             f"每行≈{config.row_size_kb}KB ({config.columns_count}列)")

        # ---- 引擎预热：用前几个请求预热线程池和编解码路径 ----
        warmup_count = min(WARMUP_ROUNDS, total_requests)
        if warmup_count > 0:
            _log(f"引擎预热: 发送 {warmup_count} 个请求...")
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
            _log(f"引擎预热完成")

        # ---- GC 控制：正式测试期间禁用 GC ----
        gc.collect()
        gc_was_enabled = gc.isenabled()
        gc.disable()

        stats = {'processed': 0, 'failed': 0, 'errors': []}
        latency_stats = LatencyStats()
        lock = threading.Lock()
        start_time = time.time()
        log_interval = max(config.total_rows // 10, 1)

        try:
            def _send_request(request_index):
                """发送一次请求，从预构建的 requests 中取出请求数据。"""
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
                            _log(f"进度 {current}/{config.total_rows} "
                                 f"({current * 100 / config.total_rows:.1f}%), "
                                 f"速度 {speed:.0f} 行/秒")
                except Exception as exc:
                    with lock:
                        _, expected_rows = requests[request_index]
                        stats['failed'] += expected_rows
                        stats['errors'].append(str(exc))
                        _log(f"{operation_name} 失败 request={request_index}: {exc}", 'ERROR')

            with ThreadPoolExecutor(max_workers=config.concurrent_workers) as pool:
                futures = [
                    pool.submit(_send_request, i)
                    for i in range(total_requests)
                ]
                for future in as_completed(futures, timeout=1200):
                    try:
                        future.result()
                    except Exception as exc:
                        _log(f"Worker 异常: {exc}", 'ERROR')
        finally:
            if gc_was_enabled:
                gc.enable()
            gc.collect()

        elapsed = time.time() - start_time
        total = stats['processed'] + stats['failed']
        success_rate = stats['processed'] / config.total_rows * 100 if config.total_rows else 0

        _log(f"===== {config.name} ({operation_name}) 完成 =====", 'SUCCESS')
        _log(f"成功={stats['processed']}, 失败={stats['failed']}, "
             f"成功率={success_rate:.2f}%", 'SUCCESS')
        _log(f"耗时={elapsed:.2f}s, "
             f"平均速度={total / elapsed:.0f} 行/秒", 'SUCCESS')

        latency_stats.report(f"{config.name} {operation_name} 请求延时")

        if stats['errors']:
            _log(f"前 5 条错误: {stats['errors'][:5]}", 'ERROR')

        self.assertGreaterEqual(
            success_rate, 95,
            f"{config.name} ({operation_name}) 成功率 {success_rate:.2f}% 低于 95%"
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
            req = _make_batch_write(self.TABLE_NAME, start_gid, actual_batch, config.columns_count)
            requests.append((req, actual_batch))
        return requests

    # ---- 各阶段 ----
    def test_stage_small(self):
        """小压力：500 行 × 1KB，5 并发，batch=10"""
        config = STAGE_CONFIGS['small']
        requests = self._build_batch_write_requests(config)
        _log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batch_write_row, operation_name='batch_write')

    def test_stage_basic(self):
        """基础压力：5000 行 × 4KB，30 并发"""
        config = STAGE_CONFIGS['basic']
        requests = self._build_batch_write_requests(config)
        _log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batch_write_row, operation_name='batch_write')

    def test_stage_basic_small_batch(self):
        """基础压力(小批次)：5000 行 × 4KB，30 并发，batch=20"""
        config = STAGE_CONFIGS['basic_small_batch']
        requests = self._build_batch_write_requests(config)
        _log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batch_write_row, operation_name='batch_write')

    def test_stage_basic_large_row(self):
        """基础压力(大行)：2000 行 × 8KB，20 并发，100 列"""
        config = STAGE_CONFIGS['basic_large_row']
        requests = self._build_batch_write_requests(config)
        _log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batch_write_row, operation_name='batch_write')

    def test_stage_medium(self):
        """中等压力：5 万行 × 4KB，50 并发"""
        config = STAGE_CONFIGS['medium']
        requests = self._build_batch_write_requests(config)
        _log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batch_write_row, operation_name='batch_write')

    def test_stage_medium_high_concurrency(self):
        """中等压力(高并发)：5 万行 × 4KB，100 并发，batch=50"""
        config = STAGE_CONFIGS['medium_high_concurrency']
        requests = self._build_batch_write_requests(config)
        _log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batch_write_row, operation_name='batch_write')

    def test_stage_high(self):
        """高压力：5 万行 × 4KB，100 并发"""
        config = STAGE_CONFIGS['high']
        requests = self._build_batch_write_requests(config)
        _log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batch_write_row, operation_name='batch_write')
    
    def test_stage_very_high(self):
        """高压力：5 万行 × 4KB，500 并发"""
        config = STAGE_CONFIGS['very_high']
        requests = self._build_batch_write_requests(config)
        _log(f"预构建 {len(requests)} 个 batch_write 请求...")
        self._run_stage(config, requests, self.client.batch_write_row, operation_name='batch_write')

    # ---- put_row 压力测试 ----
    def test_put_row_basic(self):
        """put_row 基础压力测试：20000 行，300 并发，单行操作"""
        config = StressTestConfig(
            name='put_row 基础压力测试',
            total_rows=20000,
            concurrent_workers=300,
            rows_per_batch=1,
            columns_count=50,
            row_size_kb=4,
        )
        requests = []
        for gid in range(config.total_rows):
            item = _make_row_item(gid, gid, config.columns_count)
            requests.append(((self.TABLE_NAME, item.row, item.condition), 1))
        _log(f"预构建 {len(requests)} 个 put_row 请求...")
        send_fn = lambda args: self.client.put_row(*args)
        self._run_stage(config, requests, send_fn, operation_name='put_row')

    # ---- get_row 压力测试 ----
    def test_get_row_basic(self):
        """get_row 基础压力测试：5000 行，30 并发，单行操作"""
        config = StressTestConfig(
            name='get_row 基础压力测试',
            total_rows=5000,
            concurrent_workers=30,
            rows_per_batch=1,
            columns_count=50,
            row_size_kb=4,
        )
        # 先写入测试数据
        _log("写入测试数据...")
        for offset in range(0, config.total_rows, BATCH_SIZE):
            batch_size = min(BATCH_SIZE, config.total_rows - offset)
            req = _make_batch_write(self.TABLE_NAME, offset, batch_size, config.columns_count)
            self.client.batch_write_row(req)
        _log(f"写入完成，共 {config.total_rows} 行")
        
        requests = []
        for gid in range(config.total_rows):
            pk = [('gid', gid), ('uid', gid)]
            requests.append((pk, 1))
        _log(f"预构建 {len(requests)} 个 get_row 请求...")
        send_fn = lambda pk: self.client.get_row(self.TABLE_NAME, pk, max_version=1)
        self._run_stage(config, requests, send_fn, operation_name='get_row')

    # ---- batch_get_row 压力测试 ----
    def test_batch_get_row_basic(self):
        """batch_get_row 基础压力测试：5000 行，30 并发，batch=100"""
        config = StressTestConfig(
            name='batch_get_row 基础压力测试',
            total_rows=50000,
            concurrent_workers=100,
            rows_per_batch=100,
        )
        # 先写入测试数据
        _log("写入测试数据...")
        for offset in range(0, config.total_rows, BATCH_SIZE):
            batch_size = min(BATCH_SIZE, config.total_rows - offset)
            req = _make_batch_write(self.TABLE_NAME, offset, batch_size, config.columns_count)
            self.client.batch_write_row(req)
        _log(f"写入完成，共 {config.total_rows} 行")
        
        total_requests = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
        requests = []
        for i in range(total_requests):
            start_gid = i * config.rows_per_batch
            actual_batch = min(config.rows_per_batch, config.total_rows - start_gid)
            if actual_batch <= 0:
                break
            req = _make_batch_get(self.TABLE_NAME, start_gid, actual_batch)
            requests.append((req, actual_batch))
        _log(f"预构建 {len(requests)} 个 batch_get_row 请求...")
        self._run_stage(config, requests, self.client.batch_get_row, operation_name='batch_get_row')

    # ---- get_range 压力测试 ----
    def test_get_range_basic(self):
        """get_range 基础压力测试：5000 行，30 并发，每次查询 100 行范围"""
        config = StressTestConfig(
            name='get_range 基础压力测试',
            total_rows=5000,
            concurrent_workers=30,
            rows_per_batch=100,
            columns_count=50,
            row_size_kb=4,
        )
        # 先写入测试数据
        _log("写入测试数据...")
        for offset in range(0, config.total_rows, BATCH_SIZE):
            batch_size = min(BATCH_SIZE, config.total_rows - offset)
            req = _make_batch_write(self.TABLE_NAME, offset, batch_size, config.columns_count)
            self.client.batch_write_row(req)
        _log(f"写入完成，共 {config.total_rows} 行")
        
        total_requests = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
        requests = []
        for i in range(total_requests):
            start_gid = i * config.rows_per_batch
            actual_batch = min(config.rows_per_batch, config.total_rows - start_gid)
            if actual_batch <= 0:
                break
            pk_start = [('gid', start_gid), ('uid', INF_MIN)]
            pk_end = [('gid', start_gid), ('uid', INF_MAX)]
            requests.append(((pk_start, pk_end, actual_batch), actual_batch))
        _log(f"预构建 {len(requests)} 个 get_range 请求...")
        
        def send_get_range(args):
            pk_start, pk_end, limit = args
            self.client.get_range(self.TABLE_NAME, 'FORWARD', pk_start, pk_end, max_version=1, limit=limit)
        
        self._run_stage(config, requests, send_get_range, operation_name='get_range')


# ---------------------------------------------------------------------------
#  Native 编解码器对比性能测试
# ---------------------------------------------------------------------------
class NativeCompareStressTest(unittest.TestCase):
    """对比 Native C++ 编解码器开启/关闭时各数据读写接口的性能差异。"""

    TABLE_NAME = make_table_name('NativeCompareTable')

    @classmethod
    def setUpClass(cls):
        cls.client = _new_client()
        _ensure_table(cls.client, cls.TABLE_NAME)
        _warmup_connection_pool(cls.client, cls.TABLE_NAME)

    SEARCH_INDEX_NAME = 'native_compare_search_idx'

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.delete_search_index(cls.TABLE_NAME, cls.SEARCH_INDEX_NAME)
        except Exception:
            pass
        try:
            cls.client.delete_table(cls.TABLE_NAME)
        except Exception:
            pass

    # ---- 核心执行引擎（返回统计数据） ----
    def _run_benchmark(self, config, requests, send_fn, operation_name='benchmark'):
        """
        执行压力测试并返回统计摘要，用于对比分析。

        包含引擎预热和 GC 控制，确保基准测试数据的稳定性和可比性。

        返回 dict: {
            'elapsed_s': float,
            'rows_per_sec': float,
            'processed': int,
            'failed': int,
            'latency': dict (LatencyStats.summary()),
        }
        """
        total_requests = len(requests)

        # ---- 引擎预热：用前几个请求预热线程池和编解码路径 ----
        warmup_count = min(WARMUP_ROUNDS, total_requests)
        if warmup_count > 0:
            _log(f"引擎预热: 发送 {warmup_count} 个请求...")
            with ThreadPoolExecutor(max_workers=min(config.concurrent_workers, warmup_count)) as warmup_pool:
                warmup_futures = []
                for i in range(warmup_count):
                    request_data, _ = requests[i]
                    warmup_futures.append(warmup_pool.submit(send_fn, request_data))
                try:
                    for future in as_completed(warmup_futures, timeout=600):
                        try:
                            future.result()
                        except Exception:
                            pass
                except TimeoutError:
                    pass
            _log(f"引擎预热完成")

        # ---- GC 控制：正式测试期间禁用 GC ----
        gc.collect()
        gc_was_enabled = gc.isenabled()
        gc.disable()

        stats = {'processed': 0, 'failed': 0}
        latency_stats = LatencyStats()
        lock = threading.Lock()
        start_time = time.time()

        try:
            def _send_request(request_index):
                try:
                    request_start = time.monotonic()
                    request_data, expected_rows = requests[request_index]
                    send_fn(request_data)
                    request_elapsed_ms = (time.monotonic() - request_start) * 1000
                    latency_stats.record(request_elapsed_ms)
                    with lock:
                        stats['processed'] += expected_rows
                except Exception as exc:
                    with lock:
                        _, expected_rows = requests[request_index]
                        stats['failed'] += expected_rows
                        _log(f"{operation_name} 失败 request={request_index}: {exc}", 'ERROR')

            with ThreadPoolExecutor(max_workers=config.concurrent_workers) as pool:
                futures = [pool.submit(_send_request, i) for i in range(total_requests)]
                try:
                    for future in as_completed(futures, timeout=600):
                        try:
                            future.result()
                        except Exception as exc:
                            _log(f"Worker 异常: {exc}", 'ERROR')
                except TimeoutError:
                    pass
        finally:
            if gc_was_enabled:
                gc.enable()
            gc.collect()

        elapsed = time.time() - start_time
        total = stats['processed'] + stats['failed']
        rows_per_sec = total / elapsed if elapsed > 0 else 0

        return {
            'elapsed_s': elapsed,
            'rows_per_sec': rows_per_sec,
            'processed': stats['processed'],
            'failed': stats['failed'],
            'latency': latency_stats.summary(),
        }

    # ---- Native 开关控制 ----
    def _set_native(self, enabled):
        """设置 Native 编解码器的开关状态。"""
        self.client.protocol.encoder._use_native_encoder = enabled
        self.client.protocol.decoder._use_native_decoder = enabled

    # ---- 对比报告输出 ----
    def _print_compare_report(self, operation_name, config, result_native, result_python):
        """打印 Native vs Python 的对比报告。"""
        _log(f"")
        _log(f"╔══════════════════════════════════════════════════════════════╗")
        _log(f"║  {operation_name} Native 编解码器对比报告")
        _log(f"║  配置: {config.total_rows} 行, {config.concurrent_workers} 并发, "
             f"batch={config.rows_per_batch}, {config.columns_count} 列")
        _log(f"╠══════════════════════════════════════════════════════════════╣")
        _log(f"║  {'指标':<16} {'Native 开启':>14} {'Native 关闭':>14} {'差异':>12}")
        _log(f"╠══════════════════════════════════════════════════════════════╣")

        native_speed = result_native['rows_per_sec']
        python_speed = result_python['rows_per_sec']
        speed_diff = ((native_speed - python_speed) / python_speed * 100) if python_speed > 0 else 0

        native_elapsed = result_native['elapsed_s']
        python_elapsed = result_python['elapsed_s']
        elapsed_diff = ((python_elapsed - native_elapsed) / python_elapsed * 100) if python_elapsed > 0 else 0

        _log(f"║  {'耗时(s)':<16} {native_elapsed:>14.2f} {python_elapsed:>14.2f} {elapsed_diff:>+11.1f}%")
        _log(f"║  {'吞吐量(行/秒)':<14} {native_speed:>14.0f} {python_speed:>14.0f} {speed_diff:>+11.1f}%")
        _log(f"║  {'成功行数':<16} {result_native['processed']:>14} {result_python['processed']:>14} {'':>12}")
        _log(f"║  {'失败行数':<16} {result_native['failed']:>14} {result_python['failed']:>14} {'':>12}")

        native_lat = result_native.get('latency', {})
        python_lat = result_python.get('latency', {})
        if native_lat and python_lat:
            _log(f"╠══════════════════════════════════════════════════════════════╣")
            _log(f"║  {'延时指标':<16} {'Native 开启':>14} {'Native 关闭':>14} {'差异':>12}")
            _log(f"╠══════════════════════════════════════════════════════════════╣")
            for label, key in [('Avg', 'avg_ms'), ('P50', 'p50_ms'), ('P90', 'p90_ms'),
                               ('P95', 'p95_ms'), ('P99', 'p99_ms'), ('Max', 'max_ms')]:
                native_val = native_lat.get(key, 0)
                python_val = python_lat.get(key, 0)
                diff = ((native_val - python_val) / python_val * 100) if python_val > 0 else 0
                _log(f"║  {label + '(ms)':<16} {native_val:>14.2f} {python_val:>14.2f} {diff:>+11.1f}%")

        _log(f"╚══════════════════════════════════════════════════════════════╝")
        _log(f"")

    # ---- 通用对比执行方法 ----
    def _run_compare(self, config, build_requests_fn, send_fn, operation_name,
                     prepare_data_fn=None):
        """
        对同一操作分别在 Native 开启和关闭时执行压力测试，输出对比报告。

        build_requests_fn: callable() -> list of (request_data, expected_rows)
        send_fn: callable(request_data)
        prepare_data_fn: 可选，在测试前准备数据（如写入读取所需的行）
        """
        if prepare_data_fn:
            self._set_native(True)
            prepare_data_fn()

        # --- Native 开启 ---
        _log(f"===== {operation_name} [Native 开启] 开始 =====")
        self._set_native(True)
        requests_native = build_requests_fn()
        _log(f"预构建 {len(requests_native)} 个请求 (Native 开启)...")
        result_native = self._run_benchmark(config, requests_native, send_fn, operation_name)
        _log(f"===== {operation_name} [Native 开启] 完成: "
             f"{result_native['rows_per_sec']:.0f} 行/秒, "
             f"耗时 {result_native['elapsed_s']:.2f}s =====", 'SUCCESS')

        # --- Native 关闭 ---
        _log(f"===== {operation_name} [Native 关闭] 开始 =====")
        self._set_native(False)
        requests_python = build_requests_fn()
        _log(f"预构建 {len(requests_python)} 个请求 (Native 关闭)...")
        result_python = self._run_benchmark(config, requests_python, send_fn, operation_name)
        _log(f"===== {operation_name} [Native 关闭] 完成: "
             f"{result_python['rows_per_sec']:.0f} 行/秒, "
             f"耗时 {result_python['elapsed_s']:.2f}s =====", 'SUCCESS')

        # --- 对比报告 ---
        self._print_compare_report(operation_name, config, result_native, result_python)

        return result_native, result_python

    # ---- 写入测试数据 ----
    def _prepare_read_data(self, total_rows, columns_count):
        """写入读取测试所需的数据。"""
        _log(f"写入测试数据 ({total_rows} 行)...")
        for offset in range(0, total_rows, BATCH_SIZE):
            batch_size = min(BATCH_SIZE, total_rows - offset)
            req = _make_batch_write(self.TABLE_NAME, offset, batch_size, columns_count)
            self.client.batch_write_row(req)
        _log(f"写入完成，共 {total_rows} 行")

    # ---- Search Index 管理 ----
    def _ensure_search_index(self, table_name, index_name):
        """创建 search index，字段与 _make_row_item 生成的列匹配。"""
        # 删除已有的同名索引（幂等）
        try:
            self.client.delete_search_index(table_name, index_name)
            _log(f"已删除旧的 search index: {index_name}")
            time.sleep(2)
        except Exception:
            pass

        fields = [
            FieldSchema('col_000', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True),
            FieldSchema('col_001', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True),
        ]
        index_setting = IndexSetting(routing_fields=['gid'])
        index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)])
        index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=index_sort)
        self.client.create_search_index(table_name, index_name, index_meta)
        _log(f"创建 search index: {index_name}")

    def _wait_search_index_ready(self, table_name, index_name, total_count):
        """轮询等待 search index 数据同步完成。"""
        max_wait_time = 300
        interval_time = 2
        start_time = time.time()
        _log(f"等待 search index [{index_name}] 数据同步 (目标: {total_count} 行)...")
        while max_wait_time > 0:
            search_response = self.client.search(
                table_name=table_name,
                index_name=index_name,
                search_query=SearchQuery(MatchAllQuery(), limit=0, get_total_count=True),
                columns_to_get=ColumnsToGet(return_type=ColumnReturnType.NONE),
            )
            if search_response.total_count >= total_count:
                elapsed = time.time() - start_time
                _log(f"search index [{index_name}] 就绪! 耗时 {elapsed:.1f}s, "
                     f"total_count={search_response.total_count}")
                return
            time.sleep(interval_time)
            max_wait_time -= interval_time
        raise RuntimeError(
            f"search index [{index_name}] 在 300s 内未就绪, "
            f"耗时 {time.time() - start_time:.1f}s"
        )

    def _prepare_search_data(self, total_rows, columns_count):
        """创建索引 + 写入数据 + 等待就绪。"""
        self._ensure_search_index(self.TABLE_NAME, self.SEARCH_INDEX_NAME)
        self._prepare_read_data(total_rows, columns_count)
        self._wait_search_index_ready(self.TABLE_NAME, self.SEARCH_INDEX_NAME, total_rows)

    # ================================================================
    #  对比测试用例
    # ================================================================

    def test_compare_put_row(self):
        """对比 put_row: Native 开启 vs 关闭"""
        config = StressTestConfig(
            name='put_row Native 对比',
            total_rows=50000,
            concurrent_workers=100,
            rows_per_batch=1,
            columns_count=50,
            row_size_kb=4,
        )

        def build_requests():
            requests = []
            for gid in range(config.total_rows):
                item = _make_row_item(gid, gid, config.columns_count)
                requests.append(((self.TABLE_NAME, item.row, item.condition), 1))
            return requests

        send_fn = lambda args: self.client.put_row(*args)
        self._run_compare(config, build_requests, send_fn, 'put_row')

    def test_compare_batch_write_row(self):
        """对比 batch_write_row: Native 开启 vs 关闭"""
        config = StressTestConfig(
            name='batch_write_row Native 对比',
            total_rows=50000,
            concurrent_workers=100,
            rows_per_batch=BATCH_SIZE,
            columns_count=50,
            row_size_kb=4,
        )

        def build_requests():
            total_batches = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
            requests = []
            for i in range(total_batches):
                start_gid = i * config.rows_per_batch
                actual_batch = min(config.rows_per_batch, config.total_rows - start_gid)
                if actual_batch <= 0:
                    break
                req = _make_batch_write(self.TABLE_NAME, start_gid, actual_batch, config.columns_count)
                requests.append((req, actual_batch))
            return requests

        self._run_compare(config, build_requests, self.client.batch_write_row, 'batch_write_row')

    def test_compare_get_row(self):
        """对比 get_row: Native 开启 vs 关闭"""
        config = StressTestConfig(
            name='get_row Native 对比',
            total_rows=50000,
            concurrent_workers=100,
            rows_per_batch=1,
            columns_count=50,
            row_size_kb=4,
        )

        def build_requests():
            requests = []
            for gid in range(config.total_rows):
                pk = [('gid', gid), ('uid', gid)]
                requests.append((pk, 1))
            return requests

        send_fn = lambda pk: self.client.get_row(self.TABLE_NAME, pk, max_version=1)
        prepare_fn = lambda: self._prepare_read_data(config.total_rows, config.columns_count)
        self._run_compare(config, build_requests, send_fn, 'get_row', prepare_data_fn=prepare_fn)

    def test_compare_batch_get_row(self):
        """对比 batch_get_row: Native 开启 vs 关闭"""
        config = StressTestConfig(
            name='batch_get_row Native 对比',
            total_rows=10000,
            concurrent_workers=100,
            rows_per_batch=BATCH_SIZE,
            columns_count=50,
            row_size_kb=4,
        )

        def build_requests():
            total_batches = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
            requests = []
            for i in range(total_batches):
                start_gid = i * config.rows_per_batch
                actual_batch = min(config.rows_per_batch, config.total_rows - start_gid)
                if actual_batch <= 0:
                    break
                req = _make_batch_get(self.TABLE_NAME, start_gid, actual_batch)
                requests.append((req, actual_batch))
            return requests

        prepare_fn = lambda: self._prepare_read_data(config.total_rows, config.columns_count)
        self._run_compare(config, build_requests, self.client.batch_get_row, 'batch_get_row',
                          prepare_data_fn=prepare_fn)

    def test_compare_get_range(self):
        """对比 get_range: Native 开启 vs 关闭"""
        config = StressTestConfig(
            name='get_range Native 对比',
            total_rows=50000,
            concurrent_workers=100,
            rows_per_batch=BATCH_SIZE,
            columns_count=50,
            row_size_kb=4,
        )

        def build_requests():
            total_batches = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
            requests = []
            for i in range(total_batches):
                start_gid = i * config.rows_per_batch
                actual_batch = min(config.rows_per_batch, config.total_rows - start_gid)
                if actual_batch <= 0:
                    break
                pk_start = [('gid', start_gid), ('uid', INF_MIN)]
                pk_end = [('gid', start_gid), ('uid', INF_MAX)]
                requests.append(((pk_start, pk_end, actual_batch), actual_batch))
            return requests

        def send_get_range(args):
            pk_start, pk_end, limit = args
            self.client.get_range(self.TABLE_NAME, 'FORWARD', pk_start, pk_end,
                                  max_version=1, limit=limit)

        prepare_fn = lambda: self._prepare_read_data(config.total_rows, config.columns_count)
        self._run_compare(config, build_requests, send_get_range, 'get_range',
                          prepare_data_fn=prepare_fn)

    def test_compare_search(self):
        """对比 search: Native 开启 vs 关闭"""
        config = StressTestConfig(
            name='search Native 对比',
            total_rows=5000,
            concurrent_workers=30,
            rows_per_batch=1,
            columns_count=50,
            row_size_kb=4,
        )

        def build_requests():
            requests = []
            query = MatchAllQuery()
            for i in range(config.total_rows):
                requests.append(((query, i), 1))
            return requests

        def send_search(args):
            query, _ = args
            self.client.search(
                self.TABLE_NAME, self.SEARCH_INDEX_NAME,
                SearchQuery(query, limit=1, get_total_count=False),
                ColumnsToGet(return_type=ColumnReturnType.NONE),
            )

        prepare_fn = lambda: self._prepare_search_data(config.total_rows, config.columns_count)
        self._run_compare(config, build_requests, send_search, 'search',
                          prepare_data_fn=prepare_fn)

    def test_compare_parallel_scan(self):
        """对比 parallel_scan: Native 开启 vs 关闭"""
        config = StressTestConfig(
            name='parallel_scan Native 对比',
            total_rows=600,
            concurrent_workers=30,
            rows_per_batch=1,
            columns_count=50,
            row_size_kb=4,
        )

        def build_requests():
            # 每次构建请求前获取新的 session（避免 session 过期）
            compute_response = self.client.compute_splits(
                self.TABLE_NAME, self.SEARCH_INDEX_NAME)
            session_id = compute_response.session_id
            splits_size = compute_response.splits_size
            query = MatchAllQuery()
            requests = []
            for i in range(config.total_rows):
                scan_query = ScanQuery(
                    query, limit=1, next_token=None,
                    current_parallel_id=i % splits_size,
                    max_parallel=splits_size, alive_time=60,
                )
                requests.append(((scan_query, session_id), 1))
            return requests

        def send_parallel_scan(args):
            scan_query, session_id = args
            self.client.parallel_scan(
                self.TABLE_NAME, self.SEARCH_INDEX_NAME,
                scan_query, session_id,
                columns_to_get=ColumnsToGet(return_type=ColumnReturnType.NONE),
            )

        prepare_fn = lambda: self._prepare_search_data(config.total_rows, config.columns_count)
        self._run_compare(config, build_requests, send_parallel_scan,
                          'parallel_scan', prepare_data_fn=prepare_fn)

    # ---- 汇总对比测试 ----
    def test_compare_all_operations(self):
        """一次性对比所有操作的 Native 性能差异，输出汇总报告"""
        config = StressTestConfig(
            name='全接口 Native 对比',
            total_rows=2000,
            concurrent_workers=20,
            rows_per_batch=BATCH_SIZE,
            columns_count=50,
            row_size_kb=4,
        )

        # 准备读取测试数据
        self._set_native(True)
        self._prepare_read_data(config.total_rows, config.columns_count)

        results = {}

        # --- put_row ---
        put_config = StressTestConfig(
            name='put_row', total_rows=config.total_rows,
            concurrent_workers=config.concurrent_workers,
            rows_per_batch=1, columns_count=config.columns_count, row_size_kb=config.row_size_kb,
        )

        def build_put_requests():
            return [((self.TABLE_NAME, _make_row_item(gid, gid, config.columns_count).row,
                      _make_row_item(gid, gid, config.columns_count).condition), 1)
                    for gid in range(config.total_rows)]

        put_send = lambda args: self.client.put_row(*args)
        results['put_row'] = self._run_compare(put_config, build_put_requests, put_send, 'put_row')

        # --- batch_write_row ---
        def build_bw_requests():
            total_batches = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
            reqs = []
            for i in range(total_batches):
                start_gid = i * config.rows_per_batch
                actual = min(config.rows_per_batch, config.total_rows - start_gid)
                if actual <= 0:
                    break
                reqs.append((_make_batch_write(self.TABLE_NAME, start_gid, actual, config.columns_count), actual))
            return reqs

        results['batch_write'] = self._run_compare(config, build_bw_requests,
                                                    self.client.batch_write_row, 'batch_write_row')

        # --- get_row ---
        get_config = StressTestConfig(
            name='get_row', total_rows=config.total_rows,
            concurrent_workers=config.concurrent_workers,
            rows_per_batch=1, columns_count=config.columns_count, row_size_kb=config.row_size_kb,
        )

        def build_get_requests():
            return [([('gid', gid), ('uid', gid)], 1) for gid in range(config.total_rows)]

        get_send = lambda pk: self.client.get_row(self.TABLE_NAME, pk, max_version=1)
        results['get_row'] = self._run_compare(get_config, build_get_requests, get_send, 'get_row')

        # --- batch_get_row ---
        def build_bg_requests():
            total_batches = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
            reqs = []
            for i in range(total_batches):
                start_gid = i * config.rows_per_batch
                actual = min(config.rows_per_batch, config.total_rows - start_gid)
                if actual <= 0:
                    break
                reqs.append((_make_batch_get(self.TABLE_NAME, start_gid, actual), actual))
            return reqs

        results['batch_get'] = self._run_compare(config, build_bg_requests,
                                                  self.client.batch_get_row, 'batch_get_row')

        # --- get_range ---
        def build_gr_requests():
            total_batches = (config.total_rows + config.rows_per_batch - 1) // config.rows_per_batch
            reqs = []
            for i in range(total_batches):
                start_gid = i * config.rows_per_batch
                actual = min(config.rows_per_batch, config.total_rows - start_gid)
                if actual <= 0:
                    break
                pk_start = [('gid', start_gid), ('uid', INF_MIN)]
                pk_end = [('gid', start_gid), ('uid', INF_MAX)]
                reqs.append(((pk_start, pk_end, actual), actual))
            return reqs

        def send_range(args):
            pk_start, pk_end, limit = args
            self.client.get_range(self.TABLE_NAME, 'FORWARD', pk_start, pk_end,
                                  max_version=1, limit=limit)

        results['get_range'] = self._run_compare(config, build_gr_requests, send_range, 'get_range')

        # --- search ---
        self._ensure_search_index(self.TABLE_NAME, self.SEARCH_INDEX_NAME)
        self._wait_search_index_ready(self.TABLE_NAME, self.SEARCH_INDEX_NAME, config.total_rows)

        search_config = StressTestConfig(
            name='search', total_rows=config.total_rows,
            concurrent_workers=config.concurrent_workers,
            rows_per_batch=1, columns_count=config.columns_count, row_size_kb=config.row_size_kb,
        )

        def build_search_requests():
            query = MatchAllQuery()
            return [((query, i), 1) for i in range(config.total_rows)]

        def send_search_all(args):
            query, _ = args
            self.client.search(
                self.TABLE_NAME, self.SEARCH_INDEX_NAME,
                SearchQuery(query, limit=1, get_total_count=False),
                ColumnsToGet(return_type=ColumnReturnType.NONE),
            )

        results['search'] = self._run_compare(search_config, build_search_requests,
                                               send_search_all, 'search')

        # --- parallel_scan ---
        ps_config = StressTestConfig(
            name='parallel_scan', total_rows=config.total_rows,
            concurrent_workers=config.concurrent_workers,
            rows_per_batch=1, columns_count=config.columns_count, row_size_kb=config.row_size_kb,
        )

        def build_ps_requests():
            compute_response = self.client.compute_splits(
                self.TABLE_NAME, self.SEARCH_INDEX_NAME)
            session_id = compute_response.session_id
            splits_size = compute_response.splits_size
            query = MatchAllQuery()
            reqs = []
            for i in range(config.total_rows):
                scan_query = ScanQuery(
                    query, limit=1, next_token=None,
                    current_parallel_id=i % splits_size,
                    max_parallel=splits_size, alive_time=60,
                )
                reqs.append(((scan_query, session_id), 1))
            return reqs

        def send_ps(args):
            scan_query, session_id = args
            self.client.parallel_scan(
                self.TABLE_NAME, self.SEARCH_INDEX_NAME,
                scan_query, session_id,
                columns_to_get=ColumnsToGet(return_type=ColumnReturnType.NONE),
            )

        results['parallel_scan'] = self._run_compare(ps_config, build_ps_requests,
                                                      send_ps, 'parallel_scan')

        # --- 汇总报告 ---
        _log(f"")
        _log(f"╔══════════════════════════════════════════════════════════════════════╗")
        _log(f"║                    Native 编解码器全接口对比汇总                      ║")
        _log(f"║  配置: {config.total_rows} 行, {config.concurrent_workers} 并发, "
             f"{config.columns_count} 列, 每行≈{config.row_size_kb}KB")
        _log(f"╠══════════════════════════════════════════════════════════════════════╣")
        _log(f"║  {'操作':<18} {'Native(行/秒)':>14} {'Python(行/秒)':>14} {'提升':>10}")
        _log(f"╠══════════════════════════════════════════════════════════════════════╣")

        for op_name, (result_native, result_python) in results.items():
            native_speed = result_native['rows_per_sec']
            python_speed = result_python['rows_per_sec']
            improvement = ((native_speed - python_speed) / python_speed * 100) if python_speed > 0 else 0
            _log(f"║  {op_name:<18} {native_speed:>14.0f} {python_speed:>14.0f} {improvement:>+9.1f}%")

        _log(f"╚══════════════════════════════════════════════════════════════════════╝")
        _log(f"")


# ---------------------------------------------------------------------------
#  并发 & 异步场景测试
# ---------------------------------------------------------------------------
class ConcurrentStressTest(unittest.TestCase):
    """并发场景和异步场景的压力测试"""

    TABLE_NAME = make_table_name('ConcurrentStressTable')

    @classmethod
    def setUpClass(cls):
        cls.client = _new_client()
        _ensure_table(cls.client, cls.TABLE_NAME)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.delete_table(cls.TABLE_NAME)
        except Exception:
            pass

    # ---- 异步写入 ----
    def test_async_concurrent_writes(self):
        """异步并发写入压力测试：100 并发 × 5000 行 = 50w 行"""
        task_count = 100
        rows_per_task = 5000
        total_rows = task_count * rows_per_task

        _log(f"异步并发写入压力测试: {task_count} 并发 × {rows_per_task} 行 = {total_rows} 行")

        # 提前构造所有请求，避免在并发执行阶段临时构造
        _log("构造写入请求...")
        all_requests = []
        for task_id in range(task_count):
            base_gid = task_id * rows_per_task
            task_requests = [
                _make_batch_write(self.TABLE_NAME, base_gid + offset, min(BATCH_SIZE, rows_per_task - offset), columns_count=50)
                for offset in range(0, rows_per_task, BATCH_SIZE)
            ]
            all_requests.append(task_requests)
        _log(f"请求构造完成: {task_count} 任务 × {len(all_requests[0])} 批次")

        start = time.time()

        async def _write_rows(aclient, requests):
            for req in requests:
                await aclient.batch_write_row(req)
            return len(requests)

        async def _run():
            async with _OriginalAsyncOTSClient(
                test_config.OTS_ENDPOINT,
                test_config.OTS_ACCESS_KEY_ID,
                test_config.OTS_ACCESS_KEY_SECRET,
                test_config.OTS_INSTANCE,
                region=test_config.OTS_REGION,
                enable_native=test_config.OTS_ENABLE_NATIVE,
                native_fallback=test_config.OTS_NATIVE_FALLBACK,
            ) as aclient:
                tasks = [
                    _write_rows(aclient, all_requests[i])
                    for i in range(task_count)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                succeeded = sum(1 for r in results if not isinstance(r, Exception))
                total_batches = sum(r for r in results if isinstance(r, int))
                return succeeded, total_batches

        success, total_batches = asyncio.run(_run())
        elapsed = time.time() - start
        speed = total_rows / elapsed if elapsed > 0 else 0
        _log(f"异步写入完成: {success}/{task_count} 并发成功, "
             f"共 {total_batches} 批次, {total_rows} 行, "
             f"耗时 {elapsed:.2f}s, {speed:.0f} 行/秒", 'SUCCESS')
        self.assertEqual(success, task_count)

    # ---- 异步读取 ----
    def test_async_concurrent_reads(self):
        """异步并发读取压力测试：先写 5000 行，再 100 并发 × 5000 行读取"""
        task_count = 100
        rows_per_task = 5000
        seed_rows = 5000

        # 先写入种子数据
        _log(f"写入 {seed_rows} 行种子数据...")
        for offset in range(0, seed_rows, BATCH_SIZE):
            batch_rows = min(BATCH_SIZE, seed_rows - offset)
            self.client.batch_write_row(
                _make_batch_write(self.TABLE_NAME, offset, batch_rows)
            )

        # 提前构造所有读取请求，避免在并发执行阶段临时构造
        _log("构造读取请求...")
        all_requests = []
        for task_id in range(task_count):
            task_requests = [
                _make_batch_get(self.TABLE_NAME, offset % seed_rows, min(BATCH_SIZE, rows_per_task - offset))
                for offset in range(0, rows_per_task, BATCH_SIZE)
            ]
            all_requests.append(task_requests)
        _log(f"请求构造完成: {task_count} 任务 × {len(all_requests[0])} 批次")

        _log(f"异步并发读取压力测试: {task_count} 并发 × {rows_per_task} 行")
        start = time.time()

        async def _read_rows(aclient, requests):
            for req in requests:
                await aclient.batch_get_row(req)
            return len(requests)

        async def _run():
            async with _OriginalAsyncOTSClient(
                test_config.OTS_ENDPOINT,
                test_config.OTS_ACCESS_KEY_ID,
                test_config.OTS_ACCESS_KEY_SECRET,
                test_config.OTS_INSTANCE,
                region=test_config.OTS_REGION,
                enable_native=test_config.OTS_ENABLE_NATIVE,
                native_fallback=test_config.OTS_NATIVE_FALLBACK,
            ) as aclient:
                tasks = [
                    _read_rows(aclient, all_requests[i])
                    for i in range(task_count)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                succeeded = sum(1 for r in results if not isinstance(r, Exception))
                total_batches = sum(r for r in results if isinstance(r, int))
                return succeeded, total_batches

        success, total_batches = asyncio.run(_run())
        elapsed = time.time() - start
        total_rows = task_count * rows_per_task
        speed = total_rows / elapsed if elapsed > 0 else 0
        _log(f"异步读取完成: {success}/{task_count} 并发成功, "
             f"共 {total_batches} 批次, {total_rows} 行, "
             f"耗时 {elapsed:.2f}s, {speed:.0f} 行/秒", 'SUCCESS')
        self.assertEqual(success, task_count)

    # ---- 异步混合读写 ----
    def test_async_mixed_operations(self):
        """异步混合读写压力测试：50 写并发 × 5000 行 + 50 读并发 × 5000 行 = 50w 行"""
        write_count = 50
        read_count = 50
        rows_per_task = 5000
        total_rows = (write_count + read_count) * rows_per_task

        _log(f"异步混合读写压力测试: {write_count} 写 + {read_count} 读, "
             f"每并发 {rows_per_task} 行, 总计 {total_rows} 行")

        # 提前构造所有写入请求
        _log("构造写入请求...")
        write_requests = []
        for task_id in range(write_count):
            base_gid = task_id * rows_per_task
            task_requests = [
                _make_batch_write(self.TABLE_NAME, base_gid + offset, min(BATCH_SIZE, rows_per_task - offset))
                for offset in range(0, rows_per_task, BATCH_SIZE)
            ]
            write_requests.append(task_requests)

        # 提前构造所有读取请求
        _log("构造读取请求...")
        read_requests = []
        for task_id in range(read_count):
            task_requests = [
                _make_batch_get(self.TABLE_NAME, offset % rows_per_task, min(BATCH_SIZE, rows_per_task - offset))
                for offset in range(0, rows_per_task, BATCH_SIZE)
            ]
            read_requests.append(task_requests)
        _log(f"请求构造完成: {write_count} 写任务 + {read_count} 读任务, "
             f"每任务 {len(write_requests[0])} 批次")

        start = time.time()

        async def _write_rows(aclient, requests):
            for req in requests:
                await aclient.batch_write_row(req)
            return len(requests)

        async def _read_rows(aclient, requests):
            for req in requests:
                await aclient.batch_get_row(req)
            return len(requests)

        async def _run():
            async with _OriginalAsyncOTSClient(
                test_config.OTS_ENDPOINT,
                test_config.OTS_ACCESS_KEY_ID,
                test_config.OTS_ACCESS_KEY_SECRET,
                test_config.OTS_INSTANCE,
                region=test_config.OTS_REGION,
                enable_native=test_config.OTS_ENABLE_NATIVE,
                native_fallback=test_config.OTS_NATIVE_FALLBACK,
            ) as aclient:
                tasks = []
                for i in range(write_count):
                    tasks.append(_write_rows(aclient, write_requests[i]))
                for i in range(read_count):
                    tasks.append(_read_rows(aclient, read_requests[i]))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                succeeded = sum(1 for r in results if not isinstance(r, Exception))
                total_batches = sum(r for r in results if isinstance(r, int))
                return succeeded, len(tasks), total_batches

        success, total_tasks, total_batches = asyncio.run(_run())
        elapsed = time.time() - start
        speed = total_rows / elapsed if elapsed > 0 else 0
        _log(f"混合操作完成: {success}/{total_tasks} 并发成功, "
             f"共 {total_batches} 批次, {total_rows} 行, "
             f"耗时 {elapsed:.2f}s, {speed:.0f} 行/秒", 'SUCCESS')
        self.assertGreater(success, total_tasks * 0.8)

    # ---- 多线程写入 ----
    def test_threading_concurrent_writes(self):
        """多线程并发写入：10 线程 × 30 行"""
        thread_count = 10
        rows_per_thread = 30

        _log(f"多线程写入: {thread_count} 线程 × {rows_per_thread} 行")
        start = time.time()

        def _write(start_gid, count):
            client = _new_client()
            req = _make_batch_write(self.TABLE_NAME, start_gid, count)
            return client.batch_write_row(req)

        with ThreadPoolExecutor(max_workers=thread_count) as pool:
            futures = [
                pool.submit(_write, i * rows_per_thread, rows_per_thread)
                for i in range(thread_count)
            ]
            results = []
            for f in as_completed(futures):
                try:
                    results.append(f.result(timeout=60))
                except Exception as exc:
                    _log(f"线程写入失败: {exc}", 'ERROR')

        _log(f"多线程写入完成: {len(results)}/{thread_count} 成功, "
             f"耗时 {time.time() - start:.2f}s", 'SUCCESS')
        self.assertEqual(len(results), thread_count)

    # ---- 多线程读取 ----
    def test_threading_concurrent_reads(self):
        """多线程并发读取：先写 100 行，10 线程 × 10 行"""
        thread_count = 10
        rows_per_thread = 10

        self.client.batch_write_row(
            _make_batch_write(self.TABLE_NAME, 0, 100)
        )

        _log(f"多线程读取: {thread_count} 线程 × {rows_per_thread} 行")
        start = time.time()

        def _read(start_gid, count):
            client = _new_client()
            req = _make_batch_get(self.TABLE_NAME, start_gid, count)
            return client.batch_get_row(req)

        with ThreadPoolExecutor(max_workers=thread_count) as pool:
            futures = [
                pool.submit(_read, i * rows_per_thread, rows_per_thread)
                for i in range(thread_count)
            ]
            results = []
            for f in as_completed(futures):
                try:
                    results.append(f.result(timeout=60))
                except Exception as exc:
                    _log(f"线程读取失败: {exc}", 'ERROR')

        _log(f"多线程读取完成: {len(results)}/{thread_count} 成功, "
             f"耗时 {time.time() - start:.2f}s", 'SUCCESS')
        self.assertEqual(len(results), thread_count)

    # ---- 线程 + 异步混合高并发 ----
    def test_high_concurrency_mixed(self):
        """高并发混合：10 线程 + 10 异步任务"""
        thread_count = 10
        async_count = 10
        rows_per_task = 50

        _log(f"高并发混合: {thread_count} 线程 + {async_count} 异步")
        start = time.time()

        stats = {'thread_ok': 0, 'async_ok': 0, 'errors': []}
        lock = threading.Lock()

        def _thread_op(start_gid):
            try:
                client = _new_client()
                req = _make_batch_write(self.TABLE_NAME, start_gid, rows_per_task)
                client.batch_write_row(req)
                with lock:
                    stats['thread_ok'] += 1
            except Exception as exc:
                with lock:
                    stats['errors'].append(str(exc))

        async def _async_op(aclient, start_gid):
            try:
                req = _make_batch_write(self.TABLE_NAME, start_gid, rows_per_task)
                await aclient.batch_write_row(req)
                with lock:
                    stats['async_ok'] += 1
            except Exception as exc:
                with lock:
                    stats['errors'].append(str(exc))

        # 启动线程
        threads = []
        for i in range(thread_count):
            t = threading.Thread(target=_thread_op, args=(i * rows_per_task,))
            threads.append(t)
            t.start()

        # 启动异步任务
        async def _run_async():
            async with _OriginalAsyncOTSClient(
                test_config.OTS_ENDPOINT,
                test_config.OTS_ACCESS_KEY_ID,
                test_config.OTS_ACCESS_KEY_SECRET,
                test_config.OTS_INSTANCE,
                region=test_config.OTS_REGION,
                enable_native=test_config.OTS_ENABLE_NATIVE,
                native_fallback=test_config.OTS_NATIVE_FALLBACK,
            ) as aclient:
                tasks = [
                    _async_op(aclient, (thread_count + i) * rows_per_task)
                    for i in range(async_count)
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run(_run_async())
        for t in threads:
            t.join(timeout=60)

        total = thread_count + async_count
        success = stats['thread_ok'] + stats['async_ok']
        rate = success / total if total else 0

        _log(f"线程成功: {stats['thread_ok']}/{thread_count}", 'SUCCESS')
        _log(f"异步成功: {stats['async_ok']}/{async_count}", 'SUCCESS')
        _log(f"总成功率: {rate:.2%}, 耗时 {time.time() - start:.2f}s", 'SUCCESS')

        self.assertGreater(rate, 0.8)


# ---------------------------------------------------------------------------
#  性能基准测试
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
#  pytest-benchmark 性能基准测试
# ---------------------------------------------------------------------------

@pytest.fixture(scope='class')
def perf_client():
    """创建并预热 OTS 客户端，供 TestPerformanceBenchmark 使用。"""
    client = _new_client()
    table_name = make_table_name('PerfBenchTable')
    _ensure_table(client, table_name)
    _warmup_connection_pool(client, table_name, rounds=200)
    yield client, table_name
    try:
        client.delete_table(table_name)
    except Exception:
        pass


def _build_requests(table_name, total_rows, columns_count, operation, client=None):
    """预构建批量请求列表，返回 [(request, row_count), ...]。"""
    rows_per_batch = BATCH_SIZE
    total_requests = (total_rows + rows_per_batch - 1) // rows_per_batch
    requests = []

    if operation == 'write':
        for i in range(total_requests):
            start_gid = i * rows_per_batch
            actual_batch = min(rows_per_batch, total_rows - start_gid)
            if actual_batch <= 0:
                break
            requests.append((
                _make_batch_write(table_name, start_gid, actual_batch, columns_count),
                actual_batch,
            ))
    else:
        seed_rows = min(total_rows, 5000)
        _log(f"写入 {seed_rows} 行种子数据...")
        for offset in range(0, seed_rows, rows_per_batch):
            batch = min(rows_per_batch, seed_rows - offset)
            client.batch_write_row(
                _make_batch_write(table_name, offset, batch, columns_count)
            )
        for i in range(total_requests):
            start_gid = (i * rows_per_batch) % seed_rows
            actual_batch = min(rows_per_batch, total_rows - i * rows_per_batch)
            if actual_batch <= 0:
                break
            requests.append((
                _make_batch_get(table_name, start_gid, actual_batch),
                actual_batch,
            ))

    return requests


def _rebuild_write_requests(table_name, total_rows, total_requests, columns_count):
    """重新构建写入请求（预热后使用不同的 gid 避免幂等性问题）。"""
    rows_per_batch = BATCH_SIZE
    requests = []
    for i in range(total_requests):
        start_gid = (total_rows + i) * rows_per_batch
        actual_batch = min(rows_per_batch, total_rows - i * rows_per_batch)
        if actual_batch <= 0:
            break
        requests.append((
            _make_batch_write(table_name, start_gid, actual_batch, columns_count),
            actual_batch,
        ))
    return requests


def _execute_concurrent_batch(client, requests, concurrent_workers, operation):
    """并发执行批量请求，返回 (processed_rows, failed_rows, latency_stats)。"""
    total_requests = len(requests)
    stats = {'processed': 0, 'failed': 0}
    latency_stats = LatencyStats()
    lock = threading.Lock()

    def _send(index):
        try:
            req_data, expected_rows = requests[index]
            req_start = time.monotonic()
            if operation == 'write':
                client.batch_write_row(req_data)
            else:
                client.batch_get_row(req_data)
            latency_stats.record((time.monotonic() - req_start) * 1000)
            with lock:
                stats['processed'] += expected_rows
        except Exception:
            with lock:
                _, expected_rows = requests[index]
                stats['failed'] += expected_rows

    with ThreadPoolExecutor(max_workers=concurrent_workers) as pool:
        futures = [pool.submit(_send, i) for i in range(total_requests)]
        for future in as_completed(futures, timeout=1200):
            try:
                future.result()
            except Exception:
                pass

    return stats['processed'], stats['failed'], latency_stats


@pytest.mark.skipif(not _CONFIG_READY, reason=_SKIP_REASON)
@pytest.mark.usefixtures('perf_client')
class TestPerformanceBenchmark:
    """pytest-benchmark 并发性能基准测试。

    每个测试方法通过 @pytest.mark.parametrize 参数化并发级别，
    pytest-benchmark 自动统计每个 round 的耗时，
    自定义的 LatencyStats 通过 extra_info 附加请求级别的 P50/P99/吞吐量。
    """

    def _run_one_round(self, perf_client, concurrent_workers, total_rows,
                       columns_count, operation):
        """执行一轮并发基准测试，返回 total_rows（供 benchmark 计算吞吐量）。"""
        client, table_name = perf_client

        # 预构建请求
        requests = _build_requests(
            table_name, total_rows, columns_count, operation, client
        )
        total_request_count = len(requests)

        # 引擎预热
        warmup_count = min(100, total_request_count)
        with ThreadPoolExecutor(max_workers=min(concurrent_workers, warmup_count)) as pool:
            warmup_futures = []
            for i in range(warmup_count):
                req_data, _ = requests[i]
                if operation == 'write':
                    warmup_futures.append(pool.submit(client.batch_write_row, req_data))
                else:
                    warmup_futures.append(pool.submit(client.batch_get_row, req_data))
            for future in as_completed(warmup_futures, timeout=1200):
                try:
                    future.result()
                except Exception:
                    pass

        # 写入操作需要重建请求避免幂等性问题
        if operation == 'write':
            requests = _rebuild_write_requests(
                table_name, total_rows, total_request_count, columns_count
            )

        # 正式执行
        processed, failed, latency_stats = _execute_concurrent_batch(
            client, requests, concurrent_workers, operation
        )

        return {
            'total_rows': total_rows,
            'processed': processed,
            'failed': failed,
            'latency_stats': latency_stats,
        }

    def _attach_extra_info(self, benchmark_fixture, result):
        """将自定义指标附加到 benchmark.extra_info。"""
        latency_stats = result['latency_stats']
        summary = latency_stats.summary()
        if summary:
            benchmark_fixture.extra_info.update({
                'total_rows': result['total_rows'],
                'processed_rows': result['processed'],
                'failed_rows': result['failed'],
                'success_rate': f"{result['processed'] / result['total_rows'] * 100:.1f}%",
                'req_p50_ms': round(summary['p50_ms'], 2),
                'req_p75_ms': round(summary['p75_ms'], 2),
                'req_p90_ms': round(summary['p90_ms'], 2),
                'req_p95_ms': round(summary['p95_ms'], 2),
                'req_p99_ms': round(summary['p99_ms'], 2),
                'req_avg_ms': round(summary['avg_ms'], 2),
                'req_min_ms': round(summary['min_ms'], 2),
                'req_max_ms': round(summary['max_ms'], 2),
                'request_count': summary['count'],
            })
        latency_stats.report(f"请求延时")
        assert result['processed'] / result['total_rows'] >= 0.95, \
            f"成功率过低: {result['processed']}/{result['total_rows']}"

    # ---- 并发写入基准 ----
    @pytest.mark.parametrize('concurrency', [1, 10, 50, 100, 200],
                             ids=['c1', 'c10', 'c50', 'c100', 'c200'])
    def test_write_concurrency(self, benchmark, perf_client, concurrency):
        """写入并发扩展性测试"""
        total_rows = 50000

        result = benchmark.pedantic(
            self._run_one_round,
            args=(perf_client, concurrency, total_rows, 0, 'write'),
            rounds=3,
            warmup_rounds=0,
        )

        self._attach_extra_info(benchmark, result)

    # ---- 并发读取基准 ----
    @pytest.mark.parametrize('concurrency', [1, 10, 50, 100, 200],
                             ids=['c1', 'c10', 'c50', 'c100', 'c200'])
    def test_read_concurrency(self, benchmark, perf_client, concurrency):
        """读取并发扩展性测试"""
        total_rows = 50000

        result = benchmark.pedantic(
            self._run_one_round,
            args=(perf_client, concurrency, total_rows, 0, 'read'),
            rounds=3,
            warmup_rounds=0,
        )

        self._attach_extra_info(benchmark, result)

    # ---- 大行写入并发基准 ----
    @pytest.mark.parametrize('concurrency', [10, 50, 100],
                             ids=['c10', 'c50', 'c100'])
    def test_write_large_row(self, benchmark, perf_client, concurrency):
        """大行写入并发测试：50列/行 (~4KB)"""
        total_rows = 50000

        result = benchmark.pedantic(
            self._run_one_round,
            args=(perf_client, concurrency, total_rows, 50, 'write'),
            rounds=3,
            warmup_rounds=0,
        )

        self._attach_extra_info(benchmark, result)

    # ---- 异步并发写入基准 ----
    @pytest.mark.parametrize('concurrency', [10, 50, 100, 200, 500],
                             ids=['c10', 'c50', 'c100', 'c200', 'c500'])
    def test_async_write_concurrency(self, benchmark, perf_client, concurrency):
        """异步写入并发扩展性测试"""
        total_rows = 50000
        _, table_name = perf_client
        rows_per_batch = BATCH_SIZE

        def _run_async_round():
            total_requests = (total_rows + rows_per_batch - 1) // rows_per_batch
            requests = []
            for i in range(total_requests):
                start_gid = (concurrency * 100000) + i * rows_per_batch
                actual_batch = min(rows_per_batch, total_rows - i * rows_per_batch)
                if actual_batch <= 0:
                    break
                requests.append(
                    _make_batch_write(table_name, start_gid, actual_batch)
                )

            latency_stats = LatencyStats()

            async def _run(reqs, max_concurrency):
                semaphore = asyncio.Semaphore(max_concurrency)
                succeeded = 0
                async with _OriginalAsyncOTSClient(
                    test_config.OTS_ENDPOINT,
                    test_config.OTS_ACCESS_KEY_ID,
                    test_config.OTS_ACCESS_KEY_SECRET,
                    test_config.OTS_INSTANCE,
                    region=test_config.OTS_REGION,
                    enable_native=test_config.OTS_ENABLE_NATIVE,
                    native_fallback=test_config.OTS_NATIVE_FALLBACK,
                ) as aclient:
                    async def _send(req):
                        nonlocal succeeded
                        async with semaphore:
                            req_start = time.monotonic()
                            await aclient.batch_write_row(req)
                            latency_stats.record(
                                (time.monotonic() - req_start) * 1000
                            )
                            succeeded += 1

                    await asyncio.gather(
                        *[_send(r) for r in reqs],
                        return_exceptions=True,
                    )
                return succeeded

            success_count = asyncio.run(_run(requests, concurrency))
            processed_rows = success_count * rows_per_batch

            return {
                'total_rows': total_rows,
                'processed': processed_rows,
                'failed': total_rows - processed_rows,
                'latency_stats': latency_stats,
            }

        result = benchmark.pedantic(
            _run_async_round,
            rounds=3,
            warmup_rounds=0,
        )

        self._attach_extra_info(benchmark, result)


# ---------------------------------------------------------------------------
#  稳定性测试（长时间运行，检测内存泄漏）
# ---------------------------------------------------------------------------

import os
import tracemalloc

def _get_rss_mb():
    """获取当前进程的 RSS（Resident Set Size），单位 MB。

    优先使用 psutil（更准确），回退到 resource 模块（仅 Linux/macOS）。
    """
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except ImportError:
        pass
    try:
        import resource
        # resource.getrusage 在 Linux 上返回 KB，macOS 上返回 bytes
        import platform
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system() == 'Darwin':
            return usage / (1024 * 1024)
        return usage / 1024
    except ImportError:
        return 0.0


@dataclass
class MemorySnapshot:
    """单个时间点的内存快照"""
    round_index: int
    elapsed_seconds: float
    rss_mb: float
    tracemalloc_mb: float
    total_operations: int
    errors: int


@unittest.skipUnless(_CONFIG_READY, _SKIP_REASON)
class StabilityTest(unittest.TestCase):
    """长时间运行稳定性测试，检测内存泄漏。

    通过环境变量控制运行参数：
    - STABILITY_DURATION_MINUTES: 运行时长（分钟），默认 60
    - STABILITY_ROUND_SECONDS: 每轮持续时间（秒），默认 30
    - STABILITY_CONCURRENT_WORKERS: 并发线程数，默认 20
    - STABILITY_MEMORY_GROWTH_THRESHOLD: 允许的最大内存增长比例，默认 0.20（20%）
    - STABILITY_COLUMNS_COUNT: 每行列数，默认 50
    """

    TABLE_NAME = make_table_name('StabilityTest')

    # 从环境变量读取配置
    DURATION_MINUTES = int(os.environ.get('STABILITY_DURATION_MINUTES', '60'))
    ROUND_SECONDS = int(os.environ.get('STABILITY_ROUND_SECONDS', '30'))
    CONCURRENT_WORKERS = int(os.environ.get('STABILITY_CONCURRENT_WORKERS', '20'))
    MEMORY_GROWTH_THRESHOLD = float(os.environ.get('STABILITY_MEMORY_GROWTH_THRESHOLD', '0.20'))
    COLUMNS_COUNT = int(os.environ.get('STABILITY_COLUMNS_COUNT', '50'))

    @classmethod
    def setUpClass(cls):
        cls.client = _new_client()
        _ensure_table(cls.client, cls.TABLE_NAME)
        _warmup_connection_pool(cls.client, cls.TABLE_NAME)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.delete_table(cls.TABLE_NAME)
        except Exception:
            pass

    def _run_mixed_operations(self, round_index: int, duration_seconds: int) -> tuple:
        """在指定时间内持续执行混合读写操作，返回 (总操作数, 错误数)。"""
        total_operations = 0
        error_count = 0
        deadline = time.monotonic() + duration_seconds
        base_gid = round_index * 10000

        def _worker(worker_id: int):
            nonlocal total_operations, error_count
            local_ops = 0
            local_errors = 0
            operation_index = 0

            while time.monotonic() < deadline:
                gid = base_gid + worker_id * 1000 + (operation_index % 1000)
                operation_type = operation_index % 5

                try:
                    if operation_type == 0:
                        # put_row
                        item = _make_row_item(gid, gid, self.COLUMNS_COUNT)
                        self.client.put_row(self.TABLE_NAME, item.row, item.condition)
                    elif operation_type == 1:
                        # get_row
                        self.client.get_row(
                            self.TABLE_NAME,
                            [('gid', gid), ('uid', gid)],
                            max_version=1,
                        )
                    elif operation_type == 2:
                        # batch_write_row
                        batch_count = min(10, BATCH_SIZE)
                        req = _make_batch_write(self.TABLE_NAME, gid, batch_count, self.COLUMNS_COUNT)
                        self.client.batch_write_row(req)
                    elif operation_type == 3:
                        # batch_get_row
                        batch_count = min(10, BATCH_SIZE)
                        req = _make_batch_get(self.TABLE_NAME, gid, batch_count)
                        self.client.batch_get_row(req)
                    elif operation_type == 4:
                        # get_range
                        pk_start = [('gid', gid), ('uid', INF_MIN)]
                        pk_end = [('gid', gid), ('uid', INF_MAX)]
                        self.client.get_range(
                            self.TABLE_NAME, 'FORWARD',
                            pk_start, pk_end,
                            max_version=1, limit=10,
                        )
                    local_ops += 1
                except Exception:
                    local_errors += 1

                operation_index += 1

            with threading.Lock():
                pass  # 使用原子操作更新

            return local_ops, local_errors

        worker_results = []
        with ThreadPoolExecutor(max_workers=self.CONCURRENT_WORKERS) as pool:
            futures = [
                pool.submit(_worker, worker_id)
                for worker_id in range(self.CONCURRENT_WORKERS)
            ]
            for future in as_completed(futures):
                try:
                    ops, errs = future.result(timeout=duration_seconds + 30)
                    worker_results.append((ops, errs))
                except Exception as exc:
                    _log(f"Worker 异常: {exc}", 'ERROR')

        total_operations = sum(ops for ops, _ in worker_results)
        error_count = sum(errs for _, errs in worker_results)
        return total_operations, error_count

    def _print_memory_trend(self, snapshots: list):
        """打印内存趋势报告。"""
        if not snapshots:
            return

        _log("")
        _log("╔══════════════════════════════════════════════════════════════════════════════╗")
        _log("║                          内存稳定性趋势报告                                  ║")
        _log("╠══════════════════════════════════════════════════════════════════════════════╣")
        _log(f"║  {'轮次':>4}  {'耗时':>8}  {'RSS(MB)':>10}  {'TraceMalloc(MB)':>16}  "
             f"{'操作数':>8}  {'错误':>6}  ║")
        _log("╠══════════════════════════════════════════════════════════════════════════════╣")

        for snapshot in snapshots:
            elapsed_str = f"{snapshot.elapsed_seconds / 60:.1f}min"
            _log(f"║  {snapshot.round_index:>4}  {elapsed_str:>8}  {snapshot.rss_mb:>10.1f}  "
                 f"{snapshot.tracemalloc_mb:>16.2f}  {snapshot.total_operations:>8}  "
                 f"{snapshot.errors:>6}  ║")

        # 计算内存增长（使用半程基线，排除冷启动阶段的干扰）
        midpoint = len(snapshots) // 2
        mid_rss = snapshots[midpoint].rss_mb
        final_rss = snapshots[-1].rss_mb
        first_rss = snapshots[0].rss_mb
        peak_rss = max(s.rss_mb for s in snapshots)
        rss_growth = (final_rss - mid_rss) / mid_rss if mid_rss > 0 else 0

        mid_trace = snapshots[midpoint].tracemalloc_mb
        final_trace = snapshots[-1].tracemalloc_mb
        first_trace = snapshots[0].tracemalloc_mb
        peak_trace = max(s.tracemalloc_mb for s in snapshots)
        trace_growth = (final_trace - mid_trace) / mid_trace if mid_trace > 0 else 0

        total_ops = sum(s.total_operations for s in snapshots)
        total_errors = sum(s.errors for s in snapshots)
        total_elapsed = snapshots[-1].elapsed_seconds

        _log("╠══════════════════════════════════════════════════════════════════════════════╣")
        _log(f"║  RSS:        首轮 {first_rss:.1f} MB, 半程(轮次{midpoint + 1}) {mid_rss:.1f} MB "
             f"→ 最终 {final_rss:.1f} MB (峰值 {peak_rss:.1f} MB)")
        _log(f"║  RSS 后半段增长: {rss_growth:+.1%} (半程基线 → 最终)")
        _log(f"║  TraceMalloc: 首轮 {first_trace:.2f} MB, 半程(轮次{midpoint + 1}) {mid_trace:.2f} MB "
             f"→ 最终 {final_trace:.2f} MB (峰值 {peak_trace:.2f} MB)")
        _log(f"║  TraceMalloc 后半段增长: {trace_growth:+.1%} (半程基线 → 最终)")
        _log(f"║  总操作: {total_ops:,}  总错误: {total_errors:,}  "
             f"总耗时: {total_elapsed / 60:.1f} 分钟")
        _log(f"║  平均吞吐: {total_ops / total_elapsed:.0f} ops/s" if total_elapsed > 0 else "")
        _log("╚══════════════════════════════════════════════════════════════════════════════╝")
        _log("")

    def test_long_running_memory_stability(self):
        """长时间运行稳定性测试：持续混合读写，监控内存增长。

        默认运行 1 小时，每 30 秒一轮，记录 RSS 和 tracemalloc 内存快照。
        最终断言内存增长不超过阈值（默认 20%）。
        """
        duration_minutes = self.DURATION_MINUTES
        round_seconds = self.ROUND_SECONDS
        total_rounds = (duration_minutes * 60) // round_seconds

        _log(f"═══ 稳定性测试启动 ═══")
        _log(f"  运行时长: {duration_minutes} 分钟 ({total_rounds} 轮 × {round_seconds} 秒)")
        _log(f"  并发线程: {self.CONCURRENT_WORKERS}")
        _log(f"  每行列数: {self.COLUMNS_COUNT}")
        _log(f"  内存增长阈值: {self.MEMORY_GROWTH_THRESHOLD:.0%}")
        _log("")

        # 启动 tracemalloc
        tracemalloc.start()

        # 强制 GC 建立干净基线
        gc.collect()
        gc.collect()

        snapshots: list = []
        start_time = time.monotonic()

        for round_index in range(total_rounds):
            round_start = time.monotonic()

            # 执行混合操作
            total_ops, errors = self._run_mixed_operations(round_index, round_seconds)

            # 强制 GC 后采集内存快照
            gc.collect()
            gc.collect()

            current_rss = _get_rss_mb()
            current_trace, _ = tracemalloc.get_traced_memory()
            current_trace_mb = current_trace / (1024 * 1024)
            elapsed = time.monotonic() - start_time

            snapshot = MemorySnapshot(
                round_index=round_index,
                elapsed_seconds=elapsed,
                rss_mb=current_rss,
                tracemalloc_mb=current_trace_mb,
                total_operations=total_ops,
                errors=errors,
            )
            snapshots.append(snapshot)

            # 每轮打印进度
            progress = (round_index + 1) / total_rounds * 100
            _log(f"[{progress:5.1f}%] 轮次 {round_index + 1}/{total_rounds}  "
                 f"RSS={current_rss:.1f}MB  TraceMalloc={current_trace_mb:.2f}MB  "
                 f"ops={total_ops}  errors={errors}  "
                 f"elapsed={elapsed / 60:.1f}min")

            # 每 10 轮输出一次 tracemalloc top 10（帮助定位泄漏源）
            if (round_index + 1) % 10 == 0:
                top_stats = tracemalloc.take_snapshot().statistics('lineno')[:10]
                _log("  Top 10 内存分配:")
                for stat in top_stats:
                    _log(f"    {stat}")

        tracemalloc.stop()

        # 输出完整报告
        self._print_memory_trend(snapshots)

        # 断言：内存增长不超过阈值
        # 使用半程轮次作为基线（而非第一轮），排除冷启动阶段的内存增长干扰
        if len(snapshots) >= 4:
            midpoint = len(snapshots) // 2
            # 半程基线：取半程附近 3 轮的平均值，减少波动
            mid_window = snapshots[midpoint:midpoint + min(3, len(snapshots) - midpoint)]
            baseline_rss = sum(s.rss_mb for s in mid_window) / len(mid_window)

            # 最终值：取最后 3 轮的平均值
            final_snapshots = snapshots[-min(3, len(snapshots)):]
            final_rss = sum(s.rss_mb for s in final_snapshots) / len(final_snapshots)

            if baseline_rss > 0:
                rss_growth_ratio = (final_rss - baseline_rss) / baseline_rss
                _log(f"RSS 内存增长检测: 半程基线(轮次{midpoint + 1})={baseline_rss:.1f}MB, "
                     f"最终={final_rss:.1f}MB, 增长={rss_growth_ratio:+.1%}")
                self.assertLessEqual(
                    rss_growth_ratio,
                    self.MEMORY_GROWTH_THRESHOLD,
                    f"RSS 内存增长 {rss_growth_ratio:.1%} 超过阈值 {self.MEMORY_GROWTH_THRESHOLD:.0%}。"
                    f" 半程基线(轮次{midpoint + 1}): {baseline_rss:.1f}MB, 最终: {final_rss:.1f}MB。"
                    f" 可能存在内存泄漏，请检查 tracemalloc 报告。"
                )

            # 同时检查 tracemalloc 级别的增长
            baseline_trace = sum(s.tracemalloc_mb for s in mid_window) / len(mid_window)
            final_trace = sum(s.tracemalloc_mb for s in final_snapshots) / len(final_snapshots)
            if baseline_trace > 0:
                trace_growth_ratio = (final_trace - baseline_trace) / baseline_trace
                _log(f"TraceMalloc 内存增长检测: 半程基线(轮次{midpoint + 1})={baseline_trace:.2f}MB, "
                     f"最终={final_trace:.2f}MB, 增长={trace_growth_ratio:+.1%}")
                self.assertLessEqual(
                    trace_growth_ratio,
                    self.MEMORY_GROWTH_THRESHOLD,
                    f"TraceMalloc 内存增长 {trace_growth_ratio:.1%} 超过阈值 {self.MEMORY_GROWTH_THRESHOLD:.0%}。"
                    f" 半程基线(轮次{midpoint + 1}): {baseline_trace:.2f}MB, 最终: {final_trace:.2f}MB。"
                    f" 可能存在 Python 层面的内存泄漏。"
                )

        _log("═══ 稳定性测试通过 ═══", 'SUCCESS')

    def test_native_vs_python_memory_stability(self):
        """对比 Native 编解码器开启/关闭时的内存稳定性。

        各运行 15 分钟（可通过 STABILITY_DURATION_MINUTES 调整，取 1/4），
        对比两种模式下的内存增长趋势。
        """
        quarter_duration = max(self.DURATION_MINUTES // 4, 5)
        round_seconds = self.ROUND_SECONDS
        total_rounds = (quarter_duration * 60) // round_seconds

        _log(f"═══ Native vs Python 内存稳定性对比 ═══")
        _log(f"  每种模式运行: {quarter_duration} 分钟 ({total_rounds} 轮)")

        results = {}

        for mode_name, native_enabled in [('Python', False), ('Native', True)]:
            # 切换编解码器模式
            if native_enabled:
                # Check if native encoder/decoder is available
                if not hasattr(self.client.protocol.encoder, '_use_native_encoder'):
                    _log(f"  跳过 Native 模式: Native 编解码器不可用", 'WARN')
                    continue
                self.client.protocol.encoder._use_native_encoder = True
                self.client.protocol.decoder._use_native_decoder = True
            else:
                self.client.protocol.encoder._use_native_encoder = False
                self.client.protocol.decoder._use_native_decoder = False

            _log(f"\n  ── {mode_name} 模式开始 ──")
            tracemalloc.start()
            gc.collect()
            gc.collect()

            snapshots = []
            start_time = time.monotonic()

            for round_index in range(total_rounds):
                total_ops, errors = self._run_mixed_operations(
                    round_index + (0 if native_enabled else 10000),
                    round_seconds,
                )

                gc.collect()
                gc.collect()

                current_rss = _get_rss_mb()
                current_trace, _ = tracemalloc.get_traced_memory()
                elapsed = time.monotonic() - start_time

                snapshots.append(MemorySnapshot(
                    round_index=round_index,
                    elapsed_seconds=elapsed,
                    rss_mb=current_rss,
                    tracemalloc_mb=current_trace / (1024 * 1024),
                    total_operations=total_ops,
                    errors=errors,
                ))

                progress = (round_index + 1) / total_rounds * 100
                _log(f"  [{mode_name}] [{progress:5.1f}%] 轮次 {round_index + 1}/{total_rounds}  "
                     f"RSS={current_rss:.1f}MB  ops={total_ops}")

            tracemalloc.stop()
            results[mode_name] = snapshots

        # 输出对比报告
        _log("")
        _log("╔══════════════════════════════════════════════════════════════════════╗")
        _log("║              Native vs Python 内存稳定性对比报告                     ║")
        _log("╠══════════════════════════════════════════════════════════════════════╣")

        for mode_name, snapshots in results.items():
            if not snapshots:
                continue
            if len(snapshots) >= 4:
                midpoint = len(snapshots) // 2
                mid_window = snapshots[midpoint:midpoint + min(3, len(snapshots) - midpoint)]
                baseline_rss = sum(s.rss_mb for s in mid_window) / len(mid_window)
            else:
                baseline_rss = snapshots[0].rss_mb
            final_snapshots = snapshots[-min(3, len(snapshots)):]
            final_rss = sum(s.rss_mb for s in final_snapshots) / len(final_snapshots)
            peak_rss = max(s.rss_mb for s in snapshots)
            growth = (final_rss - baseline_rss) / baseline_rss if baseline_rss > 0 else 0
            total_ops = sum(s.total_operations for s in snapshots)
            total_elapsed = snapshots[-1].elapsed_seconds

            _log(f"║  [{mode_name:>6}]  RSS: {baseline_rss:.1f} → {final_rss:.1f} MB "
                 f"(峰值 {peak_rss:.1f}, 半程基线增长 {growth:+.1%})  "
                 f"ops: {total_ops:,}  "
                 f"avg: {total_ops / total_elapsed:.0f} ops/s")

        _log("╚══════════════════════════════════════════════════════════════════════╝")
        _log("")

        # 断言两种模式都没有严重内存泄漏（使用半程基线）
        for mode_name, snapshots in results.items():
            if len(snapshots) >= 4:
                midpoint = len(snapshots) // 2
                mid_window = snapshots[midpoint:midpoint + min(3, len(snapshots) - midpoint)]
                baseline_rss = sum(s.rss_mb for s in mid_window) / len(mid_window)
                final_snapshots = snapshots[-min(3, len(snapshots)):]
                final_rss = sum(s.rss_mb for s in final_snapshots) / len(final_snapshots)
                if baseline_rss > 0:
                    growth_ratio = (final_rss - baseline_rss) / baseline_rss
                    _log(f"  [{mode_name}] RSS 半程基线(轮次{midpoint + 1})={baseline_rss:.1f}MB, "
                         f"最终={final_rss:.1f}MB, 增长={growth_ratio:+.1%}")
                    self.assertLessEqual(
                        growth_ratio,
                        self.MEMORY_GROWTH_THRESHOLD,
                        f"{mode_name} 模式 RSS 内存增长 {growth_ratio:.1%} "
                        f"超过阈值 {self.MEMORY_GROWTH_THRESHOLD:.0%}"
                        f" (半程基线: {baseline_rss:.1f}MB, 最终: {final_rss:.1f}MB)",
                    )

        _log("═══ Native vs Python 内存稳定性对比通过 ═══", 'SUCCESS')


if __name__ == '__main__':
    unittest.main()
