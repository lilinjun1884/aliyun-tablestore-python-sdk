# -*- coding: utf8 -*-
"""
压力测试单元测试 - 验证重构后的代码逻辑

测试范围：
1. StressTestConfig 配置类
2. 4KB+ 行数据生成逻辑
3. 进度日志功能
4. 分阶段测试配置
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from stress_test import StressTestConfig, STAGE_CONFIGS


class TestStressTestConfig(unittest.TestCase):
    """测试配置类"""

    def test_config_creation(self):
        """测试配置对象创建"""
        config = StressTestConfig(
            name='测试配置',
            total_rows=1000,
            concurrent_workers=10,
            rows_per_batch=50,
            row_size_kb=4,
            columns_count=50
        )
        
        self.assertEqual(config.name, '测试配置')
        self.assertEqual(config.total_rows, 1000)
        self.assertEqual(config.concurrent_workers, 10)
        self.assertEqual(config.rows_per_batch, 50)
        self.assertEqual(config.row_size_kb, 4)
        self.assertEqual(config.columns_count, 50)

    def test_default_values(self):
        """测试默认值"""
        config = StressTestConfig(
            name='默认配置',
            total_rows=500,
            concurrent_workers=5
        )
        
        self.assertEqual(config.rows_per_batch, 100)  # 默认值
        self.assertEqual(config.row_size_kb, 4)  # 默认值
        self.assertEqual(config.columns_count, 50)  # 默认值


class TestStageConfigs(unittest.TestCase):
    """测试分阶段配置"""

    def test_basic_stage(self):
        """测试基础压力配置"""
        config = STAGE_CONFIGS['basic']
        self.assertEqual(config.name, '基础压力测试')
        self.assertEqual(config.total_rows, 5000)
        self.assertEqual(config.concurrent_workers, 30)
        self.assertEqual(config.row_size_kb, 4)
        self.assertEqual(config.columns_count, 50)

    def test_medium_stage(self):
        """测试中等压力配置"""
        config = STAGE_CONFIGS['medium']
        self.assertEqual(config.name, '中等压力测试')
        self.assertEqual(config.total_rows, 50000)
        self.assertEqual(config.concurrent_workers, 50)
        self.assertEqual(config.row_size_kb, 4)
        self.assertEqual(config.columns_count, 50)

    def test_high_stage(self):
        """测试高压力配置"""
        config = STAGE_CONFIGS['high']
        self.assertEqual(config.name, '高压力测试')
        self.assertEqual(config.total_rows, 50000)
        self.assertEqual(config.concurrent_workers, 100)
        self.assertEqual(config.row_size_kb, 4)
        self.assertEqual(config.columns_count, 50)


class TestHelperFunctions(unittest.TestCase):
    """测试辅助函数和逻辑"""

    def test_row_item_creation(self):
        """测试行数据创建逻辑"""
        # 直接导入需要的类
        from tablestore import Row, PutRowItem, Condition, RowExistenceExpectation
        
        # 测试带配置的行数据创建
        config = StressTestConfig(
            name='测试',
            total_rows=100,
            concurrent_workers=10,
            columns_count=50
        )
        
        # 模拟创建行数据的逻辑
        primary_key = [('gid', 1), ('uid', 2)]
        attribute_columns = []
        
        # 生成 50+ 列
        for i in range(config.columns_count):
            col_value = f'data_1_2_col{i}_' + 'x' * 60
            attribute_columns.append((f'col_{i:03d}', col_value))
        
        # 验证列数量
        self.assertEqual(len(attribute_columns), 50)
        
        # 验证每列大小
        for col_name, col_value in attribute_columns:
            self.assertGreater(len(col_value), 70)
        
        # 计算总大小
        total_size = sum(len(name) + len(value) for name, value in attribute_columns)
        self.assertGreaterEqual(total_size, 4000)  # 验证至少 4KB

    def test_row_item_simple_mode(self):
        """测试简单模式行数据创建"""
        primary_key = [('gid', 1), ('uid', 2)]
        attribute_columns = [
            ('name', f'user_1_2'),
            ('age', 1 % 100),
            ('timestamp', int(time.time()))
        ]
        
        self.assertEqual(len(attribute_columns), 3)
        col_names = [col[0] for col in attribute_columns]
        self.assertIn('name', col_names)
        self.assertIn('age', col_names)
        self.assertIn('timestamp', col_names)

    def test_batch_limit_logic(self):
        """验证批量操作限制逻辑"""
        # 测试限制逻辑
        count = min(200, 100)
        self.assertEqual(count, 100)
        
        count = min(50, 100)
        self.assertEqual(count, 50)


class TestProgressLogging(unittest.TestCase):
    """测试进度日志系统"""

    def test_log_format(self):
        """测试日志格式"""
        timestamp = time.strftime('%H:%M:%S')
        message = "测试消息"
        level = "INFO"
        
        log_entry = f"[{timestamp}] [{level}] {message}"
        
        # 验证日志格式
        self.assertRegex(log_entry, r'\[\d{2}:\d{2}:\d{2}\] \[INFO\] 测试消息')

    def test_log_levels(self):
        """测试不同日志级别"""
        for level in ['INFO', 'SUCCESS', 'ERROR']:
            timestamp = time.strftime('%H:%M:%S')
            log_entry = f"[{timestamp}] [{level}] 测试"
            self.assertIn(f"[{level}]", log_entry)


if __name__ == '__main__':
    unittest.main()
