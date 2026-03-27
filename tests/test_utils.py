# -*- coding: utf8 -*-

"""
测试工具模块

提供测试中使用的通用工具函数，如随机表名生成等。
"""

import random
import string


def generate_random_suffix(length=16):
    """Generate a random suffix of letters and digits.
    
    Args:
        length: Length of the suffix, default 16
        
    Returns:
        Random string of specified length
    """
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def make_table_name(base_name):
    """Create a table name with random suffix.
    
    Args:
        base_name: Base table name
        
    Returns:
        Table name with random suffix appended
    """
    return f"{base_name}_{generate_random_suffix()}"
