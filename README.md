# Aliyun Tablestore SDK for Python

[![Software License](https://img.shields.io/badge/license-apache2-brightgreen.svg)](LICENSE)
[![Version](https://badge.fury.io/gh/aliyun%2Faliyun-tablestore-python-sdk.svg)]( https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk)

# 概述

- 此 Python SDK 基于 `阿里云表格存储服务 <http://www.aliyun.com/product/ots/>`_  API 构建。
- 阿里云表格存储是构建在阿里云飞天分布式系统之上的 NoSQL 数据存储服务，提供海量结构化数据的存储和实时访问。

# 运行环境

- 安装 Python 即可运行，支持 python3.8、Python3.9、python3.10、python3.11、python3.12。

# 安装方法

## 1. PIP安装
```shell
  pip install tablestore
```

## 2. 源码安装

1. 下载源码
```shell
  git clone https://github.com/aliyun/aliyun-tablestore-python-sdk.git
```
2. 构建 whl (构建好的whl文件在dist目录下)
```shell
  poetry build
```
3. 安装
```shell
  pip install dist/tablestore-{替换为实际版本}-py3-none-any.whl
```

# 示例代码

### 表（Table）示例：
- [表操作（表的创建、获取、更新和删除）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/table_operations.py)
- [单行写（向表内写入一行数据）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/put_row.py)
- [单行读（从表内读出一样数据）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/get_row.py)
- [更新单行（更新某一行的部分字段）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/update_row.py)
- [删除某行（从表内删除某一行数据）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/delete_row.py)
- [批量写（向多张表，一次性写入多行数据）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/batch_write_row.py)
- [批量读（从多张表，一次性读出多行数据）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/batch_get_row.py)
- [范围扫描（给定一个范围，扫描出该范围内的所有数据）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/get_range.py)
- [主键自增列（主键自动生成一个递增ID）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/pk_auto_incr.py)
- [全局二级索引](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/secondary_index_operations.py)
- [局部事务（提交事务）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/transaction_and_commit.py)
- [局部事务（舍弃事务）](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/transaction_and_abort.py)

### 多元索引（Search）示例：

- [基础搜索](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/search_index.py)
- [并发圈选数据](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/parallel_scan.py)
- [全文检索](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/full_text_search.py)
- [向量检索](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/parallel_scan.py)
- [Max/Min/Sum/Avg/Count/DistinctCount等](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/agg.py)
- [GroupBy/Histogram等](https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/group_by.py)


# 贡献代码
- 请参考 [开发指南](./DEVELOPER_GUIDE.md) 进行开发.
- 我们非常欢迎大家为 Tablestore Python SDK 以及其他 Tablestore SDK 贡献代码。
- 非常感谢 [@Wall-ee](https://github.com/Wall-ee) 对 4.3.0 版本的贡献。

# 联系我们

- [阿里云 Tablestore 官方网站](http://www.aliyun.com/product/ots)
- [阿里云官网联系方式](https://help.aliyun.com/document_detail/61890.html)
- [阿里云 Tablestore 官方文档](https://help.aliyun.com/zh/tablestore/product-overview)


