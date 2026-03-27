# ots_sdk.so — OTS Python SDK C++ 扩展模块

## 概述

`ots_sdk.so` 是 OTS Python SDK 的 C++ 扩展模块，基于 pybind11 构建，提供高性能的 protobuf 编解码能力。它将所有三方依赖静态链接，仅依赖系统库（`libc`、`librt`、`libpthread`），无需额外安装依赖。

## 如何构建

构建 `ots_sdk.so` 需要使用 C++ SDK 仓库 `ots/ots-cpp-sdk-for-unified` 中的构建脚本。

详细的构建指南请参考 skill 文档：

- **本仓库 skill**：`.aone_copilot/skills/ots-sdk-build/SKILL.md`
- **C++ SDK 仓库 skill**：`ots-cpp-sdk-for-unified/.aone_copilot/skills/ots-sdk-build/SKILL.md`

### 快速开始

#### 前置条件

- `python3` 在 PATH 中，且已安装 `pybind11`（`pip install pybind11`）
- `alimake` 构建工具可用
- C++ SDK 仓库已克隆到 `/apsarapangu/disk2/ots-cpp-sdk-for-unified`

#### 一键编译 + 部署

```bash
cd /apsarapangu/disk2/ots-cpp-sdk-for-unified && \
bash scripts/build_static.sh release && \
cp src/python/ots_sdk/ots_sdk.so \
   /apsarapangu/disk2/aliyun-tablestore-python-sdk/tablestore/ots_sdk/ots_sdk.so
```

#### 测试验证

```bash
cd /apsarapangu/disk2/aliyun-tablestore-python-sdk
python -m pytest tests/row_op_test.py -v -s
```

验证要点：
- 32 个测试全部 PASSED
- 不能出现 `Native encode_xxx failed, falling back to Python` 的 WARNING

## 文件说明

| 文件 | 说明 |
|---|---|
| `ots_sdk.so` | C++ 扩展模块（pybind11 构建） |
| `__init__.py` | Python 端加载入口，自动检测并加载 C++ 扩展 |

## 注意事项

- 仅替换 `ots_sdk.so` 文件，不要修改 `__init__.py`
- `ots_sdk.so` 已经过 `strip` 优化，约 13 MB
- 如需调试，可使用 `debug` 模式构建（不带 strip，约 80 MB）
