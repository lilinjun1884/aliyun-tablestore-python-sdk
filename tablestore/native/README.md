# Native C 扩展手动编译指南

本目录包含 TableStore SDK 的 native C 扩展，用于提供 PlainBuffer 解析操作的显著性能提升。

## 概述

native 扩展包含以下文件：
- `native_plainbuffer.c` - PlainBuffer 解析的 C 源代码
- `__init__.py` - Python 包装器，包含回退机制
- `native_plainbuffer.cpython-*.so` - 编译后的共享库（生成的文件）

## 编译前准备

在编译之前，请确保已安装必要的开发工具：

### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install build-essential python3-dev
```

### Linux (CentOS/RHEL/Fedora)
```bash
# CentOS/RHEL
sudo yum groupinstall "Development Tools"
sudo yum install python3-devel

# Fedora
sudo dnf groupinstall "Development Tools"
sudo dnf install python3-devel
```

### macOS
```bash
# 安装 Xcode 命令行工具
xcode-select --install

# 或者通过 Homebrew 安装
brew install gcc
```

### Windows
- 安装 Visual Studio Build Tools 或 Visual Studio Community
- 确保安装了 Python 开发工作负载

## 手动编译

### Linux/macOS
```bash
# 进入 native 目录
cd tablestore/native

# 手动编译 C 扩展
gcc -shared -fPIC -O3 -std=c99 \
    -I$(python3 -c "import sysconfig; print(sysconfig.get_path('include'))") \
    -o native_plainbuffer$(python3-config --extension-suffix) \
    native_plainbuffer.c

# 对于 macOS，使用 bundle 模式：
gcc -fPIC -O3 -std=c99 \
    -I$(python3 -c "import sysconfig; print(sysconfig.get_path('include'))") \
    -isysroot$(xcrun --show-sdk-path) \
    -bundle -undefined dynamic_lookup \
    -o native_plainbuffer$(python3-config --extension-suffix) \
    native_plainbuffer.c
```

### Windows (使用 Visual Studio 开发者命令提示符)
```cmd
cl /LD /O2 /I"C:\Python3X\include" native_plainbuffer.c /link /LIBPATH:"C:\Python3X\libs" python3X.lib /OUT:native_plainbuffer.pyd
```

## 常见问题排查

### 1. 缺少编译器
```
error: Microsoft Visual C++ 14.0 is required (Windows)
error: gcc: command not found (Linux)
```

**解决方案：**
- Windows: 安装 Visual Studio Build Tools
- Linux: 安装 build-essential 包
- macOS: 安装 Xcode 命令行工具

### 2. 缺少 Python 头文件
```
fatal error: Python.h: No such file or directory
```

**解决方案：**
```bash
# Linux
sudo apt-get install python3-dev  # Ubuntu/Debian
sudo yum install python3-devel    # CentOS/RHEL

# macOS (如果通过官方安装程序安装 Python 通常不需要)
brew install python3

# Windows (如果通过官方安装程序安装 Python 通常不需要)
# 重新安装 Python 并包含开发头文件
```

### 3. 架构不匹配 (macOS)
```
ld: warning: ignoring file, building for macOS-x86_64 but attempting to link with file built for macOS-arm64
```

**解决方案：**
```bash
# 对于 Apple Silicon Mac
export ARCHFLAGS="-arch arm64"
gcc -fPIC -O3 -std=c99 -arch arm64 \
    -I$(python3 -c "import sysconfig; print(sysconfig.get_path('include'))") \
    -isysroot$(xcrun --show-sdk-path) \
    -bundle -undefined dynamic_lookup \
    -o native_plainbuffer$(python3-config --extension-suffix) \
    native_plainbuffer.c

# 对于 Intel Mac
export ARCHFLAGS="-arch x86_64"
gcc -fPIC -O3 -std=c99 -arch x86_64 \
    -I$(python3 -c "import sysconfig; print(sysconfig.get_path('include'))") \
    -isysroot$(xcrun --show-sdk-path) \
    -bundle -undefined dynamic_lookup \
    -o native_plainbuffer$(python3-config --extension-suffix) \
    native_plainbuffer.c
```

### 4. 扩展加载失败
```
ImportError: dynamic module does not define module export function
```

**解决方案：**
- 确保 C 文件具有正确的模块初始化函数
- 检查编译的扩展具有正确的文件扩展名
- 验证 Python 版本兼容性

## 性能优势

当 native 扩展成功编译和加载时：
- PlainBuffer 解析性能提升 2-3 倍
- 数据密集型操作的 CPU 使用率降低
- 内存分配开销更低

## 回退机制

SDK 包含强大的回退机制：
- 如果 native 扩展编译或加载失败，SDK 会自动回退到纯 Python 实现
- 不会丢失任何功能，只是性能会降低
- 使用回退时会显示警告消息

## 验证

要验证 native 扩展是否正常工作：

```python
from tablestore.native import NATIVE_AVAILABLE, parse_plainbuffer

print(f"Native extension available: {NATIVE_AVAILABLE}")
if NATIVE_AVAILABLE:
    print(f"Native function: {parse_plainbuffer}")
    print("✅ Native 扩展工作正常")
else:
    print("⚠️  使用 Python 回退实现")
```

## 文件结构

成功编译后，您应该看到：
```
tablestore/native/
├── __init__.py                           # Python 包装器
├── native_plainbuffer.c                  # C 源代码
├── native_plainbuffer.cpython-*.so       # 编译的扩展 (Linux/macOS)
├── native_plainbuffer.*.pyd              # 编译的扩展 (Windows)
└── README.md                             # 本文件
```

## 支持

如果遇到本指南未涵盖的问题：
1. 检查是否已安装所有先决条件
2. 验证您的 Python 版本受支持 (3.8-3.12)
3. 检查错误消息以了解具体缺少的依赖项