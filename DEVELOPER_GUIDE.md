
# 开发指南
本项目使用 [poetry](https://poetry.pythonlang.cn/docs/) 进行项目管理。

## 1. 安装 poetry

> 具体可参考： 
> 1. pipx 安装：https://pipx.pypa.io/stable/installation/#on-linux
> 2. poetry 安装：https://poetry.pythonlang.cn/docs/#installation

```shell
  pipx install poetry
```

## 2. 安装项目依赖
```shell
  poetry install
```

## 3. 代码生成 

```shell
  # PB
  sh ./protoc.sh
```

```shell
  # flatbuffers
  sh ./flatc.sh
```

## 4. 测试

#### 1. 设置环境变量

**注意：测试 case 中会有清理某个实例下所有表的动作，所以请使用专门的测试实例来测试。**

```shell

   export OTS_TEST_ACCESS_KEY_ID=<your access key id>
   export OTS_TEST_ACCESS_KEY_SECRET=<your access key secret>
   export OTS_TEST_ENDPOINT=<tablestore service endpoint>
   export OTS_TEST_INSTANCE=<tablestore instance name>
   export OTS_TEST_REGION=<tablestore region>
```
#### 2. Pycharm 中运行
直接编辑器里右击文件即可，或者运行某一个函数。

#### 3. 命令行运行

```shell
   # 运行某一个测试
  poetry run pytest tests/full_text_search_test.py -v -s
```

```shell
  # 运行全部
  poetry run pytest tests -v -s
```


## 5. 发布到 pypi 和 测试
> 参考 `.aoneci/upload.sh`，在 gitlab 的 持续集成中，运行即可触发发布。 注意要先发布到测试仓库，再发布到外部仓库。

发布到测试仓库后，使用如下命令进行引用，可测试功能。

```shell
  pip install --index-url https://test.pypi.org/simple/ tablestore==你的版本
```
