#!/bin/bash

# 运行测试脚本
echo "正在运行测试脚本..."
./test.sh

# 读取表名
tableName="index_test_db"

# 检查并删除表名对应的文件夹
if [ -d "$tableName" ]; then
    echo "发现文件夹 $tableName,正在删除..."
    rm -rf "$tableName"
    echo "文件夹 $tableName 已删除"
else
    echo "文件夹 $tableName 不存在"
fi

# 运行数据库命令
echo "正在运行数据库命令..."
build/bin/rmdb $tableName