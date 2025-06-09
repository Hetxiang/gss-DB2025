#!/bin/bash

# 设置颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 帮助信息
show_help() {
    echo -e "${YELLOW}用法: $0 [选项]${NC}"
    echo "选项:"
    echo "  -h, --help     显示帮助信息"
    echo "  -c, --clean    清理构建目录"
    echo
}

# 错误处理函数
error_exit() {
    echo -e "${RED}错误: $1${NC}" 1>&2
    exit 1
}

# 清理函数
clean_build() {
    echo -e "${YELLOW}清理构建目录...${NC}"
    rm -rf ./build
    echo -e "${GREEN}清理完成${NC}"
    exit 0
}

# 处理命令行参数
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -h|--help) show_help; exit 0 ;;
        -c|--clean) clean_build ;;
        *) error_exit "未知选项: $1" ;;
    esac
    shift
done

# 检查并创建build目录
if [ ! -d "./build" ]; then
    echo -e "${YELLOW}创建build目录...${NC}"
    mkdir -p build || error_exit "无法创建build目录"
fi

# 进入build目录
cd ./build || error_exit "无法进入build目录"

# 运行cmake
echo -e "${YELLOW}运行cmake...${NC}"
cmake .. || error_exit "cmake配置失败"
echo -e "${GREEN}cmake配置成功${NC}"

# 编译rmdb
echo -e "${YELLOW}编译rmdb...${NC}"
make rmdb -j8 || error_exit "rmdb编译失败"
echo -e "${GREEN}rmdb编译成功${NC}"

# 编译unit_test
echo -e "${YELLOW}编译unit_test...${NC}"
make unit_test -j8 || error_exit "unit_test编译失败"
echo -e "${GREEN}unit_test编译成功${NC}"

# 运行测试
echo -e "${YELLOW}运行unit_test...${NC}"
./bin/unit_test || error_exit "unit_test执行失败"
echo -e "${GREEN}所有测试完成${NC}"