#!/bin/bash
# 批量测试脚本

# 测试次数
num_tests=1
# 测试脚本
name_test="3"

count=0

# 循环运行测试n次
for i in $(seq 1 $num_tests)
do
    # 在同一行显示当前测试进度
    echo -ne "Running test $i of $num_tests...\r"

    # 运行测试并将输出保存到变量中
    output=$(go test -run $name_test -race)

    # 通过grep搜索"FAIL"
    if echo "$output" | grep -q "FAIL"; then
        # 如果找到"FAIL"，则计数器加1
        ((count++))
        # 将输出保存到文件中
        timestamp=$(date +"%Y%m%d%H%M%S")
        echo "$output" > "fail_output_${timestamp}.txt"
    fi
done

echo ""

# 输出失败的总次数
echo "FAIL occurred $count times"
