#!/usr/bin/env bash
# pay attention to the params below and set refer to the notes
# 计算当前目录
BENCHMARK_HOME="$(cd "`dirname "$0"`"/.; pwd)"
echo $BENCHMARK_HOME
#编译
mvn clean package -Dmaven.test.skip=true
# 参数路径
PARAM_PATH=${BENCHMARK_HOME}"/"param.properties
# 启动程序
CLASSPATH=""
for f in ${BENCHMARK_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi
MAIN_CLASS="TSDBTest"

DATA_PATH="${BENCHMARK_HOME}"

rm -rf "${DATA_PATH}"/farm
rm -rf "${DATA_PATH}"/device
##测试数据库选择
# 1:influxdb ;2:timescaledb ;3:iotdb ;4 opentsdb;5 druid;7 TDengine
DB_CODE=1
##测试项选择
# 0: generate; 1: i,w,r; 2: w,r; 3: i; 4 w; 5: r; 6 i,r;
TEST_METHOD=1
##导入的数据量
# 1: 432 Millon; 2: 2160 Millon
I_METHOD_MOD=1
###METHOD==6时有效
##导入数据时的使用的线程数
THREAD_NUM=4
##每批次导入的数据量
##每单位表示50行数据（即2500个数据点）
##需保证CACHELINE可以被THREAD_NUM整除
CACHELINE=400

exec "$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS" "$DB_CODE" "${TEST_METHOD}" "${DATA_PATH}" "${THREAD_NUM}" "${CACHELINE}" "${I_METHOD_MOD}"
exit $?
