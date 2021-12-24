使用说明
==============
[使用说明](./source_file/README.md)

安装
==============
```shell
git clone https://github.com/michael-liumh/binlog2sql.git && \
cd binlog2sql && \
pip3 install -r requirements.txt
```
git与pip的安装问题请自行搜索解决。

修改
==============
* 添加日志模块
* 修改requirement.txt文件
  * 修改 mysql-replication 版本为 0.23
* 添加对 json 数据类型的支持
* 关闭 flashback 模式临时文件的输出（测试发现从临时文件读取数据，部分二进制数据会被截断，从而导致输出异常，替代方案是直接print结果到终端）
* 新增一个对结果文件的排序脚本
  * sort_dml.sh
* 修改连接mysql实例使用的默认字符集为 utf8mb4
* 新增一个过滤结构的脚本
  * 介绍：
    * 将没有变化的字段去除(条件中的主键会保留)，如：
    * update t1 set updated_at=123, c1=100 where id=100 and updated_at=123 and c1=99  
    * ---> 
    * update t1 set c1=100 where id=100 and c1=99
  * 用法：
    * python3 filter_binlog2sql_result.py -f 1.sql -o 2.sql
* 添加对 blob 数据类型的支持（包括 binary、varbinary 等二进制类型）
* 添加 binlog file 的解析支持
* 添加 binlog file dir，可指定解析某个目录下特定的几个 binlog file
  * --check 检查选择的文件是否正确
  * --file-dir 选择目录
  * --file-regex 正则匹配 binlog file
  * --start-file 选择目录中起始的 binlog file
  * --stop-file 选择目录中终止的 binlog file
* 添加忽略特定库、表、列的支持，方便解析后直接在另一个实例上执行
* 添加将 insert into 语句替换成 replace into 和 insert ignore into 语句的支持，方便解析后直接在另一个实例上执行
* 添加 update、delete 语句根据主键进行更新的支持，方便解析后直接在另一个实例上执行
* 添加将 binlogfile2sql 解析结果保存到特定文件、目录的支持，方便另一个应用直接读取这个目录下 SQL 文件来执行
* 添加 binlogfile2sql 只解析修改时间在指定分钟数前的支持，防止解析到未写入完成的 binlog，方便解析后直接在另一个实例上执行
* 添加对结果 SQL 中的库名进行重命名的支持，方便解析后直接在另一个实例上执行
* 添加去除注释的支持，方便解析后直接在另一个实例上执行
* 添加 binlogfile2sql 忽略虚拟列的支持
* 添加支持根据 GTID 过滤

测试
==============
* 仅测试了mysql 5.7、python 3.8.5

TODO
==============
- [x] 添加对 json 数据类型的支持
- [x] 添加对 blob 数据类型的支持（包括 binary、varbinary 等二进制类型）
- [x] 直接解析指定目录下binlog文件（可指定binlog目录前缀，排除非binlog文件）
- [x] 修复 binlogfile2sql json 数据解析处理的SQL执行不了的 bug
- [x] 修复 binlogfile2sql start-position 参数不生效的 bug
- [x] 添加 GTID 支持