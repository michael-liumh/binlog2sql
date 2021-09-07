使用说明
==============
使用说明请参考：https://github.com/danfengcao/binlog2sql

安装
==============
```shell
git clone https://github.com/Michaelsky0913/binlog2sql.git && \
cd binlog2sql && \
pip3 install -r requirements.txt
```
git与pip的安装问题请自行搜索解决。

修改
==============
* 添加日志模块
  * logger
  * 使用前需要执行 set_log_format() 函数设置格式
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
    * python3 filter_binlog2sql_result.py -s 1.sql -o 2.sql
* 添加对 blob 数据类型的支持（包括 binary、varbinary 等二进制类型）
* 添加 binlog file 的解析支持
* 添加 binlog file dir，可指定解析某个目录下特定的几个 binlog file，用 --check 检查选择的文件是否正确，--file-dir 选择目录，--file-regex 正则匹配 binlog file

测试
==============
* 仅测试了mysql 5.7、python 3.8.5

TODO
==============
- [x] 添加对 json 数据类型的支持
- [x] 添加对 blob 数据类型的支持（包括 binary、varbinary 等二进制类型）
- [x] 直接解析指定目录下binlog文件（可指定binlog目录前缀，排除非binlog文件）