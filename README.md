使用说明
==============
使用说明请参考：https://github.com/danfengcao/binlog2sql

安装
==============
```
git clone https://github.com/Michaelsky0913/binlog2sql.git && cd binlog2sql && \
pip install -r requirements.txt
```
git与pip的安装问题请自行搜索解决。

修改
==============
* 1. 添加日志异常日志输出（主要输出blob数据类型解码失败的表，解码失败的sql不会输出）
* 2. 修改requirement.txt文件
* 3. 添加一些注释
* 4. 添加对json数据类型的支持
* 5. 关闭flashback模式临时文件的输出（测试发现从临时文件读取数据，部分二进制数据会被截断，从而导致输出异常，替代方案是直接print结果到终端）
* 6. 新增一个对结果文件的排序脚本
* 7. 修改连接mysql实例使用的默认字符集为 utf8mb4

测试
==============
* 1. 仅测试了mysql 5.7
