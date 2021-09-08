# SparkSQLOnHBase
该项目基于`spark sql`开发，实现了在`HBase`上进行sql查询的功能， 支持标准sql查询操作，也包含了spark sql本身的谓词下推，剪枝等优化，后续有空闲时间会增加支持插入，删除，建表相关的ddl语句的实现
 [`rowkey`生成策略与sql结合的问题尚未找到较好的解决方案]。 
 
 注：项目中使用到的spark、hbase均为2021-09-07 13:00:00从github中clone并编译的版本，版本不同时部分接口略有出入可能导致程序无法运行

1. HBase的`Namespace`与SQL中的`Database`对应, 在未制定`Database`的情况下, 可以使用`namespace.table`进行表查询, 例如`select * from namespace.table`
2. 目前列名部分暂定为`cf_qualifier`形式, `ColumnFamily`与`Qualifier`之间以`_`分隔, 因此想要查询HBase表`A`中的列`cf:1`时，需要输入查询语句应为：`select cf_1 from A`
3. HBase表以及Namespace的操作默认通过`org.apache.spark.sql.hbase.client.HBaseClientImpl`实现，可修改spark_hbase.properties中`spark.hbase.client.impl`值修改为其他自定义实现方式
 
由于spark sql不同大版本实现差异过大，因此该项目只支持spark2.x及以上版本


# GetStarted
1. 替换resource目录下hbase-site.xml，修改spark_hbase.properties的各属性值
2. 依次运行项目下test/scala中的 `org.apache.spark.sql.hbase.client.TestHBase`的`createUserNamespaceAndTable()`, 
`insertData()`以及
`scan()`方法导入数据
3. 执行`org.apache.spark.sql.hbase.HBaseSQLClient`的main方法启动程序，输入sql语句检查程序是否正常运行

## 程序部分截图如下：
### 1. show databases;
![Image text](https://github.com/wangpy1995/Spark-SQL-HBase/blob/master/src/main/resources/show/show_databases.png)
### 2. show tables;
![Image text](https://github.com/wangpy1995/Spark-SQL-HBase/blob/master/src/main/resources/show/show_tables.png)
### 3. select * from meta;
![Image text](https://github.com/wangpy1995/Spark-SQL-HBase/blob/master/src/main/resources/show/select_from_meta.png)
### 4. select * from wpy1.test where cf1_cf1_0 like "%24%";
![Image text](https://github.com/wangpy1995/Spark-SQL-HBase/blob/master/src/main/resources/show/select_from_test_like.png)
### 5. select cf2_cf2_0 from wpy1.test where cf1_cf1_0 like "%24%";
![Image text](https://github.com/wangpy1995/Spark-SQL-HBase/blob/master/src/main/resources/show/select_one_col_from_test_like.png)


####由于环境限制，程序还有许多部分未来得及测试，诸多不完善之处，还请大家多多提出宝贵意见
