# SparkSQLOnHBase
 利用spark sql在HBase上搭建的sql查询， 支持标准sql查询操作，后续有空闲时间会增加支持插入，删除，建表相关的ddl 语法（rowkey生成策略 部分尚未找到较好的解决方案，hbase查询 table也有待修改[目前暂定为TableMapper]）。 
 
目前列名部分暂定为cf_qualifier 形式。。。。  例如 cf:1列的查询为 select cf_1 from ***
此版本只支持spark2.0及以上  
注: relation目前暂定为CatalogRelation，后续将使用自定义的relation更好的关联hbase操作。
