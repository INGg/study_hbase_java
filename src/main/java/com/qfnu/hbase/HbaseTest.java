package com.qfnu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseTest {
    public static void main(String[] args) throws Exception{
        //创建Hadoop配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master:2181,slave1:2181,slave2:2181");
        //创建连接对象Connection
        Connection conn = ConnectionFactory.createConnection(conf);
        //得到数据库管理对象
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("a1");
        //创建表描述并指定表名
        HTableDescriptor desc = new HTableDescriptor(tableName);
        //创建列族描述
        HColumnDescriptor family = new HColumnDescriptor("b1");
        //指定列族
        desc.addFamily(family);
        admin.createTable(desc);
        System.out.println("create table success!!");
    }
}
