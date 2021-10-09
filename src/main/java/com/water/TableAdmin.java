package com.water;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Scanner;

public class TableAdmin {

    private Connection connection;
    private Admin admin;

    @Before
    public void beforeDo() throws IOException {
        // 使用方法创建Hbase配置
        Configuration configuration = HBaseConfiguration.create();
        // 使用方法创建Hbase连接
        connection = ConnectionFactory.createConnection(configuration);
        // 创建表，需要基于Hbase连接和获取admin管理对象，需要和Hmaster进行连接
        admin = connection.getAdmin();
    }

    @Test
    public void createTable() throws IOException {
//        Scanner sc = new Scanner(System.in);
//
//        System.out.println("请输入表名：");
//        String name = sc.nextLine();

        // 构建表名
        TableName tablename = TableName.valueOf("12345");

        // 判断表是否存在
        if(admin.tableExists(tablename)) return;

        // 构建表
        // 使用构建表描述构建器tableDescriptorBuilder
        // tableDescriptor:表描述器，描述这个表有几个列族、其他的属性
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tablename);

        // 使用列族描述构建器构造列族
        // 创建列族也需要构建器
        // 经常使用到Bytes工具类（在hbase包下的），这个类可以将其他类型的转换为byte[]数组
        // 也可以将byte[]转化为指定类型
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C1"));

        // 构建列族描述，构建表描述
        ColumnFamilyDescriptor cfdes = columnFamilyDescriptorBuilder.build();

        // 建立表和列族的关联
        tableDescriptorBuilder.setColumnFamily(cfdes);
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        // 创建表，要由admin来操作
        admin.createTable(tableDescriptor);
    }

    @Test
    public void deleteTableTest() throws IOException {
        // 判断表存在

        TableName tablename = TableName.valueOf("12345");

        if(admin.tableExists(tablename)){
            // 存在就进行删除步骤
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
    }

    @After
    public void afterDo() throws IOException {
        // 关闭连接
        admin.close();
        connection.close();
    }
}
