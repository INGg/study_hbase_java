package com.qfnu.hbase;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class DataDoTest {
    private Connection connection;
    public String TABLE_NAME = "12345";

    @Before
    public void beforeDo() throws IOException {
        // 使用方法创建Hbase配置
        Configuration configuration = HBaseConfiguration.create();
        // 使用方法创建Hbase连接
        connection = ConnectionFactory.createConnection(configuration);
    }

    @Test
    public void putTest() throws IOException {
        TableName tablename = TableName.valueOf("12345");

        // 1.连接获取Htable
        Table table = connection.getTable(tablename);

        // 2.构建ROWKEY 列族名、列名对象
        String rowkey = "4944191";
        String columnFamily = "C1";
        String column = "NAME";

        // 3.构建Put对象
        Put put = new Put(Bytes.toBytes(rowkey));

        // 4.添加列
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes("XIAOMING"));

        // 5.发请求使用表对象进行put操作
        // 轻量级API，它是非线程安全的
        table.put(put);

        // 6.关闭
        table.close();
    }

    @Test
    public void getTest() throws IOException {
        // 获取表
        Table table = connection.getTable(TableName.valueOf("12345"));

        // 使用rowkey构建对象
        Get get = new Get(Bytes.toBytes("4944191"));

        // 执行get请求
        Result result = table.get(get);

        // 列出所有的单元格
        List<Cell> cells = result.listCells();

        // 打印rowkey
        byte[] row = result.getRow();
        System.out.println(Bytes.toString(row));

        // 迭代单元格列表
        for (Cell cell : cells) {
            // 字节转化为字符串，获取列族的名称
            String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            // 获取列的名称
            String cn = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            // 获取值
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(cf + " : " + cn + " -> " + value);
        }

        table.close();
    }

    public void deleteTest() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        // 根据rowkey构建delete对象
        Delete delete = new Delete(Bytes.toBytes("4944191"));

        // 执行delete请求
        table.delete(delete);

        // 关闭
        table.close();
    }

    @After
    public void afterDo() throws IOException {
        connection.close();
    }
}
