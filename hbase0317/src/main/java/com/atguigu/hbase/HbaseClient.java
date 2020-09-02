package com.atguigu.hbase;

import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class HbaseClient {

    //TODO 判断表是否存在
    public static boolean isTableExist(TableName tableName) throws IOException {

        //1.创建配置信息并配置
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop202,hadoop203,hadoop204");

        //2.获取与HBase的连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //3.获取DDL操作对象
        Admin admin = connection.getAdmin();

        //4.判断表是否存在操作
        boolean exists = admin.tableExists(TableName.valueOf(String.valueOf(tableName)));

        //5.关闭连接
        admin.close();
        connection.close();

        //6.返回结果
        return exists;
    }


    /*查询单条数据 get*/
    @Test
    public void getdata() throws IOException {
        //表名转换格式
        TableName tableName = TableName.valueOf("student");

        // 创建配置，添加链接
        Configuration entries = HBaseConfiguration.create();
        entries.set("hbase.zookeeper.quorum","hadoop202,hadoop203,hadoop204");
        Connection connection = ConnectionFactory.createConnection(entries);

        Table table = connection.getTable(tableName);
        byte[] bytes = "1001".getBytes();
        Get get = new Get(bytes);
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.write(CellUtil.cloneFamily(cell));
            System.out.write(' ');
            System.out.write(CellUtil.cloneQualifier(cell));
            System.out.write(' ');
            System.out.write(CellUtil.cloneRow(cell));
            System.out.write(' ');
            System.out.write(CellUtil.cloneValue(cell));
            System.out.println();
        }
        table.close();
        connection.close();
    }

    /*扫描表中数据scan*/
    @Test
    public void scantable() throws IOException {
        //表名转换格式
        TableName tableName = TableName.valueOf("student");

        // 创建配置，添加链接
        Configuration entries = HBaseConfiguration.create();
        entries.set("hbase.zookeeper.quorum","hadoop202,hadoop203,hadoop204");
        Connection connection = ConnectionFactory.createConnection(entries);

        // 创建表管理器
        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.write(CellUtil.cloneFamily(cell));
                System.out.write(' ');
                System.out.write(CellUtil.cloneQualifier(cell));
                System.out.write(' ');
                System.out.write(CellUtil.cloneRow(cell));
                System.out.write(' ');
                System.out.write(CellUtil.cloneValue(cell));
                System.out.println();
            }

        }
        table.close();
        connection.close();
    }

    /*清空表中数据*/
    @Test
    public  void deldata() throws IOException {
        //表名转换格式
        TableName tableName = TableName.valueOf("student");

        // 创建配置，添加链接
        Configuration entries = HBaseConfiguration.create();
        entries.set("hbase.zookeeper.quorum","hadoop202,hadoop203,hadoop204");
        Connection connection = ConnectionFactory.createConnection(entries);

        // 创建表管理器
        Table table = connection.getTable(tableName);
        byte[] rowkey = "1003".getBytes();

        // 定义删除行
        Delete delete = new Delete(rowkey);
        // 删除
        table.delete(delete);
        // 关闭资源
        table.close();
        connection.close();

    }

    /*插入数据*/
    @Test
    public void insetdata() throws IOException {
        //表名转换格式
        TableName tableName = TableName.valueOf("student");

        // 创建配置，添加链接
        Configuration entries = HBaseConfiguration.create();
        entries.set("hbase.zookeeper.quorum","hadoop202,hadoop203,hadoop204");
        Connection connection = ConnectionFactory.createConnection(entries);

        // 创建表管理器
        Table table = connection.getTable(tableName);

        // 行标识转换
        byte[] rowkey = "1003".getBytes();

        // 创建行管理器，确定插入的行
        Put put = new Put(rowkey);

        //  在行管理器中，定义插入信息
        put.add( new KeyValue(
                rowkey,"info".getBytes(),"name".getBytes(),"zhangsan".getBytes()
        ));

        // 在表管理器中插入信息
        table.put(put);

        // 关闭资源
        table.close();
        connection.close();


    }

    /*创建命名空间*/
    @Test
    public void nameSpcae() throws IOException {

        Configuration entries = HBaseConfiguration.create();
        entries.set("hbase.zookeeper.quorum","hadoop202,hadoop203,hadoop204");
        Connection connection = ConnectionFactory.createConnection(entries);
        Admin admin = connection.getAdmin();

        NamespaceDescriptor nn = NamespaceDescriptor.create("nn").build();
        admin.createNamespace(nn);
        admin.close();
        connection.close();
    }

    /*删除表*/
    @Test
    public void deltable() throws IOException {

        // 创建configuration，获取连接
        Configuration entries = HBaseConfiguration.create();
        entries.set("hbase.zookeeper.quorum","hadoop202,hadoop203,hadoop204");
        Connection connection = ConnectionFactory.createConnection(entries);

        //获取表操作器
        Admin admin = connection.getAdmin();
        // 获取表名
        TableName tableName = TableName.valueOf("test3");
        // 表下线
        admin.disableTable(tableName);
        // 删除表
        admin.deleteTable(tableName);

        //关闭资源
        admin.close();
        connection.close();

    }

    /* 建表*/
    public static void main(String[] args) throws IOException {
        //表名
        TableName tablename = TableName.valueOf("test3");

        // 创建configuration，获取连接
        Configuration entries = HBaseConfiguration.create();
        entries.set("hbase.zookeeper.quorum","hadoop202,hadoop203,hadoop204");
        Connection connection = ConnectionFactory.createConnection(entries);
        //初始化建表工具
        Admin admin = null;
        try{
            //获取建表工具
             admin = connection.getAdmin();
             // 依据表名建表的描述器对象，
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tablename);
            // 创建列族信息
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes());
            //  通过描述器添加列族信息
            TableDescriptor build = tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build()).build();
            // 通过建表工具建表并输入列族及表名
            admin.createTable(build);

        }finally {
            if(admin != null){
                admin.close();
            }
            connection.close();
        }
    }

}
