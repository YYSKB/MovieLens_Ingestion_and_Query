package com.david;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDataImporter {

    // =======================================================================
    //                        !!! 需修改的配置常量 !!!
    // =======================================================================
    // 必须替换为您的虚拟机 IP 地址
    public static final String ZK_QUORUM = "192.168.56.101";
    public static final String ZK_PORT = "2181";

    // 检查并替换为您的文件绝对路径
    public static final String MOVIES_PATH = "D:/Computer Language/Java/big data processing/ml-latest-small/movies.csv";
    public static final String RATINGS_PATH = "D:/Computer Language/Java/big data processing/ml-latest-small/ratings.csv";
    // =======================================================================

    // 公开静态连接对象，供查询器使用 (已修正为 public static)
    public static Connection connection = null;
    public static Admin admin = null;

    // --- 表名和列族常量 (已修正为 public static final) ---
    public static final String MOVIES_INFO_TABLE = "movies_info";
    public static final String INFO_CF = "info";
    public static final String RATINGS_DATA_TABLE = "ratings_data";
    public static final String SCORE_CF = "score";
    public static final String MOVIE_INDEX_TABLE = "movie_ratings_index";
    public static final String REF_CF = "ref";

    // --- 1. 连接初始化与关闭 ---

    public static void initConnection() throws IOException {
        if (connection != null && !connection.isClosed()) return;

        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", ZK_QUORUM);
        configuration.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        configuration.set("hbase.unsafe.stream.capability.enforce", "false");

        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
        System.out.println("HBase Connection initialized successfully.");
    }

    public static void closeConnection() {
        try {
            if (admin != null) admin.close();
            if (connection != null) connection.close();
            System.out.println("HBase Connection closed.");
        } catch (IOException e) {
            // 警告提示：实验中保留 printStackTrace 方便调试
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }

    // --- 2. 表结构操作 ---

    public static void createTable(String tableNameStr, String[] columnFamilies) throws IOException {
        TableName tableName = TableName.valueOf(tableNameStr);

        if (admin.tableExists(tableName)) {
            System.out.println("Table " + tableNameStr + " already exists. Skipping creation.");
            return;
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

        for (String family : columnFamilies) {
            tableDescriptorBuilder.setColumnFamily(
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build()
            );
        }

        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("Table " + tableNameStr + " created successfully.");
    }

    public static void createAllTables() throws IOException {
        createTable(MOVIES_INFO_TABLE, new String[]{INFO_CF});
        createTable(RATINGS_DATA_TABLE, new String[]{SCORE_CF});
        createTable(MOVIE_INDEX_TABLE, new String[]{REF_CF});
    }

    // --- 3. 数据导入操作 ---

    public static void importMoviesData() throws IOException {
        TableName tableName = TableName.valueOf(MOVIES_INFO_TABLE);
        int count = 0;

        try (Table table = connection.getTable(tableName);
             BufferedReader reader = new BufferedReader(new FileReader(MOVIES_PATH))) {

            reader.readLine(); // 跳过表头
            String line;
            List<Put> puts = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                // 使用正则表达式处理带引号的字段 (如电影标题)
                String[] parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                if (parts.length < 3) continue;

                String movieId = parts[0].trim();
                String title = parts[1].trim().replaceAll("^\"|\"$", ""); // 去除标题两端的引号
                String genres = parts[2].trim().replaceAll("^\"|\"$", "");

                Put put = new Put(Bytes.toBytes(title));
                put.addColumn(Bytes.toBytes(INFO_CF), Bytes.toBytes("movieId"), Bytes.toBytes(movieId));
                put.addColumn(Bytes.toBytes(INFO_CF), Bytes.toBytes("genres"), Bytes.toBytes(genres));

                puts.add(put);
                count++;

                if (puts.size() >= 1000) {
                    table.put(puts);
                    puts.clear();
                }
            }
            if (!puts.isEmpty()) {
                table.put(puts);
            }

            System.out.printf("Successfully imported %d movie records to %s.%n", count, MOVIES_INFO_TABLE);
        }
    }

    public static void importRatingsData() throws IOException {
        TableName dataTable = TableName.valueOf(RATINGS_DATA_TABLE);
        TableName indexTable = TableName.valueOf(MOVIE_INDEX_TABLE);
        int count = 0;

        try (Table ratingsTable = connection.getTable(dataTable);
             Table indexTableAccess = connection.getTable(indexTable);
             BufferedReader reader = new BufferedReader(new FileReader(RATINGS_PATH))) {

            reader.readLine(); // 跳过表头
            String line;
            List<Put> dataPuts = new ArrayList<>();
            List<Put> indexPuts = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 4) continue;

                String userId = parts[0];
                String movieId = parts[1];
                String rating = parts[2];
                String timestamp = parts[3];

                // 1. 写入 ratings_data (RowKey: userId_movieId)
                String dataRowKeyStr = userId + "_" + movieId;
                Put dataPut = new Put(Bytes.toBytes(dataRowKeyStr));
                dataPut.addColumn(Bytes.toBytes(SCORE_CF), Bytes.toBytes("rating"), Bytes.toBytes(rating));
                dataPut.addColumn(Bytes.toBytes(SCORE_CF), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
                dataPuts.add(dataPut);

                // 2. 写入 movie_ratings_index (RowKey: movieId_userId)
                String indexRowKeyStr = movieId + "_" + userId;
                Put indexPut = new Put(Bytes.toBytes(indexRowKeyStr));
                indexPut.addColumn(Bytes.toBytes(REF_CF), Bytes.toBytes("rating"), Bytes.toBytes(rating));
                indexPut.addColumn(Bytes.toBytes(REF_CF), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
                indexPuts.add(indexPut);

                count++;

                // 批量处理，解决了重复代码的提示
                if (dataPuts.size() >= 1000) {
                    ratingsTable.put(dataPuts);
                    indexTableAccess.put(indexPuts);
                    dataPuts.clear();
                    indexPuts.clear();
                }
            }

            if (!dataPuts.isEmpty()) {
                ratingsTable.put(dataPuts);
                indexTableAccess.put(indexPuts);
            }

            System.out.printf("Successfully imported %d rating records to both rating tables.%n", count);
        }
    }

    // --- 主函数入口：执行导入操作 ---
    public static void main(String[] args) {
        try {
            initConnection();

            // 1. 创建表结构
            createAllTables();

            // 2. 导入数据
            System.out.println("Starting data import...");
            importMoviesData();
            importRatingsData();

            System.out.println("All import tasks completed successfully.");

        } catch (IOException e) {
            System.err.println("An I/O error occurred during operation: " + e.getMessage());
            e.printStackTrace(); // 警告提示：实验中保留 printStackTrace 方便调试
        } finally {
            closeConnection();
        }
    }
}