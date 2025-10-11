package com.david;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

// 从 HBaseDataImporter 中静态导入连接对象和常量
import static com.david.HBaseDataImporter.*;

public class HBaseQueryer {

    // --- 1. 查询需求 1: 查询电影详情 (Get) ---
    public static void queryMovieDetail(String movieTitle) throws IOException {
        TableName tableName = TableName.valueOf(MOVIES_INFO_TABLE);

        try (Table table = connection.getTable(tableName)) {

            Get get = new Get(Bytes.toBytes(movieTitle));
            get.addFamily(Bytes.toBytes(INFO_CF));

            Result result = table.get(get);

            if (result.isEmpty()) {
                System.out.println("❌ 查询失败：未找到电影 [" + movieTitle + "]");
                return;
            }

            String genres = Bytes.toString(result.getValue(Bytes.toBytes(INFO_CF), Bytes.toBytes("genres")));
            String movieId = Bytes.toString(result.getValue(Bytes.toBytes(INFO_CF), Bytes.toBytes("movieId")));


            System.out.println("\n===========================================");
            System.out.println("✅ 查询 1 结果 - 电影详情:");
            System.out.println("  Title (行键): " + movieTitle);
            System.out.println("  Movie ID: " + movieId);
            System.out.println("  Genres: " + genres);
            System.out.println("===========================================");

        }
    }

    // --- 2. 查询需求 2: 用户评分 (Scan + PrefixFilter) ---
    public static void queryUserRatings(String userId) throws IOException {
        TableName tableName = TableName.valueOf(RATINGS_DATA_TABLE);

        try (Table table = connection.getTable(tableName)) {

            Scan scan = new Scan();
            // PrefixFilter 只返回 Row Key 以 userId_ 开头的行
            scan.setRowPrefixFilter(Bytes.toBytes(userId + "_"));
            scan.addFamily(Bytes.toBytes(SCORE_CF));

            ResultScanner scanner = table.getScanner(scan);

            System.out.println("\n===========================================");
            System.out.println("✅ 查询 2 结果 - 用户 [" + userId + "] 的所有评分:");
            int count = 0;

            for (Result result : scanner) {
                String rowKeyStr = Bytes.toString(result.getRow());
                String movieId = rowKeyStr.substring(rowKeyStr.indexOf('_') + 1);

                String rating = Bytes.toString(result.getValue(Bytes.toBytes(SCORE_CF), Bytes.toBytes("rating")));
                String timestamp = Bytes.toString(result.getValue(Bytes.toBytes(SCORE_CF), Bytes.toBytes("timestamp")));

                System.out.printf("  -> Movie ID: %s, Rating: %s, Timestamp: %s%n", movieId, rating, timestamp);
                count++;
            }

            if (count == 0) {
                System.out.println("❌ 未找到用户 [" + userId + "] 的任何评分记录。");
            } else {
                System.out.println("  总共找到 " + count + " 条记录。");
            }
            System.out.println("===========================================");
        }
    }

    // --- 3. 查询需求 3: 电影所有评分 (间接查询) ---
    public static void queryMovieRatingsByTitle(String movieTitle) throws IOException {
        // Step 1: 通过电影名称获取 MovieId
        String movieId = getMovieIdByTitle(movieTitle);

        if (movieId == null) {
            System.out.println("\n===========================================");
            System.out.println("❌ 查询 3 失败：未找到电影 [" + movieTitle + "] 的 ID。");
            System.out.println("===========================================");
            return;
        }

        TableName tableName = TableName.valueOf(MOVIE_INDEX_TABLE);

        try (Table table = connection.getTable(tableName)) {

            Scan scan = new Scan();
            // PrefixFilter 扫描 movie_ratings_index 表中所有以 movieId_ 开头的行
            scan.setRowPrefixFilter(Bytes.toBytes(movieId + "_"));
            scan.addFamily(Bytes.toBytes(REF_CF));

            ResultScanner scanner = table.getScanner(scan);

            System.out.println("\n===========================================");
            System.out.println("✅ 查询 3 结果 - 电影 [" + movieTitle + "] 获得的所有评分:");
            int count = 0;

            for (Result result : scanner) {
                String rowKeyStr = Bytes.toString(result.getRow());
                String userId = rowKeyStr.substring(rowKeyStr.indexOf('_') + 1);

                String rating = Bytes.toString(result.getValue(Bytes.toBytes(REF_CF), Bytes.toBytes("rating")));
                String timestamp = Bytes.toString(result.getValue(Bytes.toBytes(REF_CF), Bytes.toBytes("timestamp")));

                System.out.printf("  -> User ID: %s, Rating: %s, Timestamp: %s%n", userId, rating, timestamp);
                count++;
            }

            if (count == 0) {
                System.out.println("❌ 电影 [" + movieTitle + "] 暂无评分记录。");
            } else {
                System.out.println("  总共找到 " + count + " 条记录。");
            }
            System.out.println("===========================================");
        }
    }

    /**
     * 辅助方法：通过电影名称 (title) 在 movies_info 表中获取其 MovieId (用于查询 3)
     */
    private static String getMovieIdByTitle(String movieTitle) throws IOException {
        TableName tableName = TableName.valueOf(MOVIES_INFO_TABLE);
        try (Table table = connection.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(movieTitle));
            Result result = table.get(get);
            if (!result.isEmpty()) {
                byte[] movieIdBytes = result.getValue(Bytes.toBytes(INFO_CF), Bytes.toBytes("movieId"));
                return Bytes.toString(movieIdBytes);
            }
            return null;
        }
    }

    // --- 主函数入口：执行查询操作测试 ---
    public static void main(String[] args) {
        try {
            // 1. 初始化连接
            initConnection();

            // 2. 执行所有查询测试
            queryMovieDetail("Toy Story (1995)");
            queryUserRatings("1");
            queryMovieRatingsByTitle("Toy Story (1995)");

        } catch (IOException e) {
            System.err.println("An I/O error occurred during query operation: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }
}