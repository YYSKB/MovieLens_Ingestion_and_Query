package com.david;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// 从 HBaseDataImporter 中静态导入连接对象和常量
import static com.david.HBaseDataImporter.*;

public class HBaseQueryer {

    // =======================================================================
    //                       1. 查询需求 1: 电影详情 (Get)
    // =======================================================================
    /**
     * 查询需求 1: 查询电影详情 (按名称)
     * 使用 Get 操作在 movies_info 表中通过 Row Key (title) 进行精确查询。
     * @param movieTitle 电影名称 (Row Key)
     * @return 电影详情的 Map，如果未找到则返回 null。
     */
    public static Map<String, String> queryMovieDetail(String movieTitle) throws IOException {
        TableName tableName = TableName.valueOf(MOVIES_INFO_TABLE);

        try (Table table = connection.getTable(tableName)) {

            Get get = new Get(Bytes.toBytes(movieTitle));
            get.addFamily(Bytes.toBytes(INFO_CF));

            Result result = table.get(get);

            if (result.isEmpty()) {
                return null;
            }

            // 封装结果：使用 LinkedHashMap 保持键值顺序
            Map<String, String> details = new LinkedHashMap<>();
            details.put("title", movieTitle);
            details.put("movieId", Bytes.toString(result.getValue(Bytes.toBytes(INFO_CF), Bytes.toBytes("movieId"))));
            details.put("genres", Bytes.toString(result.getValue(Bytes.toBytes(INFO_CF), Bytes.toBytes("genres"))));

            return details;

        }
    }

    // =======================================================================
    //                       2. 查询需求 2: 用户评分 (Scan + PrefixFilter)
    // =======================================================================
    /**
     * 查询需求 2: 查询某用户的所有评分记录 (按用户 ID)
     * 使用 Scan 操作和 PrefixFilter 在 ratings_data 表中进行范围查询。
     * @param userId 要查询的用户 ID
     * @return 该用户的所有评分记录列表，每条记录是一个 Map。
     */
    public static List<Map<String, String>> queryUserRatings(String userId) throws IOException {
        TableName tableName = TableName.valueOf(RATINGS_DATA_TABLE);
        List<Map<String, String>> ratingsList = new ArrayList<>();

        try (Table table = connection.getTable(tableName)) {

            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(userId + "_"));
            scan.addFamily(Bytes.toBytes(SCORE_CF));

            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                String rowKeyStr = Bytes.toString(result.getRow());
                String movieId = rowKeyStr.substring(rowKeyStr.indexOf('_') + 1);

                String rating = Bytes.toString(result.getValue(Bytes.toBytes(SCORE_CF), Bytes.toBytes("rating")));
                String timestamp = Bytes.toString(result.getValue(Bytes.toBytes(SCORE_CF), Bytes.toBytes("timestamp")));

                Map<String, String> record = new LinkedHashMap<>();
                record.put("movieId", movieId);
                record.put("rating", rating);
                record.put("timestamp", timestamp);
                record.put("userId", userId);

                ratingsList.add(record);
            }

            return ratingsList;
        }
    }

    // =======================================================================
    //                       3. 查询需求 3: 电影所有评分 (间接查询)
    // =======================================================================

    /**
     * 查询需求 3: 查询某部电影的所有评分 (按电影名称)
     * @param movieTitle 电影名称
     * @return 该电影的所有评分记录列表，每条记录是一个 Map。
     */
    public static List<Map<String, String>> queryMovieRatingsByTitle(String movieTitle) throws IOException {
        // Step 1: 通过电影名称获取 MovieId
        String movieId = getMovieIdByTitle(movieTitle);
        List<Map<String, String>> ratingsList = new ArrayList<>();

        if (movieId == null) {
            return ratingsList; // 返回空列表
        }

        TableName tableName = TableName.valueOf(MOVIE_INDEX_TABLE);

        try (Table table = connection.getTable(tableName)) {

            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(movieId + "_"));
            scan.addFamily(Bytes.toBytes(REF_CF));

            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                String rowKeyStr = Bytes.toString(result.getRow());
                String userId = rowKeyStr.substring(rowKeyStr.indexOf('_') + 1);

                String rating = Bytes.toString(result.getValue(Bytes.toBytes(REF_CF), Bytes.toBytes("rating")));
                String timestamp = Bytes.toString(result.getValue(Bytes.toBytes(REF_CF), Bytes.toBytes("timestamp")));

                Map<String, String> record = new LinkedHashMap<>();
                record.put("movieId", movieId);
                record.put("userId", userId);
                record.put("rating", rating);
                record.put("timestamp", timestamp);

                ratingsList.add(record);
            }

            return ratingsList;
        }
    }

    /**
     * 辅助方法：通过电影名称 (title) 在 movies_info 表中获取其 MovieId
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

    // 注意：这里的 main 方法已失效，请运行 Spring Boot 的 Application 类来测试 Web 接口。
    // 如果您想进行本地测试，请自行添加测试逻辑。
    // public static void main(String[] args) { ... }
}