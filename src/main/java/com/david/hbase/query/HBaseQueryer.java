package com.david.hbase.query;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// 静态导入数据类的常量和方法
import static com.david.hbase.importer.HBaseDataImporter.*;

public class HBaseQueryer {
    // 日志对象
    private static final Logger logger = LoggerFactory.getLogger(HBaseQueryer.class);

    // =======================================================================
    // 1. 查询电影详情（按名称）
    // =======================================================================
    public static Map<String, String> queryMovieDetail(String movieTitle) throws IOException {
        if (movieTitle == null || movieTitle.trim().isEmpty()) {
            logger.warn("查询电影详情失败：电影名称为空");
            return null;
        }

        TableName tableName = TableName.valueOf(MOVIES_INFO_TABLE);
        try (Table table = getConnection().getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(movieTitle.trim()));
            get.addFamily(Bytes.toBytes(INFO_CF));

            Result result = table.get(get);
            if (result.isEmpty()) {
                logger.info("未找到电影 [{}] 的详情", movieTitle);
                return null;
            }

            // 封装结果
            Map<String, String> details = new LinkedHashMap<>();
            details.put("title", movieTitle);
            details.put("movieId", Bytes.toString(
                    result.getValue(
                            Bytes.toBytes(INFO_CF),
                            Bytes.toBytes("movieId"))));
            details.put("genres", Bytes.toString(result.getValue(Bytes.toBytes(INFO_CF), Bytes.toBytes("genres"))));

            return details;
        } catch (IOException e) {
            logger.error("查询电影详情失败：{}", e.getMessage(), e);
            throw e; // 向上抛出，让调用方处理
        }
    }

    // =======================================================================
    // 2. 查询用户的所有评分（按用户ID）
    // =======================================================================
    public static List<Map<String, String>> queryUserRatings(String userId) throws IOException {
        if (userId == null || userId.trim().isEmpty()) {
            logger.warn("查询用户评分失败：用户ID为空");
            return new ArrayList<>();
        }
        userId = userId.trim();

        TableName tableName = TableName.valueOf(RATINGS_DATA_TABLE);
        List<Map<String, String>> ratingsList = new ArrayList<>();

        try (Table table = getConnection().getTable(tableName)) {
            // 扫描行键前缀为 "userId_" 的记录
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(userId + "_"));
            scan.addFamily(Bytes.toBytes(SCORE_CF));

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                String movieId = rowKey.substring(rowKey.indexOf('_') + 1);
                String movieTitle = getMovieTitleByMovieId(movieId);

                // 封装单条评分记录
                Map<String, String> record = new LinkedHashMap<>();
                record.put("userId", userId);
                record.put("movieId", movieId);
                record.put("movieTitle", movieTitle);
                record.put("rating", Bytes.toString(result.getValue(Bytes.toBytes(SCORE_CF), Bytes.toBytes("rating"))));
                record.put("timestamp", Bytes.toString(result.getValue(Bytes.toBytes(SCORE_CF), Bytes.toBytes("timestamp"))));

                ratingsList.add(record);
            }

            logger.info("查询到用户 [{}] 的 {} 条评分记录", userId, ratingsList.size());
            return ratingsList;
        } catch (IOException e) {
            logger.error("查询用户评分失败：{}", e.getMessage(), e);
            throw e;
        }
    }

    // =======================================================================
    // 3. 查询电影的所有评分（按电影名称）
    // =======================================================================
    public static List<Map<String, String>> queryMovieRatingsByTitle(String movieTitle) throws IOException {
        if (movieTitle == null || movieTitle.trim().isEmpty()) {
            logger.warn("查询电影评分失败：电影名称为空");
            return new ArrayList<>();
        }
        movieTitle = movieTitle.trim();

        // 先通过电影名称获取movieId
        String movieId = getMovieIdByTitle(movieTitle);
        if (movieId == null) {
            logger.info("未找到电影 [{}] 的ID，无法查询评分", movieTitle);
            return new ArrayList<>();
        }

        TableName tableName = TableName.valueOf(MOVIE_INDEX_TABLE);
        List<Map<String, String>> ratingsList = new ArrayList<>();

        try (Table table = getConnection().getTable(tableName)) {
            // 扫描行键前缀为 "movieId_" 的记录
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(movieId + "_"));
            scan.addFamily(Bytes.toBytes(REF_CF));

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                String userId = rowKey.substring(rowKey.indexOf('_') + 1);

                // 封装单条评分记录
                Map<String, String> record = new LinkedHashMap<>();
                record.put("movieTitle", movieTitle);
                record.put("movieId", movieId);
                record.put("userId", userId);
                record.put("rating", Bytes.toString(result.getValue(Bytes.toBytes(REF_CF), Bytes.toBytes("rating"))));
                record.put("timestamp", Bytes.toString(result.getValue(Bytes.toBytes(REF_CF), Bytes.toBytes("timestamp"))));

                ratingsList.add(record);
            }

            logger.info("查询到电影 [{}] 的 {} 条评分记录", movieTitle, ratingsList.size());
            return ratingsList;
        } catch (IOException e) {
            logger.error("查询电影评分失败：{}", e.getMessage(), e);
            throw e;
        }
    }

    // =======================================================================
    // 辅助方法：通过movieId查电影名称
    // =======================================================================
    private static String getMovieTitleByMovieId(String movieId) throws IOException {
        if (movieId == null || movieId.trim().isEmpty()) {
            return "未知电影（ID为空）"; // 增强参数校验，处理空字符串
        }
        movieId = movieId.trim(); // 去除空格，与索引表RowKey格式保持一致

        // 从索引表查询（而非原电影表），索引表RowKey=movieId
        TableName indexTableName = TableName.valueOf(MOVIE_ID_TITLE_INDEX_TABLE);
        try (Table indexTable = getConnection().getTable(indexTableName)) {
            // 1. 创建Get对象，按movieId（索引表RowKey）精确查询
            Get get = new Get(Bytes.toBytes(movieId));
            // 2. 只查询需要的列（索引表的cf.idx:title），减少数据传输
            get.addColumn(Bytes.toBytes(INDEX_CF), Bytes.toBytes("title"));

            // 3. 执行查询
            Result result = indexTable.get(get);

            // 4. 解析结果：存在则返回标题，否则返回默认值
            if (!result.isEmpty()) {
                byte[] titleBytes = result.getValue(Bytes.toBytes(INDEX_CF), Bytes.toBytes("title"));
                return titleBytes != null ? Bytes.toString(titleBytes) : "未知电影（标题为空）";
            } else {
                return "未知电影（ID：" + movieId + "）";
            }
        }
    }

    // =======================================================================
    // 辅助方法：通过电影名称查movieId
    // =======================================================================
    private static String getMovieIdByTitle(String movieTitle) throws IOException {
        TableName tableName = TableName.valueOf(MOVIES_INFO_TABLE);
        try (Table table = getConnection().getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(movieTitle));
            Result result = table.get(get);
            if (!result.isEmpty()) {
                return Bytes.toString(result.getValue(Bytes.toBytes(INFO_CF), Bytes.toBytes("movieId")));
            }
            return null;
        }
    }
}