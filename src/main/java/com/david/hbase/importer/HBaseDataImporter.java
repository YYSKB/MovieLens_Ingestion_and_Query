package com.david.hbase.importer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HBaseDataImporter {
    // 日志对象
    private static final Logger logger = LoggerFactory.getLogger(HBaseDataImporter.class);
    // 配置对象
    private static final Properties props = new Properties();

    // HBase核心连接对象（私有化，通过getter提供访问）
    private static Connection connection = null;
    private static Admin admin = null;

    // 表名和列族常量（公开，供查询类使用）
    public static String MOVIES_INFO_TABLE;
    public static String RATINGS_DATA_TABLE;
    public static String MOVIE_INDEX_TABLE;
    public static String INFO_CF;
    public static String SCORE_CF;
    public static String REF_CF;
    public static final String MOVIE_ID_TITLE_INDEX_TABLE;
    public static final String INDEX_CF;

    // 配置参数（私有，内部使用）
    private static String ZK_QUORUM;
    private static String ZK_PORT;
    private static String MOVIES_PATH;
    private static String RATINGS_PATH;
    private static int BATCH_SIZE;

    // 静态块：初始化配置
    static {
        try (InputStream is = HBaseDataImporter.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is == null) {
                throw new FileNotFoundException("配置文件 hbase-import.properties 未找到，请检查resources目录");
            }
            props.load(is);

            // 加载配置到常量
            ZK_QUORUM = props.getProperty("hbase.zookeeper.quorum");
            ZK_PORT = props.getProperty("hbase.zookeeper.port");
            MOVIES_PATH = props.getProperty("data.movies.path");
            RATINGS_PATH = props.getProperty("data.ratings.path");
            BATCH_SIZE = Integer.parseInt(props.getProperty("batch.size"));
            MOVIES_INFO_TABLE = props.getProperty("table.movies");
            RATINGS_DATA_TABLE = props.getProperty("table.ratings");
            MOVIE_INDEX_TABLE = props.getProperty("table.index");
            INFO_CF = props.getProperty("cf.info");
            SCORE_CF = props.getProperty("cf.score");
            REF_CF = props.getProperty("cf.ref");
            MOVIE_ID_TITLE_INDEX_TABLE = props.getProperty("table.index_id");
            INDEX_CF = props.getProperty("cf.idx");

            // 校验必填配置
            checkRequiredConfig();
            logger.info("配置文件加载成功");
        } catch (Exception e) {
            logger.error("初始化配置失败，程序无法启动", e);
            throw new RuntimeException("配置初始化失败", e);
        }
    }

    // 校验必填配置
    private static void checkRequiredConfig() {
        List<String> missing = new ArrayList<>();
        if (ZK_QUORUM == null) missing.add("hbase.zookeeper.quorum");
        if (MOVIES_PATH == null) missing.add("data.movies.path");
        if (RATINGS_PATH == null) missing.add("data.ratings.path");
        if (MOVIES_INFO_TABLE == null) missing.add("table.movies");
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("配置文件缺少必填项：" + missing);
        }
    }

    // --- 连接管理 ---

    /**
     * 初始化HBase连接
     */
    public static void initConnection() throws IOException {
        if (connection != null && !connection.isClosed()) {
            logger.info("HBase连接已存在，无需重复初始化");
            return;
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZK_QUORUM);
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        conf.set("hbase.unsafe.stream.capability.enforce", "false");

        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        logger.info("HBase连接初始化成功（ZooKeeper: {}:{}）", ZK_QUORUM, ZK_PORT);
    }

    /**
     * 关闭HBase连接
     */
    public static void closeConnection() {
        try {
            if (admin != null) {
                admin.close();
                admin = null;
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
                connection = null;
            }
            logger.info("HBase连接已关闭");
        } catch (IOException e) {
            logger.error("关闭HBase连接时发生错误", e);
        }
    }

    /**
     * 提供外部访问连接的方法（供查询类使用）
     */
    public static Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            throw new IllegalStateException("HBase连接未初始化或已关闭，请先调用initConnection()");
        }
        return connection;
    }

    // --- 表结构管理 ---

    /**
     * 创建HBase表（支持预分裂）
     */
    public static void createTable(String tableNameStr, String[] columnFamilies, byte[][] splitKeys) throws IOException {
        TableName tableName = TableName.valueOf(tableNameStr);

        if (admin.tableExists(tableName)) {
            logger.info("表 [{}] 已存在，跳过创建", tableNameStr);
            return;
        }

        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tableName);
        for (String cf : columnFamilies) {
            ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            tableDescBuilder.setColumnFamily(cfDesc);
        }

        if (splitKeys != null && splitKeys.length > 0) {
            admin.createTable(tableDescBuilder.build(), splitKeys);
            logger.info("表 [{}] 创建成功，预分裂为 {} 个Region", tableNameStr, splitKeys.length + 1);
        } else {
            admin.createTable(tableDescBuilder.build());
            logger.info("表 [{}] 创建成功（默认1个Region）", tableNameStr);
        }
    }

    /**
     * 创建所有业务表
     */
    public static void createAllTables() throws IOException {
        // 电影信息表（无需预分裂）
        createTable(MOVIES_INFO_TABLE, new String[]{INFO_CF}, null);

        // 评分表和索引表（预分裂）
        byte[][] splitKeys = new byte[][]{
                Bytes.toBytes("20000_"),
                Bytes.toBytes("40000_"),
                Bytes.toBytes("60000_"),
                Bytes.toBytes("80000_")
        };
        createTable(RATINGS_DATA_TABLE, new String[]{SCORE_CF}, splitKeys);
        createTable(MOVIE_INDEX_TABLE, new String[]{REF_CF}, splitKeys);
    }

    // --- 数据导入 ---

    /**
     * 导入电影数据
     */
    public static void importMoviesData() throws IOException {
        checkFileExists(MOVIES_PATH, "电影数据");

        Table table = getConnection().getTable(TableName.valueOf(MOVIES_INFO_TABLE));
        int totalCount = 0;
        List<Put> puts = new ArrayList<>(BATCH_SIZE);

        try (BufferedReader reader = new BufferedReader(new FileReader(MOVIES_PATH));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord record : csvParser) {
                String movieId = record.get("movieId").trim();
                String title = record.get("title").trim().replaceAll("^\"|\"$", "");
                String genres = record.get("genres").trim();

                Put put = new Put(Bytes.toBytes(title));
                put.addColumn(Bytes.toBytes(INFO_CF),
                        Bytes.toBytes("movieId"),
                        Bytes.toBytes(movieId));
                put.addColumn(Bytes.toBytes(INFO_CF),
                        Bytes.toBytes("genres"),
                        Bytes.toBytes(genres));

                puts.add(put);
                totalCount++;

                if (puts.size() >= BATCH_SIZE) {
                    batchPut(table, puts);
                }
            }

            if (!puts.isEmpty()) {
                batchPut(table, puts);
            }

            logger.info("电影数据导入完成，共导入 {} 条记录", totalCount);
        } finally {
            table.close();
        }
    }
    /**
     * 从已有的电影表中同步数据到索引表（movieId→标题），用于补全索引
     */
    public static void syncMovieIdTitleIndex() throws IOException {
        // 1. 检查索引表是否存在（如果不存在，先创建）
        createTableIfNotExists(MOVIE_ID_TITLE_INDEX_TABLE, new String[]{INDEX_CF}, 3);

        // 2. 获取电影表和索引表的操作对象
        Table moviesTable = getConnection().getTable(TableName.valueOf(MOVIES_INFO_TABLE));
        Table indexTable = getConnection().getTable(TableName.valueOf(MOVIE_ID_TITLE_INDEX_TABLE));

        int totalSynced = 0;
        List<Put> indexPuts = new ArrayList<>(BATCH_SIZE); // 索引表的批量Put

        try {
            // 3. 扫描电影表的所有记录（只需要movieId列和RowKey（标题））
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(INFO_CF), Bytes.toBytes("movieId")); // 只扫描需要的movieId列
            ResultScanner scanner = moviesTable.getScanner(scan);

            for (Result result : scanner) {
                // 4. 提取电影表的RowKey（标题）和movieId列值
                String title = Bytes.toString(result.getRow()); // 电影表RowKey即标题
                String movieId = Bytes.toString(result.getValue(
                        Bytes.toBytes(INFO_CF),
                        Bytes.toBytes("movieId")
                ));

                if (movieId == null || movieId.trim().isEmpty()) {
                    logger.warn("跳过无效记录：标题={}，movieId为空", title);
                    continue;
                }
                movieId = movieId.trim();

                // 5. 构建索引表的Put（RowKey=movieId，值=标题）
                Put indexPut = new Put(Bytes.toBytes(movieId));
                indexPut.addColumn(
                        Bytes.toBytes(INDEX_CF),
                        Bytes.toBytes("title"),
                        Bytes.toBytes(title)
                );
                indexPuts.add(indexPut);
                totalSynced++;

                // 6. 批量提交（达到批次大小则写入）
                if (indexPuts.size() >= BATCH_SIZE) {
                    batchPut(indexTable, indexPuts);
                    indexPuts.clear();
                    logger.info("已同步 {} 条索引记录", totalSynced); // 打印进度，方便监控
                }
            }

            // 7. 提交剩余的记录
            if (!indexPuts.isEmpty()) {
                batchPut(indexTable, indexPuts);
            }

            logger.info("索引表同步完成，共同步 {} 条记录", totalSynced);

        } finally {
            // 8. 关闭资源
            moviesTable.close();
            indexTable.close();
        }
    }

    /**
     * 辅助方法：如果表不存在则创建（避免手动创建表的麻烦）
     */
    private static void createTableIfNotExists(String tableName, String[] columnFamilies, int regions) throws IOException {
        Admin admin = getConnection().getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if (!admin.tableExists(tn)) {
            // 第三个参数传null（不预分裂），适配原有createTable方法
            createTable(tableName, columnFamilies, null);
            logger.info("索引表 {} 不存在，已自动创建（不预分裂）", tableName);
        }
        admin.close();
    }

    /**
     * 导入评分数据
     */
    public static void importRatingsData() throws IOException {
        checkFileExists(RATINGS_PATH, "评分数据");

        Table ratingsTable = getConnection().getTable(TableName.valueOf(RATINGS_DATA_TABLE));
        Table indexTable = getConnection().getTable(TableName.valueOf(MOVIE_INDEX_TABLE));
        int totalCount = 0;
        List<Put> dataPuts = new ArrayList<>(BATCH_SIZE);
        List<Put> indexPuts = new ArrayList<>(BATCH_SIZE);

        try (BufferedReader reader = new BufferedReader(new FileReader(RATINGS_PATH));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord record : csvParser) {
                String userId = record.get("userId").trim();
                String movieId = record.get("movieId").trim();
                String rating = record.get("rating").trim();
                String timestamp = record.get("timestamp").trim();

                // 写入评分表
                String dataRowKey = userId + "_" + movieId;
                Put dataPut = new Put(Bytes.toBytes(dataRowKey));
                dataPut.addColumn(Bytes.toBytes(SCORE_CF), Bytes.toBytes("rating"), Bytes.toBytes(rating));
                dataPut.addColumn(Bytes.toBytes(SCORE_CF), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
                dataPuts.add(dataPut);

                // 写入索引表
                String indexRowKey = movieId + "_" + userId;
                Put indexPut = new Put(Bytes.toBytes(indexRowKey));
                indexPut.addColumn(Bytes.toBytes(REF_CF), Bytes.toBytes("rating"), Bytes.toBytes(rating));
                indexPut.addColumn(Bytes.toBytes(REF_CF), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
                indexPuts.add(indexPut);

                totalCount++;

                if (dataPuts.size() >= BATCH_SIZE) {
                    batchPut(ratingsTable, dataPuts);
                    batchPut(indexTable, indexPuts);
                }
            }

            if (!dataPuts.isEmpty()) {
                batchPut(ratingsTable, dataPuts);
                batchPut(indexTable, indexPuts);
            }

            logger.info("评分数据导入完成，共导入 {} 条记录", totalCount);
        } finally {
            ratingsTable.close();
            indexTable.close();
        }
    }




    // --- 工具方法 ---

    private static void batchPut(Table table, List<Put> puts) throws IOException {
        if (puts.isEmpty()) return;
        table.put(puts);
        logger.debug("批量提交 {} 条记录到表 [{}]", puts.size(), table.getName().getNameAsString());
        puts.clear();
    }

    private static void checkFileExists(String filePath, String fileDesc) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            logger.error("{}文件不存在：{}", fileDesc, filePath);
            throw new IllegalArgumentException(fileDesc + "文件路径无效：" + filePath);
        }
    }

    // 主方法：执行导入
    public static void main(String[] args) {
        try {
            initConnection();
//            createAllTables();
            logger.info("开始导入数据...");
//            importMoviesData();
//            importRatingsData();
            syncMovieIdTitleIndex();
            logger.info("所有数据导入完成！");
        } catch (Exception e) {
            logger.error("导入失败", e);
        } finally {
            closeConnection();
        }
    }
}