package com.david;

import com.david.hbase.importer.HBaseDataImporter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.io.IOException;

@SpringBootApplication
public class HbaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(HbaseApplication.class, args);
    }

    /**
     * 监听应用启动事件，一旦应用准备就绪，就初始化 HBase 连接。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initHbaseConnection() {
        try {
            // 在 Web 应用启动时初始化 HBase 连接，只需执行一次
            HBaseDataImporter.initConnection();
            System.out.println("✅ Spring Boot 应用启动成功，HBase 连接已初始化。");

            // 提示：通常在生产环境中，您会配置连接池而不是直接使用静态连接。

        } catch (IOException e) {
            System.err.println("❌ 严重错误：HBase 连接初始化失败！请检查 IP 和 ZooKeeper 状态。");
            e.printStackTrace();
            // 如果连接失败，可以选择退出应用
            // System.exit(1);
        }
    }
}