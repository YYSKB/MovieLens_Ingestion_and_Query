package com.david;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/movie") // 统一前缀，便于管理

public class MovieQueryController {

    /**
     * 接口 1: 查询电影详情 (按名称)
     * URL: GET /api/v1/movie/detail?title=Toy Story (1995)
     */
    @GetMapping("/detail")
    public ResponseEntity<?> getMovieDetail(@RequestParam("title") String movieTitle) {
        try {
            Map<String, String> result = HBaseQueryer.queryMovieDetail(movieTitle);

            if (result == null || result.isEmpty()) {
                // HTTP 404 Not Found
                return new ResponseEntity<>(
                        Map.of("message", "未找到该电影: " + movieTitle),
                        HttpStatus.NOT_FOUND
                );
            }
            // HTTP 200 OK，返回电影详情 Map (自动转为 JSON)
            return ResponseEntity.ok(result);

        } catch (IOException e) {
            // HTTP 500 Internal Server Error
            System.err.println("HBase查询电影详情失败: " + e.getMessage());
            return new ResponseEntity<>(
                    Map.of("message", "后端数据服务错误", "error", e.getMessage()),
                    HttpStatus.INTERNAL_SERVER_ERROR
            );
        }
    }

    /**
     * 接口 2: 查询用户评分 (按用户 ID)
     * URL: GET /api/v1/movie/userRatings?userId=1
     */
    @GetMapping("/userRatings")
    public ResponseEntity<List<Map<String, String>>> getUserRatings(@RequestParam("userId") String userId) {
        try {
            List<Map<String, String>> result = HBaseQueryer.queryUserRatings(userId);

            // HTTP 200 OK，即使列表为空也返回 200，表示查询成功，但结果集为空
            return ResponseEntity.ok(result);

        } catch (IOException e) {
            System.err.println("HBase查询用户评分失败: " + e.getMessage());
            // 返回一个包含错误信息的空列表或特定错误体
            return new ResponseEntity<>(
                    Collections.emptyList(),
                    HttpStatus.INTERNAL_SERVER_ERROR
            );
        }
    }

    /**
     * 接口 3: 查询某部电影的所有评分 (按名称)
     * URL: GET /api/v1/movie/allRatings?title=Toy Story (1995)
     */
    @GetMapping("/allRatings")
    public ResponseEntity<List<Map<String, String>>> getMovieAllRatings(@RequestParam("title") String movieTitle) {
        try {
            List<Map<String, String>> result = HBaseQueryer.queryMovieRatingsByTitle(movieTitle);

            // HTTP 200 OK
            return ResponseEntity.ok(result);

        } catch (IOException e) {
            System.err.println("HBase查询电影所有评分失败: " + e.getMessage());
            return new ResponseEntity<>(
                    Collections.emptyList(),
                    HttpStatus.INTERNAL_SERVER_ERROR
            );
        }
    }
}