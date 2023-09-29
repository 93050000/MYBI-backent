package com.zhao.MYBI.manager;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class RedisLimiterManagerTest {

    @Resource
    private RedisLimiterManager redisLimiterManager;

    @Test
    void doRateLimit() {
        String userId = "aa";
        for (int i = 0 ; i < 6 ; i++){
            redisLimiterManager.doRateLimit(userId);
            System.out.println("成功");
        }
    }
}