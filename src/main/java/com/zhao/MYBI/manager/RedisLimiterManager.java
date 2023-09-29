package com.zhao.MYBI.manager;

import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.exception.BusinessException;
import com.zhao.MYBI.utils.ExcelUtils;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;


/**
 * 提供RedisLimiter 限流基础服务
 */
@Service
public class RedisLimiterManager {

    @Resource
    private RedissonClient redissonClient;

    /**
     * key 的作用是区分不同的限流，比如不同的用户
     *
     * @param key
     */
    public void doRateLimit(String key) {

        RRateLimiter rateLimiter = redissonClient.getRateLimiter(key);

        rateLimiter.trySetRate(RateType.OVERALL, 2, 1, RateIntervalUnit.SECONDS);

//        每当一个操作者进来后，请求一个令牌
        boolean conOp = rateLimiter.tryAcquire(3);
        if (!conOp) {
         throw  new BusinessException(ErrorCode.TOO_MANY_REQUEST);
        }

    }
}
