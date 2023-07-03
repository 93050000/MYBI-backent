package com.zhao.MYBI.service;

import com.zhao.MYBI.model.entity.PostThumb;
import com.baomidou.mybatisplus.extension.service.IService;
import com.zhao.MYBI.model.entity.User;

/**
 * 帖子点赞服务
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
public interface PostThumbService extends IService<PostThumb> {

    /**
     * 点赞
     *
     * @param PostId
     * @param loginUser
     * @return
     */
    int doPostThumb(long PostId, User loginUser);

    /**
     * 帖子点赞（内部服务）
     *
     * @param userId
     * @param PostId
     * @return
     */
    int doPostThumbInner(long userId, long PostId);
}
