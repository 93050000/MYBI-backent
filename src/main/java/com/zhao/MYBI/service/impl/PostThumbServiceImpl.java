package com.zhao.MYBI.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.exception.BusinessException;
import com.zhao.MYBI.mapper.PostThumbMapper;
import com.zhao.MYBI.model.entity.Post;
import com.zhao.MYBI.model.entity.PostThumb;
import com.zhao.MYBI.model.entity.User;
import com.zhao.MYBI.service.PostService;
import com.zhao.MYBI.service.PostThumbService;
import javax.annotation.Resource;

import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 帖子点赞服务实现
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
@Service
public class PostThumbServiceImpl extends ServiceImpl<PostThumbMapper, PostThumb>
        implements PostThumbService {

    @Resource
    private PostService PostService;

    /**
     * 点赞
     *
     * @param PostId
     * @param loginUser
     * @return
     */
    @Override
    public int doPostThumb(long PostId, User loginUser) {
        // 判断实体是否存在，根据类别获取实体
        Post Post = PostService.getById(PostId);
        if (Post == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR);
        }
        // 是否已点赞
        long userId = loginUser.getId();
        // 每个用户串行点赞
        // 锁必须要包裹住事务方法
        PostThumbService PostThumbService = (PostThumbService) AopContext.currentProxy();
        synchronized (String.valueOf(userId).intern()) {
            return PostThumbService.doPostThumbInner(userId, PostId);
        }
    }

    /**
     * 封装了事务的方法
     *
     * @param userId
     * @param PostId
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public int doPostThumbInner(long userId, long PostId) {
        PostThumb PostThumb = new PostThumb();
        PostThumb.setUserId(userId);
        PostThumb.setPostId(PostId);
        QueryWrapper<PostThumb> thumbQueryWrapper = new QueryWrapper<>(PostThumb);
        PostThumb oldPostThumb = this.getOne(thumbQueryWrapper);
        boolean result;
        // 已点赞
        if (oldPostThumb != null) {
            result = this.remove(thumbQueryWrapper);
            if (result) {
                // 点赞数 - 1
                result = PostService.update()
                        .eq("id", PostId)
                        .gt("thumbNum", 0)
                        .setSql("thumbNum = thumbNum - 1")
                        .update();
                return result ? -1 : 0;
            } else {
                throw new BusinessException(ErrorCode.SYSTEM_ERROR);
            }
        } else {
            // 未点赞
            result = this.save(PostThumb);
            if (result) {
                // 点赞数 + 1
                result = PostService.update()
                        .eq("id", PostId)
                        .setSql("thumbNum = thumbNum + 1")
                        .update();
                return result ? 1 : 0;
            } else {
                throw new BusinessException(ErrorCode.SYSTEM_ERROR);
            }
        }
    }

}




