package com.zhao.MYBI.service.impl;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.exception.BusinessException;
import com.zhao.MYBI.mapper.PostFavourMapper;
import com.zhao.MYBI.model.entity.Post;
import com.zhao.MYBI.model.entity.PostFavour;
import com.zhao.MYBI.model.entity.User;
import com.zhao.MYBI.service.PostFavourService;
import com.zhao.MYBI.service.PostService;
import javax.annotation.Resource;

import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 帖子收藏服务实现
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
@Service
public class PostFavourServiceImpl extends ServiceImpl<PostFavourMapper, PostFavour>
        implements PostFavourService {

    @Resource
    private PostService PostService;

    /**
     * 帖子收藏
     *
     * @param PostId
     * @param loginUser
     * @return
     */
    @Override
    public int doPostFavour(long PostId, User loginUser) {
        // 判断是否存在
        Post Post = PostService.getById(PostId);
        if (Post == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR);
        }
        // 是否已帖子收藏
        long userId = loginUser.getId();
        // 每个用户串行帖子收藏
        // 锁必须要包裹住事务方法
        PostFavourService PostFavourService = (PostFavourService) AopContext.currentProxy();
        synchronized (String.valueOf(userId).intern()) {
            return PostFavourService.doPostFavourInner(userId, PostId);
        }
    }

    @Override
    public Page<Post> listFavourPostByPage(IPage<Post> page, Wrapper<Post> queryWrapper, long favourUserId) {
        if (favourUserId <= 0) {
            return new Page<>();
        }
        return baseMapper.listFavourPostByPage(page, queryWrapper, favourUserId);
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
    public int doPostFavourInner(long userId, long PostId) {
        PostFavour PostFavour = new PostFavour();
        PostFavour.setUserId(userId);
        PostFavour.setPostId(PostId);
        QueryWrapper<PostFavour> PostFavourQueryWrapper = new QueryWrapper<>(PostFavour);
        PostFavour oldPostFavour = this.getOne(PostFavourQueryWrapper);
        boolean result;
        // 已收藏
        if (oldPostFavour != null) {
            result = this.remove(PostFavourQueryWrapper);
            if (result) {
                // 帖子收藏数 - 1
                result = PostService.update()
                        .eq("id", PostId)
                        .gt("favourNum", 0)
                        .setSql("favourNum = favourNum - 1")
                        .update();
                return result ? -1 : 0;
            } else {
                throw new BusinessException(ErrorCode.SYSTEM_ERROR);
            }
        } else {
            // 未帖子收藏
            result = this.save(PostFavour);
            if (result) {
                // 帖子收藏数 + 1
                result = PostService.update()
                        .eq("id", PostId)
                        .setSql("favourNum = favourNum + 1")
                        .update();
                return result ? 1 : 0;
            } else {
                throw new BusinessException(ErrorCode.SYSTEM_ERROR);
            }
        }
    }

}




