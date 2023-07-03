package com.zhao.MYBI.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.zhao.MYBI.model.dto.Post.PostQueryRequest;
import com.zhao.MYBI.model.entity.Post;
import com.zhao.MYBI.model.vo.PostVO;

import javax.servlet.http.HttpServletRequest;

/**
 * 帖子服务
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
public interface PostService extends IService<Post> {

    /**
     * 校验
     *
     * @param Post
     * @param add
     */
    void validPost(Post Post, boolean add);

    /**
     * 获取查询条件
     *
     * @param PostQueryRequest
     * @return
     */
    QueryWrapper<Post> getQueryWrapper(PostQueryRequest PostQueryRequest);

    /**
     * 从 ES 查询
     *
     * @param PostQueryRequest
     * @return
     */
    Page<Post> searchFromEs(PostQueryRequest PostQueryRequest);

    /**
     * 获取帖子封装
     *
     * @param Post
     * @param request
     * @return
     */
    PostVO getPostVO(Post Post, HttpServletRequest request);

    /**
     * 分页获取帖子封装
     *
     * @param PostPage
     * @param request
     * @return
     */
    Page<PostVO> getPostVOPage(Page<Post> PostPage, HttpServletRequest request);
}
