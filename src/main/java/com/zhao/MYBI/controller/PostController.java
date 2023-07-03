package com.zhao.MYBI.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.gson.Gson;
import com.zhao.MYBI.annotation.AuthCheck;
import com.zhao.MYBI.common.BaseResponse;
import com.zhao.MYBI.common.DeleteRequest;
import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.common.ResultUtils;
import com.zhao.MYBI.constant.UserConstant;
import com.zhao.MYBI.exception.BusinessException;
import com.zhao.MYBI.exception.ThrowUtils;
import com.zhao.MYBI.model.dto.Post.PostAddRequest;
import com.zhao.MYBI.model.dto.Post.PostEditRequest;
import com.zhao.MYBI.model.dto.Post.PostQueryRequest;
import com.zhao.MYBI.model.dto.Post.PostUpdateRequest;
import com.zhao.MYBI.model.entity.Post;
import com.zhao.MYBI.model.entity.User;
import com.zhao.MYBI.model.vo.PostVO;
import com.zhao.MYBI.service.PostService;
import com.zhao.MYBI.service.UserService;
import java.util.List;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 帖子接口
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
@RestController
@RequestMapping("/Post")
@Slf4j
public class PostController {

    @Resource
    private PostService PostService;

    @Resource
    private UserService userService;

    private final static Gson GSON = new Gson();

    // region 增删改查

    /**
     * 创建
     *
     * @param PostAddRequest
     * @param request
     * @return
     */
    @PostMapping("/add")
    public BaseResponse<Long> addPost(@RequestBody PostAddRequest PostAddRequest, HttpServletRequest request) {
        if (PostAddRequest == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        Post Post = new Post();
        BeanUtils.copyProperties(PostAddRequest, Post);
        List<String> tags = PostAddRequest.getTags();
        if (tags != null) {
            Post.setTags(GSON.toJson(tags));
        }
        PostService.validPost(Post, true);
        User loginUser = userService.getLoginUser(request);
        Post.setUserId(loginUser.getId());
        Post.setFavourNum(0);
        Post.setThumbNum(0);
        boolean result = PostService.save(Post);
        ThrowUtils.throwIf(!result, ErrorCode.OPERATION_ERROR);
        long newPostId = Post.getId();
        return ResultUtils.success(newPostId);
    }

    /**
     * 删除
     *
     * @param deleteRequest
     * @param request
     * @return
     */
    @PostMapping("/delete")
    public BaseResponse<Boolean> deletePost(@RequestBody DeleteRequest deleteRequest, HttpServletRequest request) {
        if (deleteRequest == null || deleteRequest.getId() <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        User user = userService.getLoginUser(request);
        long id = deleteRequest.getId();
        // 判断是否存在
        Post oldPost = PostService.getById(id);
        ThrowUtils.throwIf(oldPost == null, ErrorCode.NOT_FOUND_ERROR);
        // 仅本人或管理员可删除
        if (!oldPost.getUserId().equals(user.getId()) && !userService.isAdmin(request)) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
        }
        boolean b = PostService.removeById(id);
        return ResultUtils.success(b);
    }

    /**
     * 更新（仅管理员）
     *
     * @param PostUpdateRequest
     * @return
     */
    @PostMapping("/update")
    @AuthCheck(mustRole = UserConstant.ADMIN_ROLE)
    public BaseResponse<Boolean> updatePost(@RequestBody PostUpdateRequest PostUpdateRequest) {
        if (PostUpdateRequest == null || PostUpdateRequest.getId() <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        Post Post = new Post();
        BeanUtils.copyProperties(PostUpdateRequest, Post);
        List<String> tags = PostUpdateRequest.getTags();
        if (tags != null) {
            Post.setTags(GSON.toJson(tags));
        }
        // 参数校验
        PostService.validPost(Post, false);
        long id = PostUpdateRequest.getId();
        // 判断是否存在
        Post oldPost = PostService.getById(id);
        ThrowUtils.throwIf(oldPost == null, ErrorCode.NOT_FOUND_ERROR);
        boolean result = PostService.updateById(Post);
        return ResultUtils.success(result);
    }

    /**
     * 根据 id 获取
     *
     * @param id
     * @return
     */
    @GetMapping("/get/vo")
    public BaseResponse<PostVO> getPostVOById(long id, HttpServletRequest request) {
        if (id <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        Post Post = PostService.getById(id);
        if (Post == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR);
        }
        return ResultUtils.success(PostService.getPostVO(Post, request));
    }

    /**
     * 分页获取列表（封装类）
     *
     * @param PostQueryRequest
     * @param request
     * @return
     */
    @PostMapping("/list/page/vo")
    public BaseResponse<Page<PostVO>> listPostVOByPage(@RequestBody PostQueryRequest PostQueryRequest,
            HttpServletRequest request) {
        long current = PostQueryRequest.getCurrent();
        long size = PostQueryRequest.getPageSize();
        // 限制爬虫
        ThrowUtils.throwIf(size > 20, ErrorCode.PARAMS_ERROR);
        Page<Post> PostPage = PostService.page(new Page<>(current, size),
                PostService.getQueryWrapper(PostQueryRequest));
        return ResultUtils.success(PostService.getPostVOPage(PostPage, request));
    }

    /**
     * 分页获取当前用户创建的资源列表
     *
     * @param PostQueryRequest
     * @param request
     * @return
     */
    @PostMapping("/my/list/page/vo")
    public BaseResponse<Page<PostVO>> listMyPostVOByPage(@RequestBody PostQueryRequest PostQueryRequest,
            HttpServletRequest request) {
        if (PostQueryRequest == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        User loginUser = userService.getLoginUser(request);
        PostQueryRequest.setUserId(loginUser.getId());
        long current = PostQueryRequest.getCurrent();
        long size = PostQueryRequest.getPageSize();
        // 限制爬虫
        ThrowUtils.throwIf(size > 20, ErrorCode.PARAMS_ERROR);
        Page<Post> PostPage = PostService.page(new Page<>(current, size),
                PostService.getQueryWrapper(PostQueryRequest));
        return ResultUtils.success(PostService.getPostVOPage(PostPage, request));
    }

    // endregion

    /**
     * 分页搜索（从 ES 查询，封装类）
     *
     * @param PostQueryRequest
     * @param request
     * @return
     */
    @PostMapping("/search/page/vo")
    public BaseResponse<Page<PostVO>> searchPostVOByPage(@RequestBody PostQueryRequest PostQueryRequest,
            HttpServletRequest request) {
        long size = PostQueryRequest.getPageSize();
        // 限制爬虫
        ThrowUtils.throwIf(size > 20, ErrorCode.PARAMS_ERROR);
        Page<Post> PostPage = PostService.searchFromEs(PostQueryRequest);
        return ResultUtils.success(PostService.getPostVOPage(PostPage, request));
    }

    /**
     * 编辑（用户）
     *
     * @param PostEditRequest
     * @param request
     * @return
     */
    @PostMapping("/edit")
    public BaseResponse<Boolean> editPost(@RequestBody PostEditRequest PostEditRequest, HttpServletRequest request) {
        if (PostEditRequest == null || PostEditRequest.getId() <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        Post Post = new Post();
        BeanUtils.copyProperties(PostEditRequest, Post);
        List<String> tags = PostEditRequest.getTags();
        if (tags != null) {
            Post.setTags(GSON.toJson(tags));
        }
        // 参数校验
        PostService.validPost(Post, false);
        User loginUser = userService.getLoginUser(request);
        long id = PostEditRequest.getId();
        // 判断是否存在
        Post oldPost = PostService.getById(id);
        ThrowUtils.throwIf(oldPost == null, ErrorCode.NOT_FOUND_ERROR);
        // 仅本人或管理员可编辑
        if (!oldPost.getUserId().equals(loginUser.getId()) && !userService.isAdmin(loginUser)) {
            throw new BusinessException(ErrorCode.NO_AUTH_ERROR);
        }
        boolean result = PostService.updateById(Post);
        return ResultUtils.success(result);
    }

}
