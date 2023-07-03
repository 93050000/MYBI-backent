package com.zhao.MYBI.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zhao.MYBI.common.BaseResponse;
import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.common.ResultUtils;
import com.zhao.MYBI.exception.BusinessException;
import com.zhao.MYBI.exception.ThrowUtils;
import com.zhao.MYBI.model.dto.Post.PostQueryRequest;
import com.zhao.MYBI.model.dto.Postfavour.PostFavourAddRequest;
import com.zhao.MYBI.model.dto.Postfavour.PostFavourQueryRequest;
import com.zhao.MYBI.model.entity.Post;
import com.zhao.MYBI.model.entity.User;
import com.zhao.MYBI.model.vo.PostVO;
import com.zhao.MYBI.service.PostFavourService;
import com.zhao.MYBI.service.PostService;
import com.zhao.MYBI.service.UserService;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 帖子收藏接口
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
@RestController
@RequestMapping("/Post_favour")
@Slf4j
public class PostFavourController {

    @Resource
    private PostFavourService PostFavourService;

    @Resource
    private PostService PostService;

    @Resource
    private UserService userService;

    /**
     * 收藏 / 取消收藏
     *
     * @param PostFavourAddRequest
     * @param request
     * @return resultNum 收藏变化数
     */
    @PostMapping("/")
    public BaseResponse<Integer> doPostFavour(@RequestBody PostFavourAddRequest PostFavourAddRequest,
                                              HttpServletRequest request) {
        if (PostFavourAddRequest == null || PostFavourAddRequest.getPostId() <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        // 登录才能操作
        final User loginUser = userService.getLoginUser(request);
        long PostId = PostFavourAddRequest.getPostId();
        int result = PostFavourService.doPostFavour(PostId, loginUser);
        return ResultUtils.success(result);
    }

    /**
     * 获取我收藏的帖子列表
     *
     * @param PostQueryRequest
     * @param request
     */
    @PostMapping("/my/list/page")
    public BaseResponse<Page<PostVO>> listMyFavourPostByPage(@RequestBody PostQueryRequest PostQueryRequest,
                                                             HttpServletRequest request) {
        if (PostQueryRequest == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        User loginUser = userService.getLoginUser(request);
        long current = PostQueryRequest.getCurrent();
        long size = PostQueryRequest.getPageSize();
        // 限制爬虫
        ThrowUtils.throwIf(size > 20, ErrorCode.PARAMS_ERROR);
        Page<Post> PostPage = PostFavourService.listFavourPostByPage(new Page<>(current, size),
                PostService.getQueryWrapper(PostQueryRequest), loginUser.getId());
        return ResultUtils.success(PostService.getPostVOPage(PostPage, request));
    }

    /**
     * 获取用户收藏的帖子列表
     *
     * @param PostFavourQueryRequest
     * @param request
     */
    @PostMapping("/list/page")
    public BaseResponse<Page<PostVO>> listFavourPostByPage(@RequestBody PostFavourQueryRequest PostFavourQueryRequest,
            HttpServletRequest request) {
        if (PostFavourQueryRequest == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        long current = PostFavourQueryRequest.getCurrent();
        long size = PostFavourQueryRequest.getPageSize();
        Long userId = PostFavourQueryRequest.getUserId();
        // 限制爬虫
        ThrowUtils.throwIf(size > 20 || userId == null, ErrorCode.PARAMS_ERROR);
        Page<Post> PostPage = PostFavourService.listFavourPostByPage(new Page<>(current, size),
                PostService.getQueryWrapper(PostFavourQueryRequest.getPostQueryRequest()), userId);
        return ResultUtils.success(PostService.getPostVOPage(PostPage, request));
    }
}
