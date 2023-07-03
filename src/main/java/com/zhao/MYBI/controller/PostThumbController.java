package com.zhao.MYBI.controller;

import com.zhao.MYBI.common.BaseResponse;
import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.common.ResultUtils;
import com.zhao.MYBI.exception.BusinessException;
import com.zhao.MYBI.model.dto.Postthumb.PostThumbAddRequest;
import com.zhao.MYBI.model.entity.User;
import com.zhao.MYBI.service.PostThumbService;
import com.zhao.MYBI.service.UserService;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 帖子点赞接口
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
@RestController
@RequestMapping("/Post_thumb")
@Slf4j
public class PostThumbController {

    @Resource
    private PostThumbService PostThumbService;

    @Resource
    private UserService userService;

    /**
     * 点赞 / 取消点赞
     *
     * @param PostThumbAddRequest
     * @param request
     * @return resultNum 本次点赞变化数
     */
    @PostMapping("/")
    public BaseResponse<Integer> doThumb(@RequestBody PostThumbAddRequest PostThumbAddRequest,
                                         HttpServletRequest request) {
        if (PostThumbAddRequest == null || PostThumbAddRequest.getPostId() <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        // 登录才能点赞
        final User loginUser = userService.getLoginUser(request);
        long PostId = PostThumbAddRequest.getPostId();
        int result = PostThumbService.doPostThumb(PostId, loginUser);
        return ResultUtils.success(result);
    }

}
