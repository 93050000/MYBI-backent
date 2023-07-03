package com.zhao.MYBI.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zhao.MYBI.model.dto.Post.PostQueryRequest;
import com.zhao.MYBI.model.entity.Post;
import javax.annotation.Resource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 帖子服务测试
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
@SpringBootTest
class PostServiceTest {

    @Resource
    private PostService PostService;

    @Test
    void searchFromEs() {
        PostQueryRequest PostQueryRequest = new PostQueryRequest();
        PostQueryRequest.setUserId(1L);
        Page<Post> PostPage = PostService.searchFromEs(PostQueryRequest);
        Assertions.assertNotNull(PostPage);
    }

}