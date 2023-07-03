package com.zhao.MYBI.esdao;

import com.zhao.MYBI.model.dto.Post.PostEsDTO;
import com.zhao.MYBI.model.dto.Post.PostQueryRequest;
import com.zhao.MYBI.model.entity.Post;
import com.zhao.MYBI.service.PostService;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

/**
 * 帖子 ES 操作测试
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
@SpringBootTest
public class PostEsDaoTest {

    @Resource
    private PostEsDao PostEsDao;

    @Resource
    private PostService PostService;

    @Test
    void test() {
        PostQueryRequest PostQueryRequest = new PostQueryRequest();
        com.baomidou.mybatisplus.extension.plugins.pagination.Page<Post> page =
                PostService.searchFromEs(PostQueryRequest);
        System.out.println(page);
    }

    @Test
    void testSelect() {
        System.out.println(PostEsDao.count());
        Page<PostEsDTO> PostPage = PostEsDao.findAll(
                PageRequest.of(0, 5, Sort.by("createTime")));
        List<PostEsDTO> PostList = PostPage.getContent();
        System.out.println(PostList);
    }

    @Test
    void testAdd() {
        PostEsDTO PostEsDTO = new PostEsDTO();
        PostEsDTO.setId(1L);
        PostEsDTO.setTitle("test");
        PostEsDTO.setContent("test");
        PostEsDTO.setTags(Arrays.asList("java", "python"));
        PostEsDTO.setThumbNum(1);
        PostEsDTO.setFavourNum(1);
        PostEsDTO.setUserId(1L);
        PostEsDTO.setCreateTime(new Date());
        PostEsDTO.setUpdateTime(new Date());
        PostEsDTO.setIsDelete(0);
        PostEsDao.save(PostEsDTO);
        System.out.println(PostEsDTO.getId());
    }

    @Test
    void testFindById() {
        Optional<PostEsDTO> PostEsDTO = PostEsDao.findById(1L);
        System.out.println(PostEsDTO);
    }

    @Test
    void testCount() {
        System.out.println(PostEsDao.count());
    }

    @Test
    void testFindByCategory() {
        List<PostEsDTO> PostEsDaoTestList = PostEsDao.findByUserId(1L);
        System.out.println(PostEsDaoTestList);
    }
}
