package com.zhao.MYBI.job.cycle;

import com.zhao.MYBI.esdao.PostEsDao;
import com.zhao.MYBI.mapper.PostMapper;
import com.zhao.MYBI.model.dto.Post.PostEsDTO;
import com.zhao.MYBI.model.entity.Post;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Resource;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 增量同步帖子到 es
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
// todo 取消注释开启任务
//@Component
@Slf4j
public class IncSyncPostToEs {

    @Resource
    private PostMapper PostMapper;

    @Resource
    private PostEsDao PostEsDao;

    /**
     * 每分钟执行一次
     */
    @Scheduled(fixedRate = 60 * 1000)
    public void run() {
        // 查询近 5 分钟内的数据
        Date fiveMinutesAgoDate = new Date(new Date().getTime() - 5 * 60 * 1000L);
        List<Post> PostList = PostMapper.listPostWithDelete(fiveMinutesAgoDate);
        if (CollectionUtils.isEmpty(PostList)) {
            log.info("no inc Post");
            return;
        }
        List<PostEsDTO> PostEsDTOList = PostList.stream()
                .map(PostEsDTO::objToDto)
                .collect(Collectors.toList());
        final int pageSize = 500;
        int total = PostEsDTOList.size();
        log.info("IncSyncPostToEs start, total {}", total);
        for (int i = 0; i < total; i += pageSize) {
            int end = Math.min(i + pageSize, total);
            log.info("sync from {} to {}", i, end);
            PostEsDao.saveAll(PostEsDTOList.subList(i, end));
        }
        log.info("IncSyncPostToEs end, total {}", total);
    }
}
