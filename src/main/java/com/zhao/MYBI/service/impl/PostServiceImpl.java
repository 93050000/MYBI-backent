package com.zhao.MYBI.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.gson.Gson;
import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.constant.CommonConstant;
import com.zhao.MYBI.exception.BusinessException;
import com.zhao.MYBI.exception.ThrowUtils;
import com.zhao.MYBI.mapper.PostMapper;
import com.zhao.MYBI.mapper.PostFavourMapper;
import com.zhao.MYBI.mapper.PostMapper;
import com.zhao.MYBI.mapper.PostThumbMapper;
import com.zhao.MYBI.model.dto.Post.PostEsDTO;
import com.zhao.MYBI.model.dto.Post.PostQueryRequest;
import com.zhao.MYBI.model.entity.Post;
import com.zhao.MYBI.model.entity.Post;
import com.zhao.MYBI.model.entity.PostFavour;
import com.zhao.MYBI.model.entity.PostThumb;
import com.zhao.MYBI.model.entity.User;
import com.zhao.MYBI.model.vo.PostVO;
import com.zhao.MYBI.model.vo.PostVO;
import com.zhao.MYBI.model.vo.UserVO;
import com.zhao.MYBI.service.PostService;
import com.zhao.MYBI.service.UserService;
import com.zhao.MYBI.utils.SqlUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

/**
 * 帖子服务实现
 *
 * @author <a href="https://github.com/liyupi">程序员鱼皮</a>
 * @from <a href="https://yupi.icu">编程导航知识星球</a>
 */
@Service
@Slf4j
public class PostServiceImpl extends ServiceImpl<PostMapper, Post> implements PostService {

    private final static Gson GSON = new Gson();

    @Resource
    private UserService userService;

    @Resource
    private PostThumbMapper PostThumbMapper;

    @Resource
    private PostFavourMapper PostFavourMapper;

    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Override
    public void validPost(Post Post, boolean add) {
        if (Post == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR);
        }
        String title = Post.getTitle();
        String content = Post.getContent();
        String tags = Post.getTags();
        // 创建时，参数不能为空
        if (add) {
            ThrowUtils.throwIf(StringUtils.isAnyBlank(title, content, tags), ErrorCode.PARAMS_ERROR);
        }
        // 有参数则校验
        if (StringUtils.isNotBlank(title) && title.length() > 80) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "标题过长");
        }
        if (StringUtils.isNotBlank(content) && content.length() > 8192) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "内容过长");
        }
    }

    /**
     * 获取查询包装类
     *
     * @param PostQueryRequest
     * @return
     */
    @Override
    public QueryWrapper<Post> getQueryWrapper(PostQueryRequest PostQueryRequest) {
        QueryWrapper<Post> queryWrapper = new QueryWrapper<>();
        if (PostQueryRequest == null) {
            return queryWrapper;
        }
        String searchText = PostQueryRequest.getSearchText();
        String sortField = PostQueryRequest.getSortField();
        String sortOrder = PostQueryRequest.getSortOrder();
        Long id = PostQueryRequest.getId();
        String title = PostQueryRequest.getTitle();
        String content = PostQueryRequest.getContent();
        List<String> tagList = PostQueryRequest.getTags();
        Long userId = PostQueryRequest.getUserId();
        Long notId = PostQueryRequest.getNotId();
        // 拼接查询条件
        if (StringUtils.isNotBlank(searchText)) {
            queryWrapper.like("title", searchText).or().like("content", searchText);
        }
        queryWrapper.like(StringUtils.isNotBlank(title), "title", title);
        queryWrapper.like(StringUtils.isNotBlank(content), "content", content);
        if (CollectionUtils.isNotEmpty(tagList)) {
            for (String tag : tagList) {
                queryWrapper.like("tags", "\"" + tag + "\"");
            }
        }
        queryWrapper.ne(ObjectUtils.isNotEmpty(notId), "id", notId);
        queryWrapper.eq(ObjectUtils.isNotEmpty(id), "id", id);
        queryWrapper.eq(ObjectUtils.isNotEmpty(userId), "userId", userId);
        queryWrapper.eq("isDelete", false);
        queryWrapper.orderBy(SqlUtils.validSortField(sortField), sortOrder.equals(CommonConstant.SORT_ORDER_ASC),
                sortField);
        return queryWrapper;
    }

    @Override
    public Page<Post> searchFromEs(PostQueryRequest PostQueryRequest) {
        Long id = PostQueryRequest.getId();
        Long notId = PostQueryRequest.getNotId();
        String searchText = PostQueryRequest.getSearchText();
        String title = PostQueryRequest.getTitle();
        String content = PostQueryRequest.getContent();
        List<String> tagList = PostQueryRequest.getTags();
        List<String> orTagList = PostQueryRequest.getOrTags();
        Long userId = PostQueryRequest.getUserId();
        // es 起始页为 0
        long current = PostQueryRequest.getCurrent() - 1;
        long pageSize = PostQueryRequest.getPageSize();
        String sortField = PostQueryRequest.getSortField();
        String sortOrder = PostQueryRequest.getSortOrder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 过滤
        boolQueryBuilder.filter(QueryBuilders.termQuery("isDelete", 0));
        if (id != null) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("id", id));
        }
        if (notId != null) {
            boolQueryBuilder.mustNot(QueryBuilders.termQuery("id", notId));
        }
        if (userId != null) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("userId", userId));
        }
        // 必须包含所有标签
        if (CollectionUtils.isNotEmpty(tagList)) {
            for (String tag : tagList) {
                boolQueryBuilder.filter(QueryBuilders.termQuery("tags", tag));
            }
        }
        // 包含任何一个标签即可
        if (CollectionUtils.isNotEmpty(orTagList)) {
            BoolQueryBuilder orTagBoolQueryBuilder = QueryBuilders.boolQuery();
            for (String tag : orTagList) {
                orTagBoolQueryBuilder.should(QueryBuilders.termQuery("tags", tag));
            }
            orTagBoolQueryBuilder.minimumShouldMatch(1);
            boolQueryBuilder.filter(orTagBoolQueryBuilder);
        }
        // 按关键词检索
        if (StringUtils.isNotBlank(searchText)) {
            boolQueryBuilder.should(QueryBuilders.matchQuery("title", searchText));
            boolQueryBuilder.should(QueryBuilders.matchQuery("description", searchText));
            boolQueryBuilder.should(QueryBuilders.matchQuery("content", searchText));
            boolQueryBuilder.minimumShouldMatch(1);
        }
        // 按标题检索
        if (StringUtils.isNotBlank(title)) {
            boolQueryBuilder.should(QueryBuilders.matchQuery("title", title));
            boolQueryBuilder.minimumShouldMatch(1);
        }
        // 按内容检索
        if (StringUtils.isNotBlank(content)) {
            boolQueryBuilder.should(QueryBuilders.matchQuery("content", content));
            boolQueryBuilder.minimumShouldMatch(1);
        }
        // 排序
        SortBuilder<?> sortBuilder = SortBuilders.scoreSort();
        if (StringUtils.isNotBlank(sortField)) {
            sortBuilder = SortBuilders.fieldSort(sortField);
            sortBuilder.order(CommonConstant.SORT_ORDER_ASC.equals(sortOrder) ? SortOrder.ASC : SortOrder.DESC);
        }
        // 分页
        PageRequest pageRequest = PageRequest.of((int) current, (int) pageSize);
        // 构造查询
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(boolQueryBuilder)
                .withPageable(pageRequest).withSorts(sortBuilder).build();
        SearchHits<PostEsDTO> searchHits = elasticsearchRestTemplate.search(searchQuery, PostEsDTO.class);
        Page<Post> page = new Page<>();
        page.setTotal(searchHits.getTotalHits());
        List<Post> resourceList = new ArrayList<>();
        // 查出结果后，从 db 获取最新动态数据（比如点赞数）
        if (searchHits.hasSearchHits()) {
            List<SearchHit<PostEsDTO>> searchHitList = searchHits.getSearchHits();
            List<Long> PostIdList = searchHitList.stream().map(searchHit -> searchHit.getContent().getId())
                    .collect(Collectors.toList());
            List<Post> PostList = baseMapper.selectBatchIds(PostIdList);
            if (PostList != null) {
                Map<Long, List<Post>> idPostMap = PostList.stream().collect(Collectors.groupingBy(Post::getId));
                PostIdList.forEach(PostId -> {
                    if (idPostMap.containsKey(PostId)) {
                        resourceList.add(idPostMap.get(PostId).get(0));
                    } else {
                        // 从 es 清空 db 已物理删除的数据
                        String delete = elasticsearchRestTemplate.delete(String.valueOf(PostId), PostEsDTO.class);
                        log.info("delete Post {}", delete);
                    }
                });
            }
        }
        page.setRecords(resourceList);
        return page;
    }

    @Override
    public PostVO getPostVO(Post post, HttpServletRequest request) {
        PostVO postVO = PostVO.objToVo(post);
        long postId = post.getId();
        // 1. 关联查询用户信息
        Long userId = post.getUserId();
        User user = null;
        if (userId != null && userId > 0) {
            user = userService.getById(userId);
        }
        UserVO userVO = userService.getUserVO(user);
        postVO.setUser(userVO);
        // 2. 已登录，获取用户点赞、收藏状态
        User loginUser = userService.getLoginUserPermitNull(request);
        if (loginUser != null) {
            // 获取点赞
            QueryWrapper<PostThumb> PostThumbQueryWrapper = new QueryWrapper<>();
            PostThumbQueryWrapper.in("postId", postId);
            PostThumbQueryWrapper.eq("userId", loginUser.getId());
            PostThumb postThumb = PostThumbMapper.selectOne(PostThumbQueryWrapper);
            postVO.setHasThumb(postThumb != null);
            // 获取收藏
            QueryWrapper<PostFavour> PostFavourQueryWrapper = new QueryWrapper<>();
            PostFavourQueryWrapper.in("postId", postId);
            PostFavourQueryWrapper.eq("userId", loginUser.getId());
            PostFavour PostFavour = PostFavourMapper.selectOne(PostFavourQueryWrapper);
            postVO.setHasFavour(PostFavour != null);
        }
        return postVO;
    }

    @Override
    public Page<PostVO> getPostVOPage(Page<Post> PostPage, HttpServletRequest request) {
        List<Post> PostList = PostPage.getRecords();
        Page<PostVO> PostVOPage = new Page<>(PostPage.getCurrent(), PostPage.getSize(), PostPage.getTotal());
        if (CollectionUtils.isEmpty(PostList)) {
            return PostVOPage;
        }
        // 1. 关联查询用户信息
        Set<Long> userIdSet = PostList.stream().map(Post::getUserId).collect(Collectors.toSet());
        Map<Long, List<User>> userIdUserListMap = userService.listByIds(userIdSet).stream()
                .collect(Collectors.groupingBy(User::getId));
        // 2. 已登录，获取用户点赞、收藏状态
        Map<Long, Boolean> PostIdHasThumbMap = new HashMap<>();
        Map<Long, Boolean> PostIdHasFavourMap = new HashMap<>();
        User loginUser = userService.getLoginUserPermitNull(request);
        if (loginUser != null) {
            Set<Long> PostIdSet = PostList.stream().map(Post::getId).collect(Collectors.toSet());
            loginUser = userService.getLoginUser(request);
            // 获取点赞
            QueryWrapper<PostThumb> PostThumbQueryWrapper = new QueryWrapper<>();
            PostThumbQueryWrapper.in("PostId", PostIdSet);
            PostThumbQueryWrapper.eq("userId", loginUser.getId());
            List<PostThumb> PostPostThumbList = PostThumbMapper.selectList(PostThumbQueryWrapper);
            PostPostThumbList.forEach(PostPostThumb -> PostIdHasThumbMap.put(PostPostThumb.getPostId(), true));
            // 获取收藏
            QueryWrapper<PostFavour> PostFavourQueryWrapper = new QueryWrapper<>();
            PostFavourQueryWrapper.in("PostId", PostIdSet);
            PostFavourQueryWrapper.eq("userId", loginUser.getId());
            List<PostFavour> PostFavourList = PostFavourMapper.selectList(PostFavourQueryWrapper);
            PostFavourList.forEach(PostFavour -> PostIdHasFavourMap.put(PostFavour.getPostId(), true));
        }
        // 填充信息
        List<PostVO> PostVOList = PostList.stream().map(Post -> {
            PostVO postVO = PostVO.objToVo(Post);
            Long userId = Post.getUserId();
            User user = null;
            if (userIdUserListMap.containsKey(userId)) {
                user = userIdUserListMap.get(userId).get(0);
            }
            postVO.setUser(userService.getUserVO(user));
            postVO.setHasThumb(PostIdHasThumbMap.getOrDefault(Post.getId(), false));
            postVO.setHasFavour(PostIdHasFavourMap.getOrDefault(Post.getId(), false));
            return postVO;
        }).collect(Collectors.toList());
        PostVOPage.setRecords(PostVOList);
        return PostVOPage;
    }

}




