package com.zhao.MYBI.controller;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.gson.Gson;
import com.zhao.MYBI.annotation.AuthCheck;
import com.zhao.MYBI.common.BaseResponse;
import com.zhao.MYBI.common.DeleteRequest;
import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.common.ResultUtils;
import com.zhao.MYBI.constant.CommonConstant;
import com.zhao.MYBI.constant.UserConstant;
import com.zhao.MYBI.exception.BusinessException;
import com.zhao.MYBI.exception.ThrowUtils;
import com.zhao.MYBI.manager.AiManager;
import com.zhao.MYBI.manager.RedisLimiterManager;
import com.zhao.MYBI.model.dto.chart.*;
import com.zhao.MYBI.model.entity.Chart;
import com.zhao.MYBI.model.entity.User;
import com.zhao.MYBI.model.vo.BIResponse;
import com.zhao.MYBI.service.ChartService;
import com.zhao.MYBI.service.UserService;
import com.zhao.MYBI.utils.ExcelUtils;
import com.zhao.MYBI.utils.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.parsson.JsonUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 异步化测试
 */

@RestController
@RequestMapping("/queue")
//@CrossOrigin(origins = "http://localhost:8000", methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.PUT, RequestMethod.DELETE})
@CrossOrigin(origins = "http://localhost:8000", allowCredentials = "true")
@Slf4j
@Profile({"dev","local"})
public class QueueController {

    @Resource
    private ThreadPoolExecutor threadPoolExecutor;

    @PostMapping("/add")
    public void add(String name){
        CompletableFuture.runAsync(()->{
            System.out.println("执行人"+Thread.currentThread().getName()+",任务执行中"+name);

            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        },threadPoolExecutor);

    }

    @GetMapping("/get")
    public String get(){
        Map<String,Object> map = new HashMap<String, Object>();
        int size = threadPoolExecutor.getQueue().size();
        map.put("队列长度",size);
        long taskCount = threadPoolExecutor.getTaskCount();
        map.put("任务总数",taskCount);
        long completedTaskCount = threadPoolExecutor.getCompletedTaskCount();
        map.put("已经完成的任务总数", completedTaskCount);
        int activeCount = threadPoolExecutor.getActiveCount();
        map.put("当前正在工作的线程数", activeCount);

        return JSONUtil.toJsonStr(map);
    }


}