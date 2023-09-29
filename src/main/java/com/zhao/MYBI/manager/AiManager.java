package com.zhao.MYBI.manager;

import com.yupi.yucongming.dev.client.YuCongMingClient;
import com.yupi.yucongming.dev.common.BaseResponse;
import com.yupi.yucongming.dev.model.DevChatRequest;
import com.yupi.yucongming.dev.model.DevChatResponse;
import com.zhao.MYBI.common.ErrorCode;
import com.zhao.MYBI.exception.BusinessException;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class AiManager {

    @Resource
    private YuCongMingClient yuCongMingClient;


    /**
     * AI对话
     * @param modelId
     * @param message
     * @return
     */
    public String doChat(Long modelId, String message){
        DevChatRequest devChatRequest = new DevChatRequest();
        devChatRequest.setModelId(modelId);
        devChatRequest.setMessage(message);
        BaseResponse<DevChatResponse> response = yuCongMingClient.doChat(devChatRequest);
        if (response == null){
            throw new BusinessException(ErrorCode.SYSTEM_ERROR,"AI响应错误");
        }if (response.getCode() == 40301){
            throw new BusinessException(ErrorCode.YUCONGMING_ERROR,"AI回答次数已达到上限！");
        }
        System.out.println(response.getData().getContent());
        return response.getData().getContent();
    }


}
