package com.zhao.MYBI.model.vo;


import lombok.Data;

/**BI返回结果
 *
 */
@Data
public class BIResponse {

    /**
     * 返回图表的代码
     */
    private String chartCode;

    /**
     * 返回结论
     */
    private String conclusion;

    /**
     * 图标ID
     */
    private Long chartId;

}
