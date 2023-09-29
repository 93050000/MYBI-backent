package com.zhao.MYBI.common;

public enum StatusCode {

    WAIT("wait"),
    RUN("running"),
    SUCCEED("succeed"),
    FAIL("failed");


    private final String message;

    StatusCode(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }


}
