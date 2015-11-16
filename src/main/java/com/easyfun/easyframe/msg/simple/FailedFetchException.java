package com.easyfun.easyframe.msg.simple;

/**
 * RuntimeException: 取Topic出错的时候抛出
 * 
 * @author linzhaoming
 *
 * @Created 2014
 */
public class FailedFetchException extends RuntimeException {

    public FailedFetchException(String message) {
        super(message);
    }

    public FailedFetchException(Exception e) {
        super(e);
    }
}
