package com.baidu.sqlengine.config.util;

public class ConfigException extends RuntimeException {

    private static final long serialVersionUID = -180146385688342818L;

    public ConfigException() {
        super();
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(Throwable cause) {
        super(cause);
    }

}