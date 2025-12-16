package com.sporty.exception;

import java.io.IOException;

public class SCacheSerializeException extends IOException {
    public SCacheSerializeException(String message, Throwable cause) {
        super(message, cause);
    }
}
