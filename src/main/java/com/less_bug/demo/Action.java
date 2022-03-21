package com.less_bug.demo;

import java.util.stream.Stream;

public enum Action {

    UPLOAD,
    DOWNLOAD,
    LIST;


    public static Action fromString(String code) {
        return Stream.of(Action.values()).filter(operation -> operation.name().equalsIgnoreCase(code)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid operation code: " + code));
    }
}
