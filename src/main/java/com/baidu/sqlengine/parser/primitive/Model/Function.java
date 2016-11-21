package com.baidu.sqlengine.parser.primitive.Model;

import java.util.LinkedList;
import java.util.List;

public class Function extends Identifier {
    private final List<Identifier> arguments;

    public Function(String name) {
        super(name);
        this.arguments = new LinkedList<>();
    }

    public List<Identifier> getArguments() {
        return arguments;
    }
}
