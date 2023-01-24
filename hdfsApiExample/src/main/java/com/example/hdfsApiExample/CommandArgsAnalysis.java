package com.example.hdfsApiExample;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CommandArgsAnalysis {

    public Map<String, Command> analysis(String[] args) {
        Map<String, Command> map = new HashMap<>();
        Command command = null;
        for (String arg : args) {
            if (arg.startsWith("-")) {
                command = new Command();
                command.commandName = arg;
                command.args = new String[args.length];
                command.argsLen = 0;
                map.put(arg, command);
            } else {
                if (command != null) {
                    command.args[command.argsLen] = arg;
                    command.argsLen++;
                }
            }
        }
        for (Command value : map.values()) {
            value.args = Arrays.copyOf(value.args, value.argsLen);
        }
        return map;
    }

    public static class Command {
        /**
         * 命令名称
         */
        public String commandName;
        /**
         * 命令参数
         */
        public String[] args;
        /**
         * 命令长度
         */
        public int argsLen;
    }
}
