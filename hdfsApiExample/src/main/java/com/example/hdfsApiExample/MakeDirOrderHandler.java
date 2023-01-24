package com.example.hdfsApiExample;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 创建目录命令的处理器
 */
public class MakeDirOrderHandler extends OrderHandler {

    public MakeDirOrderHandler(FileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    public String verifyOrderArgs(String[] args) {
        if (args.length < 1) {
            return "args error,mkdir order need 1 args";
        }
        for (String arg : args) {
            if (arg == null || arg.isEmpty()) {
                throw new IllegalArgumentException("arg is empty");
            }
        }
        return null;
    }

    /**
     *
     * @param args args[0]为需要创建目录的全局路径
     */
    @Override
    public String handlerOrder(String[] args) throws Exception {
        getFileSystem().mkdirs(new Path(args[0]));
        return null;
    }
}
