package com.example.hdfsApiExample;

import org.apache.hadoop.fs.FileSystem;

public abstract class OrderHandler {
    private FileSystem fileSystem;

    public OrderHandler(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * 验证参数是否存在
     *
     * @param args 命令参数
     * @return 错误消息，验证成功返回null
     */
    public abstract String verifyOrderArgs(String[] args);

    /**
     * 执行命令
     *
     * @param args 命令参数
     * @return 执行错误消息，成功返回null
     */
    public abstract String handlerOrder(String[] args) throws Exception;

    public FileSystem getFileSystem() {
        return fileSystem;
    }

}
