package com.example.hdfsApiExample;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 创建目录命令的处理器
 */
public class RemoveOrderHandler extends OrderHandler {

    public RemoveOrderHandler(FileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    public String verifyOrderArgs(String[] args) {
        if (args.length < 1) {
            return "args error,cp order need 1 args";
        }
        if ("r".equals(args[0]) && args.length < 2) {
            return "args error,rm order need 2 args";
        }
        for (String arg : args) {
            if (arg == null || arg.isEmpty()) {
                throw new IllegalArgumentException("arg is empty");
            }
        }
        return null;
    }

    /**
     * @param args args[0]可选，如果需要递归删除，则额外需要参数-r
     *             args[1]为需要删除的路径
     */
    @Override
    public String handlerOrder(String[] args) throws Exception {
        boolean recursion = args.length > 1 && "r".equals(args[0]);
        Path path;
        if ("r".equals(args[0])) {
            path = new Path(args[1]);
        } else {
            path = new Path(args[0]);
        }
        if (path.isRoot()) {
            return "can't remove root";
        }
        FileStatus fileStatus = getFileSystem().getFileStatus(path);
        if (fileStatus.isDirectory()) {
            if (!recursion) {
                FileStatus[] fileStatuses = getFileSystem().listStatus(path);
                if (fileStatuses.length > 0) {
                    return path.getName() + " is not empty,use rm r " + path;
                }
            }
        }
        getFileSystem().delete(path, recursion);
        return null;
    }
}
