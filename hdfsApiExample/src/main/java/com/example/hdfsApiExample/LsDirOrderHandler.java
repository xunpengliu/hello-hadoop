package com.example.hdfsApiExample;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 创建目录命令的处理器
 */
public class LsDirOrderHandler extends OrderHandler {

    public LsDirOrderHandler(FileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    public String verifyOrderArgs(String[] args) {
        if (args.length < 1) {
            return "args error,ls order need 1 args,such as '-ls /'";
        }
        for (String arg : args) {
            if (arg == null || arg.isEmpty()) {
                throw new IllegalArgumentException("arg is empty");
            }
        }
        return null;
    }

    /**
     * @param args arg[0]为需要创建目录的全局路径
     */
    @Override
    public String handlerOrder(String[] args) throws Exception {
        FileStatus[] fileStatusArr = getFileSystem().listStatus(new Path(args[0]));
        printLine(new String[]{
                "permission ",
                "replication ",
                "owner ",
                "group ",
                "size ",
                "modify-time  ",
                "file-name"
        });
        int dirNum = 0;
        int fileNum = 0;
        for (FileStatus fileStatus : fileStatusArr) {
            printFileState(fileStatus);
            if (fileStatus.isDirectory()) {
                dirNum++;
            } else {
                fileNum++;
            }
        }
        System.out.println((dirNum + fileNum) + " total files");
        System.out.println(dirNum + " directories");
        System.out.println(fileNum + " files");
        return null;
    }

    private void printFileState(FileStatus fileStatus) {
        String[] strings = new String[7];

        FsPermission permission = fileStatus.getPermission();
        if (fileStatus.isDirectory()) {
            strings[0] = 'd'
                    + permission.getUserAction().SYMBOL
                    + permission.getGroupAction().SYMBOL
                    + permission.getOtherAction().SYMBOL;
        } else {
            strings[0] = '-'
                    + permission.getUserAction().SYMBOL
                    + permission.getGroupAction().SYMBOL
                    + permission.getOtherAction().SYMBOL;
        }

        //副本数
        strings[1] = "" + fileStatus.getReplication();
        //所有者
        strings[2] = fileStatus.getOwner();
        //所属组
        strings[3] = fileStatus.getGroup();
        //大小
        strings[4] = "" + fileStatus.getLen();
        //修改时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        strings[5] = sdf.format(new Date(fileStatus.getModificationTime()));
        //文件名
        strings[6] = fileStatus.getPath().getName();

        printLine(strings);
    }

    private void printLine(String[] strings) {
        StringBuilder sb = new StringBuilder(100);
        //permission
        sb.append(String.format("%-11s", emptyString(strings[0])));
        //replication
        sb.append(String.format("%-12s", emptyString(strings[1])));
        //owner
        sb.append(String.format("%-13s", emptyString(strings[2])));
        //group
        sb.append(String.format("%-13s", emptyString(strings[3])));
        //size
        sb.append(String.format("%-11s", emptyString(strings[4])));
        //modify-time
        sb.append(String.format("%-22s", emptyString(strings[5])));
        //file-name
        sb.append(strings[6]);

        System.out.println(sb);
    }

    private String emptyString(String str) {
        return str != null ? str : "";
    }
}
