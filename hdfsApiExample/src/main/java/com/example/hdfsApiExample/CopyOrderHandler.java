package com.example.hdfsApiExample;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 拷贝命令cp的处理器
 */
public class CopyOrderHandler extends OrderHandler {
    private final String schema = "hdfs:";

    public CopyOrderHandler(FileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    public String verifyOrderArgs(String[] args) {
        if (args.length < 2) {
            return "args error,cp order need 2 args";
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
     * @param args args[0] 源文件路径，args[1]目标路径
     *             如果路径前面带有hdfs:则表示文件系统为HDFS
     */
    @Override
    public String handlerOrder(String[] args) throws Exception {
        String verifyOrderRes = verifyOrderArgs(args);
        if (verifyOrderRes != null) {
            return verifyOrderRes;
        }

        String inPathStr = args[0];
        String outPathStr = args[1];

        try (InputStream in = createSourceStream(inPathStr);
             OutputStream out = createOutputStream(outPathStr)) {
            byte[] buff = new byte[4 * 1024];
            int len;
            while ((len = in.read(buff)) != -1) {
                out.write(buff, 0, len);
            }

            //如果输出流是hdfs的，调用一次同步，保证写入到硬盘上
            if (out instanceof FSDataOutputStream) {
                //如果流被关闭，会自动调用一次flush
                //但是这里用的是hsync，注意与hflush的区别
                ((FSDataOutputStream) out).hsync();
            }
        }

        return null;
    }

    private InputStream createSourceStream(String pathStr) throws IOException {
        if (pathStr.startsWith(schema)) {
            return getFileSystem().open(new Path(pathStr.substring(schema.length())));
        } else {
            return new BufferedInputStream(new FileInputStream(pathStr));
        }
    }

    private OutputStream createOutputStream(String pathStr) throws IOException {
        if (pathStr.startsWith(schema)) {
            boolean exists = getFileSystem().exists(new Path(pathStr.substring(schema.length())));
            if (exists) {
                throw new IllegalArgumentException(pathStr + " file already exists");
            }
            return getFileSystem().create(new Path(pathStr.substring(schema.length())), false);
        } else {
            File file = new File(pathStr);
            if (file.exists()) {
                throw new IllegalArgumentException(pathStr + " file already exists");
            }
            if (!file.getParentFile().exists()) {
                throw new IllegalArgumentException(file.getParentFile().getPath() + " is not exists");
            }
            return new BufferedOutputStream(new FileOutputStream(file));
        }
    }
}
