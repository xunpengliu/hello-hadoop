package org.example.helloHadoop.common.generator;

import org.apache.commons.lang3.RandomUtils;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FileWriteManager implements Closeable {
    /**
     * 生成文件数量
     */
    private int fileCount;
    /**
     * 文件父目录
     */
    private String parentPath;
    /**
     * 输出流
     */
    private OutputStream[] outputStream;

    public FileWriteManager(int fileCount, String parentPath) throws FileNotFoundException {
        this.fileCount = fileCount;
        this.parentPath = parentPath;
        if (fileCount < 1) {
            throw new IllegalArgumentException("fileCount need >= 1");
        }
        File parentFile = new File(parentPath);
        if (parentFile.exists()) {
            if (!parentFile.isDirectory()) {
                throw new IllegalArgumentException("parentFile need is directory");
            }
        }

        init();
    }

    /**
     * 随机给出输出流
     */
    public OutputStream randomOutputStream() {
        int index = RandomUtils.nextInt(0, fileCount);
        return outputStream[index];
    }

    private void init() throws FileNotFoundException {
        File parentFile = new File(parentPath);
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        outputStream = new OutputStream[fileCount];
        for (int i = 0; i < fileCount; i++) {
            String fileName = "file-" + i;
            outputStream[i] = new BufferedOutputStream(new FileOutputStream(new File(parentPath, fileName)));
        }
    }

    @Override
    public void close() throws IOException {
        for (OutputStream stream : outputStream) {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (IOException ignore) {
            }
        }
    }
}
