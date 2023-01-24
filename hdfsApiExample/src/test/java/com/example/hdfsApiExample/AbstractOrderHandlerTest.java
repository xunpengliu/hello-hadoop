package com.example.hdfsApiExample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public abstract class AbstractOrderHandlerTest {

    public FileSystem getFileSystem() {
        try {
            return FileSystem.get(new URI(getFsUri()), getConfiguration(), getUser());
        } catch (IOException | InterruptedException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] readFull(File file) throws IOException {
        if (!file.exists()) {
            return new byte[0];
        }
        try (InputStream in = new FileInputStream(file)) {
            return readFull(in);
        }
    }

    public byte[] readFull(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buff = new byte[4 * 1024];
        int len;
        while ((len = in.read(buff)) != -1) {
            out.write(buff, 0, len);
        }
        return out.toByteArray();
    }

    protected String getFsUri() {
        return "hdfs://192.168.73.130:8082/";
    }

    protected Configuration getConfiguration() {
        return new Configuration();
    }

    protected String getUser() {
        return "debian";
    }
}
