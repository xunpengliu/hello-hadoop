package com.example.hdfsApiExample;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class CopyOrderHandlerTest extends AbstractOrderHandlerTest {
    private OrderHandler orderHandler;

    @Before
    public void before() throws URISyntaxException {
        orderHandler = new CopyOrderHandler(getFileSystem());
    }

    @Test
    public void handlerLocalFileOrder() throws Exception {
        String[] args = new String[]{
                "C:\\Users\\l3789\\Desktop\\share\\doc\\hadoop\\api\\index.html",
                "d:/a.html"
        };
        File source = new File(args[0]);
        File target = new File(args[1]);

        try {
            try {
                orderHandler.handlerOrder(args);
            } catch (Exception e) {
                throw new Exception(e);
            }
            String sourceMd5 = DigestUtils.md5Hex(readFull(source));
            String targetMd5 = DigestUtils.md5Hex(readFull(target));
            assertEquals(targetMd5, sourceMd5);
        } finally {
            if (target.exists()) {
                target.delete();
            }
        }
    }

    @Test
    public void testLocalToHDFSOrder() throws Exception {
        String[] args = new String[]{
                "C:\\Users\\l3789\\Desktop\\share\\doc\\hadoop\\api\\index.html",
                "hdfs:/testData/test.html"
        };
        File source = new File(args[0]);

        try {
            try {
                orderHandler.handlerOrder(args);
            } catch (Exception e) {
                throw new Exception(e);
            }
            String sourceMd5 = DigestUtils.md5Hex(readFull(source));
            String targetMd5;
            try (InputStream in = getFileSystem().open(new Path(args[1].substring(5)))) {
                targetMd5 = DigestUtils.md5Hex(readFull(in));
            }
            assertEquals(targetMd5, sourceMd5);
        } finally {
            getFileSystem().delete(new Path(args[1].substring(5)), true);
        }
    }
}