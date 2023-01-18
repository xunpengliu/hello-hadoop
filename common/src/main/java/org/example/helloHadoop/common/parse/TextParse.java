package org.example.helloHadoop.common.parse;

/**
 * 输入text记录，解析数据实体
 */
public interface TextParse<T> {
    /**
     * 解码数据行，返回数据实体
     */
    T parse(String text);
}
