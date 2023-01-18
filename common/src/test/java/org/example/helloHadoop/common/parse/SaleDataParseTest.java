package org.example.helloHadoop.common.parse;

import org.example.helloHadoop.common.entity.SaleDataEntity;
import org.junit.Test;

import java.text.SimpleDateFormat;

import static org.junit.Assert.*;

public class SaleDataParseTest {
    private TextParse<SaleDataEntity> saleDataParse = new SaleDataParse();

    @Test
    public void parse() {
        String text = "1$2020-01-01$998";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SaleDataEntity parse = saleDataParse.parse(text);
        //verify
        assertNotNull(parse);
        assertNotNull(parse.userId);
        assertNotNull(parse.countDate);
        assertNotNull(parse.saleCount);
        assertEquals("2020-01-01", sdf.format(parse.countDate));
        assertEquals(1, parse.userId.longValue());
        assertEquals(998, parse.saleCount.intValue());
    }
}