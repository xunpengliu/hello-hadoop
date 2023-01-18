package org.example.helloHadoop.common.parse;

import org.example.helloHadoop.common.entity.SaleDataEntity;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;

/**
 * 销售数据解释器
 * 销售数据格式为
 * userId$countDate(yyyy-MM-dd)$saleCount
 */
public class SaleDataParse implements TextParse<SaleDataEntity> {

    @Override
    public SaleDataEntity parse(String text) {
        if (text == null) {
            return null;
        }
        text = text.trim();
        if (text.isEmpty()) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String[] split = text.split("\\$");
        SaleDataEntity data = new SaleDataEntity();
        data.userId = Long.valueOf(split[0]);
        data.countDate = sdf.parse(split[1], new ParsePosition(0));
        data.saleCount = Integer.valueOf(split[2]);
        return data;
    }
}
