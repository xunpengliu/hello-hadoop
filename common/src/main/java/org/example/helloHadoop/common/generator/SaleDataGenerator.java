package org.example.helloHadoop.common.generator;

import org.apache.commons.lang3.RandomUtils;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SaleDataGenerator {

    public static void main(String[] args) throws Exception {
        //销售人员数量
        int userCount = 100000;
        //日期限制，[start,end]
        String[] dateLimit = new String[]{"2021-01-01", "2022-12-31"};
        //生成文件数量
        int fileCount = 5;
        //生成
        String parentPath = "C:\\Users\\l3789\\Desktop\\新建文件夹";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        //生成用户id数据
        List<Long> userIdList = new ArrayList<>(userCount);
        for (int i = 0; i < userCount; i++) {
            userIdList.add((long) i);
        }

        try (FileWriteManager fileWriteManager = new FileWriteManager(fileCount, parentPath)) {
            long day = 0;
            Date startDate = sdf.parse(dateLimit[0]);
            Date endDate = sdf.parse(dateLimit[1]);
            while (true) {
                if (startDate.getTime() + day * 24 * 60 * 60 * 1000 > endDate.getTime()) {
                    break;
                }
                Date countDate = new Date(startDate.getTime() + day * 24 * 60 * 60 * 1000);
                for (Long userId : userIdList) {
                    int saleCount = RandomUtils.nextInt(1, 9999);
                    String record = genSaleDataRecord(userId, countDate, saleCount);
                    OutputStream outputStream = fileWriteManager.randomOutputStream();
                    outputStream.write(record.getBytes(StandardCharsets.UTF_8));
                    outputStream.write('\r');
                    outputStream.write('\n');
                }

                day++;
            }
        }

    }

    private static String genSaleDataRecord(Long userId, Date countDate, int saleCount) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return userId + "$" + sdf.format(countDate) + "$" + saleCount;
    }
}
