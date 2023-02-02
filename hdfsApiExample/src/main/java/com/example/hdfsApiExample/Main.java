package com.example.hdfsApiExample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandArgsAnalysis commandArgsAnalysis = new CommandArgsAnalysis();
        Map<String, CommandArgsAnalysis.Command> analysis = commandArgsAnalysis.analysis(args);
        Configuration conf = getConf();

        Map<String, Class<? extends OrderHandler>> orderMap = new HashMap<>();
        orderMap.put("mkdir", MakeDirOrderHandler.class);
        orderMap.put("rm", RemoveOrderHandler.class);
        orderMap.put("cp", CopyOrderHandler.class);
        orderMap.put("ls", LsDirOrderHandler.class);

        if (args.length == 0 || "-help".equals(args[0])) {
            printHelp();
            return 0;
        }
        CommandArgsAnalysis.Command orderCommand = analysis.get("-o");
        if (orderCommand == null || orderCommand.argsLen < 2) {
            printHelp();
            return 0;
        }
        final Class<? extends OrderHandler> orderHandlerClass = orderMap.get(orderCommand.args[0]);
        if (orderHandlerClass == null) {
            System.out.println("not support order.");
            printHelp();
            return 0;
        }

        FileSystem fileSystem;
        try {
            String defaultFS = conf.get("fs.defaultFS");
            String user;
            CommandArgsAnalysis.Command userCommand = analysis.get("-u");
            if (userCommand != null && userCommand.argsLen > 0) {
                user = userCommand.args[0];
            } else {
                user = null;
            }
            System.out.println("use fs namenode->" + defaultFS + " user->" + user);
            if (user == null) {
                fileSystem = FileSystem.newInstance(new URI(defaultFS), new Configuration());
            } else {
                fileSystem = FileSystem.newInstance(new URI(defaultFS), new Configuration(), user);
            }
        } catch (URISyntaxException e) {
            System.out.println("fsURI error");
            return 0;
        } catch (IOException | InterruptedException e) {
            System.out.println("can't link hdfs");
            e.printStackTrace();
            return 0;
        }
        Constructor<? extends OrderHandler> constructor = orderHandlerClass.getConstructor(FileSystem.class);
        OrderHandler orderHandler = constructor.newInstance(fileSystem);
        String[] orderArgs = new String[orderCommand.args.length - 1];
        System.arraycopy(orderCommand.args, 1, orderArgs, 0, orderArgs.length);
        String checkOrderRes = orderHandler.verifyOrderArgs(orderArgs);
        if (checkOrderRes != null) {
            System.out.println("check order fail you need read help,use -help arg,msg->" + checkOrderRes);
            return 0;
        }
        try {
            String errorMsg = orderHandler.handlerOrder(orderArgs);
            if (errorMsg != null) {
                System.out.println("exec order fail,may args error? msg->" + errorMsg);
            }
        } catch (Exception e) {
            System.out.println("exec order exception,may be bug?");
            e.printStackTrace();
        }
        return 0;
    }

    private static void printHelp() {
        System.out.println("support order:");
        System.out.println("-u ${fsUser} -o mkdir ${dir}");
        System.out.println("-u ${fsUser} -o rm ${dir}");
        System.out.println("-u ${fsUser} -o cp ${source file} ${target file}");
        System.out.println("-u ${fsUser} -o ls ${dir}");
        System.out.println("only cp support HDFS and local file.If file is HDFS," +
                "should add prefix 'hdfs:'.such as hdfs:/test/test.sh");
    }
}
