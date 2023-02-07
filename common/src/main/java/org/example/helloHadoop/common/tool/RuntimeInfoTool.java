package org.example.helloHadoop.common.tool;

import org.apache.commons.lang3.StringUtils;
import org.example.helloHadoop.common.entity.RuntimeInfoEntity;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 运行时信息工具。可以获取当前进程id，主类信息
 */
public class RuntimeInfoTool {
    private static volatile RuntimeInfoEntity data;

    /**
     * 获取当前虚拟机运行时信息
     */
    public static RuntimeInfoEntity getRunInfo() {
        if (data == null) {
            synchronized (RuntimeInfoTool.class) {
                if (data == null) {
                    RuntimeInfoEntity.RuntimeInfoEntityBuild build = RuntimeInfoEntity.createBuild();
                    build.setMainClass(getMainClass())
                            .setArgs(getArgs())
                            .setStartTime(getStartTime())
                            .setPid(getPid());
                    data = build.build();
                }
            }
        }

        RuntimeInfoEntity.RuntimeInfoEntityBuild build = RuntimeInfoEntity.createBuild();
        build.setMainClass(data.getMainClass())
                .setArgs(data.getArgs())
                .setStartTime(data.getStartTime())
                .setPid(data.getPid())
                .setIpv4(getIpv4());

        return build.build();
    }

    private static String[] getArgs() {
        Object mainClassName = System.getProperties().get("sun.java.command");
        if (mainClassName != null) {
            String s = mainClassName.toString();
            String[] split = s.split(" ");
            if (split.length == 1) {
                return new String[0];
            }
            String[] args = new String[split.length - 1];
            System.arraycopy(split, 1, args, 0, args.length);
            return args;
        }
        return new String[0];
    }

    /**
     * 获取启动时间
     */
    private static Date getStartTime() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        long startTime = runtimeMXBean.getStartTime();
        return new Date(startTime);
    }

    /**
     * 获取当前ip
     */
    private static String getIpv4() {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface network = networkInterfaces.nextElement();
                if (network.isVirtual() || !network.isUp() || network.isLoopback()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = network.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address) {
                        return address.getHostAddress();
                    }
                }
            }
            return null;
        } catch (SocketException e) {
            return null;
        }
    }

    /**
     * 获取当前进程pid
     */
    private static Integer getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        if (name == null) {
            return null;
        }
        String[] names = name.split("@");
        if (StringUtils.isNumeric(names[0])) {
            return Integer.parseInt(names[0]);
        }
        return null;
    }

    /**
     * 获取主类名称
     */
    private static String getMainClass() {
        Object mainClassName = System.getProperties().get("sun.java.command");
        if (mainClassName != null) {
            String s = mainClassName.toString();
            return s.split(" ")[0];
        }

        return foundMainClassByStackTraces();
    }

    /**
     * 根据当前系统的线程栈获取主类名称
     */
    private static String foundMainClassByStackTraces() {
        Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> threadEntry : allStackTraces.entrySet()) {
            String threadName = threadEntry.getKey().getName();
            if (!"main".equals(threadName)) {
                continue;
            }
            StackTraceElement[] stackTrace = threadEntry.getValue();
            for (int i = stackTrace.length - 1; i >= 0; i--) {
                StackTraceElement stack = stackTrace[i];
                if (stack.isNativeMethod() || !"main".equals(stack.getMethodName())) {
                    continue;
                }
                String className;
                Method mainMethod;
                try {
                    className = stack.getClassName();
                    Class<?> clz = Class.forName(className);
                    mainMethod = clz.getDeclaredMethod("main", String[].class);
                    if (mainMethod.getReturnType() != void.class) {
                        continue;
                    }
                } catch (ClassNotFoundException | NoSuchMethodException e) {
                    continue;
                }
                int modifiers = mainMethod.getModifiers();
                if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers)) {
                    return className;
                }
            }
        }

        return null;
    }
}
