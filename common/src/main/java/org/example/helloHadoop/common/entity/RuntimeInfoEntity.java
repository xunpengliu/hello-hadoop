package org.example.helloHadoop.common.entity;

import java.util.Date;

public class RuntimeInfoEntity {
    /**
     * 主类名称
     */
    private String mainClass;
    /**
     * 运行参数
     */
    private String[] args;
    /**
     * 启动时间
     */
    private Date startTime;
    /**
     * 进程pid
     */
    private Integer pid;
    /**
     * 当前系统ipv4地址
     */
    private String ipv4;

    private RuntimeInfoEntity() {
    }

    public String getMainClass() {
        return mainClass;
    }

    public String[] getArgs() {
        String[] args = new String[this.args.length];
        System.arraycopy(this.args, 0, args, 0, args.length);
        return args;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Integer getPid() {
        return pid;
    }

    public String getIpv4() {
        return ipv4;
    }

    public static RuntimeInfoEntityBuild createBuild() {
        return new RuntimeInfoEntityBuild();
    }

    public static class RuntimeInfoEntityBuild {
        private final RuntimeInfoEntity entity = new RuntimeInfoEntity();

        public RuntimeInfoEntity build() {
            RuntimeInfoEntity data = new RuntimeInfoEntity();
            data.mainClass = entity.mainClass;
            data.args = entity.args;
            data.startTime = entity.startTime;
            data.pid = entity.pid;
            data.ipv4 = entity.ipv4;
            return data;
        }

        public RuntimeInfoEntityBuild setMainClass(String mainClass) {
            entity.mainClass = mainClass;
            return this;
        }
        public RuntimeInfoEntityBuild setArgs(String[] args) {
            if (args != null) {
                entity.args = new String[args.length];
                System.arraycopy(args, 0, entity.args, 0, args.length);
            }
            return this;
        }

        public RuntimeInfoEntityBuild setStartTime(Date startTime) {
            entity.startTime = startTime;
            return this;
        }

        public RuntimeInfoEntityBuild setPid(Integer pid) {
            entity.pid = pid;
            return this;
        }

        public RuntimeInfoEntityBuild setIpv4(String ipv4) {
            entity.ipv4 = ipv4;
            return this;
        }
    }
}
