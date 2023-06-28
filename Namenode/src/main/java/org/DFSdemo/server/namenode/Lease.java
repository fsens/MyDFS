package org.DFSdemo.server.namenode;// Lease.java

import java.util.Objects;

public class Lease {
    private final String path;           // 文件路径
    private final String holder;         // 租约持有者
    private long expirationTime;   // 租约过期时间

    public Lease(String path, String holder, long expirationTime) {
        this.path = path;
        this.holder = holder;
        this.expirationTime = expirationTime;
    }

    public String getPath() {
        return path;
    }

    public String getHolder() {
        return holder;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lease lease = (Lease) o;
        return Objects.equals(path, lease.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }
}