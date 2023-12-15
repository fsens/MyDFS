package org.DFSdemo.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 这是MyDFS RPC的配置类，程序运行时的配置信息从里面获取
 * 程序运行时的配置也是set在指定的文件中。注意：这里没有使用store方法进行持久化存储，所以程序运行结束，所有set的值都将丢失
 */
public class Configuration {
    Properties properties;
    public static final String TRUE_STR = "true";
    public static final String FALSE_STR = "false";

    /** 读取配置文件Server.properties */
    public Configuration() throws IOException {
        InputStream inStream = ClassLoader.getSystemResourceAsStream("Server.properties");
        properties = new Properties();
        properties.load(inStream);
    }

    public void set(String name, String value) {
        properties.setProperty(name, value);
    }

    public String get(String name) {
        return properties.getProperty(name);
    }

    /** 指定默认key查找指定的key，若没找到则返回默认的key */
    public String get(String name, String defaultValue) {
        return properties.getProperty(name, defaultValue);
    }

    /** 指定默认key查找指定的key，若没找到则返回默认的key */
    public int getInt(String name, int defaultValue) {
        String valueStr = get(name);
        if (valueStr == null) {
            return defaultValue;
        }
        return Integer.parseInt(valueStr);
    }

    /** 指定默认key查找指定的key，若没找到则返回默认的key */
    public boolean getBoolean(String name, boolean defaultValue) {
        String valueStr = get(name);
        if (valueStr == null) {
            return defaultValue;
        }

        valueStr = valueStr.toLowerCase();
        if (TRUE_STR.equals(valueStr)) {
            return true;
        } else if (FALSE_STR.equals(valueStr)) {
            return false;
        } else {
            return defaultValue;
        }
    }

    /** 设置类到配置文件中 */
    public void setClass(String name, Class<?> theClass, Class<?> xface) {
        // 判断theClass是否是xface或其的子类，如果不是则抛出异常
        if (!xface.isAssignableFrom(theClass)) {
            throw new RuntimeException(theClass + " not " + xface.getName());
        }

        set(name, theClass.getName());
    }

    public Class<?> getClassByName(String clsName) throws ClassNotFoundException {
        return Class.forName(clsName);
    }

    /** 获取指定key的类，若没找到则返回默认的类 */
    public Class<?> getClass(String name, Class<?> defaultValue) {
        String className = get(name);
        if (className == null) {
            return defaultValue;
        }

        try {
            return getClassByName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
