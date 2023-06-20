package org.DFSdemo.util;

import java.lang.reflect.Constructor;

public class ReflectionUtils {
    /** 空的Class类数组(空参数列表)，用于下面newInstance方法得到默认的构造方法 */
    private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

    /**
     * 通过反射创建对象，调用的是类默认的构造方法
     *
     * @param theClass 需要实例化的类
     * @return 通过默认构造方法实例化的对象
     * @param <T>
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theClass){
        T result;
        try {
            Constructor<T> meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
            meth.setAccessible(true);
            result = meth.newInstance();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        return result;
    }
}
