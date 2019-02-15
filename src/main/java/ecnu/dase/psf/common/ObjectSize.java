package ecnu.dase.psf.common;

import java.lang.instrument.Instrumentation;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/15 14:27
 */
public class ObjectSize {
    private static volatile Instrumentation instru;

    public static void premain(String args, Instrumentation inst) {
        instru = inst;
    }

    public static Long getSizeOf(Object object) {
        if (instru == null) {
            throw new IllegalStateException("Instrumentation is null");
        }
        return instru.getObjectSize(object);
    }
}