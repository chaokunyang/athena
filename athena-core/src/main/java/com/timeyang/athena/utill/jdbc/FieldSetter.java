package com.timeyang.athena.utill.jdbc;

import com.timeyang.athena.utill.Asserts;
import com.timeyang.athena.utill.StringUtils;

import java.util.Arrays;

/**
 * @author https://github.com/chaokunyang
 */
public class FieldSetter {
    private String fieldName;
    private Object[] objects;
    private String methodName;
    private Class<?>[] signature;

    private FieldSetter(String fieldName, Object value, Object... extraValues) {
        this.fieldName = fieldName;
        this.objects = merge(value, extraValues);
        this.methodName = getMethodName(value);
        this.signature = createSignature(this.objects);
    }

    private String getMethodName(Object value) {
        if (value == null) {
            return "setString";
        } else if (value instanceof Integer) {
            return "setInt";
        } else {
            return "set" + StringUtils.capitalize(value.getClass().getSimpleName());
        }
    }

    private Object[] merge(Object value, Object... extraValues) {
        if (extraValues != null && extraValues.length > 0) {
            Object[] r = new Object[extraValues.length + 1];
            r[0] = value;
            System.arraycopy(extraValues, 0, r, 1, extraValues.length);
            return r;
        } else {
            return new Object[] {value};
        }
    }

    private static Class<?>[] createSignature(Object[] objects) {
        Class<?>[] classes = new Class<?>[objects.length + 1];
        classes[0] = int.class;

        for (int i = 1; i <= objects.length; i++) {
            Class<?> clz = objects[i-1] == null ? String.class : objects[i-1].getClass();
            if (clz == Boolean.class) {
                classes[i] = boolean.class;
            } else if(clz == Byte.class) {
                classes[i] = byte.class;
            // }  else if(clz == Character.class) {
            //     classes[i] = char.class;
            }  else if(clz == Integer.class) {
                classes[i] = int.class;
            } else if(clz == Long.class) {
                classes[i] = long.class;
            }  else if(clz == Short.class) {
                classes[i] = short.class;
            } else if(clz == Double.class) {
                classes[i] = double.class;
            } else if(clz == Float.class) {
                classes[i] = float.class;
            } else {
                classes[i] = clz;
            }
        }
        return classes;
    }

    @Override
    public String toString() {
        return "FieldSetter{" +
                "fieldName='" + fieldName + '\'' +
                ", objects=" + Arrays.toString(objects) +
                ", methodName='" + methodName + '\'' +
                ", signature='" + Arrays.toString(signature) + '\'' +
                '}';
    }

    String getFieldName() {
        return fieldName;
    }

    Object[] getObjects() {
        return objects;
    }

    String getMethodName() {
        return methodName;
    }

    public Class<?>[] getSignature() {
        return signature;
    }

    public static FieldSetter makeSetter(String fieldName, Object value, Object... objects) {
        return new FieldSetter(fieldName, value, objects);
    }

    public static FieldSetter makeSetterWithMethod(String fieldName, String methodName, Object value, Object... extraValues) {
        FieldSetter setter = new FieldSetter(fieldName, value, extraValues);
        // Asserts.check(setter.methodName.equals(methodName), String.format("methodName name [%s] is wrong", methodName));
        setter.methodName = methodName;
        return setter;
    }
}
