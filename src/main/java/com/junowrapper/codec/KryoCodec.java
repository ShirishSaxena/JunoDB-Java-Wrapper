package com.junowrapper.codec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.Set;

public class KryoCodec extends BaseCodec {

    private final Set<Class<?>> clazz = new HashSet<>();


    @Override
    public <T> byte[] serialize(T object) {
        return serialize(object, (Class<T>) object.getClass());
    }

    @Override
    public <T> byte[] serialize(T object, Class<T> tClass) {
        System.out.println(tClass.getName());
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             Output output = new Output(byteArrayOutputStream)) {
//            getInstance().writeObject(output, object);
            getInstance().writeClassAndObject(output, object);
            output.close();
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            // to change to logger
            System.out.println("Error fail to serialize(): " + e);
            return new byte[0];
        }
    }

    @Override
    public <T> T deserialize(byte[] _arr, Class<T> type) {
        System.out.println(type.getName());
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(_arr);
             Input input = new Input(byteArrayInputStream)) {
//            Object o = getInstance().readObject(input, type);
            Object o = getInstance().readClassAndObject(input);
            T deserializedObject = (T) o;
            input.close();
            return deserializedObject;
        } catch (Exception e) {
            System.out.println("Error fail to deserialize(): " + e);
            return null;
        }
    }

    private Kryo getInstance() {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        return kryo;
    }
}
