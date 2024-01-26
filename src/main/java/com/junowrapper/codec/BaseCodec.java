package com.junowrapper.codec;

public abstract class BaseCodec {
    public abstract <T> byte[] serialize(T object);

    public abstract <T> byte[] serialize(T object, Class<T> tClass);

    public abstract <T> T deserialize(byte[] _arr, Class<T> tClass);
}
