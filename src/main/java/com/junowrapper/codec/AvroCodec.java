package com.junowrapper.codec;

public class AvroCodec extends BaseCodec{
    @Override
    public <T> byte[] serialize(T object) {
        return new byte[0];
    }

    @Override
    public <T> byte[] serialize(T object, Class<T> tClass) {
        return new byte[0];
    }

    @Override
    public <T> T deserialize(byte[] _arr, Class<T> tClass) {
        return null;
    }
}
