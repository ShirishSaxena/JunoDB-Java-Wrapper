package com.junowrapper.codec;

import java.io.*;
import java.util.Objects;

public class DefaultCodec extends BaseCodec {
    @Override
    public <T> byte[] serialize(T object) {
        return serialize(object, (Class<T>) object.getClass());
    }

    @Override
    public <T> byte[] serialize(T object, Class<T> tClass) {
        try (ByteArrayOutputStream boas = new ByteArrayOutputStream(); ObjectOutputStream ois = new ObjectOutputStream(boas)) {
            Objects.requireNonNull(object, "Can not serialize null object");
            ois.writeObject(object);
            return boas.toByteArray();
        } catch (Exception ioe) {
            System.out.println("Error serializeObject(): " + ioe);
        }
        return null;
    }

    @Override
    public <T> T deserialize(byte[] _arr, Class<T> tClass) {
        try (InputStream is = new ByteArrayInputStream(_arr); ObjectInputStream ois = new ObjectInputStream(is)) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException ioe) {
            System.out.println("Error [IO/ClassNotFound] deserializeObject(): " + ioe);
        } catch (Exception e) {
            System.out.println("Error deserializeObject(): " + e);
        }
        return null;
    }
}
