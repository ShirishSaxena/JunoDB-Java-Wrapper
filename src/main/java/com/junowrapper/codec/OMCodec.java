package com.junowrapper.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class OMCodec extends BaseCodec {

    private final ObjectMapper om = new ObjectMapper();

    @Override
    public <T> byte[] serialize(T object) {
        return serialize(object, (Class<T>) object.getClass());
    }

    @Override
    public <T> byte[] serialize(T object, Class<T> tClass) {
        try {
            Objects.requireNonNull(object, "Can not serialize null object");
            return om.writeValueAsBytes(object);
        } catch (IOException ioe) {
            System.out.println("Error [I/O] serializeObject(): " + ioe);
        } catch (Exception e) {
            System.out.println("Error serializeObject(): " + e);
        }
        return null;
    }

    @Override
    public <T> T deserialize(byte[] _arr, Class<T> tClass) {
        try (InputStream is = new ByteArrayInputStream(_arr)) {
            Objects.requireNonNull(_arr, "Can not deserialize null object");
            // Type Erasure; might not work;
            return om.readValue(is, new TypeReference<T>() {
            });
        } catch (IOException ioe) {
            System.out.println("Error [I/O] deserializeObject(): " + ioe);
        } catch (Exception e) {
            System.out.println("Error deserializeObject(): " + e);
        }
        return null;
    }
}
