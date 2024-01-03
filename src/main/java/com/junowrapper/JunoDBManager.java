package com.junowrapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.conf.JunoPropertiesProvider;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public final class JunoDBManager {

    public static JunoClient junoClient;
    public static final long MAX_TTL_ALLOWED = 259200;

    private static final ObjectMapper objectMapper = new ObjectMapper();


    static {
        JunoDBConfig junoDBConfig = new JunoDBConfig("127.0.0.1", 8080, "Test", "jCache");
        initialize(junoDBConfig);
    }

    public static void initialize(JunoDBConfig junoDBConfig) {
        try {
            if (junoDBConfig != null) {
                junoClient = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(junoDBConfig.getProperties()));
            }
        } catch (Throwable e) {
            System.out.println("Exception occur JunoDBManager.initialize(): " + e);
        }
    }


    /**
     * Get a record from Juno DB and Extend the TTL
     *
     * @param key      - Key of the record to be retrieved
     * @param timeUnit - Provide type of newTTL
     * @param newTTL   - Replace previous TTL
     * @return JunoResponse - Juno Response object which contains the status of the operation,
     * version of the record and value of the record.
     */
    public static <T> Optional<JunoResponse> getJResponse(T key, TimeUnit timeUnit, long newTTL) {
        try {
            return Optional.ofNullable(junoClient.get(serializeObject(key), timeUnit.toSeconds(newTTL)));
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'get': " + key);
        }
        return Optional.empty();
    }

    /**
     * Get a record from Juno DB and Extend the TTL
     *
     * @param key      - Key of the record to be retrieved
     * @param timeUnit - Provide type of newTTL
     * @param newTTL   - Replace previous TTL
     * @return Optional<V>      - Optional of deserialized object
     */
    public static <T, V> Optional<V> get(T key, TimeUnit timeUnit, long newTTL) {
        try {
            JunoResponse junoResponse = junoClient.get(serializeObject(key), timeUnit.toSeconds(newTTL));
            if (junoResponse.getStatus() == OperationStatus.Success) {
                V v = deserializeObject(junoResponse.getValue());
                return Optional.ofNullable(v);
            }
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'get': " + key);
        }
        return Optional.empty();
    }

    public static <T> Optional<JunoResponse> getJResponse(T key) {
        try {
            return Optional.ofNullable(junoClient.get(serializeObject(key)));
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'getJResponse': " + key);
        }
        return Optional.empty();
    }

    public static <T, V> Optional<V> get(T key, long timeToLiveSec) {
        return get(key, TimeUnit.SECONDS, timeToLiveSec);
    }

    public static <T, V> Optional<V> get(T key) {
        return get(key, MAX_TTL_ALLOWED);
    }


    /**
     * Update the record if present in Juno DB and extend its TTL else create that record with the supplied TTL.
     *
     * @param key      - Key of the record
     * @param value    - Value to associate with key
     * @param timeUnit - Provide type of newTTL
     * @param newTTL   - Replace previous TTL if record already exists, otherwise creates it
     * @return Optional<JunoResponse>   - Optional of Juno Response object which contains the status of the operation
     * version of the record and value of the record.
     */
    public static <T, V> Optional<JunoResponse> createJResponse(T key, V value, TimeUnit timeUnit, long newTTL) {
        try {
            return Optional.ofNullable(junoClient.set(serializeObject(key), serializeObject(value), timeUnit.toSeconds(newTTL)));
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'create': " + key);
        }
        return Optional.empty();
    }

    public static <T, V> boolean create(T key, V value, long timeToLiveSec) {
        Optional<JunoResponse> junoResponse = createJResponse(key, value, TimeUnit.SECONDS, timeToLiveSec);
        return junoResponse.map(r -> r.getStatus() == OperationStatus.Success).orElse(false);
    }

    public static <T, V> boolean create(T key, V value) {
        return create(key, value, MAX_TTL_ALLOWED);
    }


    /**
     * Delete the record from Juno DB
     *
     * @param key - Record Key to be deleted
     * @return Optional<JunoResponse> - Optional of Juno Response object which contains the status of the operation
     */
    public static <T> Optional<JunoResponse> deleteJResponse(T key) {
        try {
            return Optional.ofNullable(junoClient.delete(serializeObject(key)));
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'delete': " + key);
        }
        return Optional.empty();
    }

    public static <T> boolean delete(T key) {
        Optional<JunoResponse> junoResponse = deleteJResponse(key);
        return junoResponse.map(r -> r.getStatus() == OperationStatus.Success).orElse(false);
    }


    public static <T, V> List<V> getAll(Collection<T> keys, long timeToLiveSec) {
        List<V> values = new ArrayList<>();
        try {
            List<JunoRequest> batchGetReq = keys.stream().map(k -> new JunoRequest(serializeObject(k), null, 0, timeToLiveSec, JunoRequest.OperationType.Get)).collect(Collectors.toList());

            if (batchGetReq.isEmpty()) {
                return values;
            }

            Iterable<JunoResponse> junoResponses = doBatch(batchGetReq);
            if (junoResponses == null) {
                return values;
            }

            junoResponses.forEach(r -> {
                if (r.getStatus() == OperationStatus.Success && r.getValue() != null && r.getValue().length > 0) {
                    values.add(deserializeObject(r.getValue()));
                }
            });
        } catch (Exception exception) {
            System.out.println("Error getAll(): " + exception);
            values.clear();
        }
        return values;
    }

    public static <T, V> List<V> getAll(Collection<T> keys) {
        return getAll(keys, MAX_TTL_ALLOWED);
    }

    public static Iterable<JunoResponse> doBatch(Iterable<JunoRequest> requests) {
        try {
            return junoClient.doBatch(requests);
        } catch (Exception exception) {
            System.out.println("Error doBatch(): " + exception);
        }
        return null;
    }

    public static <K> JunoRequest setJunoRequest(K key, JunoRequest.OperationType operationType) {
        return setJunoRequest(key, operationType, MAX_TTL_ALLOWED);
    }

    public static <K> JunoRequest setJunoRequest(K key, JunoRequest.OperationType operationType, long timeToLiveSec) {
        return new JunoRequest(serializeObject(key), null, 0, timeToLiveSec, operationType);
    }

    public static <K, V> JunoRequest setJunoRequest(K key, V value, JunoRequest.OperationType operationType) {
        return setJunoRequest(key, value, operationType, TimeUnit.SECONDS, MAX_TTL_ALLOWED);
    }

    public static <K, V> JunoRequest setJunoRequest(K key, V value, JunoRequest.OperationType operationType, long timeToLiveSec) {
        return setJunoRequest(key, value, operationType, TimeUnit.SECONDS, timeToLiveSec);
    }

    public static <K, V> JunoRequest setJunoRequest(K key, V value, JunoRequest.OperationType operationType, TimeUnit timeUnit, long newTTL) {
        byte[] serializedKey = serializeObject(key);
        byte[] serializedValue = serializeObject(value);
        return new JunoRequest(serializedKey, serializedValue, 0, timeUnit.toSeconds(newTTL), operationType);
    }

//    public static <T> byte[] serializeObject(T object) {
//        try {
//            Objects.requireNonNull(object, "Can not serialize null object");
////            if (object instanceof String) {
////                return ((String) object).getBytes();
////            }
//            return objectMapper.writeValueAsBytes(object);
//        } catch (IOException ioe) {
//            System.out.println("Error serializeObject(): " + object.getClass().getName());
//        }
//        return null;
//    }


//    public static <T> T deserializeObject(byte[] bytes, TypeReference<T> typeReference) {
//        try (InputStream is = new ByteArrayInputStream(bytes)) {
//            Objects.requireNonNull(bytes, "Can not deserialize null object");
//            return objectMapper.readValue(is, typeReference);
//        } catch (IOException ioe) {
//            System.out.println("Error deserializeObject(): " + objectMapper.getTypeFactory().constructType(typeReference).getRawClass());
//
//            for (StackTraceElement stackTraceElement : ioe.getStackTrace()) {
//                System.out.println(stackTraceElement);
//            }
//        }
//        return null;
//    }


    public static <T> byte[] serializeObject(T object) {
        try (ByteArrayOutputStream boas = new ByteArrayOutputStream(); ObjectOutputStream ois = new ObjectOutputStream(boas)) {
            Objects.requireNonNull(object, "Can not serialize null object");
            ois.writeObject(object);
            return boas.toByteArray();
        } catch (Exception ioe) {
            System.out.println("Errr: " + ioe);
            ioe.printStackTrace();
        }
        return null;
    }

    public static <V> V deserializeObject(byte[] buff) {
        ;
        try (InputStream is = new ByteArrayInputStream(buff); ObjectInputStream ois = new ObjectInputStream(is)) {
            return (V) ois.readObject();
        } catch (IOException | ClassNotFoundException ioe) {
            ioe.printStackTrace();
        } catch (Exception e) {
            System.out.println("Ex" + e);
        }
        return null;
    }
}
