package com.junowrapper.juno;

import com.junowrapper.codec.BaseCodec;
import com.junowrapper.codec.KryoCodec;
import com.junowrapper.juno.model.JunoDBConfig;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.conf.JunoPropertiesProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public final class JunoDBManager {

    public final JunoClient junoClient;
    public static final long MAX_TTL_ALLOWED = 259200;

    private final BaseCodec codec;


    public JunoDBManager(JunoDBConfig junoDBConfig) {
        this(junoDBConfig, new KryoCodec());
    }

    public JunoDBManager(JunoDBConfig junoDBConfig, BaseCodec codec) {
        junoClient = initializeJunoClient(junoDBConfig);
        this.codec = codec;
    }

    public JunoDBManager(String serverHost, int serverPort, String applicationName, String recordNameSpace) {
        this(serverHost, serverPort, applicationName, recordNameSpace, new KryoCodec());
    }

    public JunoDBManager(String serverHost, int serverPort, String applicationName, String recordNameSpace, BaseCodec codec) {
        JunoDBConfig junoDBConfig = new JunoDBConfig(serverHost, serverPort, applicationName, recordNameSpace);
        junoClient = initializeJunoClient(junoDBConfig);
        this.codec = codec;
    }


    private JunoClient initializeJunoClient(JunoDBConfig junoDBConfig) {
        JunoClient junoClient = null;
        try {
            if (junoDBConfig != null) {
                junoClient = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(junoDBConfig.getProperties()));
            }
        } catch (Throwable e) {
            System.out.println("Exception occur JunoDBManager.initialize(): " + e);
        }
        return junoClient;
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
    public <T> Optional<JunoResponse> getJResponse(T key, TimeUnit timeUnit, long newTTL) {
        try {
            return Optional.ofNullable(junoClient.get(codec.serialize(key), timeUnit.toSeconds(newTTL)));
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
    public <T, V> Optional<V> get(T key, Class<V> vClass, TimeUnit timeUnit, long newTTL) {
        try {
            JunoResponse junoResponse = junoClient.get(codec.serialize(key), timeUnit.toSeconds(newTTL));
            if (junoResponse.getStatus() == OperationStatus.Success) {
                V v = codec.deserialize(junoResponse.getValue(), vClass);
                return Optional.ofNullable(v);
            }
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'get': " + key);
        }
        return Optional.empty();
    }

    public <T> Optional<JunoResponse> getJResponse(T key) {
        try {
            return Optional.ofNullable(junoClient.get(codec.serialize(key)));
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'getJResponse': " + key);
        }
        return Optional.empty();
    }

    public <T, V> Optional<V> get(T key, Class<V> vClass, long timeToLiveSec) {
        return get(key, vClass, TimeUnit.SECONDS, timeToLiveSec);
    }

    public <T, V> Optional<V> get(T key, Class<V> vClass) {
        return get(key, vClass, MAX_TTL_ALLOWED);
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
    public <T, V> Optional<JunoResponse> createJResponse(T key, V value, TimeUnit timeUnit, long newTTL) {
        try {
            return Optional.ofNullable(junoClient.set(codec.serialize(key), codec.serialize(value), timeUnit.toSeconds(newTTL)));
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'create': " + key);
        }
        return Optional.empty();
    }

    public <T, V> boolean create(T key, V value, long timeToLiveSec) {
        Optional<JunoResponse> junoResponse = createJResponse(key, value, TimeUnit.SECONDS, timeToLiveSec);
        return junoResponse.map(r -> r.getStatus() == OperationStatus.Success).orElse(false);
    }

    public <T, V> boolean create(T key, V value) {
        return create(key, value, MAX_TTL_ALLOWED);
    }


    /**
     * Delete the record from Juno DB
     *
     * @param key - Record Key to be deleted
     * @return Optional<JunoResponse> - Optional of Juno Response object which contains the status of the operation
     */
    public <T> Optional<JunoResponse> deleteJResponse(T key) {
        try {
            return Optional.ofNullable(junoClient.delete(codec.serialize(key)));
        } catch (Exception e) {
            System.out.println("JunoDB failed to 'delete': " + key);
        }
        return Optional.empty();
    }

    public <T> boolean delete(T key) {
        Optional<JunoResponse> junoResponse = deleteJResponse(key);
        return junoResponse.map(r -> r.getStatus() == OperationStatus.Success).orElse(false);
    }


    public <T, V> List<V> getAll(Collection<T> keys, Class<V> vClass, long timeToLiveSec) {
        List<V> values = new ArrayList<>();
        try {
            List<JunoRequest> batchGetReq = keys.stream().map(k -> new JunoRequest(codec.serialize(k), null, 0, timeToLiveSec, JunoRequest.OperationType.Get)).collect(Collectors.toList());

            if (batchGetReq.isEmpty()) {
                return values;
            }

            Iterable<JunoResponse> junoResponses = doBatch(batchGetReq);
            if (junoResponses == null) {
                return values;
            }

            junoResponses.forEach(r -> {
                if (r.getStatus() == OperationStatus.Success && r.getValue() != null && r.getValue().length > 0) {
                    values.add(codec.deserialize(r.getValue(), vClass));
                }
            });
        } catch (Exception exception) {
            System.out.println("Error getAll(): " + exception);
            values.clear();
        }
        return values;
    }

    public <T, V> List<V> getAll(Collection<T> keys, Class<V> vClass) {
        return getAll(keys, vClass, MAX_TTL_ALLOWED);
    }

    public Iterable<JunoResponse> doBatch(Iterable<JunoRequest> requests) {
        try {
            return junoClient.doBatch(requests);
        } catch (Exception exception) {
            System.out.println("Error doBatch(): " + exception);
        }
        return null;
    }

    public <K> JunoRequest setJunoRequest(K key, JunoRequest.OperationType operationType) {
        return setJunoRequest(key, operationType, MAX_TTL_ALLOWED);
    }

    public <K> JunoRequest setJunoRequest(K key, JunoRequest.OperationType operationType, long timeToLiveSec) {
        return new JunoRequest(codec.serialize(key), null, 0, timeToLiveSec, operationType);
    }

    public <K, V> JunoRequest setJunoRequest(K key, V value, JunoRequest.OperationType operationType) {
        return setJunoRequest(key, value, operationType, TimeUnit.SECONDS, MAX_TTL_ALLOWED);
    }

    public <K, V> JunoRequest setJunoRequest(K key, V value, JunoRequest.OperationType operationType, long timeToLiveSec) {
        return setJunoRequest(key, value, operationType, TimeUnit.SECONDS, timeToLiveSec);
    }

    public <K, V> JunoRequest setJunoRequest(K key, V value, JunoRequest.OperationType operationType, TimeUnit timeUnit, long newTTL) {
        byte[] serializedKey = codec.serialize(key);
        byte[] serializedValue = codec.serialize(value);
        return new JunoRequest(serializedKey, serializedValue, 0, timeUnit.toSeconds(newTTL), operationType);
    }

    public BaseCodec getCodec() {
        return codec;
    }
}
