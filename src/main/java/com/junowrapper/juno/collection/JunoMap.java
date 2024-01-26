package com.junowrapper.juno.collection;


import com.junowrapper.juno.JunoDBManager;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class JunoMap<K, V> implements Map<K, V>, Cloneable, Serializable {

    private static final long serialVersionUID = -5989264568613454654L;

    private final String junoKey;
    private static final String KEY_DELIMITER = ":";
    private final Set<K> junoSet; // using it to keep track of all entries associated with this specific map.
    private final long timeToLiveSec; // TTL for each entry/record.
    private final JunoDBManager junoDBManager;

    public JunoMap(String junoKey, JunoDBManager junoDBManager) {
        this(junoKey, TimeUnit.SECONDS, JunoDBManager.MAX_TTL_ALLOWED, junoDBManager);
    }


    public JunoMap(String junoKey, TimeUnit timeUnit, long timeToLive, JunoDBManager junoDBManager) {
        this.timeToLiveSec = Math.min(timeUnit.toSeconds(timeToLive), JunoDBManager.MAX_TTL_ALLOWED);
        this.junoSet = new JunoSet<>(junoKey, TimeUnit.SECONDS, timeToLiveSec, junoDBManager);
        this.junoDBManager = junoDBManager;
        this.junoKey = junoKey;
    }

    @Override
    public int size() {
        return junoSet.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        Objects.requireNonNull(key, "Null key not supported");
        return junoDBManager.get(encodeKey(key), Object.class, timeToLiveSec).isPresent();
    }

    @Override
    @Deprecated
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        Objects.requireNonNull(key, "Null key not supported");
        return (V) junoDBManager.get(encodeKey(key), Object.class).orElse(null);
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "Null key not supported");
        Objects.requireNonNull(value, "Null value not supported");

        byte[] keyBytes = junoDBManager.getCodec().serialize(encodeKey(key));
        byte[] valueBytes = junoDBManager.getCodec().serialize(value);
        JunoResponse set = junoDBManager.junoClient.set(keyBytes, valueBytes);
        if (set.getStatus() == OperationStatus.Success) {
            junoSet.add(key);
            return value;
        }

        return null;
    }

    @Override
    public V remove(Object key) {
        Objects.requireNonNull(key, "Null key not supported");
        V v = get(key);
        return v != null && junoDBManager.delete(encodeKey(key)) && junoSet.remove(key) ? v : null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
//        /** The batch has a client-side queue limit (08-Dec-2023), yet to be tested extensively... **/
//        List<JunoRequest> junoRequests = m.entrySet()
//                .stream()
//                .filter(entry -> Objects.nonNull(entry.getKey()))
//                .filter(entry -> Objects.nonNull(entry.getValue()))
//                .map((entry) -> JunoDBManager.setJunoRequest(encodeKey.apply(entry.getKey()), entry.getValue(), JunoRequest.OperationType.Set, timeToLiveSec))
//                .collect(Collectors.toList());
//
//        if (junoRequests.isEmpty()) {
//            return;
//        }
//
//        Iterable<JunoResponse> junoResponses = JunoDBManager.doBatch(junoRequests);
//        if (junoResponses == null) {
//            return;
//        }
//
//        List<K> addedKeys = new ArrayList<>();
//        for (JunoResponse junoResponse : junoResponses) {
//            if (junoResponse.getStatus() != OperationStatus.Success) {
//                continue;
//            }
//
//            String encodedKey = JunoDBManager.deserializeObject(junoResponse.getKey());
//            K k = decodeKey.apply(encodedKey);
//            if (k != null) {
//                addedKeys.add(k);
//            }
//        }
//
//        junoSet.addAll(addedKeys);
    }

    @Override
    public void clear() {
        /** Batch delete **/
//        List<JunoRequest> bulkRequest = junoSet.stream()
//                .map(k -> JunoDBManager.setJunoRequest(encodeKey.apply(k), JunoRequest.OperationType.Destroy))
//                .collect(Collectors.toList());
//
//        if (bulkRequest.isEmpty()) {
//            return;
//        }
//
//        Iterable<JunoResponse> junoResponses = JunoDBManager.doBatch(bulkRequest);
//        if (junoResponses == null) {
//            return;
//        }
//
//        // Assumed everything is deleted, if not max TTL is 3 days, so ¯\_(ツ)_/¯
//        junoSet.clear();
    }

    @Override
    public Set<K> keySet() {
        return junoSet;
    }

    @Override
    public Collection<V> values() {
        Set<String> encKeySet = junoSet.stream()
                .map(this::encodeKey)
                .collect(Collectors.toSet());
        List<Object> all = junoDBManager.getAll(encKeySet, Object.class, timeToLiveSec);
        System.out.println(all);
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> entrySet = new HashSet<>();
//        List<JunoRequest> junoRequests = keySet().stream()
//                .map(k -> JunoDBManager.setJunoRequest(encodeKey.apply(k), JunoRequest.OperationType.Get, timeToLiveSec))
//                .collect(Collectors.toList());
//
//        if (junoRequests.isEmpty()) {
//            return entrySet;
//        }
//
//        Iterable<JunoResponse> junoResponses = JunoDBManager.doBatch(junoRequests);
//        if (junoResponses == null) {
//            return entrySet;
//        }
//
//        for (JunoResponse junoResponse : junoResponses) {
//            if (junoResponse.getStatus() != OperationStatus.Success || junoResponse.getValue() == null || junoResponse.getValue().length == 0) {
//                continue;
//            }
//
//            String encodedKey = new String(junoResponse.getKey());
//            K k = decodeKey.apply(encodedKey);
//            V v = JunoDBManager.deserializeObject(junoResponse.getValue(), valueTypeReference);
//            if (v == null || k == null) {
//                continue;
//            }
//
//            entrySet.add(new AbstractMap.SimpleEntry<>(k, v));
//        }
        return entrySet;
    }

    @Override
    public JunoMap<K, V> clone() throws CloneNotSupportedException {
        try {
            return (JunoMap) super.clone();
        } catch (CloneNotSupportedException e) {
            throw e;
        }
    }

    private String encodeKey(Object key) {
        return junoKey + KEY_DELIMITER + key;
    }

    private K decodeKey(String encKey) {
        String substring = encKey.substring(encKey.lastIndexOf(KEY_DELIMITER) + 1);
        return (K) substring;
    }
}
