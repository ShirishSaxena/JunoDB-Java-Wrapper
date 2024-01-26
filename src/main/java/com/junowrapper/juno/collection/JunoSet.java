package com.junowrapper.juno.collection;

import com.junowrapper.juno.JunoDBManager;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class JunoSet<E> extends AbstractSet<E> implements Set<E>, Serializable {

    private static final long serialVersionUID = -3568975462455467924L;
    private final String junoKey;
    private long timeToLiveSec = JunoDBManager.MAX_TTL_ALLOWED;

    private final JunoDBManager junoDBManager;

    public JunoSet(String junoKey, JunoDBManager junoDBManager) {
        this.junoKey = junoKey;
        this.junoDBManager = junoDBManager;
    }

    public JunoSet(String junoKey, TimeUnit timeUnit, long timeToLive, JunoDBManager junoDBManager) {
        this(junoKey, junoDBManager);
        this.timeToLiveSec = Math.min(timeUnit.toSeconds(timeToLive), timeToLiveSec);
    }


    @Override
    public int size() {
        return get().size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return get().contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return get().iterator();
    }

    @Override
    public Object[] toArray() {
        return get().toArray();
    }

    @Override
    public boolean add(E o) {
        Set<E> junoSet = get();
        return junoSet.add(o) && push(junoSet);
    }

    @Override
    public boolean remove(Object o) {
        Set<E> junoSet = get();
        return junoSet.remove(o) && push(junoSet);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        Set<E> junoSet = get();
        return junoSet.addAll(c) && push(junoSet);
    }

    @Override
    public void clear() {
        Set<E> junoSet = get();
        junoSet.clear();
        push(junoSet);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        Set<E> junoSet = get();
        return junoSet.removeAll(c) && push(junoSet);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Set<E> junoSet = get();
        return junoSet.retainAll(c) && push(junoSet);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        Set<E> junoSet = get();
        return junoSet.containsAll(c) && push(junoSet);
    }

    @Override
    public Object[] toArray(Object[] a) {
        return get().toArray(a);
    }

    public String getJunoKey() {
        return junoKey;
    }

    private boolean push(Set<E> localSet) {
        return junoDBManager.create(junoKey, localSet, timeToLiveSec);
    }

    private Set<E> get() {
        Optional<HashSet> jSetFromKey = junoDBManager.get(junoKey, HashSet.class, TimeUnit.SECONDS, timeToLiveSec);
        return jSetFromKey.orElse(new HashSet<>());
    }
}
