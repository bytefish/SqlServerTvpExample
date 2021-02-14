package de.bytefish.sqlservertvpexample.utils;

import java.util.Objects;

public class Tuple3<T1, T2, T3> {
    private final T1 t1;
    private final T2 t2;
    private final T3 t3;

    public Tuple3(T1 t1, T2 t2, T3 t3) {
        this.t1 = t1;
        this.t2 = t2;
        this.t3 = t3;
    }

    public T1 getT1() {
        return t1;
    }

    public T2 getT2() {
        return t2;
    }

    public T3 getT3() {
        return t3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;
        return Objects.equals(t1, tuple3.t1) && Objects.equals(t2, tuple3.t2) && Objects.equals(t3, tuple3.t3);
    }

    @Override
    public int hashCode() {
        return Objects.hash(t1, t2, t3);
    }
}
