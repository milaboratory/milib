/*
 * Copyright 2020 MiLaboratory, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.milaboratory.util;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Objects;

public class Cache {
    private HashMap<Object, WeakReference<Object>> map;

    private void ensureInitialized() {
        if (map == null)
            map = new HashMap<>();
    }

    private void clean() {
        map.entrySet().removeIf(e -> e.getValue().get() == null);
    }

    public <T> T get(Object key0) {
        if (map == null)
            return null;
        WeakReference<Object> ref = map.get(key0);
        if (ref == null)
            return null;
        Object o = ref.get();
        if (o == null)
            clean();
        return (T) o;
    }

    public <T> T get(Object key0, Object key1) {
        return get(new Tuple2(key0, key1));
    }

    public <T> T get(Object key0, Object key1, Object key2) {
        return get(new Tuple3(key0, key1, key2));
    }

    public <T> T get(Object key0, Object key1, Object key2, Object key3) {
        return get(new Tuple4(key0, key1, key2, key3));
    }

    public <T> T store(Object key0, T value) {
        ensureInitialized();
        map.put(key0, new WeakReference<>(value));
        return value;
    }

    public <T> T store(Object key0, Object key1, T value) {
        return store(new Tuple2(key0, key1), value);
    }

    public <T> T store(Object key0, Object key1, Object key2, T value) {
        return store(new Tuple3(key0, key1, key2), value);
    }

    public <T> T store(Object key0, Object key1, Object key2, Object key3, T value) {
        return store(new Tuple4(key0, key1, key2, key3), value);
    }

    private final static class Tuple2 {
        final Object o1, o2;

        public Tuple2(Object o1, Object o2) {
            this.o1 = o1;
            this.o2 = o2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple2 tuple2 = (Tuple2) o;
            return Objects.equals(o1, tuple2.o1) &&
                    Objects.equals(o2, tuple2.o2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(o1, o2);
        }
    }

    private final static class Tuple3 {
        final Object o1, o2, o3;

        public Tuple3(Object o1, Object o2, Object o3) {
            this.o1 = o1;
            this.o2 = o2;
            this.o3 = o3;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple3 tuple3 = (Tuple3) o;
            return Objects.equals(o1, tuple3.o1) &&
                    Objects.equals(o2, tuple3.o2) &&
                    Objects.equals(o3, tuple3.o3);
        }

        @Override
        public int hashCode() {
            return Objects.hash(o1, o2, o3);
        }
    }

    private final static class Tuple4 {
        final Object o1, o2, o3, o4;

        public Tuple4(Object o1, Object o2, Object o3, Object o4) {
            this.o1 = o1;
            this.o2 = o2;
            this.o3 = o3;
            this.o4 = o4;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple4 tuple4 = (Tuple4) o;
            return Objects.equals(o1, tuple4.o1) &&
                    Objects.equals(o2, tuple4.o2) &&
                    Objects.equals(o3, tuple4.o3) &&
                    Objects.equals(o4, tuple4.o4);
        }

        @Override
        public int hashCode() {
            return Objects.hash(o1, o2, o3, o4);
        }
    }
}
