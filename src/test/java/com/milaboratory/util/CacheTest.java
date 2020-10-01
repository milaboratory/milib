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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class CacheTest {
    @Test
    public void test1() {
        Cache cache = new Cache();
        for (int i = 0; i < 100; i++) {
            assertNull(cache.get(i));
            assertNull(cache.get(i, i));
            assertNull(cache.get(i, i, i));
            assertNull(cache.get(i, i, i, i));

            cache.store(i, i);
            cache.store(i, i, i);
            cache.store(i, i, i, i);
            cache.store(i, i, i, i, i);

            cache.store(-i, null);
            cache.store(-i, -i, null);
            cache.store(-i, -i, -i, null);
            cache.store(-i, -i, -i, -i, null);
        }

        int good = 0;
        for (int i = 0; i < 100; i++) {
            if (cache.<Integer>get(i) != null)
                ++good;
            if (cache.<Integer>get(i, i) != null)
                ++good;
            if (cache.<Integer>get(i, i, i) != null)
                ++good;
            if (cache.<Integer>get(i, i, i, i) != null)
                ++good;

            if (cache.<Integer>computeIfAbsent(-i, () -> 1) == null)
                ++good;
            if (cache.<Integer>computeIfAbsent(-i, -i, () -> 1) == null)
                ++good;
            if (cache.<Integer>computeIfAbsent(-i, -i, -i, () -> 1) == null)
                ++good;
            if (cache.<Integer>computeIfAbsent(-i, -i, -i, -i, () -> 1) == null)
                ++good;
        }

        Assert.assertTrue(good > 700);
        System.out.println("Cache misses:" + Cache.totalCacheMisses());
        System.out.println("Cache hits:" + Cache.totalCacheHits());
    }
}