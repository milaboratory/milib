/*
 * Copyright 2015 MiLaboratory.com
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

public final class ArraysUtils {
    private ArraysUtils() {
    }

    public static void reverse(int[] array, int from, int to) {
        int i, v;
        int length = to - from;
        for (i = 0; i < (length + 1) / 2; ++i) {
            v = array[from + i];
            array[from + i] = array[from + length - i - 1];
            array[from + length - i - 1] = v;
        }
    }

    public static void reverse(int[] array) {
        int i, v;
        for (i = 0; i < (array.length + 1) / 2; ++i) {
            v = array[i];
            array[i] = array[array.length - i - 1];
            array[array.length - i - 1] = v;
        }
    }

    public static void reverse(byte[] array) {
        int i;
        byte v;
        for (i = 0; i < (array.length + 1) / 2; ++i) {
            v = array[i];
            array[i] = array[array.length - i - 1];
            array[array.length - i - 1] = v;
        }
    }
}
