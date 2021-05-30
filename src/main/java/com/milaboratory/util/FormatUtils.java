/*
 * Copyright 2016 MiLaboratory.com
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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

public class FormatUtils {
    private static final DecimalFormatSymbols DFS;

    public static final DecimalFormat PERCENT_FORMAT;

    static {
        DFS = new DecimalFormatSymbols();
        DFS.setNaN("NaN");
        DFS.setInfinity("Inf");
        PERCENT_FORMAT = new DecimalFormat("#.##", DFS);
    }

    public static final DecimalFormat TIME_FORMAT = PERCENT_FORMAT;

    public static final DecimalFormat SIZE_FORMAT = PERCENT_FORMAT;

    public static final long NANOSECONDS_IN_SECOND = 1_000_000_000;

    public static final long MILLISECONDS_IN_SECOND = 1_000;

    public static final long NANOSECONDS_IN_MILLISECOND = 1_000_000;

    public static String percent(double numerator, double denominator) {
        return PERCENT_FORMAT.format((denominator == 0) ? 0 : 100.0 * numerator / denominator) + "%";
    }

    public static String nanoTimeToString(long t) {
        double v = t;
        if ((t /= 1000) == 0)
            return "" + TIME_FORMAT.format(v) + "ns";

        v /= 1000;
        if ((t /= 1000) == 0)
            return "" + TIME_FORMAT.format(v) + "us";

        v /= 1000;
        if ((t /= 1000) == 0)
            return "" + TIME_FORMAT.format(v) + "ms";

        v /= 1000;
        if ((t /= 60) == 0)
            return "" + TIME_FORMAT.format(v) + "s";

        v /= 60;
        return "" + TIME_FORMAT.format(v) + "m";
    }

    public static String bytesToStringDiv(long s, long den) {
        if (den == 0) return "NaN";
        else return bytesToString(s / den);
    }

    public static String bytesToString(long s) {
        double v = s;

        if ((s /= 1024) == 0)
            return "" + TIME_FORMAT.format(v) + "B";

        v /= 1024;
        if ((s /= 1024) == 0)
            return "" + TIME_FORMAT.format(v) + "KiB";

        v /= 1024;
        if ((s /= 1024) == 0)
            return "" + TIME_FORMAT.format(v) + "MiB";

        v /= 1024;
        if ((s /= 1024) == 0)
            return "" + TIME_FORMAT.format(v) + "GiB";

        v /= 1024;
        return "" + TIME_FORMAT.format(v) + "TiB";
    }
}
