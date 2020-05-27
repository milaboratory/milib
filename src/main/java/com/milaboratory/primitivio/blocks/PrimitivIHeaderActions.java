/*
 * Copyright 2019 MiLaboratory, LLC
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
package com.milaboratory.primitivio.blocks;

import java.util.function.Function;

public final class PrimitivIHeaderActions {
    private static final PrimitivIHeaderAction EOF = new PrimitivIHeaderAction() {};
    private static final PrimitivIHeaderAction Skip = new PrimitivIHeaderAction() {};
    private static final PrimitivIHeaderAction Error = new PrimitivIHeaderAction() {};

    private static final class OutputObject<O> implements PrimitivIHeaderAction<O> {
        final O obj;

        public OutputObject(O obj) {
            this.obj = obj;
        }
    }

    public static <O> PrimitivIHeaderAction<O> stopReading() {
        return EOF;
    }

    public static boolean isStopReading(PrimitivIHeaderAction action) {
        return EOF == action;
    }

    public static <O> PrimitivIHeaderAction<O> skip() {
        return Skip;
    }

    public static boolean isSkip(PrimitivIHeaderAction action) {
        return Skip == action;
    }

    public static <O> PrimitivIHeaderAction<O> error() {
        return Error;
    }

    public static boolean isError(PrimitivIHeaderAction action) {
        return Error == action;
    }

    public static <O> PrimitivIHeaderAction<O> outputObject(O obj) {
        return new OutputObject<>(obj);
    }

    public static <O> O tryExtractOutputObject(PrimitivIHeaderAction<O> action) {
        return (action instanceof OutputObject) ? ((OutputObject<O>) action).obj : null;
    }

    public static <O> Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> skipAll() {
        return header -> Skip;
    }

    public static <O> Function<PrimitivIOBlockHeader, PrimitivIHeaderAction<O>> stopReadingOnAny() {
        return header -> EOF;
    }
}
