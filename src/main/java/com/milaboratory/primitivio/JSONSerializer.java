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
package com.milaboratory.primitivio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.milaboratory.util.GlobalObjectMappers;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public final class JSONSerializer implements Serializer {
    final Class<?> type;
    final Function<String, String> preprocessor;

    /**
     * Constructor for backward-compatibility mocking serializers
     *
     * @param type         target type
     * @param preprocessor JSON string content preprocessor
     */
    public JSONSerializer(Class<?> type, Function<String, String> preprocessor) {
        this.type = Objects.requireNonNull(type);
        this.preprocessor = preprocessor;
    }

    /**
     * Normal constructor, not intended for direct use,
     * use com.milaboratory.primitivio.annotations.Serializable#asJson() instead.
     *
     * @param type target type
     */
    public JSONSerializer(Class<?> type) {
        this(type, null);
    }

    @Override
    public void write(PrimitivO output, Object object) {
        try {
            if (preprocessor != null)
                throw new IllegalStateException("");
            output.writeUTF(GlobalObjectMappers.ONE_LINE.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object read(PrimitivI input) {
        String str = input.readUTF();
        try {
            return GlobalObjectMappers.ONE_LINE.readValue(
                    preprocessor != null
                            ? preprocessor.apply(str)
                            : str,
                    type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isReference() {
        return false;
    }

    @Override
    public boolean handlesReference() {
        return false;
    }
}
