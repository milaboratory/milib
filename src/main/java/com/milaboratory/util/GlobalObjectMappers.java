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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Created by dbolotin on 24.01.14.
 */
public class GlobalObjectMappers {
    public static final ObjectMapper ONE_LINE = new ObjectMapper();
    public static final ObjectMapper PRETTY = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static String toOneLine(Object object) throws JsonProcessingException {
        String str = GlobalObjectMappers.ONE_LINE.writeValueAsString(object);

        if (str.contains("\n"))
            throw new RuntimeException("Internal error.");

        return str;
    }
}
