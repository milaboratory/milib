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
package com.milaboratory.core.mutations;

import com.milaboratory.test.TestUtil;
import org.junit.Test;

public class VariantsAssemblerParametersTest {
    @Test
    public void test1() throws Exception {
        VariantsAssemblerParameters params = new VariantsAssemblerParameters(10, 25,
                new AggregatedMutations.SimpleMutationsFilter(12, 0.8), 0.95f, 10.0f, 1.0f);
        TestUtil.assertJson(params);
    }
}