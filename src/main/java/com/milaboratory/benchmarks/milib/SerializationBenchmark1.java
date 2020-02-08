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
package com.milaboratory.benchmarks.milib;

import picocli.CommandLine;

import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command()
public final class SerializationBenchmark1 implements Callable<Integer> {
    @Option(names = "-x")
    int x;

    @Override
    public Integer call() {
        System.out.printf("hi from foo, x=%d%n", x);
        boolean ok = true;
        return ok ? 0 : 1;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new SerializationBenchmark1()).execute(args);
        System.exit(exitCode);
    }
}
