/*
 * Copyright 2018 MiLaboratory.com
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
package com.milaboratory.cli;

import java.util.Objects;

public class BinaryFileInfo {
    /**
     * File type, or null if unrecognized format.
     */
    public final String fileType;

    /**
     * Full magic bytes string.
     */
    public final String fullMagic;

    /**
     * True if file integrity check succeeded. The command generated the file was not prematurely interrupted.
     */
    public final boolean valid;

    public BinaryFileInfo(String fileType, String fullMagic, boolean valid) {
        this.fileType = fileType;
        this.fullMagic = fullMagic;
        this.valid = valid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;
        BinaryFileInfo that = (BinaryFileInfo)o;
        if (valid != that.valid) return false;
        if ((fileType != null) ? !fileType.equals(that.fileType) : (that.fileType != null)) return false;
        return (fullMagic != null) ? fullMagic.equals(that.fullMagic) : (that.fullMagic == null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileType, fullMagic, valid);
    }
}
