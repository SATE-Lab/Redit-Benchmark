/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.ByteAccessStrategy;
import com.hazelcast.internal.memory.MemoryAccessor;

/**
 * A {@link ByteAccessStrategy} that works with a {@link MemoryAccessor}.
 */
public final class CustomByteAccessStrategy implements ByteAccessStrategy<MemoryAccessor> {

    public static final CustomByteAccessStrategy INSTANCE = new CustomByteAccessStrategy();

    private CustomByteAccessStrategy() {
    }

    @Override
    public byte getByte(MemoryAccessor mem, long offset) {
        return mem.getByte(offset);
    }

    @Override
    public void putByte(MemoryAccessor mem, long offset, byte value) {
        mem.putByte(offset, value);
    }
}
