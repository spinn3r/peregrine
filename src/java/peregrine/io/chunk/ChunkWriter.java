/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.io.chunk;

import java.io.*;

import peregrine.*;

/**
 * Write key/values to chunks.
 */
public interface ChunkWriter extends Closeable, Flushable {

    /**
     * Write a key value pair.  This is the main method for IO to a chunk.
     */
    public void write( StructReader key, StructReader value ) throws IOException;

    /**
     * Return the length of bytes of this chunk.
     */
    public long length() throws IOException;

    /**
     * Shutdown any pending IO without blocking.
     */
    public void shutdown() throws IOException;

}
