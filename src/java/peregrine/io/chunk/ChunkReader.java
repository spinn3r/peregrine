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
 * Interface for reading data from a chunk in key/value form.
 */
public interface ChunkReader extends Closeable {

    /**
     * Return true if there is a next item.  Initially the reader is positioned
     * on the first item so that hasNext() returns true and you can then call
     * key() and value()
     */
    public boolean hasNext() throws IOException;

    /**
     * Read the key from the current entry.
     *
     * Both key() and value() must be called before moving to the next item.
     */
    public StructReader key() throws IOException;

    /**
     * Read the value from the current entry.
     *
     * Both key() and value() must be called before moving to the next item.
     */
    public StructReader value() throws IOException;

    /**
     * Close the ChunkReader.
     */
    @Override
    public void close() throws IOException;
    
}
