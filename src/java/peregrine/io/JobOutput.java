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
package peregrine.io;

import java.io.*;

import peregrine.*;

/**
 * Represents the output stream from a Mapper / Merger job which can write to an
 * emit stream of key/value pairs.
 */
public interface JobOutput extends Closeable, Flushable {

    /**
     * Emit a key / value pair to this job output.
     */
    public void emit( StructReader key , StructReader value );

    /**
     * Close this job output.  Mappers/reducers should NOT call this method but
     * instead leave it up to the task to close any output.
     * 
     */
    @Override
    public void close() throws IOException;

    @Override
    public void flush() throws IOException;
    
}
