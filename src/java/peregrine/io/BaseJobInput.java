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

import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

/**
 * Read data from a partition from local storage.
 */
public abstract class BaseJobInput {

    private List<ChunkStreamListener> listeners = new ArrayList();

    protected ChunkReference chunkRef = null;

    protected void fireOnChunk() {

        for( ChunkStreamListener listener : listeners ) {
            listener.onChunk( chunkRef );
        }
        
    }
    
    protected void fireOnChunkEnd() {

        if ( chunkRef != null && chunkRef.local >= 0 ) {

            for( ChunkStreamListener listener : listeners ) {
                listener.onChunkEnd( chunkRef );
            }

        }

    }

    public void addListener( ChunkStreamListener listener ) {
        listeners.add( listener );
    }

    public void addListeners( List<ChunkStreamListener> listeners ) {

        for( ChunkStreamListener listener : listeners ) {
            addListener( listener );
        }

    }

}
