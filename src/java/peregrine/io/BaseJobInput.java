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
public abstract class BaseJobInput implements JobInput {

    private List<ChunkStreamListener> listeners = new ArrayList();

    protected void fireOnChunk( ChunkReference chunkRef ) {

        for( ChunkStreamListener listener : listeners ) {
            listener.onChunk( chunkRef );
        }
        
    }
    
    protected void fireOnChunkEnd( ChunkReference chunkRef ) {

        if ( chunkRef != null && chunkRef.local >= 0 ) {

            for( ChunkStreamListener listener : listeners ) {
                listener.onChunkEnd( chunkRef );
            }

        }

    }

    @Override
    public void addListener( ChunkStreamListener listener ) {
        listeners.add( listener );
    }

    @Override
    public void addListeners( List<ChunkStreamListener> listeners ) {

        for( ChunkStreamListener listener : listeners ) {
            addListener( listener );
        }

    }

    @Override
    public int count() {
        throw new RuntimeException( "not implemented" );
    }
}
