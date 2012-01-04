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
package peregrine.io.partition;

import java.io.*;

import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

/**
 * Read data from a partition from local storage.
 */
public class LocalPartitionReader implements ChunkReader, JobInput {

    private String path = null;

    private List<DefaultChunkReader> chunkReaders = new ArrayList();

    private Iterator<DefaultChunkReader> iterator = null;

    private DefaultChunkReader chunkReader = null;

    private List<ChunkStreamListener> listeners = new ArrayList();

    private ChunkReference chunkRef = null;

    private boolean hasNext = false;

    private Partition partition;
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path ) throws IOException {
        
        this( config, partition, path, new ArrayList() );
        
    }

    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 ChunkStreamListener listener ) throws IOException {

        this( config, partition, path, new ArrayList() );

        listeners.add( listener );
        
    }
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 List<ChunkStreamListener> listeners ) throws IOException {

        this.partition = partition;
        this.chunkReaders = LocalPartition.getChunkReaders( config, partition, path );
        this.iterator = chunkReaders.iterator();
        this.listeners = listeners;
        this.path = path;

        this.chunkRef = new ChunkReference( partition );

    }

    public List<DefaultChunkReader> getDefaultChunkReaders() {
        return chunkReaders;
    }
    
    @Override
    public boolean hasNext() throws IOException {

        if ( chunkReader != null )
            hasNext = chunkReader.hasNext();

        if ( hasNext == false ) {

            fireOnChunkEnd();
            
            if ( iterator.hasNext() ) {

                chunkRef.incr();

                if ( chunkReader != null )
                    chunkReader.close();
                
                chunkReader = iterator.next();

                fireOnChunk();
                
                hasNext = chunkReader.hasNext();

            } else {
                hasNext = false;
            }

        }

        return hasNext;
        
    }

    private void fireOnChunk() {

        for( ChunkStreamListener listener : listeners ) {
            listener.onChunk( chunkRef );
        }
        
    }
    
    private void fireOnChunkEnd() {

        if ( chunkRef != null && chunkRef.local >= 0 ) {

            for( ChunkStreamListener listener : listeners ) {
                listener.onChunkEnd( chunkRef );
            }

        }

    }

    @Override
    public StructReader key() throws IOException {
        return chunkReader.key();
    }

    @Override
    public StructReader value() throws IOException {
        return chunkReader.value();
    }

    @Override
    public void close() throws IOException {

        if ( chunkReader != null ) {
            chunkReader.close();
            fireOnChunkEnd();
        }

    }

    @Override
    public String toString() {
        return String.format( "%s (%s):%s", path, partition, chunkReaders );
    }

    public void addListener( ChunkStreamListener listener ) {
        listeners.add( listener );
    }

}
