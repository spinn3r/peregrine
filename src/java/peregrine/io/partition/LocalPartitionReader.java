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
import peregrine.io.util.*;
import peregrine.os.*;
import peregrine.rpc.*;
import peregrine.task.*;

/**
 * Read data from a partition from local storage.
 */
public class LocalPartitionReader extends BaseJobInput implements SequenceReader, JobInput {

    private String path = null;

    private List<DefaultChunkReader> chunkReaders = new ArrayList();

    private Iterator<DefaultChunkReader> iterator = null;

    private DefaultChunkReader chunkReader = null;

    private boolean hasNext = false;

    private Partition partition;
    
    private ChunkReference chunkRef;
    
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

        addListener( listener );
        
    }
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 List<ChunkStreamListener> listeners ) throws IOException {

        this.partition = partition;
        this.chunkReaders = LocalPartition.getChunkReaders( config, partition, path );
        this.iterator = chunkReaders.iterator();
        this.path = path;

        this.chunkRef = new ChunkReference( partition, path );

        addListeners( listeners );
        
    }

    public List<DefaultChunkReader> getDefaultChunkReaders() {
        return chunkReaders;
    }
    
    @Override
    public boolean hasNext() throws IOException {

        if ( chunkReader != null )
            hasNext = chunkReader.hasNext();

        if ( hasNext == false ) {

            fireOnChunkEnd( chunkRef );
            
            if ( iterator.hasNext() ) {

                chunkRef.incr();
                
                //if ( chunkReader != null )
                    //chunkReader.close();
                
                chunkReader = iterator.next();

                fireOnChunk( chunkRef );
                
                hasNext = chunkReader.hasNext();

            } else {
                hasNext = false;
            }

        }

        return hasNext;
        
    }

    @Override
    public void next() throws IOException {
       	chunkReader.next();    	
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

        //TODO: migrate to using IdempotentCloser ...
        
        if ( chunkReader != null ) {
            fireOnChunkEnd( chunkRef );
        }

        new Closer( chunkReaders ).close();
        
    }

    @Override
    public String toString() {

        int offset = 0;

        if ( chunkReader != null )
            offset = chunkReader.index();

        Message message = new Message();

        message.put( "path",       path );
        message.put( "partition",  partition.getId() );
        message.put( "chunkRef",   chunkRef );
        message.put( "offset",     offset );

        return message.toString();

    }

}
