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
package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.config.partitioner.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.shuffle.*;

import com.spinn3r.log5j.Logger;

public class ShuffleJobOutputDirect extends ShuffleJobOutputBase implements Closeable, Flushable {

    private static final Logger log = Logger.getLogger();
    
    private ShuffleJobOutput parent;

    private ShuffleSender sender = null;

    private Partitioner partitioner = null;

    private boolean closed = true;

    private ShuffleJobOutputDirect() {}
    
    public ShuffleJobOutputDirect( ShuffleJobOutput parent ) {
        this.parent = parent;
        this.partitioner = parent.job.getPartitionerInstance();

        // create a basic sender.  This is used for global sort shuffling.  In
        // normal shuffling this wouldn't be used.

        ChunkReference chunkRef = new ChunkReference( parent.getPartition(), 0 );
        this.sender = new ShuffleSender( parent.config, parent.name, chunkRef );
        
    }
    
    @Override
    public void emit( StructReader key , StructReader value ) {
            
        Partition target = partitioner.partition( key, value );

        emit( target.getId(), key, value );
                    
    }

    @Override
    public void emit( int targetPartition, StructReader key , StructReader value ) {

        try {

            //assertOpen();

            sender.emit( targetPartition, key, value );
            
        } catch ( ShuffleFailedException e ) {
            // this should cause the job to (correctly) fail
            throw new RuntimeException( e );
        }

    }

    private void rollover( ChunkReference chunkRef ) {
        closeWithUncheckedException();
        sender = new ShuffleSender( parent.config, parent.name, chunkRef );
    }
    
    @Override 
    public void onChunk( ChunkReference chunkRef ) {
        rollover( chunkRef );
    }

    @Override 
    public void onChunkEnd( ChunkReference chunkRef ) {
        rollover( chunkRef );
    }

    @Override 
    public void flush() throws IOException {

        if ( sender != null ) {
            sender.flush();
            length += sender.length();
        }

    }

    @Override 
    public void close() throws IOException {
        
        if ( sender != null ) {
            sender.close();
            length += sender.length();
            sender = null;
        }

        closed = true;
        
    }

    private void assertOpen() throws RuntimeException {

        if ( closed )
            throw new RuntimeException( "closed" );
        
    }
    
    private void closeWithUncheckedException() {

        try {
            close();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

    }
    
    @Override
    public String toString() {
        return String.format( "%s:%s", getClass().getSimpleName(), parent.name );
    }

}

