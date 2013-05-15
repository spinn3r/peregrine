/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.io.chunk.*;
import peregrine.io.util.*;
import peregrine.util.*;
import peregrine.config.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * An interface to work with mulitple ShuffleInputChunkReaders when doing chunk
 * sorting.
 */
public class CompositeChunkReader implements Closeable {

	private Config config;
	
	private List<ChunkReader> readers = new ArrayList();
    
	private List<ChannelBuffer> buffers = new ArrayList();
	
    private Iterator<ChunkReader> readerIterator;

    private Iterator<ChannelBuffer> bufferIterator;
    
    /**
     * the current reader.
     */
    private ChunkReader reader;

    /**
     * The current channel buffer we're working with.
     */
    private ChannelBuffer buffer;

    /**
     * The buffer index we're on.
     */
    private int bufferIndex = -1;
    
    /**
     * The sum of items in all delegate readers.
     */
    private int count = 0;
    
    public CompositeChunkReader( Config config, final ChunkReader reader ) throws IOException {
    	this( config, new ArrayList() {{ add( reader ); }} );
    	
    }
    
	public CompositeChunkReader( Config config, 
                                 List<ChunkReader> readers ) throws IOException {

		this.config = config;
		this.readers = readers;
		
        for ( ChunkReader delegate : readers ) {

        	count += delegate.count();
            
            // we need our OWN copy of this buffer so that other threads don't
            // update the readerIndex and writerIndex
            ChannelBuffer buffer = delegate.getBuffer();
            buffer = buffer.slice( 0, buffer.writerIndex() );
            
            buffers.add( buffer );
            
        }

        readerIterator = readers.iterator();
        bufferIterator = buffers.iterator();

        if ( readerIterator.hasNext() )
            nextReader();
        
	}	

    public boolean hasNext() throws IOException {

        if ( reader == null )
            return false;
        
        boolean result = reader.hasNext();
        
        while ( result == false && readerIterator.hasNext() ) {
            nextReader();
            result = reader.hasNext();
        }

        return result;
        
    }

    public void next() throws IOException {
        reader.next();
    }

    private void nextReader() {

        ++bufferIndex;
        
        reader = readerIterator.next();

        buffer = bufferIterator.next();

    }

    public int bufferIndex() {
        return bufferIndex;
    }
    
    /**
     * Get the buffer of the current ChunkReader 
     */
    public ChannelBuffer getBuffer() {
        return buffer;
    }
    
    public List<ChannelBuffer> getBuffers() {
        return buffers;
    }
   
    public ChunkReader getChunkReader() {
        return reader;
    }
    
    @Override
    public void close() throws IOException {
        new Closer( readers ).close();
    }
    
    public int count() {
    	return count;
    }
    
}
