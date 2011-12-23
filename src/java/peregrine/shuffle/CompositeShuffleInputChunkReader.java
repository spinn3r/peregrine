package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.io.util.*;
import peregrine.util.*;
import peregrine.config.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * An interface to work with mulitple ShuffleInputChunkReaders when doing chunk
 * sorting.
 */
public class CompositeShuffleInputChunkReader implements Closeable {

	private Config config;
	private Partition partition;
	
	private List<ShuffleInputChunkReader> readers = new ArrayList();
    
	private List<ChannelBuffer> buffers = new ArrayList();
	
    private Iterator<ShuffleInputChunkReader> readerIterator;

    private Iterator<ChannelBuffer> bufferIterator;
    
    /**
     * the current reader.
     */
    private ShuffleInputChunkReader reader;

    /**
     * The current channel buffer we're working with.
     */
    private ChannelBuffer buffer;

    /**
     * The buffer index we're on.
     */
    private int index = -1;
    
    /**
     * The sum of items in all delegate readers.
     */
    private int size = 0;
    
	public CompositeShuffleInputChunkReader( Config config, 
                                             Partition partition,
                                             List<ShuffleInputChunkReader> readers ) throws IOException {

		this.config = config;
		this.partition = partition;
		this.readers = readers;
		
        for ( ShuffleInputChunkReader delegate : readers ) {

        	size += delegate.size();
            
            // we need our OWN copy of this buffer so that other threads don't
            // update the readerIndex and writerIndex
            ChannelBuffer buffer = delegate.getBuffer();
            buffer = buffer.slice( 0, buffer.writerIndex() );
            
            buffers.add( buffer );
            
        }

        readerIterator = readers.iterator();
        bufferIterator = buffers.iterator();

        nextReader();
        
	}	

    public boolean hasNext() throws IOException {

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

        ++index;
        
        reader = readerIterator.next();

        buffer = bufferIterator.next();

    }

    public int index() {
        return index;
    }
    
    /**
     * Get the buffer of the current ShuffleInputChunkReader 
     */
    public ChannelBuffer getBuffer() {
        return buffer;
    }
    
    public List<ChannelBuffer> getBuffers() {
        return buffers;
    }
   
    public ShuffleInputChunkReader getShuffleInputChunkReader() {
        return reader;
    }
    
    @Override
    public void close() throws IOException {

        new Closer( readers ).close();
        
    }
    
    public int size() {
    	return size;
    }
    
}
