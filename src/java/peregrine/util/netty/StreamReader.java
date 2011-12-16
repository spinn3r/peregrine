package peregrine.util.netty;

import org.jboss.netty.buffer.*;

import peregrine.os.*;
import peregrine.values.*;

import java.io.*;
import java.util.*;

/**
 * <p>
 * Class which restricts the usage of a ChannelBuffer to JUST relative sequential
 * read operations.  This will enable us to support prefetch and mlock, fadvise
 * on a buffer as well as CRC32 after a page has been mlocked and easy to read.
 *
 * <p>
 * This is primarily used by a DefaultChunkReader so that all operations that it
 * needs to function can be easily provided (AKA CRC32 and prefetch/mlock).
 */
public class StreamReader {

    private ChannelBuffer buff = null;

    private StreamReaderListener listener = null;
    
    public StreamReader( ChannelBuffer buff ) {
        this.buff = buff;
    }

    public StructReader read( int length ) {
        fireOnRead( length );
        return new StructReader( buff.readSlice( length ) );
    }

    public byte read() {
        fireOnRead(1);
        return buff.readByte();
    }
    
    public StreamReaderListener getListener() { 
        return this.listener;
    }

    public void setListener( StreamReaderListener listener ) { 
        this.listener = listener;
    }

    /** 
     * Event signifying that we are about to read `length' additional bytes from
     * the stream.
     */
    private void fireOnRead( int length ) {

        if ( listener == null )
            return;

        listener.onRead( length );
        
    }
    
}
