package peregrine.util.netty;

import org.jboss.netty.buffer.*;

import peregrine.os.*;

import java.io.*;
import java.util.*;

/**
 * Class which resricts the usage of a ChannelBuffer to JUST relative sequential
 * read operations.  This will enable us to support prefetch and mlock, fadvise
 * on a buffer as well as CRC32 after a page has been mlocked and easy to read.
 *
 * This is primarily used by a DefaultChunkReader so that all operations that it
 * needs to function can be easily provided (AKA CRC32 and prefetch/mlock).
 */
public class StreamReader {

    private ChannelBuffer buff = null;

    private StreamReaderListener listener = null;

    protected MappedFile mappedFile = null;
    
    public StreamReader( MappedFile mappedFile ) throws IOException {
        this.mappedFile = mappedFile;
        this.buff = mappedFile.map();;
    }

    public void readBytes( byte[] data ) {
        fireOnRead( data.length );
        buff.readBytes( data );

    }

    public byte readByte() {
        fireOnRead(1);
        return buff.readByte();
    }

    public MappedFile getMappedFile() {
        return mappedFile;
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
