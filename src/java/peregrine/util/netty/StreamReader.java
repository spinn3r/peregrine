package peregrine.util.netty;

import org.jboss.netty.buffer.*;

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
    
    public StreamReader( ChannelBuffer buff ) {
        this.buff = buff;
    }

    public void readBytes( byte[] data ) {
        buff.readBytes( data );
    }

    public byte readByte() {
        return buff.readByte();
    }
    
}
