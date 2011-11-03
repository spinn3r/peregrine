package peregrine.shuffle.sender;

import org.jboss.netty.buffer.*;

import peregrine.util.*;
import peregrine.util.primitive.IntBytes;
import peregrine.io.chunk.*;

public class ShuffleSenderExtent {

    public static final int HEADER_SIZE = 2 * IntBytes.LENGTH;
    
    protected ChannelBuffer buff = null;

    protected int emits = 0;

    public ShuffleSenderExtent( int extent_size ) {
        // TODO: consider caching these and just resetting the readerIndex each time.
        this.buff = ChannelBuffers.directBuffer( extent_size );
    }
    
    public void emit( int to_partition, int length, byte[] key, byte[] value ) {

        buff.writeInt( to_partition );
        buff.writeInt( length );
        DefaultChunkWriter.write( buff, key, value );

        ++emits;
        
    }

    public int writerIndex() {
        return buff.writerIndex();
    }
    
}
