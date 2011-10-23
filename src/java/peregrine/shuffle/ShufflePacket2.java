package peregrine.shuffle;

import org.jboss.netty.buffer.*;

public class ShufflePacket2 {

    public int from_partition;
    public int from_chunk;
    public int to_partition;

    /**
     * The byte offset of this file within its parent.
     */
    public int offset;
    public int count;
    public ChannelBuffer data; 

    public ShufflePacket2( int from_partition,
                           int from_chunk,
                           int to_partition,
                           int offset,
                           int count,
                           ChannelBuffer data ) {
        
        this.from_partition = from_partition;
        this.from_chunk = from_chunk;
        this.to_partition = to_partition;
        this.offset = offset;
        this.count = count;
        this.data = data;

    }

    public String toString() {

        return String.format( "from_partition: %s, from_chunk: %s, to_partition: %s, offset; %,d, count: %,d, length: %,d bytes",
                              from_partition,
                              from_chunk,
                              to_partition,
                              offset,
                              count,
                              data.capacity() );

    }
    
}