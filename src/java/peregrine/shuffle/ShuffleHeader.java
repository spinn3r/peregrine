package peregrine.shuffle;

public class ShuffleHeader {

    /**
     * The partition id for this header.
     */
    public int partition;

    /**
     * The position in this file where the data starts.
     */
    public int offset;

    /**
     * The number of packets for this partition in this shuffle file.
     */
    public int nr_packets;

    /**
     * The total number of key/value pairs.
     */
    public int count;

    /**
     * The length in bytes of this data.
     */
    public int length;

    public int getOffset() {
        return offset;
    }
    
    public String toString() {
        
        return String.format( "partition: %s, offset: %,d, nr_packets: %,d, count: %,d, length: %,d" ,
                              partition, offset, nr_packets, count, length );
        
    }
    
}
