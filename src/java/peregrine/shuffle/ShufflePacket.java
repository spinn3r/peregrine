package peregrine.shuffle;

public class ShufflePacket {

    public int from_partition;
    public int from_chunk;
    public int to_partition;
    public int count;
    public byte[] data; 

    public ShufflePacket( int from_partition,
                          int from_chunk,
                          int to_partition,
                          int count,
                          byte[] data ) {

        this.from_partition = from_partition;
        this.from_chunk = from_chunk;
        this.to_partition = to_partition;
        this.count = count;
        this.data = data;

    }

    public String toString() {

        return String.format( "from_partition: %s, from_chunk: %s, to_partition: %s, count: %,d, length: %,d bytes",
                              from_partition,
                              from_chunk,
                              to_partition,
                              count,
                              data.length );

    }
    
}