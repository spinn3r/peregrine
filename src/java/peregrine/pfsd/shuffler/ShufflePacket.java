package peregrine.pfsd.shuffler;

public class ShufflePacket {

    public int partition;
    public int chunk;
    public byte[] data; 

    public ShufflePacket( int from_partition,
                          int from_chunk,
                          byte[] data ) {

        this.partition = from_partition;
        this.chunk = from_chunk;
        this.data = data;

    }

}