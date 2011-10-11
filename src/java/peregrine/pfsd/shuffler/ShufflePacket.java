package peregrine.pfsd.shuffler;

public class ShufflePacket {

    public int from_partition;
    public int from_chunk;
    public int to_partition;
    public byte[] data; 

    public ShufflePacket( int from_partition,
                          int from_chunk,
                          int to_partition,
                          byte[] data ) {

        this.from_partition = from_partition;
        this.from_chunk = from_chunk;
        this.to_partition = to_partition;
        this.data = data;

    }

}