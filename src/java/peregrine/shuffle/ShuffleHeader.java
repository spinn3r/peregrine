package peregrine.shuffle;

import java.io.*;
import java.nio.*;
import java.util.*;

public class ShuffleHeader {

    public int partition;
    public int offset;
    public int nr_packets;
    public int count;
    public int length;

    public int getOffset() {
        return offset;
    }
    
    public String toString() {
        
        return String.format( "partition: %s, offset: %,d, nr_packets: %,d, count: %,d, length: %,d" ,
                              partition, offset, nr_packets, count, length );
        
    }
    
}
