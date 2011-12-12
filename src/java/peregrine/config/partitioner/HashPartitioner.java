package peregrine.config.partitioner;

import peregrine.config.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

public class HashPartitioner extends BasePartitioner {
	
	@Override
	public Partition partition( byte[] key ) {
        
        // we only need a FEW bytes to route a key , not the WHOLE thing if it
        // is a hashcode.  For example... we can route to 255 partitions with
        // just one byte... that IS if it is a hashode.  with just TWO bytes we
        // can route to 65536 partitions which is probably fine for all users
        // for a LONG time.

        long value =
            (long)((key[7] & 0xFF)     ) +
            (long)((key[6] & 0xFF) << 8)
            ;

        value = Math.abs( value );
        
        int partition = (int)value % nr_partitions;
        
        return new Partition( partition );		

	}	

}
