package peregrine.config.router;

import peregrine.config.*;
import peregrine.util.primitive.*;

public class HashPartitionRouter extends BasePartitionRouter {
	
	@Override
	public Partition route( byte[] key ) {
        
        // TODO: we only need a FEW bytes to route a key , not the WHOLE thing
        // if it is a hashcode.  For example... we can route to 255 partitions
        // with just one byte... that IS if it is a hashode.  with just TWO
        // bytes we can route to 65536 partitions which is probably fine for all
        // users for a LONG time.

        long value = Math.abs( LongBytes.toLong( key ) );
        int partition = (int)(value % nr_partitions);

        return new Partition( partition );		

	}	

}
