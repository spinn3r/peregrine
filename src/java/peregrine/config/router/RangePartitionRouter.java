package peregrine.config.router;

import peregrine.config.*;

public class RangePartitionRouter  extends BasePartitionRouter {

	private int range;
	
	@Override
    public void init( Config config ) {
		super.init( config );
    	this.range = 255 / nr_partitions;	
    }
	
	@Override
	public Partition route( byte[] key ) {
		
		// the domain of the route function...  basically the key space as an
		// integer so that we can place partitions within that space.
		int domain = (int)key[0] & 0xFF;

		// right now we only support 2^16 or 64k partitions.  I don't think we 
		// will hit this limit any time soon.  This might be famous last words.
		if ( nr_partitions > 255 ) {
			domain = domain << 8;
			domain = domain & ( key[1] & 0xFF );
		}
						
		int part = domain / range;
			
		return new Partition( part );
		
	}

}
