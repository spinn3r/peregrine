package peregrine.config.router;

import peregrine.config.*;

public interface PartitionRouter {

	/**
	 * Init the range router with the given config so that we can 
	 * @param config
	 */
	public void init( Config config );
	
	public Partition route( byte[] key );
	
}
