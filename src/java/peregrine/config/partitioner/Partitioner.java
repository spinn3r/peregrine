package peregrine.config.partitioner;

import peregrine.config.*;

public interface Partitioner {

	/**
	 * Init the range router with the given config so that we can 
	 * @param config
	 */
	public void init( Config config );
	
	public Partition partition( byte[] key );
	
}
