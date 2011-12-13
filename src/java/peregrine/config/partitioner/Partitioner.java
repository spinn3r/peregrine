package peregrine.config.partitioner;

import peregrine.config.*;

/**
 * Interface for handling partitioning keys.  Takes a given key and routes it to
 * the correct partition.
 */
public interface Partitioner {

	/**
	 * Init the range router with the given config so that we can 
	 * @param config
	 */
	public void init( Config config );
	
	public Partition partition( byte[] key );
	
}
