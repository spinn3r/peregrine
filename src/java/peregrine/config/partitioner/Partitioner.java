package peregrine.config.partitioner;

import peregrine.*;
import peregrine.config.*;

/**
 * Interface for handling partitioning keys.  Takes a given key and routes it to
 * the correct partition.
 */
public interface Partitioner {

	/**
	 * Init the range router with the given config so that we can
     * 
	 * @param config The config to read configuration data.
	 */
	public void init( Config config );

    /**
     * Route the given key to a given partition.
     */
	public Partition partition( StructReader key );
	
}
