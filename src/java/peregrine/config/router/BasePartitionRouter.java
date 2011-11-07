package peregrine.config.router;

import peregrine.config.*;

public abstract class BasePartitionRouter implements PartitionRouter {

	protected int nr_partitions;
	
	@Override
    public void init( Config config ) {
    	this.nr_partitions = config.getMembership().size();    	    	
    }

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
		
}
