package peregrine.config;

public interface PartitionRouter {

	public Partition route( byte[] key );
	
}
