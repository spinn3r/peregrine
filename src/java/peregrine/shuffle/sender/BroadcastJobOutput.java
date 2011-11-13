package peregrine.shuffle.sender;

import peregrine.config.Config;
import peregrine.config.Membership;
import peregrine.config.Partition;

public class BroadcastJobOutput extends ShuffleJobOutput {

    public BroadcastJobOutput( Config config, Partition partition ) {
        this( config, "default", partition );
    }
        
    public BroadcastJobOutput( Config config, String name, Partition partition ) {

        super( config, name, partition );

    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Membership membership = config.getMembership();

        for ( Partition target : membership.getPartitions() ) {
            emit( target.getId() , key, value );
        }

    }

    @Override
    public String toString() {
        return String.format( "%s:%s@%s", getClass().getName(), name, Integer.toHexString(hashCode()) );
    }
    
}

