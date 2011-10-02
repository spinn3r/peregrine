package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * On every chunk write in partition writer, broadcast the block ID, nr of
 * blocks written, and nr of entries.  This will require about 10 bytes of
 * storage per partition per chunk (which really isn't much).
 *
 * On a 1TB file with 100MB chunks this will require an additional 12k of
 * storage which is totally reasonable.  It can ALSO be reduced in the future if
 * we want.
 */
class StatWriter {

    private PartitionWriter parent;
    
    public StatWriter( PartitionWriter parent ) {
        this.parent = parent;
    }

    public void write() throws IOException {

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        String stat_path = parent.path + "/stat";
        
        for( Partition target : partitionMembership.keySet() ) {

            PartitionWriter writer = new PartitionWriter( target, stat_path );

            IntKey key = new IntKey( parent.partition.getId() );
            IntValue value = new IntValue( parent.count() );

            writer.write( key.toBytes(), value.toBytes() );
            writer.close();
            
        }

    }
    
}

