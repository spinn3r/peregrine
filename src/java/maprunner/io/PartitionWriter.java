package maprunner.io;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class PartitionWriter {

    private String path = null;

    private LocalPartitionWriter[] writers;

    public PartitionWriter( Partition partition,
                            String path ) throws IOException {

        this.path = path;

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        List<Host> membership = partitionMembership.get( partition );

        writers = new LocalPartitionWriter[ membership.size() ];

        for( int i = 0; i < writers.length; ++i ) {

            Host member = membership.get( i );

            writers[ i ] = new LocalPartitionWriter( Config.getDFSPath( partition, member, path ) );
        }

    }

    public void write( byte[] key, byte[] value )
        throws IOException {

        for( LocalPartitionWriter writer : writers ) {
            writer.write( key, value );
        }
        
    }

    public void close() throws IOException {

        for( LocalPartitionWriter writer : writers ) {
            writer.close();
        }

    }

    public String toString() {
        return path;
    }
    
}

