package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class PartitionWriter {

    protected String path;

    protected LocalPartitionWriter[] writers;

    protected Partition partition;
    
    private int count = 0;

    public PartitionWriter( Partition partition, String path ) throws IOException {

        this.path = path;
        this.partition = partition;

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

        ++count;

    }

    public int count() {
        return count;
    }

    public void close() throws IOException {

        for( LocalPartitionWriter writer : writers ) {
            writer.close();
        }

        //write out the stat file.

        if ( ! path.endsWith( "/stat" ) ) {
            new StatWriter( this ).write();
        }

    }

    public String toString() {
        return path;
    }
    
}

