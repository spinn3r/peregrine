package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * 
 */
public class DefaultPartitionWriter implements PartitionWriter {

    protected String path;

    protected LocalPartitionWriter[] writers;

    protected Partition partition;
    
    private int count = 0;

    public DefaultPartitionWriter( Partition partition,
                                   String path ) throws IOException {
        this( partition, path, false );
    }
    
    public DefaultPartitionWriter( Partition partition,
                                   String path,
                                   boolean append ) throws IOException {

        this.path = path;
        this.partition = partition;

        Membership partitionMembership = Config.getPartitionMembership();

        List<Host> membership = partitionMembership.getHosts( partition );

        writers = new LocalPartitionWriter[ membership.size() ];

        for( int i = 0; i < writers.length; ++i ) {

            Host member = membership.get( i );

            LocalPartitionWriter writer =
                new LocalPartitionWriter( partition, member, path, append );
            
            writers[ i ] = writer;
            
        }

    }

    public void write( byte[] key, byte[] value )
        throws IOException {

        for( LocalPartitionWriter writer : writers ) {
            writer.write( key, value );
        }

        ++count;

    }

    public int count() throws IOException{
        return count;
    }

    public void close() throws IOException {

        List<IOException> failures = new ArrayList();
        
        for( LocalPartitionWriter writer : writers ) {

            try {
                
                writer.close();
                
            } catch ( IOException e ) {

                // FIXME: log this.
                
                failures.add( e );
            }

        }

        if ( failures.size() > 0 ) {
            throw new IOException( "On or more failures happend during close: " , failures.get( 0 ) );
        }
        
    }

    public String toString() {
        return path;
    }
    
}

