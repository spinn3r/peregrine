
package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.pfsd.*;

public class Host {

    private static int sequence = 0;

    protected long id = 0;
    protected String name = null;

    protected int partitionMemberId = 0;

    protected int port;

    public Host( String name ) {
        this( name, FSDaemon.PORT );
    }

    public Host( String name, int port ) {
        this.name = name;
        this.port = port;
        this.id = LongBytes.toLong( Hashcode.getHashcode( String.format( "%s:%s", name, port ) ) );
    }

    public Host( String name, int partitionMemberId, int port ) {
        this( name, port );
        this.partitionMemberId = partitionMemberId;
    }

    public boolean equals( Object obj ) {

        if ( obj == null )
            return false;
        
        return id == ((Host)obj).id;
        
    }

    public int hashCode() {
        return name.hashCode() + port;
    } 

    public String getName() {
        return name;
    }

    public long getId() {
        return id;
    }

    public int getPort() {
        return port;
    }

    public void setPort( int port ) {
        this.port = port;
    }
    
    public int getPartitionMemberId() {
        return partitionMemberId;
    }
    
    public String toString() {
        return String.format( "%s:%s", name, port );
    }

    /**
     * Parse a host:port pair and return a new Host.
     */
    public static Host parse( String value ) {

        String[] split = value.split( ":" );

        return new Host( split[0], Integer.parseInt( split[1] ) );
        
    }

}