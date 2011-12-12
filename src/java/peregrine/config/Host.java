
package peregrine.config;

import peregrine.util.*;
import peregrine.util.primitive.LongBytes;

public class Host implements Comparable<Host> {

    public static final int DEFAULT_PORT = 11112;

    protected long id = 0;
    protected String name = null;

    protected int partitionMemberId = 0;

    protected int port;

    protected String ref;
    
    public Host( String name ) {
        this( name, DEFAULT_PORT );
    }

    public Host( String name, int port ) {
        this.name = name;
        this.port = port;
        this.id   = LongBytes.toLong( Hashcode.getHashcode( String.format( "%s:%s", name, port ) ) );
        this.ref  = String.format( "%s:%s", name, port );
    }

    public Host( String name, int partitionMemberId, int port ) {
        this( name, port );
        this.partitionMemberId = partitionMemberId;
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
    
    public int getPartitionMemberId() {
        return partitionMemberId;
    }

    @Override
    public boolean equals( Object obj ) {

        if ( obj == null )
            return false;
        
        return id == ((Host)obj).id;
        
    }

    @Override
    public int hashCode() {
        return ref.hashCode();
    } 

    @Override
    public String toString() {
        return ref;
    }

    @Override
    public int compareTo(Host o) {
        return toString().compareTo( o.toString() );
    }
    
    /**
     * Parse a host:port pair and return a new Host.
     */
    public static Host parse( String value ) {

        String[] split = value.split( ":" );

        int port =  DEFAULT_PORT;

        if ( split.length > 1 )
            port = Integer.parseInt( split[1] );
            
        return new Host( split[0], port );
        
    }

}