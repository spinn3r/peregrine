
package peregrine.config;

import peregrine.util.*;

public class Host implements Comparable<Host> {

    private static int sequence = 0;

    protected long id = 0;
    protected String name = null;

    protected int partitionMemberId = 0;

    protected int port;

    protected String ref;
    
    public Host( String name ) {
        this( name, Config.DEFAULT_PORT );
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

        return new Host( split[0], Integer.parseInt( split[1] ) );
        
    }

}