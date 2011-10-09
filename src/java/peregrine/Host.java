
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

    public Host( String name, int partitionMemberId ) {
        this( name, partitionMemberId, FSDaemon.PORT );
    }

    public Host( String name, int partitionMemberId, int port ) {
        this.name = name;
        this.id = LongBytes.toLong( Hashcode.getHashcode( String.format( "%s:%s", name, port ) ) );
        this.partitionMemberId = partitionMemberId;
        this.port = port;
    }

    public boolean equals( Object obj ) {
        return id == ((Host)obj).id;
    }

    public int hashCode() {
        return name.hashCode();
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
    
    public String toString() {
        return String.format( "%s:%s", name, port, id );
    }
    
}