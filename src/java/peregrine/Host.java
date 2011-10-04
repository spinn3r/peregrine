
package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Host {

    private static int sequence = 0;

    protected long id = 0;
    protected String name = null;

    protected int partitionMemberId = 0;
    
    public Host( String name, int partitionMemberId ) {
        this.name = name;
        this.id = LongBytes.toLong( Hashcode.getHashcode( name ) );
        this.partitionMemberId = partitionMemberId;
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

    public int getPartitionMemberId() {
        return partitionMemberId;
    }
    
    public String toString() {
        return String.format( "name=%s,id=%010d", name, id );
    }
    
}