
package maprunner;

import java.io.*;
import java.util.*;

public class Host {

    private static int sequence = 0;

    protected int id = 0;
    protected String name = null;

    protected int partitionMemberId = 0;
    
    public Host( String name, int id , int partitionMemberId ) {
        this.name = name;
        this.id = id;
        this.partitionMemberId = partitionMemberId;
    }

    public boolean equals( Object obj ) {
        return id == ((Host)obj).id;
    }

    public int hashCode() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public int getPartitionMemberId() {
        return partitionMemberId;
    }
    
    public String toString() {
        return String.format( "name=%s,id=%s", name, id );
    }
    
}