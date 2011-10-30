
package peregrine;

import java.io.*;
import java.util.*;

public class Replica implements Comparable<Replica> {
    
    protected Partition partition = null;

    protected int priority = 0;

    public Replica( Partition partition ) {
        this( partition, 0 );
    }

    public Replica( Partition partition, int priority ) {
        this.partition = partition;
        this.priority = priority;
    }

    public Partition getPartition() { 
        return this.partition;
    }

    public int getPriority() { 
        return this.priority;
    }

    @Override
    public int compareTo( Replica r ) {

        int result = priority - r.priority;

        if ( result != 0 )
            return result;

        result = partition.getId() - r.partition.getId();

        return result;
        
    }
    
    @Override
    public String toString() {
        return String.format( "replica:%s, priority=%s", partition, priority  );
    }

}