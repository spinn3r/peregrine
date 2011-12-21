
package peregrine.config;

/**
 * Represents a specific instance / replica of a partition on a given host.
 */
public class Replica implements Comparable<Replica> {
    
    protected Partition partition;

    protected int priority = 0;

    protected Host host;

    public Replica( Host host, Partition partition, int priority ) {
        this.host = host;
        this.partition = partition;
        this.priority = priority;
    }

    public Host getHost() { 
        return this.host;
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