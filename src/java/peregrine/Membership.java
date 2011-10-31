package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Membership {
    
    private int nr_hosts;

    protected Map<Partition,List<Host>> hostsByPartition = new HashMap();

    protected Map<Host,List<Partition>> partitionsByHost = new HashMap();

    protected Map<Host,List<Replica>> replicasByHost = new HashMap();

    public Membership() {}
    
    /**
     * Create new membership from an explicit mapping.
     */
    public Membership( Map<Partition,List<Host>> hostsByPartition,
                       Map<Host,List<Partition>> partitionsByHost,
                       Map<Host,List<Replica>> replicasByHost ) {

        this.hostsByPartition = hostsByPartition;
        this.partitionsByHost = partitionsByHost;
        this.replicasByHost = replicasByHost;
        
    }
    
    public Set<Partition> getPartitions() {
        return hostsByPartition.keySet();
    }

    public List<Host> getHosts( Partition part ) {
        return hostsByPartition.get( part );
    }

    public List<Partition> getPartitions( Host host ) {
        return partitionsByHost.get( host );
    }

    public List<Replica> getReplicas( Host host ) {
        return replicasByHost.get( host );
    }

    public void setPartition( Partition part, List<Host> hosts ) {
        hostsByPartition.put( part, hosts );
        updatePartitionsByHostMapping( part, hosts );
    }

    public int size() {
        return hostsByPartition.size();
    }

    public String toMatrix() {
        
        StringBuffer buff = new StringBuffer();

        List<Host> hosts = new ArrayList();
        hosts.addAll( partitionsByHost.keySet() );

        Collections.sort( hosts );
        
        for( Host host : hosts ) {

            buff.append( String.format( "%35s: ", host.getName() ) );

            List<Partition> partitions = partitionsByHost.get( host );

            for( Partition part : partitions ) {

                buff.append( String.format( "%10s", part.getId() ) );

            }
            
            buff.append( "\n" );
            
        }

        String result = buff.toString();
        return result.substring( 0, result.length() - 1);

    }

    private void updatePartitionsByHostMapping( Partition part, List<Host> hosts ) {

        for( Host host : hosts ) {

            List<Partition> partitions = partitionsByHost.get( host );

            if ( partitions == null ) {
                partitions = new ArrayList();
                partitionsByHost.put( host, partitions );
            }

            partitions.add( part );
            
        }
        
    }
    
}
