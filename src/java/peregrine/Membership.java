package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Membership {
    
    private int nr_hosts;

    protected Map<Partition,List<Host>> hostsByPartition = new HashMap();

    protected Map<Host,List<Partition>> partitionsByHost = new HashMap();

    protected Map<Host,List<Replica>> replicasByHost = new HashMap();

    protected Config config;
    
    public Membership() {}
    
    /**
     * Create new membership from an explicit mapping.
     */
    public Membership( Config config,
                       Map<Partition,List<Host>> hostsByPartition,
                       Map<Host,List<Partition>> partitionsByHost,
                       Map<Host,List<Replica>> replicasByHost ) {

        this.config = config;
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

    /**
     * Return replicas for this host, sorted by priority.
     */
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

        String host_format = "%35s: ";
        
        StringBuffer buff = new StringBuffer();

        List<Host> hosts = new ArrayList();
        hosts.addAll( partitionsByHost.keySet() );

        Collections.sort( hosts );

        buff.append( "Partition layout matrix.  (Partition names are in partition_id.priority format):\n" );
        
        buff.append( String.format( host_format, "HOST" ) );

        for( int i = 0; i < config.getPartitionsPerHost(); ++i ) {
            buff.append( String.format( "  %10s", "p" + i ) );
        }
        
        buff.append( "\n" );
        
        for( Host host : hosts ) {

            buff.append( String.format( host_format, host.getName() ) );

            List<Replica> replicas = replicasByHost.get( host );

            for( Replica replica : replicas ) {

                buff.append( String.format( "%10s.%s", replica.getPartition().getId(), replica.getPriority() ) );

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
