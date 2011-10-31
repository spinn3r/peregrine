
package peregrine.util;

import java.util.*;

import peregrine.*;
import java.nio.charset.Charset;

import com.spinn3r.log5j.Logger;

/**
 * Manages layout for the partition system.
 */
public class PartitionLayoutEngine2 {

    private static final Logger log = Logger.getLogger();

    /**
     * The host to partition matrix.
     */
    Map<Host,List<Partition>> partitionsByHost = new HashMap();

    Map<Partition,List<Host>> hostsByPartition = new HashMap();

    protected Map<Host,List<Replica>> replicasByHost = new HashMap();
    
    protected Map<Partition,List<Replica>> replicasByPartition = new HashMap();

    int nr_hosts;
    int nr_partitions_per_host;
    int nr_replicas;

    List<Host> hosts;
    
    public PartitionLayoutEngine2( Config config, List<Host> hosts ) {
        
        this.nr_hosts                = hosts.size();
        this.nr_partitions_per_host  = config.getPartitionsPerHost();
        this.nr_replicas             = config.getReplicas();
        this.hosts                   = hosts;
        
    }

    public void build() {

        log.info( "Building partition layout with %,d partitions_per_host and %s replicas." , nr_partitions_per_host, nr_replicas );

        // I think the last partition will have (nr_hosts * nr_partitions_per_host) %
        // nr_replicas copies and we can just evenly hand these out to
        // additional hosts
        
        if ( nr_hosts < nr_replicas )
            throw new RuntimeException( "Incorrect number of hosts." );

        if ( nr_partitions_per_host % nr_replicas != 0 ) {
            throw new RuntimeException( "nr_partitions_per_host % nr_replicas must equal zero to fit partitions correctly." );
        }

        int min_hosts = nr_replicas * nr_partitions_per_host;
        
        if ( nr_hosts <= min_hosts ) {
            log.warn( "For maximum parallel recovery, your nr_hosts should be > nr_replicas * nr_partitions_per_host" );
        }

        int extra_hosts = nr_hosts - min_hosts;
        
        log.info( "%,d hosts can fail before you risk partition lost due to nr_replicas." , extra_hosts );
        
        int nr_primary_per_host = nr_partitions_per_host / nr_replicas;

        log.info( "nr_partitions_per_host: %,d", nr_partitions_per_host );
        
        // init the partitionsByHost.
        for( Host host : hosts ) {
            partitionsByHost.put( host, new ArrayList() ); 
            replicasByHost.put( host, new ArrayList() ); 
        }

        // init the hostsByPartition lookup... 
        int nr_partitions = nr_primary_per_host * nr_hosts;

        log.info( "Number of unique partitions: %,d", nr_partitions );
        
        for( int i = 0; i < nr_partitions; ++i ) {
            hostsByPartition.put( new Partition( i ), new ArrayList() ); 
            replicasByPartition.put( new Partition( i ), new ArrayList() ); 
        }

        // now create the replicas... 
        for( int i = 0; i < nr_replicas; ++i ) {

            if ( i % 2 == 0 ) {

                int host_offset = (i * nr_primary_per_host) + i;

                if ( host_offset != 0 )
                    --host_offset;

                for( int j = 0; j < nr_partitions; ++j ) {

                    int host_id = (j % nr_hosts) + host_offset;

                    if ( host_id >= nr_hosts )
                        host_id = host_id - nr_hosts;
                    
                    Host host = hosts.get( host_id );
                    
                    associate( host, new Partition( j ), i );

                }

            } else { 
            
                // we don't start at the first host to avoid placing
                // the same partitions that it currently hosts on it.

                int nr_columns = nr_primary_per_host;
                
                for( int j = 0; j < nr_columns; ++j ) {

                    int host_id = j + 1;

                    for( int k = 0; k < nr_hosts ; ++k ) {
                        
                        Host host = hosts.get( host_id );

                        associate( host, new Partition( (j * nr_hosts) + k ), i );

                        host_id = (host_id == nr_hosts - 1) ? 0 : host_id + 1;
                        
                    }
                        
                }

            }
                
        }

        assertCorrectLayout();
        
    }

    /**
     * Assert that we're running with the correct partition layout.  If not then
     * we need to throw an exception as we can't run with an incorrect config.
     * Since these are TWO different algorithms the chances of catching bugs is
     * much higher.
     */
    private void assertCorrectLayout() {

        for( Host host: replicasByHost.keySet() ) {

            List<Partition> partitions = partitionsByHost.get( host );

            Set<Host> hosts = new HashSet();

            for( Partition partition : partitions ) {

                List<Replica> replicas = replicasByPartition.get( partition );

                for( Replica replica : replicas ) {

                    // replicas for this host don't count.
                    if( replica.getHost().equals( host ) )
                        continue;

                    hosts.add( replica.getHost() );

                }
                
            }

            if ( hosts.size() < nr_partitions_per_host ) {
                throw new RuntimeException( String.format( "Replica config for %s is too small: %s", host, hosts ) );
            }
            
        }
        
    }
    
    /**
     * Associate a given partition with a given host.  This will perform the
     * hostsByPartition and reverse lookups.
     */
    private void associate( Host host, Partition partition, int priority ) {
        
        partitionsByHost.get( host ).add( partition );
        hostsByPartition.get( partition ).add( host );

        // now create a Replica for this partition.

        Replica replica = new Replica( host, partition, priority );
        
        replicasByHost.get( host ).add( replica );
        replicasByPartition.get( partition ).add( replica );
        
    }

    public Membership toMembership() {

        return new Membership( hostsByPartition, partitionsByHost, replicasByHost );

    }

}