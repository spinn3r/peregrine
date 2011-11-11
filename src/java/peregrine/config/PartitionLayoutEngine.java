
package peregrine.config;

import java.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Manages layout for the partition system.
 */
public class PartitionLayoutEngine {

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

    Config config;
    
    public PartitionLayoutEngine( Config config ) {

        // build the hosts index
        this.hosts = new ArrayList();
        this.hosts.addAll( config.getHosts() );

        // sort the hosts. In the Set they are not sorted.  The implementation
        // of this was initially a HashSet which may change but we should always
        // sort the hosts to make testing easier since we know how the partition
        // layout apriori.
        
        Collections.sort( this.hosts );
        
        this.nr_hosts                = hosts.size();
        this.nr_partitions_per_host  = config.getPartitionsPerHost();
        this.nr_replicas             = config.getReplicas();
        this.config                  = config;

        if ( nr_hosts == 0 )
            throw new PartitionLayoutException( "No hosts defined." );

        if ( nr_replicas == 0 || nr_replicas > 3 )
            throw new PartitionLayoutException( "Invalid nr_replicas defined: " + nr_replicas );

    }

    public void build() {

        log.info( "Building partition layout with %,d partitions_per_host and %s replicas." , nr_partitions_per_host, nr_replicas );

        // I think the last partition will have (nr_hosts * nr_partitions_per_host) %
        // nr_replicas copies and we can just evenly hand these out to
        // additional hosts
        
        if ( nr_hosts < nr_replicas )
            throw new PartitionLayoutException( "Incorrect number of hosts." );

        if ( nr_partitions_per_host % nr_replicas != 0 ) {
            throw new PartitionLayoutException( "nr_partitions_per_host % nr_replicas must equal zero to fit partitions correctly." );
        }

        int min_hosts = nr_replicas * nr_partitions_per_host;
        
        if ( nr_hosts <= min_hosts ) {
            log.warn( "For maximum parallel recovery, your nr_hosts should be > nr_replicas * nr_partitions_per_host" );
        }

        int extra_hosts = nr_hosts - nr_partitions_per_host;

        // On startup... We need to print the number of machines we can handle
        // failing without falling below the minimum number of replicas...

        String extra_hosts_message =
            String.format( "%,d hosts can fail before you risk partition lost due to nr_replicas." , extra_hosts );

        if ( extra_hosts <= 0 && nr_hosts != 1 ) {
            throw new PartitionLayoutException( extra_hosts_message );
        } else {
            log.info( "%s", extra_hosts_message );
        }
        
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

            if ( nr_replicas > 1 && hosts.size() < nr_partitions_per_host ) {
                throw new PartitionLayoutException( String.format( "Replica config for %s is too small: %s", host, hosts ) );
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

        return new Membership( config, hostsByPartition, partitionsByHost, replicasByHost );

    }

}