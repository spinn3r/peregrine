
package peregrine.util;

import java.util.*;

import peregrine.*;
import java.nio.charset.Charset;

import com.spinn3r.log5j.Logger;

/**
 * Manages layout for the partition system.
 */
public class PartitionLayoutEngine {

    private static final Logger log = Logger.getLogger();

    /**
     * The host to partition matrix.
     */
    Map<Host,List<Partition>> matrix = new HashMap();

    /**
     * The primary partition lookup.
     */
    Map<Host,List<Partition>> primary = new HashMap();

    int nr_hosts;
    int nr_partitions_per_host;
    int nr_replicas;

    // there are two pointers here.  One 'allocates' partitions horizontally
    // and another 'grants' the allocated partitions to additional replica
    // hosts.
    
    int last_allocated_partition = 0;

    /**
     * Used with redistribute() so that we can keep track of the last host that
     * had a partition given to it.
     */
    int current_granted_host_idx = 0;

    int current_host_idx = 0;

    List<Host> hosts;
    
    public PartitionLayoutEngine( Config config, List<Host> hosts ) {
        
        this.nr_hosts = hosts.size();
        this.nr_partitions_per_host = config.getPartitionsPerHost();
        this.nr_replicas = config.getReplicas();
        this.hosts = hosts;
        
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

        // init the matrix and primary partitions
        for( Host host : hosts ) {
            matrix.put( host, new ArrayList() ); 
            primary.put( host, new ArrayList() ); 
        }

        for( ; current_host_idx < nr_hosts; ++current_host_idx ) {

            Host current_host = hosts.get( current_host_idx );

            List<Partition> partitions = matrix.get( current_host );

            // the new partitions that we granted to this box which we have to
            // distribute to other nodes now.
            List<Partition> granted = new ArrayList();
            
            for( int j = partitions.size() ; j < nr_partitions_per_host; ++j ) {
                Partition part = new Partition( last_allocated_partition++ );
                partitions.add( part );
                granted.add( part );

                if ( granted.size() <= nr_primary_per_host ) {

                    primary.get( current_host ).add( part );
                    
                }
                
            }

            redistribute( granted );
            
        }

        fixRemainder();

    }
    
    private void redistribute( List<Partition> granted ) {

        // now redistribute the granted partitions to other hosts.
        for( Partition grant : granted ) {

            int replica_count = 1;

            while( replica_count < nr_replicas ) {
                
                ++current_granted_host_idx;

                if ( current_granted_host_idx >= nr_hosts ) {
                    current_granted_host_idx = current_host_idx;
                    continue;
                }

                Host host = hosts.get( current_granted_host_idx );
                
                List<Partition> potential = matrix.get( host );

                if ( potential.size() == nr_partitions_per_host ) {

                    if ( current_granted_host_idx == nr_hosts - 1)
                        break;
                    
                    continue;
                }

                potential.add( grant );
                
                ++replica_count;
                
            }
            
        }

    }

    private void fixRemainder() {

        // now we need to make sure we have everything balanced with no
        // partitions having fewer than the required replicas.

        int required_extra_partitions = nr_replicas - ((nr_hosts * nr_partitions_per_host) % nr_replicas);

        if ( required_extra_partitions != nr_replicas ) {
        
            Partition last = new Partition( last_allocated_partition - 1 );
            
            for( int i = 0; i < required_extra_partitions; ++i ) {
                matrix.get( hosts.get( i ) ).add( last );
            }

        }

    }

    public Membership toMembership() {

        Map<Partition,List<Host>> forward = new HashMap();
        
        // make sure there is an entry per partition
        for( Host host : matrix.keySet() ) {

            for( Partition part : matrix.get( host ) ) {

                List<Host> hosts = forward.get( part );

                if ( hosts == null ) {
                    hosts = new ArrayList();
                    forward.put( part, hosts );
                }
                
                hosts.add( host );

            }
            
        }

        return new Membership( forward, matrix, primary );

    }

}