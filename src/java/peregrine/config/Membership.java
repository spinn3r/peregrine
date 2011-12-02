package peregrine.config;

import java.io.*;
import java.util.*;

import com.spinn3r.log5j.*;

import peregrine.rpc.*;
import peregrine.util.*;

public class Membership {
    
    private static final Logger log = Logger.getLogger();

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

        if ( replicasByHost.size() == 0 )
            throw new RuntimeException( "No replicas defined." );
        
        if ( ! replicasByHost.containsKey( host ) ) {
            throw new RuntimeException( String.format( "Unknown host with no replicas %s (%s) and config: %s",
                                                       host, config.getHosts(), config ) );
        }
        
        return replicasByHost.get( host );

    }

    public List<Replica> getReplicasByPriority( Host host ) {
        return getReplicasByPriority( host, 0 );
    }

    /**
     * Get the replicas for a host, but only once of a given priority.
     */
    public List<Replica> getReplicasByPriority( Host host, int priority ) {

        List<Replica> replicas = getReplicas( host );

        List<Replica> result = new ArrayList();

        for( Replica replica : replicas ) {

            if ( replica.getPriority() == priority )
                result.add( replica );
            
        }

        return result;
        
    }

    public void setPartition( Partition part, List<Host> hosts ) {
        hostsByPartition.put( part, hosts );
        updatePartitionsByHostMapping( part, hosts );
    }
    
    public int size() {
        return hostsByPartition.size();
    }

    public String toMatrix() {

        String host_format = "%35s ";
        
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

            buff.append( String.format( host_format, host ) );

            List<Replica> replicas = replicasByHost.get( host );

            for( Replica replica : replicas ) {

                buff.append( String.format( "%10s.%s", replica.getPartition().getId(), replica.getPriority() ) );

            }
            
            buff.append( "\n" );
            
        }

        String result = buff.toString();
        return result.substring( 0, result.length() - 1);

    }

    @Override 
    public String toString() {
    	return toMatrix();
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

    /**
     * Send gossip to the controller that a given host is not cooperating for 
     * functioning correctly.
     * 
     * @param host
     * @param cause
     * @throws IOException 
     */
    public boolean sendGossipToController( Host failed, Throwable cause ) {
    	
        try {
        	
			Message message = new Message();
			
			message.put( "action",     "gossip" );
			message.put( "reporter",   config.getHost() );
			message.put( "failed",     failed );
			message.put( "cause",      cause );

			new Client().invoke( config.getController(), "controller", message );

            log.info( "Sent gossip that %s failed: %s", failed, cause.getMessage() );

            return true;
            
		} catch (IOException e) {
			
			// there isn't much we can do on gossip failure.  It almost certainly 
			// means that this machine is off the network and we're shutting 
			// down anyway.  AKA *we* are the failed host.
            
			log.error( "Unable to send gossip: ", e );

            return false;
            
		}
        
    }

        public boolean sendHeartbeatToController() {
            
            Message message = new Message();
            
            message.put( "action",          "heartbeat" );
            message.put( "host",            config.getHost().toString() );

            // Include a checksum of the config.  This used so that the
            // controller and the hosts can verify that they are running in the
            // same configuration.
            
            message.put( "config_checksum", config.getChecksum() );
            
            Host controller = config.getController();

            if ( controller == null )
                throw new NullPointerException( "controller" );
            
            try {           
                
                new Client( true ).invoke( controller, "controller", message );
                
                return true;

            } catch ( IOException e ) {

                if ( Thread.currentThread().isInterrupted() )
                    return false;

                // we don't normally need to send this message because we would be overly verbose.
                log.debug( String.format( "Unable to send heartbeat to %s: %s", controller, e.getMessage() ) );
                
                return false;
            }
       
        }           

}
