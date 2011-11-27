
package peregrine.task;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.config.*;
import peregrine.controller.*;

import com.spinn3r.log5j.*;

public abstract class Scheduler {

    private static final Logger log = Logger.getLogger();

    protected final Config config;

    protected Membership membership;

    /**
     * The list of partitions that are completed.  When a new MapperTask needs
     * more work we verify that we aren't scheduling work form completed
     * partitions.
     */
    protected MarkSet<Partition> completed = new MarkSet();

    protected MarkSet<Partition> pending = new MarkSet();    
    
    /**
     * Hosts which are available for additional work.  
     */
    protected SimpleBlockingQueue<Host> available = new SimpleBlockingQueue();

    /**
     * Hosts available for additional work for speculative execution.
     */
    protected MarkSet<Host> spare = new MarkSet();

    protected IncrMap<Host> concurrency;

    protected MarkSet<Fail> failure = new MarkSet();

    /**
     * When a host has has been marked offline, we also need to keep track of
     * the partitions it hosted.  When number of offline hosts for a partition
     * is equal to the replica count the system has to fail as we have lost
     * data (ouch).
     */
    protected IncrMap<Partition> offlinePartitions;

    protected ChangedMessage changedMessage = new ChangedMessage();

    private ClusterState clusterState;

    private Job job;

    private String operation;
    
    public Scheduler( final String operation,
                      final Job job,
                      final Config config,
                      final ClusterState clusterState ) {

        this.operation = operation;
        this.job = job;
        this.config = config;
        this.membership = config.getMembership();
        this.clusterState = clusterState;
        
        concurrency = new IncrMap( config.getHosts() );

        offlinePartitions = new IncrMap( config.getMembership().getPartitions() );
        
        // import the current list of online hosts and pay attention to new
        // updates in the future.
        clusterState.getOnline().addListenerWithSnapshot( new MarkListener<Host>() {

                public void updated( Host host, MarkListener.Status status ) {

                    if ( status == MarkListener.Status.MARKED ) {
                        log.info( "Host now available: %s", host );
                        available.putWhenMissing( host );
                    }
                    
                }

            } );

        clusterState.getOffline().addListenerWithSnapshot( new MarkListener<Host>() {

                public void updated( Host host, MarkListener.Status status ) {

                    // for every partition when it is marked offline, go through
                    // and mark every partition offline.  if a partition has NO
                    // online replicas then we must abort the job.
                    List<Partition> partitions = config.getMembership().getPartitions( host );

                    for( Partition part : partitions ) {

                        offlinePartitions.incr( part );

                        if( offlinePartitions.get( part ) == config.getReplicas() ) {
                            markFailed( host, part , "*NO TRACE*" );
                            break;
                        }
                                                  
                    }
                    
                }

            } );
        
    }

    public String getOperation() {
        return operation;
    }
    
    protected void schedule( Host host ) throws Exception {

        List<Replica> replicas = membership.getReplicas( host );

        if ( replicas == null )
            throw new Exception( "No replicas defined for host: " + host );
        
        for( Replica replica : replicas ) {

            if ( replica.getPriority() > 0 ) {
                return;
            }
            
            Partition part = replica.getPartition();
            
            if ( completed.contains( part ) )
                continue;

            if ( pending.contains( part ) )
                continue;

            if ( concurrency.get( host ) >= config.getConcurrency() ) {
                return;
            }
            
            log.info( "Scheduling %s on %s with current concurrency: %,d of %,d",
                      part, host, concurrency.get( host ), config.getConcurrency() );
            
            invoke( host, part );

            // mark this host as pending so that work doesn't get executed again
            // until we want to do speculative execution

            pending.mark( part );

            concurrency.incr( host );
            
            continue;

        }

        spare.mark( host );
        
    }

    /**
     * Must be implemented by schedulers to hand out work correctly.
     */
    public abstract void invoke( Host host, Partition part ) throws Exception;

    /**
     * Mark a job as complete.  The RPC service calls this method when we are
     * List<Partition> partitions = config..getMembership()done with a job.
     */
    public void markComplete( Host host, Partition partition ) {

        log.info( "Marking partition %s complete from host: %s", partition, host );

        completed.mark( partition );

        // clear the pending status for this partition. Note that we need to do
        // this AFTER marking it complete because if we don't then it may be
        // possible to read both completed and pending at the same time and both
        // would be clear.

        pending.clear( partition );

        // add this to the list of available hosts so that we can schedule 
        // additional work.
        available.putWhenMissing( host );

        concurrency.decr( host );
        
    }

    public void markFailed( Host host,
                            Partition partition,
                            String stacktrace ) {

        log.error( "Host %s has failed on %s with trace: \n %s", host, partition, stacktrace );

        failure.mark( new Fail( host, partition, stacktrace ) );
        
    }
    
    public void markOnline( Host host ) {

    	if ( ! clusterState.getOnline().contains( host ) ) {    		
    		log.info( "Host is now online: %s", host );
    		available.putWhenMissing( host );
    	}

    }

    /**
     * Wait for all jobs to be complete.
     */
    public void waitForCompletion() throws Exception {

        log.info( "Waiting on completion %s" , job );
        
        while( true ) {
        
            if ( completed.size() == membership.size() ) {
                break;
            }

            // right now , if ANYTHING has failed, we can not continue....
            if ( failure.size() > 0 ) {

                // log all root causes.
                for( Fail fail : failure.values() ) {
                    log.error( "Failed to handle task: %s \n %s" , fail , fail.stacktrace );
                }

                // throw the current position.
                throw new Exception( "Failed: " + failure );
                
            }

            // TODO: make this a constant.
            Host availableHost = available.poll( 500, TimeUnit.MILLISECONDS );
            
            if ( availableHost != null ) {
                
                log.info( "Scheduling work on: %s", availableHost );

                try {
                    schedule( availableHost );
                } catch ( Exception e ) {
                    //FIXME: we need to gossip about this.
                    log.error( "Unable to schedule work on host: " + availableHost, e );
                }

            }

            String status = status();
            
            if ( changedMessage.hasChanged( status ) ) {
                log.info( "%s", status );
            }

            changedMessage.update( status );

        }
            
    }

    private String status() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "-- progress for %s %s: --\n", operation, job ) );
        
        buff.append( String.format( "  pending:    %s\n" + 
                                    "  completed:  %s\n" +
                                    "  available:  %s\n" +
                                    "  spare:      %s\n" +
                                    "  online:     %s\n",
                                    format( pending ), format( completed ), available, spare, clusterState.getOnline() ) );

        long perc = (long)(100 * (completed.size() / (double)membership.getPartitions().size()));
        
        buff.append( String.format( "  Perc complete: %,d %% \n", perc ) );

        buff.setLength( buff.length() - 1 ); // trim the trailing \n
        
        return buff.toString();

    }

    private String format( MarkSet<Partition> set ) {

        StringBuilder buff = new StringBuilder();

        buff.append( "[" );
        
        for( Partition part : set.values() ) {

            if ( buff.length() > 1 )
                buff.append( ", " );

            buff.append( part.getId() );
            
        }

        buff.append( "]" );
        
        return buff.toString();
        
    }
    
}

class Fail {

    protected Host host;
    protected Partition partition;

    protected String stacktrace;
    
    public Fail( Host host,
                 Partition partition,
                 String stacktrace ) {
        
        this.host = host;
        this.partition = partition;
        this.stacktrace = stacktrace;
        
    }
    
    public int hashCode() {
        return host.hashCode() + partition.hashCode();
    }
    
    public boolean equals( Object o ) {

        Fail f = (Fail)o;
        
        return host.equals( f.host ) && partition.equals( f.partition );
    }

    public String toString() {
        return String.format( "%s:%s", host, partition );
    }
    
}

/**
 * A message which returns true if it is different from the previous message.
 */
class ChangedMessage {

    public String last = null;

    public boolean hasChanged( String message ) {
        return ! message.equals( last );
    }

    public void update( String message ) {
        last = message;
    }

}

