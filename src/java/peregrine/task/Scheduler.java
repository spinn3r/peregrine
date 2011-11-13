
package peregrine.task;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.util.*;
import peregrine.config.*;
import com.spinn3r.log5j.*;

public abstract class Scheduler {

    private static final Logger log = Logger.getLogger();

    protected Config config;

    protected Membership membership;

    /**
     * The list of partitions that are completed.  When a new MapperTask needs
     * more work we verify that we aren't scheduling work form completed
     * partitions.
     */
    protected Progress<Partition> completed = new Progress();

    protected Progress<Partition> pending = new Progress();    
    
    /**
     * Hosts which are available for additional work.  
     */
    protected SimpleBlockingQueue<Host> available = new SimpleBlockingQueue();

    /**
     * Hosts available for additional work for speculative execution.
     */
    protected SimpleBlockingQueue<Host> spare = new SimpleBlockingQueue();

    protected Concurrency<Host> concurrency;

    protected ChangedMessage changedMessage = new ChangedMessage();

    protected Failure failure = new Failure();
    
    public Scheduler( Config config ) {

        this.config = config;
        this.membership = config.getMembership();

        concurrency = new Concurrency( config.getHosts() );
        
        //for( Host host : membership.getOnline().values() ) {
        //	available.put( host );
        //}
        
        for ( Host host : config.getHosts() ) {
        	available.put( host );        
        }
        
    }
    
    protected void schedule( Host host ) throws Exception {

        List<Replica> replicas = membership.getReplicas( host );

        if ( replicas == null )
            throw new Exception( "No replicas defined for host: " + host );
        
        for( Replica replica : replicas ) {

            Partition part = replica.getPartition();
            
            if ( completed.contains( part ) )
                continue;

            if ( pending.contains( part ) )
                continue;

            if ( concurrency.get( host ) >= config.getConcurrency() ) {
                return;
            }
            
            log.info( "Scheduling %s on %s with current concurrency: %,d of %,d and FIXME pending: %s completed: %s",
                      part, host, concurrency.get( host ), config.getConcurrency(), pending, completed );
            
            invoke( host, part );

            // mark this host as pending so that work doesn't get executed again
            // until we want to do speculative execution

            pending.mark( part );

            concurrency.incr( host );
            
            continue;

        }

        spare.put( host );
        
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
        available.put( host );

        concurrency.decr( host );
        
    }

    public void markFailed( Host host,
                            Partition partition,
                            String stacktrace ) {

        log.error( "Host %s has failed on %s with trace: \n %s", host, partition, stacktrace );

        failure.mark( new Fail( host, partition, stacktrace ) );
        
    }
    
    public void markOnline( Host host ) {

    	if ( ! config.getMembership().getOnline().contains( host ) ) {    		
    		log.info( "Host is now online: %s", host );
    		available.put( host );
    	}

    }

    /**
     * Wait for all jobs to be complete.
     */
    public void waitForCompletion() throws Exception {

        log.info( "Waiting on completion ... " );
        
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
            Host availableHost = available.poll( 1000, TimeUnit.MILLISECONDS );
            
            if ( availableHost != null ) {
                
                log.info( "Scheduling work on: %s", availableHost );

                try {
                    schedule( availableHost );
                } catch ( Exception e ) {
                    //FIXME: we need to gossip about this.
                    log.error( "Unable to schedule work on host: " + availableHost, e );
                }

            }

            String message = String.format( "\n" +
                                            "pending:    %s\n" + 
                                            "completed:  %s\n" +
                                            "available:  %s\n" +
                                            "spare:      %s\n" +
                                            "online:     %s",
                                            pending, completed, available, spare, membership.getOnline() );

            if ( changedMessage.hasChanged( message ) ) {
                log.info( message );
            }

            changedMessage.update( message );

        }
            
    }

}

class Progress<T> extends MarkSet<T>{ 
    
}

class Concurrency<T> {

    Map<T,AtomicInteger> map = new ConcurrentHashMap();

    public Concurrency( Set<T> list ) {

        for( T key : list ) {
            map.put( key, new AtomicInteger() );
        }
        
    }
    
    public void incr( T key ) {
        map.get( key ).getAndIncrement();
    }

    public void decr( T key ) {
        map.get( key ).getAndDecrement();
    }

    public int get( T key ) {
        return map.get( key ).get();
    }

}

class Failure extends MarkSet<Fail> {
    
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

