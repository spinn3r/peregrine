
package peregrine.task;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.shuffle.*;
import peregrine.io.*;
import peregrine.pfs.*;
import peregrine.pfsd.*;

import org.jboss.netty.handler.codec.http.*;

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

    protected SimpleBlockingQueue<Host> idleHosts = new SimpleBlockingQueue();

    /**
     * Hosts available for additional work for speculative execution.
     */
    protected SimpleBlockingQueue<Host> spareHosts = new SimpleBlockingQueue();

    public Scheduler( Config config ) {

        this.config = config;
        this.membership = config.getMembership();

        for( Host host : config.getHosts() ) {
            idleHosts.put( host );
        }

    }
    
    public void schedule( Host host ) throws Exception {

        List<Partition> partitions = membership.getPartitions( host );

        for( Partition part : partitions ) {

            if ( completed.contains( part ) )
                continue;

            if ( pending.contains( part ) )
                continue;

            log.info( "Scheduling %s on %s", part, host );
            
            invoke( host, part );

            // mark this host as pending so that work doesn't get executed again
            // until we want to do speculative execution

            pending.mark( part );
            
            return;

        }

        spareHosts.put( host );
        
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

        // add this to the list of idle hosts so that we can schedule additional work.peregrine.task
        idleHosts.put( host );

    }

    /**
     * Wait for all jobs to be complete.
     */
    public void waitForCompletion() {

        log.info( "Waiting on completion ... " );
        
        while( true ) {
        
            if ( completed.size() == membership.size() ) {
                break;
            }

            Host idle = idleHosts.poll( 1000, TimeUnit.MILLISECONDS );
            
            if ( idle != null ) {
                
                log.info( "Scheduling work on: %s", idle );

                try {
                    schedule( idle );
                } catch ( Exception e ) {
                    //FIXME: we need to gossip about this.
                    log.error( "Unable to schedule work on host: " + idle, e );
                }

            }

            log.info( "pending: %s, completed: %s, idleHosts: %s, spareHosts: %s",
                      pending, completed, idleHosts, spareHosts );

        }
            
    }

}

class SimpleBlockingQueue<T> {

    LinkedBlockingQueue<T> delegate = new LinkedBlockingQueue();

    public T poll( long timeout, TimeUnit unit ) {
        try {
            return delegate.poll( timeout, unit );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }
    
    public T take() {
        try {
            return delegate.take();
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }

    public void put( T value ) {
        try {
            delegate.put( value );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }

    @Override
    public String toString() {
        return delegate.keySet().toString();
    }
    
}

class Progress<T> {

    Map<T,T> set = new ConcurrentHashMap();

    public void mark( T entry ) {
        set.put( entry, entry );
    }

    public boolean contains( T entry ) {
        return set.get( entry ) != null;
    }

    public int size() {
        return set.size();
    }

    public String toString() {
        return set.toString();
    }
    
}
