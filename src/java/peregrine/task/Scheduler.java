
package peregrine.task;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.reduce.*;
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

    /**
     * Hosts which are available for additional work.  
     */
    protected SimpleBlockingQueue<Host> availableHosts = new SimpleBlockingQueue();

    /**
     * Hosts available for additional work for speculative execution.
     */
    protected SimpleBlockingQueue<Host> spareHosts = new SimpleBlockingQueue();

    protected Concurrency<Host> concurrency;
    
    public Scheduler( Config config ) {

        this.config = config;
        this.membership = config.getMembership();

        for( Host host : config.getHosts() ) {
            availableHosts.put( host );
        }

        concurrency = new Concurrency( config.getHosts() );
        
    }
    
    public void schedule( Host host ) throws Exception {

        List<Partition> partitions = membership.getPartitions( host );

        for( Partition part : partitions ) {

            if ( completed.contains( part ) )
                continue;

            if ( pending.contains( part ) )
                continue;

            if ( concurrency.get( host ) > config.getConcurrency() ) {
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

        // clear the pending status for this partition. Note that we need to do
        // this AFTER marking it complete because if we don't then it may be
        // possible to read both completed and pending at the same time and both
        // would be clear.

        pending.clear( partition );

        // add this to the list of idle hosts so that we can schedule additional work.peregrine.task
        availableHosts.put( host );

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

            Host idle = availableHosts.poll( 1000, TimeUnit.MILLISECONDS );
            
            if ( idle != null ) {
                
                log.info( "Scheduling work on: %s", idle );

                try {
                    schedule( idle );
                } catch ( Exception e ) {
                    //FIXME: we need to gossip about this.
                    log.error( "Unable to schedule work on host: " + idle, e );
                }

            }

            log.info( "pending: %s, completed: %s, availableHosts: %s, spareHosts: %s",
                      pending, completed, availableHosts, spareHosts );

        }
            
    }

}

class Progress<T> {

    Map<T,T> map = new ConcurrentHashMap();

    public void mark( T entry ) {
        map.put( entry, entry );
    }

    public void clear( T entry ) {
        map.remove( entry );
    }
    
    public boolean contains( T entry ) {
        return map.get( entry ) != null;
    }

    public int size() {
        return map.size();
    }

    public String toString() {
        return map.keySet().toString();
    }
    
}

class Concurrency<T> {

    Map<T,MutableInteger> map = new HashMap();

    public Concurrency( Set<T> list ) {

        for( T key : list ) {
            map.put( key, new MutableInteger() );
        }
        
    }
    
    public void incr( T key ) {
        map.get( key ).incr();
    }

    public void decr( T key ) {
        map.get( key ).decr();
    }

    public int get( T key ) {
        return map.get( key ).value();
    }
    
}

class MutableInteger {

    private int value = 0;

    public void incr() {
        ++value;
    }

    public void decr() {
        --value;
    }

    public int value() {
        return value;
    }
    
}