
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
    protected Completion<Partition> completion = new Completion();

    protected BlockingQueue result = new LinkedBlockingDeque();
    
    public Scheduler( Config config ) {
        this.config = config;
        this.membership = config.getMembership();
    }
    
    public void init() throws Exception {

        log.info( "init()" );
        
        // for each host, schedule a unit of work
        Set<Host> hosts = config.getHosts();

        for( Host host : hosts ) {
            schedule( host );
        }
        
    }

    public void schedule( Host host ) throws Exception {

        List<Partition> partitions = membership.getPartitions( host );

        for( Partition part : partitions ) {

            if ( completion.isComplete( part ) )
                continue;

            log.info( "Scheduling %s on %s", part, host );
            
            invoke( host, part );

            return;

        }

        // this HOST complete.  Let's see if all the partitions complete.
        
        try {

            if ( completion.size() == membership.size() )
                result.put( Boolean.TRUE );

        } catch ( InterruptedException e) {
            throw new RuntimeException( e );
        }
        
    }

    /**
     * Must be implemented by schedulers to hand out work correctly.
     */
    public abstract void invoke( Host host, Partition part ) throws Exception;

    /**
     * Mark a job as complete.  The RPC service calls this method when we are
        List<Partition> partitions = config..getMembership()done with a job.
     */
    public void markComplete( Host host, Partition partition ) {

        log.info( "Marking partition %s complete from host: %s", partition, host );

        completion.markComplete( partition );

        try {

            //FIXME: this machine may be down and we should gossip about this
            
            schedule( host );
        } catch ( Exception e ) {
            log.error( "Unable to schedule more work: ", e );
        }
        
    }

    /**
     * Wait for all jobs to be complete.
     */
    public void waitForCompletion() {

        log.info( "Waiting for completion." );
        
        try {
            
            Object took = result.take();

            return;

        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
        
    }

}

class Completion<T> {

    Map<T,T> set = new ConcurrentHashMap();

    public void markComplete( T entry ) {
        set.put( entry, entry );
    }

    public boolean isComplete( T entry ) {
        return set.get( entry ) != null;
    }

    public int size() {
        return set.size();
    }
    
}