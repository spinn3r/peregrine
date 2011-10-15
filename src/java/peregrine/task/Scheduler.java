
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

    /**
     * The list of partitions that are completed.  When a new MapperTask needs
     * more work we verify that we aren't scheduling work form completed
     * partitions.
     */
    protected Set<Partition> completed = new HashSet();

    protected Config config;

    protected Membership membership;

    protected Completion<Partition> completion = new Completion();

    protected BlockingQueue result = new LinkedBlockingDeque();
    
    public Scheduler( Config config ) {
        this.config = config;
        this.membership = config.getPartitionMembership();
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

        try {
            
            result.put( Boolean.TRUE );

        } catch ( InterruptedException e) {
            throw new RuntimeException( e );
        }
        
    }

    public abstract void invoke( Host host, Partition part ) throws Exception;
    
    public void markComplete( Partition partition ) {
        completion.markComplete( partition );
    }

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

    Set<T> set = new HashSet();

    public void markComplete( T entry ) {
        set.add( entry );
    }

    public boolean isComplete( T entry ) {
        return set.contains( entry );
    }

}