
package peregrine;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.shuffle.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.pfs.*;
import peregrine.pfsd.*;
import peregrine.task.*;

import org.jboss.netty.handler.codec.http.*;

import com.spinn3r.log5j.*;

public class Controller {

    private static final Logger log = Logger.getLogger();

    private Config config = null;

    private FSDaemon daemon = null;

    public Controller( Config config ) {
        this.config = config;

        // FIXME: we should not startup an ORDINARY daemon... it should be JUST
        // for RPC.... 
        this.daemon = new FSDaemon( config );
    }

    public void map( Class mapper,
                     String... paths ) throws Exception {
        map( mapper, new Input( paths ) );
    }

    public void map( final Class mapper,
                     final Input input ) throws Exception {
        map( mapper, input, null );
    }
        
    /**
     * Run map jobs on all chunks on the given path.
     */
    public void map( final Class mapper,
                     final Input input,
                     final Output output ) throws Exception {

        System.out.printf( "Starting mapper: %s\n", mapper.getName() );

        final Membership partitionMembership = config.getPartitionMembership();

        Scheduler scheduler = new Scheduler( config ) {

                public void invoke( Host host, Partition part ) throws Exception {

                    Message message = createSchedulerMessage( "map", part, input, output );
                    message.put( "mapper", mapper.getName() );

                    new Client().invoke( host, "mapper", message );
                    
                }
                
            };

        daemon.setScheduler( scheduler );

        scheduler.init();

        scheduler.waitForCompletion();

        daemon.setScheduler( null );

        System.out.printf( "Finished mapper: %s\n", mapper.getName() );

    }
    
    public void merge( Class mapper,
                       String... paths ) throws Exception {

        merge( mapper, new Input( paths ) );

    }

    public  void merge( final Class mapper,
                        final Input input ) throws Exception {

        merge( mapper, input, null );

    }

    /**
     * http://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join
     * 
     * Conceptually, a full outer join combines the effect of applying both left
     * and right outer joins. Where records in the FULL OUTER JOINed tables do not
     * match, the result set will have NULL values for every column of the table
     * that lacks a matching row. For those records that do match, a single row
     * will be produced in the result set (containing fields populated from both
     * tables)
     */
    public void merge( final Class mapper,
                       final Input input,
                       final Output output ) throws Exception {

        log.info( "Starting mapper: %s", mapper.getName() );

        final Membership partitionMembership = config.getPartitionMembership();
        
        runCallables( new CallableFactory() {

                public Callable newCallable( Partition part, Host host ) {

                    MergeTask task = new MergeTask();

                    task.init( config, partitionMembership, part, host, mapper );

                    task.setInput( input );
                    task.setOutput( output );
                    
                    return task;
                    
                }
                
            }, partitionMembership );

        log.info( "Finished mapper: %s", mapper.getName() );

    }
    
    public void reduce( final Class delegate,
                        final Input input,
                        final Output output ) 
        throws Exception {

        log.info( "Starting reducer: %s\n", delegate.getName() );

        // we need to support reading input from the shuffler.  If the user
        // doesn't specify input, use the default shuffler.

        if ( input == null )
            throw new Exception( "Input may not be null" );
        
        if ( input.getReferences().size() == 0 ) {
            input.add( new ShuffleInputReference() );
        }

        if ( input.getReferences().size() < 1 ) {
            throw new IOException( "Reducer requires at least one shuffle input." );
        }

        flushAllShufflers();

        Scheduler scheduler = new Scheduler( config ) {

                public void invoke( Host host, Partition part ) throws Exception {

                    Message message = createSchedulerMessage( "exec", part, input, output );
                    message.put( "delegate", delegate.getName() );

                    new Client().invoke( host, "reducer", message );
                    
                }
                
            };

        daemon.setScheduler( scheduler );

        scheduler.init();

        scheduler.waitForCompletion();

        daemon.setScheduler( null );

        log.info( "Finished reducer: %s", delegate.getName() );

    }

    public void flushAllShufflers() throws Exception {

        Message message = new Message();
        message.put( "action", "flush" );

        log.info( "Flushing all %,d shufflers with message: %s" , config.getHosts().size(), message );
        
        for ( Host host : config.getHosts() ) {

            new Client().invoke( host, "shuffler", message );

        }
        
    }

    private Message createSchedulerMessage( String action,
                                            Partition partition,
                                            Input input,
                                            Output output ) {

        int idx;

        Message message = new Message();
        message.put( "action",     action );
        message.put( "partition",  partition.getId() );
        
        if ( input != null ) {
        
            idx = 0;
            for( InputReference ref : input.getReferences() ) {
                message.put( "input." + idx++, ref.toString() );
            }

        }

        if ( output != null ) {
            
            idx = 0;
            for( OutputReference ref : output.getReferences() ) {
                message.put( "output." + idx++, ref.toString() );
            }

        }

        return message;
        
    }

    public void shutdown() {
        daemon.shutdown();
    }
    
    private static void runCallables( CallableFactory callableFactory,
                                      Membership partitionMembership ) 
        throws InterruptedException, ExecutionException {

        List<Callable> callables = new ArrayList( partitionMembership.size() );

        int nr_partitions = partitionMembership.size();
        
        for ( Partition part : partitionMembership.getPartitions() ) {

            List<Host> hosts = partitionMembership.getHosts( part );

            for( Host host : hosts ) {

                Callable callable = callableFactory.newCallable( part, host );

                callables.add( callable );

            }
            
        }

        waitFor( callables );

    }
                                 
    private static void waitFor( List<Callable> callables )
        throws InterruptedException, ExecutionException {

        List<Future> futures = new ArrayList( callables.size() );

        ExecutorService es = getExecutorService();

        for( Callable callable : callables ) {

            Future future = es.submit( callable );
            futures.add( future );
            
        }

        try {

            for( Future future : futures ) {
                future.get();
            }

        } finally {
            es.shutdown();
        }

    }
    
    private static ExecutorService getExecutorService() {
        
        //ExecutorService es = Executors.newCachedThreadPool() ;
        
        ExecutorService es = Executors.newSingleThreadExecutor() ;

        return es;
        
    }
    
}

interface CallableFactory {

    public Callable newCallable( Partition part, Host host );

}