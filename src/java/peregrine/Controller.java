
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
import peregrine.pfs.*;

import org.jboss.netty.handler.codec.http.*;

public class Controller {

    private Config config = null;

    public Controller( Config config ) {
        this.config = config;
    }
    
    // FIXME: a lot of this class is boilerplate and could be refactored.
    
    public void map( Class mapper, String... paths ) throws Exception {
        map( mapper, new Input( paths ) );
    }

    public void map( final Class mapper, final Input input ) throws Exception {
        map( mapper, input, null );
    }
        
    /**
     * Run map jobs on all chunks on the given path.
     */
    public void map( final Class mapper, final Input input,
                      final Output output ) throws Exception {

        System.out.printf( "Starting mapper: %s\n", mapper.getName() );

        final Membership partitionMembership = config.getPartitionMembership();
        
        runCallables( new CallableFactory() {

                public Callable newCallable( Partition part, Host host ) {

                    MapperTask task = new MapperTask();

                    task.init( config, partitionMembership, part, host, mapper );

                    task.setInput( input );
                    task.setOutput( output );
                    
                    return task;
                    
                }
                
            }, partitionMembership );

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

        System.out.printf( "Starting mapper: %s\n", mapper.getName() );

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

        System.out.printf( "Finished mapper: %s\n", mapper.getName() );

    }
    
    public void reduce( Class reducer, Input input, Output output ) 
        throws Exception {

        System.out.printf( "Starting reducer: %s\n", reducer.getName() );

        // we need to support reading input from the shuffler.  If the user
        // doesn't specify input, use the default shuffler.
        
        if ( input == null || input.getReferences().size() == 0 ) {
            input = new Input();
            input.add( new ShuffleInputReference() );
        }

        if ( input.getReferences().size() < 1 ) {
            throw new IOException( "Reducer requires at least one shuffle input." );
        }

        flushShufflers();
        
        ShuffleInputReference shuffleInput = (ShuffleInputReference)input.getReferences().get( 0 );
        System.out.printf( "using shuffle input : %s \n", shuffleInput.getName() );

        Membership partitionMembership = config.getPartitionMembership();

        // get the local partitions we are hosting...         

        List<Partition> partitions = partitionMembership.getPartitions( config.getHost() );

        List<Callable> callables = new ArrayList();
        
        for ( Partition part : partitions ) {

            ReducerTask task = new ReducerTask( config, part, reducer, shuffleInput );
            task.setInput( input );
            task.setOutput( output );

            callables.add( task );

        }

        waitFor( callables );

        Shuffler.getInstance().reset();

        System.out.printf( "Finished reducer: %s\n", reducer.getName() );
        
    }

    private void flushShufflers() throws Exception {

        // flush all the shufflers on ALL hosts....

        QueryStringEncoder encoder = new QueryStringEncoder( "" );
        encoder.addParam( "action", "flush" );
        String message = encoder.toString();

        for ( Host host : config.getHosts() ) {

            URI uri = new URI( String.format( "http://%s:%s/shuffler/RPC2", host.getName(), host.getPort() ) );
            
            RemoteChunkWriterClient client = new RemoteChunkWriterClient( uri );

            client.setMethod( HttpMethod.POST );
            client.write( message.getBytes() );
            client.close();

        }
        
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
        
        ExecutorService es = Executors.newCachedThreadPool() ;
        
        //ExecutorService es = Executors.newSingleThreadExecutor() ;

        return es;
        
    }
    
}

interface CallableFactory {

    public Callable newCallable( Partition part, Host host );

}