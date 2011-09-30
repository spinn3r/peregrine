
package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.shuffle.*;
import maprunner.io.*;

public class Controller {

    public static void map( Class mapper, String... paths ) throws Exception {
        map( mapper, Input.fromPaths( paths ) );
    }

    /**
     * Run map jobs on all chunks on the given path.
     */
    public static void map( final Class mapper, final Input input ) throws Exception {

        ShuffleManager.reset();
        
        System.out.printf( "Starting mapper: %s\n", mapper.getName() );

        final Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();
        
        runCallables( new CallableFactory() {

                public Callable newCallable( Partition part, Host host ) {

                    MapperTask task = new MapperTask();

                    task.init(  partitionMembership,
                                part,
                                host,
                                mapper );

                    task.setInput( input );

                    return task;
                    
                }
                
            }, partitionMembership );

        System.out.printf( "Finished mapper: %s\n", mapper.getName() );

    }

    public static void mergeMapWithFullOuterJoin( Class mapper, String... paths ) throws Exception {
        mergeMapWithFullOuterJoin( mapper, Input.fromPaths( paths ) );
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
    public static void mergeMapWithFullOuterJoin( final Class mapper, final Input input ) throws Exception {

        ShuffleManager.reset();
        
        System.out.printf( "Starting mapper: %s\n", mapper.getName() );

        final Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();
        
        runCallables( new CallableFactory() {

                public Callable newCallable( Partition part, Host host ) {

                    MergeWithFullOuterJoinTask task = new MergeWithFullOuterJoinTask();

                    task.init( partitionMembership,
                               part,
                               host,
                               mapper );

                    task.setInput( input );

                    return task;
                    
                }
                
            }, partitionMembership );

        System.out.printf( "Finished mapper: %s\n", mapper.getName() );

    }
        
    public static void reduce( Class reducer, String path ) 
        throws InterruptedException, ExecutionException {

        System.out.printf( "Starting reducer: %s\n", reducer.getName() );

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        Collection<MapOutputIndex> mapOutputIndexes = ShuffleManager.getMapOutput();

        List<Callable> callables = new ArrayList();

        for( MapOutputIndex mapOutputIndex : mapOutputIndexes ) {
            callables.add( new ReducerTask( mapOutputIndex, reducer, path ) );
        }

        waitFor( callables );

        System.out.printf( "Finished reducer: %s\n", reducer.getName() );
        
    }

    private static void runCallables( CallableFactory callableFactory,
                                      Map<Partition,List<Host>> partitionMembership ) 
        throws InterruptedException, ExecutionException {

        List<Callable> callables = new ArrayList( partitionMembership.size() );

        int nr_partitions = partitionMembership.size();
        
        for ( Partition part : partitionMembership.keySet() ) {

            List<Host> hosts = partitionMembership.get( part );

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