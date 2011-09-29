
package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.shuffle.*;

public class Controller {

    /**
     * Run map jobs on all chunks on the given path.
     */
    public static void map( Class mapper, String path ) throws Exception {

        //read the partitions and create jobs to be executed on given chunks

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        List<Callable> callables = new ArrayList( partitionMembership.size() );

        for ( Partition part : partitionMembership.keySet() ) {

            List<Host> hosts = partitionMembership.get( part );

            for( Host host : hosts ) {

                Callable callable = new MapperCallable( partitionMembership,
                                                        part,
                                                        host,
                                                        mapper,
                                                        path );

                callables.add( callable );

            }
            
        }

        waitFor( callables );
        
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
    public static void mapWithFullOuterJoin( Class mapper, String... path ) throws Exception {

    }
        
    public static void reduce( Class reducer ) 
        throws InterruptedException, ExecutionException {

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        Collection<MapOutputIndex> mapOutputIndexes = ShuffleManager.getMapOutput();

        List<Callable> callables = new ArrayList();

        for( MapOutputIndex mapOutputIndex : mapOutputIndexes ) {
                                                       
            callables.add( new MapOutputSortCallable( mapOutputIndex, reducer ) );

        }

        waitFor( callables );
        
    }

    private static void runCallables( Map<Partition,List<Host>> partitionMembership,
                                      Class mapper,
                                      String... path ) 
        throws InterruptedException, ExecutionException {

        List<Callable> callables = new ArrayList( partitionMembership.size() );

        int nr_partitions = partitionMembership.size();
        
        for ( Partition part : partitionMembership.keySet() ) {

            List<Host> hosts = partitionMembership.get( part );

            for( Host host : hosts ) {

                Callable callable = new MapperCallable( partitionMembership,
                                                        part,
                                                        host,
                                                        mapper,
                                                        path[0] );

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

