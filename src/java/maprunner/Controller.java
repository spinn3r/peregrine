
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
    public static void map( String path, Class mapper ) throws Exception {

        //read the partitions and create jobs to be executed on given chunks

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        List<Callable> callables = new ArrayList( partitionMembership.size() );

        int nr_partitions = partitionMembership.size();
        
        for ( Partition part : partitionMembership.keySet() ) {

            List<Host> hosts = partitionMembership.get( part );

            int nr_replicas = hosts.size();

            for( Host host : hosts ) {

                Callable callable = new MapperCallable( part, host, path, nr_partitions, nr_replicas, mapper );
                callables.add( callable );

            }
            
        }

        waitFor( callables );
        
    }

    public static void reduce( Reducer reducer ) 
        throws InterruptedException, ExecutionException {

        Collection<MapOutputIndex> mapOutputIndexes = ShuffleManager.getMapOutput();

        List<Callable> callables = new ArrayList();

        for( MapOutputIndex mapOutputIndex : mapOutputIndexes ) {
            callables.add( new MapOutputSortCallable( mapOutputIndex, reducer ) );
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

