
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
    public static void map( String path, Mapper mapper ) throws Exception {

        ExecutorService es = getExecutorService();
        
        //read the shards and create jobs to be executed on given chunks

        Map<Partition,List<Host>> shardMembership = Config.getShardMembership();

        List<Future> futures = new ArrayList();
        
        for ( Partition part : shardMembership.keySet() ) {

            List<Host> hosts = shardMembership.get( part );

            int nr_hosts = hosts.size();

            for( Host host : hosts ) {
                Callable callable = new MapCallable( part, host, path, nr_hosts, mapper );

                Future future = es.submit( callable );
                futures.add( future );
                
            }
            
        }

        for( Future future : futures ) {
            //FIXME: if they fail, we should see what their status is... 
            future.get();
        }

        es.shutdown();
        
    }

    /**
     * Merge the output from all the mappers for each shard...
     */
    public static void sortMapOutput() throws Exception {

        ExecutorService es = getExecutorService();

        Collection<MapOutputIndex> mapOutputIndexes = ShuffleManager.getMapOutput();

        List<Future> futures = new ArrayList();

        for( MapOutputIndex mapOutputIndex : mapOutputIndexes ) {

            Callable callable = new MapOutputMergeCallable( mapOutputIndex );
            Future future = es.submit( callable );
            futures.add( future );
            
        }

        for( Future future : futures ) {
            //FIXME: if they fail, we should see what their status is... 
            future.get();
        }

        es.shutdown();

    }

    private static ExecutorService getExecutorService() {

        //ExecutorService es = Executors.newCachedThreadPool() ;
        
        ExecutorService es = Executors.newSingleThreadExecutor() ;

        return es;
        
    }
    
}

