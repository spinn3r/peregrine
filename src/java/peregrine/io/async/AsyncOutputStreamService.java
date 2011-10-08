
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 */
public class AsyncOutputStreamService {

    /**
     * Number of threads to handle IO.  This should probably be configured PER
     * partition so that you could run different partitions on different disks.
     */
    public static int THREAD_POOL_SIZE = 150;
    
    private static ExecutorService executors =
        Executors.newFixedThreadPool( THREAD_POOL_SIZE, new AsyncThreadFactory() );

    public static Future submit( AsyncOutputStreamCallable callable ) {
        return executors.submit( callable );
    }

    public static void shutdown() {
        executors.shutdown();
    }
    
}

class AsyncThreadFactory implements ThreadFactory {

    public static int idx = 0;
    
    public Thread newThread(Runnable r) {
        Thread thread = new Thread( r, "peregrine.io.async:" + idx++ );
        thread.setDaemon( true );
        return thread;
    }
    
}