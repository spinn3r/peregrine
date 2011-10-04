
package peregrine.pdfsd.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 */
public class FileOutputService {

    /**
     * Number of threads to handle IO.  This should probably be configured PER
     * partition so that you could run different partitions on different disks.
     */
    public static int THREAD_POOL_SIZE = 150;
    
    private static ExecutorService executors =
        Executors.newFixedThreadPool( THREAD_POOL_SIZE );

    public static void submit( FileOutputCallable callable ) {
        executors.submit( callable );
    }
    
}