
package peregrine.pdfsd.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 */
public class FileOutputService {

    public static int THREAD_POOL_SIZE = 100;
    
    private static ExecutorService executors =
        Executors.newFixedThreadPool( THREAD_POOL_SIZE );

    public static void submit( FileOutputCallable callable ) {
        executors.submit( callable );
    }
    
}