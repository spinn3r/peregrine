
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.util.*;

/**
 */
public class AsyncOutputStreamService {

    private static ExecutorService executors =
        Executors.newCachedThreadPool( new DefaultThreadFactory( AsyncOutputStream.class) );

    public static Future submit( AsyncOutputStreamCallable callable ) {
        return executors.submit( callable );
    }

    public static void shutdown() {
        executors.shutdown();
    }
    
}
