/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.util;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import com.spinn3r.log5j.Logger;

/**
 * Keeps track of ExecutorService instances per a given class.
 */
public class ExecutorServiceManager {

    private static final Logger log = Logger.getLogger();

    private ConcurrentHashMap<Class,ExecutorService> executorServices = new ConcurrentHashMap();

    public ExecutorService getExecutorService( Class clazz ) {

        ExecutorService result;

        result = executorServices.get( clazz );

        // double check idiom
        if ( result == null ) {

            synchronized( this ) {

                result = executorServices.get( clazz );
                
                if ( result == null ) {

                    result = Executors.newCachedThreadPool( new DefaultThreadFactory( clazz ) );
                    executorServices.putIfAbsent( clazz, result );
                    result = executorServices.get( clazz );
                    
                }
                
            }
            
        }

        return result;
        
    }

    public void shutdownAndAwaitTermination() {

        for( Class clazz : executorServices.keySet() ) {

            log.info( "Shutting down executor service: %s", clazz.getName() );

            try {
                
                ExecutorService current = executorServices.get( clazz );
                current.shutdown();
                current.awaitTermination( Long.MAX_VALUE, TimeUnit.MILLISECONDS );
                
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }

        }

    }
    
}
