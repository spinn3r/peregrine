/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.io.util;

import com.spinn3r.log5j.*;

import peregrine.util.*;

import java.io.*;
import java.util.*;

/**
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables.
 *
 * http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
 */
public abstract class BaseCloser<T> extends IdempotentFunction<Object,GroupIOException> {

    private static final Logger log = Logger.getLogger();

    private boolean trace = false;

    protected List<T> delegates = new ArrayList();

    private Throwable cause = null;

    public BaseCloser() {
        super();
    }

    public BaseCloser( List<T> delegates ) {
        this();
        this.delegates = delegates;
    }
    
    public BaseCloser( T... delegates ) {
        this();
        add( delegates );
    }

    public void add( T delegate ) {
        delegates.add( delegate );
    }

    public void add( T... delegates ) {
        for( T current : delegates ) {
            this.delegates.add( current );
        }
    }

    /**
     * Get the raw backing of delegates from the Closer.  This can be used to
     * log which are being closed and in what order.
     */
    public List<T> getDelegates() {
        return delegates;
    }

    public void setCause( Throwable cause ) {
        this.cause = cause;
    }

    public boolean getTrace() { 
        return this.trace;
    }

    public void setTrace( boolean trace ) { 
        this.trace = trace;
    }

    @Override
    protected Object invoke() throws GroupIOException {

        GroupIOException exc = null;

        if ( cause != null )
            exc = new GroupIOException( cause );
        
        for ( T current : delegates ) {

            if ( current == null )
                continue;
            
            try {

                if ( trace ) {
                    log.info( "Going to handle %s" , current.getClass().getName() );
                }
                
                onDelegate( current );

            } catch ( Throwable t ) {

                if ( exc == null ) {
                    exc = new GroupIOException( t );
                } else { 
                    exc.addRepressed( t );
                }
                
            }

        }
        
        if ( exc != null ) {
            throw exc;
        }

        return null;
        
    }

    protected abstract void onDelegate( T delegate ) throws IOException;
    
}
    
