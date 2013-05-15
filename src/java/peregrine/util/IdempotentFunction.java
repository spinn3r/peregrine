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

/**
 * A function that is idempotent.  It executes once and then never executes
 * again.  We handle this with a double check idiom around a volatile executed
 * flag.  We also prevent double execution to avoid two threads triggering
 * execution before the first one stops.
 */
public abstract class IdempotentFunction<T,E extends Exception> {

    private volatile boolean executed = false;

    private T result = null;

    private E failure = null;

    private boolean enableTracing = false;

    protected Exception trace = null;
    
    protected final T getResult() throws E {
        
        if ( failure != null )
            throw (E)failure;
        
        return result;
        
    }
    
    protected final T exec() throws E {

        if ( executed ) {
            return getResult();
        }

        synchronized( this ) {

            if ( executed ) {
                return getResult();
            }

            try {

                if ( enableTracing )
                    trace = new Exception( "TRACE" );

                result = invoke();
                return result;

            } catch ( Exception e ) {
                
                failure = (E)e;

                if ( enableTracing ) {

                    Throwable root = failure;

                    while( root.getCause() != null ) {
                        root = root.getCause();
                    }

                    root.initCause( trace );
                    
                }
                
                throw failure;
                
            } finally {
                executed = true;
            }
            
        }
        
    }

    protected boolean executed() {
        return executed;
    }

    protected void setEnableTracing( boolean _enableTracing ) {
        this.enableTracing = _enableTracing;
    }
    
    /**
     * Perform the action you want to execute once.  This is the actual body of
     * your code.  You should NOT call this method in production but should
     * instead have call exec().
     */
    protected abstract T invoke() throws E;

    protected Exception getTrace() {
        return trace;
    }
    
}

