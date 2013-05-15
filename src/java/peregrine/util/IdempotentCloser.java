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

/**
 * A function that is idempotent.  It executes once and then never executes
 * again.  We handle this with a double check idiom around a volatile executed
 * flag.  We also prevent double execution to avoid two threads triggering
 * execution before the first one stops.
 */
public abstract class IdempotentCloser
    extends IdempotentFunction<Object,IOException>
    implements Closeable {

    /**
     * When true, all close/open operations use tracing to find out where this
     * was closed but this slows down performance so keep it disabled by
     * default.
     */
    public static boolean DEFAULT_ENABLE_TRACING = false;
    
    public IdempotentCloser() {
        setEnableTracing( DEFAULT_ENABLE_TRACING );
    }

    /**
     * Idempotently close the resource.  This is a final method so that we don't
     * accidentally override it during implementation.
     */
    @Override
    public final void close() throws IOException {
        exec();
    }

    public boolean closed() {
        return isClosed();
    }
    
    public boolean isClosed() {
        return executed();
    }
    
    /**
     * Implement this to close your required resource.  This is a better name
     * than invoke() since the implementation won't specify what is exactly
     * happening.
     */
    protected abstract void doClose() throws IOException;

    /**
     * Used by IdempotentFunction to make this idempotent.
     */
    protected Object invoke() throws IOException {
        doClose();
        return null;
    }

    /**
     * Get a stacktrace for who closed this object.
     */
    public Exception getCloser() {
        return getTrace();
    }
        
}

