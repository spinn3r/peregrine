/*
 * Copyright 2011-2012 Kevin A. Burton
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
package peregrine.util;

import java.io.*;

/**
 * A function that is idempotent.  It executes once and then never executes
 * again.  We handle this with a double check idiom around a volatile executed
 * flag.  We also prevent double execution to avoid two threads triggering
 * execution before the first one stops.
 */
public abstract class IdempotentCloser<Object,E extends IOException>
    extends IdempotentFunction<Object,IOException>
    implements Closeable {

    public void close() throws IOException {
        exec();
    }

    /**
     * Implement this to close your required resource.  This is a better name
     * than invoke() since the implementation won't specify what is exactly
     * happening.
     */
    protected abstract void doClose() throws IOException;

    protected Object invoke() throws IOException {
        doClose();
        return null;
    }

}

