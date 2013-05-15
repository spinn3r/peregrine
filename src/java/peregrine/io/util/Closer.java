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
package peregrine.io.util;

import java.io.*;
import java.util.*;

/**
 *
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables.
 *
 * @see <a href='http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html'>try-with-resources</a>
 */
public class Closer extends BaseCloser<Closeable> implements Closeable {

    public Closer() { }

    public Closer( List delegates ) {
        this.delegates = (List<Closeable>)delegates;
    }

    public Closer( Closeable... delegates ) {
        add( delegates );
    }

    @Override
    public void close() throws IOException {
        exec();
    }

    public boolean closed() {
        return executed();
    }

    public boolean isClosed() {
        return executed();
    }

    public void requireOpen() throws IOException {

        if ( isClosed() ) {
            throw new IOException( "closed" );
        }
        
    }
    
    protected void onDelegate( Closeable delegate ) throws IOException {
        delegate.close();
    }

}

