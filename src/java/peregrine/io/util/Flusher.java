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

import java.io.*;
import java.util.*;

/**
 * Flusher similar to {@link Closer} which calls {@link #flush}.
 */
public class Flusher extends BaseCloser<Flushable> implements Flushable {

    // TODO: the semantics of flushing and closing are somewhat different.  Once
    // a buffer is dirtied again it can be flushed again.  isFlushed should
    // return different values based on when it is called.
    
    public Flusher() { }

    public Flusher( List delegates ) {
        this.delegates = (List<Flushable>)delegates;
    }

    public Flusher( Flushable... delegates ) {
        add( delegates );
    }

    @Override
    public void flush() throws IOException {
        exec();
    }

    @Override
    protected void onDelegate( Flushable delegate ) throws IOException {
        delegate.flush();
    }

}

