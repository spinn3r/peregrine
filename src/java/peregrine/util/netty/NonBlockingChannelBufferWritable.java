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
package peregrine.util.netty;

import java.util.concurrent.*;

import java.io.*;
import org.jboss.netty.buffer.*;

/**
 *
 * <p> A channel buffer writable that allows writing to a channel without
 * blocking until a queue is full.  This class works with two threads.  The main
 * netty network IO thread and a writer thread usually performing some type of
 * disk IO.
 *
 * <p> This class acts as an intermediary between the two and writes from either
 * when appropriate.
 * 
 */
public class NonBlockingChannelBufferWritable implements ChannelBufferWritable {

    // FIXME: integrate this with HttpClient since there's some duplicate code
    // there.
    
    @Override
    public void write( ChannelBuffer buff ) throws IOException {

    }

    @Override
    public void shutdown() throws IOException {

    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void flush() throws IOException {

        // we have to wait for the queue to drain itself since IO is done in the
        // network thread.
        
    }
        
    @Override
    public void close() throws IOException {

    }

}
