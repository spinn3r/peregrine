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

import java.io.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

import peregrine.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * 
 */
public class NonBlockingChannelBufferWritable implements ChannelBufferWritable {

    protected static final Logger log = Logger.getLogger();

    private Channel channel;

    public NonBlockingChannelBufferWritable( Channel channel ) {
        this.channel = channel;
    }
    
    @Override
    public void write( ChannelBuffer buff ) throws IOException {
        channel.write( buff );
    }

    @Override
    public void shutdown() throws IOException {

    }

    @Override
    public void sync() throws IOException {
        
    }

    @Override
    public void flush() throws IOException {
        
    }
        
    @Override
    public void close() throws IOException {

        if ( channel.isOpen() ) {
            channel.close();
        }

    }
    
}
