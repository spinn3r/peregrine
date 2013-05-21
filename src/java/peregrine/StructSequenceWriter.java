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
package peregrine;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.io.*;
import peregrine.util.*;


/**
 * Used so that we can write key/value pairs to a StructReader.  This is
 * primarily so that Cassanra support can contain key/value pairs during a map
 * job since cassandra data is stored as a map.
 */
public class StructSequenceWriter implements SequenceWriter {

    private List<StructReader> keys = new ArrayList();
    private List<StructReader> values = new ArrayList();

    @Override
    public void write( StructReader key,
                       StructReader value ) throws IOException {

        keys.add( key );
        values.add( value );
        
    }

    @Override
    public void flush() throws IOException {}

    @Override
    public void close() throws IOException {}

    public StructReader toStructReader() {

        List<StructReader> structs = new ArrayList();

        structs.add( StructReaders.wrap( keys.size() ) );

        for( int i = 0; i < keys.size(); ++i ) {

            StructReader key   = keys.get( i );
            StructReader value = values.get( i );
            
            structs.add( StructReaders.wrap( key.length() ) );
            structs.add( key );
            structs.add( StructReaders.wrap( value.length() ) );
            structs.add( value );
            
        }

        ChannelBuffer[] buffs = new ChannelBuffer[structs.size()];

        for( int i = 0; i < structs.size(); ++i ) {
            buffs[i] = structs.get( i ).getChannelBuffer();
        }

        ChannelBuffer wrapped = ChannelBuffers.wrappedBuffer( buffs );

        return StructReaders.wrap( wrapped );
        
    }
    
}
