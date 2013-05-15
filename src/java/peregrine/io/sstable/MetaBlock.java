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

package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.io.chunk.*;

import org.jboss.netty.buffer.*;

public class MetaBlock extends BaseBlock {

    // FIXME: we need a set of key/value pairs for metadata here and also we
    // need to write them out.

    // used to keey track of key value pairs for this block
    private List<Record> records = new ArrayList();
    
     private void addRecord( StructReader key, StructReader value ) {
         records.add( new Record( key, value ) ); 
     }
    
    @Override
    public void read( ChannelBuffer buff ) {

        super.read( buff );

        StreamReader reader = new StreamReader( buff );

        for( int i = 0; i < count; ++i ) {
            records.add( DefaultChunkReader.read( reader ) );
        }
        
    }

    @Override
    public void write( ChannelBufferWritable writer ) throws IOException {
        count = records.size();
        super.write( writer );

        for( Record record : records ) {
            DefaultChunkWriter.write( writer, record.getKey(), record.getValue() );
        }
        
    }

}