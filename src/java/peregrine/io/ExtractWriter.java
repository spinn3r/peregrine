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
package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.partition.*;
import peregrine.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Take a given stream of input in the form of (K,V) and route it and write the
 * data to the correct partition.  If your data is already partitioned, with the correct
 * algorithm, at least right now you can run these in parallel
 */
public class ExtractWriter implements Closeable, SequenceWriter {

    private static final Logger log = Logger.getLogger();

    private List<DefaultPartitionWriter> output;

    private Config config;

    private PartitionRouteHistograph partitionWriteHistograph;
    
    public ExtractWriter( Config config, String path ) throws IOException {

        this.config = config;
        Membership membership = config.getMembership();
        
        output = new ArrayList( membership.size() );
        
        for( Partition partition : membership.getPartitions() ) {

            log.info( "Creating writer for partition: %s", partition );

            DefaultPartitionWriter writer = new DefaultPartitionWriter( config, partition, path );
            output.add( writer );
            
        }

        partitionWriteHistograph = new PartitionRouteHistograph( config );
        
    }
    
    /**
     * If the Key is already a hashcode and we can route over it specify keyIsHashcode=true.
     */
    @Override
    public void write( StructReader key, StructReader value )
        throws IOException {

        Partition partition = config.partition( key, value );
        
        write( partition, key, value );
        
    }

    private void write( Partition part, StructReader key, StructReader value )
        throws IOException {

        Hashcode.assertKeyLength( key );
        
        partitionWriteHistograph.incr( part );

        output.get( part.getId() ).write( key, value );
        
    }

    public long length() {

        long result = 0;
        
        for( PartitionWriter writer : output ) {
            result += writer.length();
        }

        return result;
        
    }

    @Override
    public void flush() throws IOException { }

    @Override
    public void close() throws IOException {

        //TODO: not sure why but this made it slower.
        for( PartitionWriter writer : output ) {
            writer.shutdown();
        }
        
        for( PartitionWriter writer : output ) {
            writer.close();
        }

        log.info( "Partition write histograph: %s" , partitionWriteHistograph );
        
    }

}
