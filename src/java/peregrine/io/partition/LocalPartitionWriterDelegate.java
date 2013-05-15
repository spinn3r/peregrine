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
package peregrine.io.partition;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

import peregrine.config.*;
import peregrine.http.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;
import peregrine.os.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.*;

/**
 * Write to a logical partition which is a stream of chunk files.
 */
public class LocalPartitionWriterDelegate extends BasePartitionWriterDelegate {

	private static final Logger log = Logger.getLogger();
  
    private Config config;
    private boolean autoSync;
    
    public LocalPartitionWriterDelegate( Config config, boolean autoSync ) {
		super();
		this.config = config;
        this.autoSync = autoSync;
	}
    
    @Override
    public String toString() {
        return path;
    }

    // PartitionWriterDelegate

    @Override
    public int append() throws IOException {

        List<File> chunks = LocalPartition.getChunkFiles( config, partition, path );

        return chunks.size();

    }
    
    @Override
    public void erase() throws IOException {

        List<File> chunks = LocalPartition.getChunkFiles( config, partition, path );

        for ( File chunk : chunks ) {
            
            if ( ! chunk.delete() )
                throw new IOException( "Unable to remove local chunk: " + chunk );
            
        }

    }

    @Override
    public ChannelBufferWritable newChunkWriter( int chunk_id ) throws IOException {
    	
    	File file = LocalPartition.getChunkFile( config, partition, path, chunk_id );

        log.info( "Creating new local chunk writer: %s" , file );

        Files.mkdirs( file.getParent() );

        if ( ! file.exists() ) {

            try {
                file.createNewFile();
            } catch ( Exception e ) {
                throw new IOException( "Unable to create file: " + file.getPath(), e );
            }
            
        }

        MappedFileWriter mappedFile = new MappedFileWriter( config, file );
        mappedFile.setAutoSync( autoSync );
        
        return mappedFile;
        
    }

}
