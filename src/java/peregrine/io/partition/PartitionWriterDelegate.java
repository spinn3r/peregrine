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
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.config.Partition;
import peregrine.util.netty.*;

/**
 * Delegates performing the actual IO to a given subsystem.  Local or remote. 
 */
public interface PartitionWriterDelegate {

    /**
     * Init the writer delegate with the given partition host and path.
     */
    public void init( Config config,
                      Partition partition,
                      Host host,
                      String path ) throws IOException;

    public void erase() throws IOException;

    /**
     * Enable append mode and return the chunk ID we should start writing to.
     */
    public int append() throws IOException;
    
    public ChannelBufferWritable newChunkWriter( int chunk_id ) throws IOException;

    public Host getHost();

}

