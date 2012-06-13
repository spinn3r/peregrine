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
package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;
import peregrine.util.*;
import peregrine.config.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * Complies with the ChunkReader interface but the results are bounded so that
 * you can only read `limit` items from the underlying delegate reader.
 */
public class LimitedChunkReader implements ChunkReader {

    private ChunkReader reader;

    private int index = 0;

    private int limit = 0;
    
    public LimitedChunkReader( int limit, ChunkReader reader ) {
                              
		this.limit = limit;
		this.reader = reader;
        
	}	

    @Override
    public boolean hasNext() throws IOException {
        return index < limit && reader.hasNext();
    }

    @Override
    public void next() throws IOException {
        reader.next();
    }

    @Override
    public StructReader key() throws IOException {
        return reader.key();
    }

    @Override
    public StructReader value() throws IOException {
        return reader.value();
    }

    @Override
    public int size() {
    	return limit;
    }

    @Override
    public void close() {
        // noop I think is the best strategy.
    }

    @Override
	public int keyOffset() throws IOException {
        return reader.keyOffset();
    }

    @Override
	public ChannelBuffer getBuffer() {
        return reader.getBuffer();
    }

}
