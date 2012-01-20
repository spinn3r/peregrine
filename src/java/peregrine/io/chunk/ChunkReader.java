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

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.io.*;

/**
 * Interface for reading data from a chunk in key/value form.
 */
public interface ChunkReader extends SequenceReader {

	/**
	 * Return the last offset of the start of the last read key.
	 */
	public int keyOffset() throws IOException;

	/**
	 * Return the number of unique key/value pairs.
	 * 
	 */
	public int size() throws IOException;

    /**
     * Get the backing channel buffer for performing random reads once this
     * ChunkReader is mlocked.
     */
	public ChannelBuffer getBuffer();
	
}
