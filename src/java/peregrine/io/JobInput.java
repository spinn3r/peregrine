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
package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;

/**
 * Input for a job which provides a set of key/value pairs.
 */
public interface JobInput extends SequenceReader {

    /**
     * Add a listener so that we can can see which chunks are being read as 
     * they are open and closed.
     */
    public void addListener( ChunkStreamListener listener );

    public void addListeners( List<ChunkStreamListener> listeners );

    /**
     * Return the size (number of records as key/value pairs) that this JobInput
     * contains. 
     */
    public int count();
    
}

