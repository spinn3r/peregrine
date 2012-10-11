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
package peregrine.split;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.util.*;
import peregrine.util.*;
import peregrine.worker.*;

import org.jboss.netty.buffer.*;

/**
 * A region of a file which has a deterministic size and ends on a record
 * boundary.  This way we can parse the file easily.  We can also still use a
 * ChannelBuffer/ByteBuffer as a backing since we can make it each one less than
 * 2GB but aggregate them on files <b>larger</b> than 2GB.
 */
public class InputSplit implements Closeable {

    public long start = 0;
    public long end = 0;

    private ChannelBuffer buff;

    private File file;
    
    private FileInputStream fis;

    private FileChannel channel;

    public InputSplit( long start,
                       long end,
                       ChannelBuffer buff,
                       File file,
                       FileInputStream fis,
                       FileChannel channel ) {
        this.start = start;
        this.end = end;
        this.buff = buff;
        this.file = file;
        this.fis = fis;
        this.channel = channel;
    }

    public ChannelBuffer getChannelBuffer() {
        return buff;
    }
    
    public String toString() {
        return String.format( "%s, start=%,d , end=%,d" , file.getPath(), start, end );
    }

    @Override
    public void close() throws IOException {
        new Closer( fis, channel );
    }

}
