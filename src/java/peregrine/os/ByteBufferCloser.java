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
package peregrine.os;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.http.*;
import peregrine.util.netty.*;
import peregrine.io.util.*;
import peregrine.config.*;

import com.spinn3r.log5j.Logger;

/**
 * A closeable which is smart enough to work on byte buffers. 
 */
public class ByteBufferCloser implements Closeable {

    private ByteBuffer buff = null;

    public ByteBufferCloser( ChannelBuffer buff ) {

        if ( buff instanceof ByteBufferBackedChannelBuffer )
            this.buff = buff.toByteBuffer();

    }

    public ByteBufferCloser( ByteBuffer buff ) {
        this.buff = buff;
    }
    
    @Override
    public void close() throws IOException {

        if ( buff == null )
            return;
        
        sun.misc.Cleaner cl = ((sun.nio.ch.DirectBuffer)buff).cleaner();

        if (cl != null) {
            cl.clean();
        }
        
    }

}
