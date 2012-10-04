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
import java.nio.channels.*;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;
import peregrine.io.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;

/**
 * Write key/value pairs to a given file on disk and include any additional
 * metadata (size, etc).
 */
public class DefaultChunkWriter implements ChunkWriter, JobOutput {

    public static int BUFFER_SIZE = 16384;
    
    protected ChannelBufferWritable writer = null;

    private int count = 0;

    protected long length = 0;

    private boolean closed = false;

    private boolean shutdown = false;

    private Throwable closedCause = null;
    
    public DefaultChunkWriter( ChannelBufferWritable writer ) throws IOException {
        init( writer );
    }

    public DefaultChunkWriter( Config config, File file ) throws IOException {
        init( new MappedFileWriter( config, file ) );
    }

    private void init( ChannelBufferWritable writer ) {
        this.writer = new BufferedChannelBufferWritable( writer, BUFFER_SIZE );
    }

    @Override /* JobOutput */
    public void emit( StructReader key, StructReader value ) {

        try {
            write( key, value );
        } catch ( IOException e ) {
            throw new RuntimeException( e ) ;
        }

    }
    
    @Override
    public void write( StructReader key, StructReader value )
        throws IOException {

        if ( closed )
            throw new IOException( "closed", closedCause );

        length += write( writer, key, value );
        
        ++count;

    }

    /**
     * Perform a DIRECT write on a ChannelBuffer of a key/value pair.
     */
    public static int write( ChannelBufferWritable writer ,
                             StructReader key,
                             StructReader value ) throws IOException {
        
    	int result = 0;

        // TODO: we have to use an atomic write so that the buffered writer can
        // make sure to get one key/value pair without truncating it
        // incorrectly.
        
        ChannelBuffer wrapped =
            ChannelBuffers.wrappedBuffer( StructReaders.varint( key.length() ).getChannelBuffer(),
                                          key.getChannelBuffer(),
                                          StructReaders.varint( value.length() ).getChannelBuffer(),
                                          value.getChannelBuffer() );

        writer.write( wrapped );
        
        result += wrapped.writerIndex();

        return result;
        
    }

    @Override
    public long length() {
        return length;
    }

    public void shutdown() throws IOException {

        if ( shutdown )
            return;
        
        // last four bytes store the number of items.

        ChannelBuffer buff = ChannelBuffers.buffer( IntBytes.LENGTH );
        buff.writeInt( count );
        writer.write( buff );
        length += IntBytes.LENGTH;

        writer.shutdown();
        
        shutdown = true;
        
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;

        shutdown();

        writer.close();

        closedCause = new Exception( "FIXME" );
        closed = true;

    }

}
