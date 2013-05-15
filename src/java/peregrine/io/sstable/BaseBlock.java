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

import peregrine.*;
import peregrine.os.*;
import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;

public abstract class BaseBlock {

    // the length in bytes of this block.
    public long length = -1;

    // the length of this block uncompressed.  This allows us to compare the
    // number of compressed bytes to the length to determine the compression
    // ratio.
    public long lengthUncompressed = -1;
    
    // the offset within the parent file of this block
    public long offset = -1;

    // the number of records in this block
    public int count = 0;

    public long getLength() { 
        return this.length;
    }

    public void setLength( long length ) { 
        this.length = length;
    }

    public long getLengthUncompressed() { 
        return this.lengthUncompressed;
    }

    public void setLengthUncompressed( long lengthUncompressed ) { 
        this.lengthUncompressed = lengthUncompressed;
    }

    public long getOffset() { 
        return this.offset;
    }

    public void setOffset( long offset ) { 
        this.offset = offset;
    }

    public int getCount() { 
        return this.count;
    }

    public void setCount( int count ) { 
        this.count = count;
    }

    public void read( ChannelBuffer buff ) {

        StructReader sr = new StructReader( buff );

        length = sr.readLong();
        lengthUncompressed = sr.readLong();
        offset = sr.readLong();
        count = (int)sr.readLong();
        
    }

    public void write( ChannelBufferWritable writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );
        sw.writeLong( length );
        sw.writeLong( lengthUncompressed );
        sw.writeLong( offset );
        sw.writeLong( (long)count );

        writer.write( sw.getChannelBuffer() );

    }

    @Override
    public String toString() {
        return String.format( "length=%,d, lengthUncompressed=%,d, offset=%,d, count=%,d",
                              length, lengthUncompressed, offset, count );
    }

}