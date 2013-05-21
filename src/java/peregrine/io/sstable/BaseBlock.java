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

import org.jboss.netty.buffer.ChannelBuffer;
import peregrine.StructReader;
import peregrine.StructWriter;
import peregrine.util.netty.ChannelBufferWritable;

import java.io.IOException;

public abstract class BaseBlock {

    // the number of records in this block.  This applies to data blocks since
    // they have records and meta blocks since they have records too.
    private int count = 0;

    // the checksum for this block.
    private long checksum = 0;

    public int getCount() {
        return this.count;
    }

    public void setCount( int count ) { 
        this.count = count;
    }

    public long getChecksum() {
        return checksum;
    }

    public void setChecksum(long checksum) {
        this.checksum = checksum;
    }

    public void incrCount() {
        ++count;
    }

    public void read( ChannelBuffer buff ) {

        StructReader sr = new StructReader( buff );

        count = (int)sr.readLong();
        checksum = sr.readLong();
        
    }

    public void write( ChannelBufferWritable writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );
        sw.writeLong(count);
        sw.writeLong(checksum);

        writer.write( sw.getChannelBuffer() );

    }

}