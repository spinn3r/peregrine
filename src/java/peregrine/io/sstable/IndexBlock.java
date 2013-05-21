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
import peregrine.util.Hex;
import peregrine.util.Integers;
import peregrine.util.Longs;
import peregrine.util.netty.ChannelBufferWritable;

import java.io.IOException;

public class IndexBlock extends BaseBlock {

    // the length in bytes of this block.
    private long length = -1;

    private byte[] firstKey = null;

    private int metaBlockOffset = 0;

    private int metaBlockLength = 0;

    public IndexBlock() { }

    public IndexBlock(byte[] firstKey) {
        this.firstKey = firstKey;
    }

    public long getLength() {
        return this.length;
    }

    public void setLength( long length ) {
        this.length = length;
    }

    public void setFirstKey( byte[] firstKey ) { 
        this.firstKey = firstKey;
    }

    public byte[] getFirstKey() { 
        return this.firstKey;
    }

    public int getMetaBlockOffset() {
        return metaBlockOffset;
    }

    public void setMetaBlockOffset(int metaBlockOffset) {
        this.metaBlockOffset = metaBlockOffset;
    }

    public int getMetaBlockLength() {
        return metaBlockLength;
    }

    public void setMetaBlockLength(int metaBlockLength) {
        this.metaBlockLength = metaBlockLength;
    }

    @Override
    public void read( ChannelBuffer buff ) {

        super.read( buff );

        StructReader sr = new StructReader( buff );

        length = sr.readLong();
        firstKey = sr.readBytes();
        metaBlockOffset = sr.readInt();
        metaBlockLength = sr.readInt();
        
    }

    @Override
    public void write( ChannelBufferWritable writer ) throws IOException {

        super.write( writer );

        StructWriter sw = new StructWriter( firstKey.length +
                                           (Integers.LENGTH * 3) +
                                           Longs.LENGTH );
        sw.writeLong(length);
        sw.writeBytes( firstKey );
        sw.writeInt(metaBlockOffset);
        sw.writeInt(metaBlockLength);
        
        //write the first key
        writer.write( sw.getChannelBuffer() );

    }

    @Override
    public String toString() {
        return String.format( "firstKey=%s, length=%,d, lengthUncompressed=%,d, offset=%,d, count=%,d",
                Hex.encode(firstKey), getLength(), getLengthUncompressed(), getOffset(), getCount());
    }

}