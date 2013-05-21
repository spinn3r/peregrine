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
import peregrine.util.netty.ChannelBufferWritable;

import java.io.IOException;

public class IndexBlock extends BaseBlock {
    
    private byte[] firstKey = null;

    public IndexBlock() { }

    public IndexBlock(byte[] firstKey) {
        this.firstKey = firstKey;
    }

    public void setFirstKey( byte[] firstKey ) { 
        this.firstKey = firstKey;
    }

    public byte[] getFirstKey() { 
        return this.firstKey;
    }

    @Override
    public void read( ChannelBuffer buff ) {

        super.read( buff );

        StructReader sr = new StructReader( buff );

        firstKey = sr.readBytes();
        
    }

    @Override
    public void write( ChannelBufferWritable writer ) throws IOException {

        super.write( writer );

        StructWriter sw = new StructWriter( firstKey.length + 4 );
        sw.writeBytes( firstKey );
        
        //write the first key
        writer.write( sw.getChannelBuffer() );

    }

    @Override
    public String toString() {
        return String.format( "%s, firstKey=%s", super.toString(), Hex.encode( firstKey ) );
    }

}