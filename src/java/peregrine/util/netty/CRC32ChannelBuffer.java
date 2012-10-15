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
package peregrine.util.netty;

import org.jboss.netty.buffer.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class CRC32ChannelBuffer extends CompositeChannelBuffer {

    private Long checksum = null;
    
    public CRC32ChannelBuffer( ChannelBuffer buff,
                               final int page_size ) {

        super( buff.order(), split( buff, page_size ) );
        
    }

    /**
     * The default mode is to only compute the checksum, If we call setChecksum
     * we also verify the the resulting checksum is correct after the buffer is
     * read off disk.
     */
    public void setChecksum( long checksum ) {
        this.checksum = new Long( checksum );
    }

    /**
     * Take the input ChannelBuffer and split it into pages.  When they are
     * first accessed we will then CRC32 them.
     */
    public static List<ChannelBuffer> split( ChannelBuffer buff, int page_size ) {

        int size = (int)Math.ceil( buff.writerIndex() / (double)page_size );
        
        List<ChannelBuffer> result = new ArrayList( size );

        for( int i = 0; i < size; ++i ) {

            int len = page_size;
            
            if ( buff.readerIndex() + page_size > buff.writerIndex() ) {
                len = buff.writerIndex() - buff.readerIndex();
            }

            result.add( buff.readSlice( len ) );
            
        }

        return result;

    }
    
}
