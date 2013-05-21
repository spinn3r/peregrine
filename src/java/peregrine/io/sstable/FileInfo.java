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
import peregrine.util.*;
import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;

/**
 * Because of the lastKey and comparatorClass this field is variable width.
 */
public class FileInfo {

    private int meanKeyLength = -1;

    private int meanValueLength = -1;

    // the last key in the entire SSTable.
    private byte[] lastKey = new byte[0];

    // default is no comparator class specified.  
    private byte[] comparatorClass = new byte[0];

    public int getMeanKeyLength() { 
        return this.meanKeyLength;
    }

    public void setMeanKeyLength( int meanKeyLength ) { 
        this.meanKeyLength = meanKeyLength;
    }

    public int getMeanValueLength() { 
        return this.meanValueLength;
    }

    public void setMeanValueLength( int meanValueLength ) { 
        this.meanValueLength = meanValueLength;
    }

    public void setLastKey( byte[] lastKey ) { 
        this.lastKey = lastKey;
    }

    public byte[] getLastKey() { 
        return this.lastKey;
    }

    public byte[] getComparatorClass() { 
        return this.comparatorClass;
    }

    public void setComparatorClass( byte[] comparatorClass ) { 
        this.comparatorClass = comparatorClass;
    }

    public void read( ChannelBuffer buff, Trailer trailer ) {

       // duplicate the buffer so the global readerIndex isn't updated.
        buff = buff.duplicate();

        buff.readerIndex( trailer.getFileInfoOffset() );

        StructReader sr = new StructReader( buff );
        
        meanKeyLength = sr.readInt();
        meanValueLength = sr.readInt();
        lastKey = sr.readBytes();
        comparatorClass = sr.readBytes();
        
    }

    public void write( ChannelBufferWritable writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );

        sw.writeInt( meanKeyLength );
        sw.writeInt( meanValueLength );
        sw.writeBytes( lastKey );
        sw.writeBytes( comparatorClass );

        writer.write( sw.getChannelBuffer() );
        
    }

    @Override
    public String toString() {

        return String.format( "meanKeyLength: %s, meanValueLength: %s, lastKey: %s, comparatorClass: '%s'" ,
                              meanKeyLength,
                              meanValueLength,
                              Hex.encode( lastKey ),
                              new String( comparatorClass ) );
        
    }
    
}