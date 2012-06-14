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
package peregrine.app.wikirank;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.nio.*;
import java.nio.channels.*;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.worker.*;
import peregrine.os.*;

import org.jboss.netty.buffer.*;

/**
 * Parse out the wikipedia sample data.
 */
public abstract class BaseParser<T> {

    private Pattern p = null;
    
    private Matcher m = null;
    
    private List<InputSplit> splits = null;

    private FileInputStream fis = null;

    private FileChannel channel = null;

    /**
     * 
     * 
     *
     */
    public BaseParser( String path, String regexp ) throws IOException {

        this.fis = new FileInputStream( path );
        this.channel = fis.getChannel();

        this.p = Pattern.compile( regexp );

        System.out.printf( "Going to read: %s\n", path );

        Splitter splitter = new Splitter( path );

        splits = splitter.getInputSplits();

        nextSplit();
        
    }

    private void nextSplit() throws IOException {

        InputSplit split = splits.remove( 0 );

        System.out.printf( "Working with split: %s\n", split );
        
        long length = split.end - split.start;
        ByteBuffer buff = channel.map( FileChannel.MapMode.READ_ONLY, split.start , length );

        ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer( buff );
        
        CharSequence sequence = new ChannelBufferCharSequence( channelBuffer, (int)length );

        m = p.matcher( sequence );

    }
    
    public T next() throws IOException {

        if ( m.find() ) {
            return newInstance( m );
        }

        if ( splits.size() > 0 ) {
            nextSplit();
            return next();
        }
        
        return null;

    }

    /**
     * Create a new result object instance from the given matcher.
     */
    public abstract T newInstance( Matcher m );

}

