/*
 * Copyright 2012 Kevin A. Burton
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

import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;

import org.jboss.netty.buffer.*;

/**
 * Char sequence backed by a file ChannelBuffer.
 */
public class ChannelBufferCharSequence implements CharSequence {

    private ChannelBuffer buff = null;
    private int length;
    
    public ChannelBufferCharSequence( ChannelBuffer buff, int length ) {
        this.buff = buff;
        this.length = length;
    }
    
    public char charAt(int index) {

        System.out.printf( "FIXME: %s\n", index );
        
        return buff.getChar( index );

    }

    public int length() {
        return length;
    }
    
    public CharSequence subSequence( int start, int end ) {

        int len = end - start;
        return new ChannelBufferCharSequence( buff.slice( start, len ), len );
    }

    public String toString() {
        return buff.toString( Charset.defaultCharset() );
    }
    
}
