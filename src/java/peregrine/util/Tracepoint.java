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
package peregrine.util;

import java.util.*;
import java.util.regex.*;

import peregrine.util.netty.*;

/**
 * A Tracepoint is basically a stacktrace which we manually emit to the log
 * along with a hashcode for debug purposes to trace every unique invocation of
 * a method.
 * 
 * This can be value for tracking down bleeding resources.
 */
public class Tracepoint {

    StringBuffer buff = new StringBuffer();

    public Tracepoint( Object... args ) {

        StackTraceElement[] frames = Thread.currentThread().getStackTrace();

        for( int i = 2; i < frames.length; ++i ) {

            StackTraceElement frame = frames[i];

            buff.append( "\t" );
            buff.append( frame.toString() );
            buff.append( "\n" );
            
        }

        String stacktrace = buff.toString();

        buff = new StringBuffer();

        String tp = Base64.encode( Hashcode.getHashcode( stacktrace ) );
        
        buff.append( String.format( "Tracepoint: %s (", tp ) );

        for( int i = 0; i < args.length; i=i + 2 ) {
            buff.append( String.format( "%s = %s, ", args[i], args[i + 1] ) );
        }

        buff.append( ")\n" );

        buff.append( stacktrace );

        //buff.append( String.format( "END Tracepoint: %s\n", tp ) );
        
    }

    public String toString() {
        return buff.toString();
    }

    public static void main( String[] args ) throws Exception {

        String path = args[0];

        CharSequence seq = new FileCharSequence( path );
        Pattern p = Pattern.compile( "(?m)Tracepoint: (.*)\n(\t.*\n)+" );
        Matcher m = p.matcher( seq );

        Map<String,String> traces = new HashMap();
        Map<String,Integer> hits  = new HashMap();

        List<String> keys = new ArrayList();
        
        while( m.find() ) {

            String key = m.group( 1 ) ;

            if ( ! traces.containsKey( key ) ) {
                
                String trace = m.group( 0 );

                keys.add( key );
                traces.put( key, trace );
                
            }

            if ( hits.containsKey( key ) ) {
                hits.put( key, hits.get( key ) + 1 );
            } else {
                hits.put( key, 1 );
            }

        }

        System.out.printf( "Found %,d tracepoints.\n", traces.size() );
        System.out.printf( "---\n" );

        for( String key : keys ) {
            System.out.printf( "%s", traces.get( key ) );
        }

    }

}

