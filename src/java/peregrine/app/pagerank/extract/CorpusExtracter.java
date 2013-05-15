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
package peregrine.app.pagerank.extract;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.worker.*;

import org.jboss.netty.buffer.*;

/**
 * Read from the 'corpus' file format which is source_node=target_node0:target_node1\n
 */
public class CorpusExtracter {

    private static Pattern pattern = Pattern.compile( "[^ ]+$" );

    private ParserListener listener = null;

    private StreamReader reader = null;

    public CorpusExtracter( String path, ParserListener listener ) throws IOException {
        this.listener = listener;

        MappedFileReader mappedFileReader = new MappedFileReader( path );

        this.reader = new StreamReader( mappedFileReader.map() );

    }

    public CorpusExtracter( ChannelBuffer buff, ParserListener listener ) {
        this.reader = new StreamReader( buff );
        this.listener = listener;
    }
    
    public void exec() throws Exception {

        int skipped = 0;
        int indexed = 0;

        while( true ) {

            String line = reader.readLineAsString();

            if ( line == null )
                break;

            String[] split;

            Matcher matcher = pattern.matcher( line );

            if ( matcher.find() ) {
                line = matcher.group( 0 );
            }
            
            split = line.split( "=" );

            if ( split.length <= 1 ) {
                ++skipped;
                continue;
            }
            
            String source  = split[0];
            List<String> targets = Strings.split( split[1], ":" );

            listener.onEntry( source, targets );
            
            ++indexed;
            
        }

    }

    public static void main( String[] args ) throws Exception {

        String path = args[0];
        new CorpusExtracter( path , new ParserListener() {

                @Override
                public void onEntry( String source, List<String> targets ) throws Exception {
                    System.out.printf( "%s=%s\n", source, targets );
                }

            } ).exec();
        
    }

}
