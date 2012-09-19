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
package peregrine.app.pagerank.extract;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.IntBytes;
import peregrine.io.*;
import peregrine.split.*;

import com.spinn3r.log5j.Logger;

/**
 * A job which takes an input file, parses the splits, then emits them.
 */
public class CorpusExtractJob {

    private static final Logger log = Logger.getLogger();

    public static class Map extends Mapper implements ParserListener {

        @Override
        public void init( Job job, List<JobOutput> output ) {

            try {
            
                String path = job.getParameters().getString( "path" );
                
                if ( path == null ) 
                    throw new NullPointerException( "path" );
                
                InputSplitter splitter = new InputSplitter( path, new LineBasedRecordFinder() );

                List<InputSplit> splits = splitter.getInputSplitsForPartitions( config, getPartition() );

                log.info( "Found %,d input splits", splits.size() );

                for( InputSplit split : splits ) {

                    log.info( "Processing split: %s", split );
                    CorpusExtracter extracter = new CorpusExtracter( split.getChannelBuffer(), this );
                    extracter.exec();
                    
                }
                
            } catch ( Exception e ) {
                log.error( "Unable to run job: ", e );
                throw new RuntimeException( e );
            }
            
        }

        @Override
        public void onEntry( String source, List<String> targets ) throws Exception {

            StructReader key = StructReaders.hashcode( source );
            
            StructReader value = StructReaders.hashcode( Strings.toArray( targets ) );

            log.info( "Going to emit.." );
            
            emit( key, value );
            
        }
        
        @Override
        public void map( StructReader key, StructReader value ) {
            //noop for now.
        }

    }

    public static class Reduce extends Reducer {

    }

}
