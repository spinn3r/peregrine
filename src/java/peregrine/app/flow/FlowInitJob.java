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
package peregrine.app.flow;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.io.*;

import com.spinn3r.log5j.Logger;

public class FlowInitJob {

    private static final Logger log = Logger.getLogger();

    public static class Map extends Mapper {

        Set<StructReader> nodes = new HashSet();
        
        @Override
        public void init( Job job, List<JobOutput> output ) {

            super.init( job, output );

            if ( ! job.getParameters().containsKey( "sources" ) ) {
                throw new RuntimeException( "No sources specified" );
            }

            String[] sources = job.getParameters().getString( "source" ).split( ":" );

            for( String source : sources ) {
                nodes.add( StructReaders.hashcode( source ) );
            }

            log.info( "Working with %,d source nodes.", nodes.size() );

        }

        @Override
        public void map( StructReader key, StructReader value ) {

            if ( nodes.contains( key ) == false ) {
                return;
            }
            
            List<StructReader> outbound = StructReaders.split( value, Hashcode.HASH_WIDTH );

            for( StructReader target : outbound ) {
                emit( target, StructReaders.wrap( true ) );
            }
            
        }

    }

    public static class Reduce extends Reducer {
        
        @Override
        public void reduce( StructReader key, List<StructReader> values ) {
            emit( key, StructReaders.wrap( true ) );
        }
        
    }

}
