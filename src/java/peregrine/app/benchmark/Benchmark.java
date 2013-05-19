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
package peregrine.app.benchmark;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

public class Benchmark {

    private static final Logger log = Logger.getLogger();

    public static class Extract extends Mapper {

        @Override
        public void init( Job job, List<JobOutput> output ) {

            super.init( job, output );
            
            int max    = job.getParameters().getInt( "max" );
            int width  = job.getParameters().getInt( "width" );

            StructReader value = StructReaders.wrap( new byte[ width ] );

            int nr_partitions = config.getMembership().size();

            int total = max / nr_partitions;
            
            for( long i = 0; i < total; ++i ) {
                
                StructReader key  = StructReaders.hashcode( i );
                emit( key, value );

            }

        }
        
    }
    
    public static class Map extends Mapper {
        
        @Override
        public void map( StructReader key,
                         StructReader value ) {

            emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        int count = 0;
        
        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            List<Integer> ints = new ArrayList();

            // decode these so we know what they actually mean.
            for( StructReader val : values ) {
                ints.add( val.readInt() );
            }

            ++count;

        }

        @Override
        public void close() throws IOException {

            if ( count == 0 )
               throw new RuntimeException( "count is zero" );
            
        }

    }

    private Config config;
    
    public Benchmark( Config config ) {
        this.config = config;
    }

}
