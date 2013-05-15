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
package peregrine.app.pagerank.compact;

import java.io.*;

import peregrine.*;
import peregrine.controller.*;
import peregrine.io.*;

import com.spinn3r.log5j.Logger;

/**
 * Build a 'compaction' of two graphs by merging the data from both imput files
 * and merging the records together.
 */
public class Compact extends Batch {
    
    private static final Logger log = Logger.getLogger();

    public Compact( String minor, String major, String output ) {

        super( Compact.class );
        
        merge( new Job().setDelegate( CompactMergeJob.Merge.class ) 
                        .setInput( minor, major )
                        .setOutput( output ) );

    }

}
