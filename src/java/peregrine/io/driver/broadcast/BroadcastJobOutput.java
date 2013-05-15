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
package peregrine.io.driver.broadcast;

import java.io.*;

import peregrine.*;
import peregrine.task.*;
import peregrine.config.*;
import peregrine.io.driver.shuffle.*;

import com.spinn3r.log5j.*;

public class BroadcastJobOutput extends ShuffleJobOutput {

    private static final Logger log = Logger.getLogger();

    private Report report;
    
    public BroadcastJobOutput( Config config, Job job, Partition partition, Report report ) {
        this( config, job, "default", partition, report );
    }
        
    public BroadcastJobOutput( Config config, Job job, String name, Partition partition, Report report ) {
        super( config, job, name, partition, report );
        this.report = report;
    }
    
    @Override
    public void emit( StructReader key , StructReader value ) {

        Membership membership = config.getMembership();

        for ( Partition target : membership.getPartitions() ) {
            emit( target.getId() , key, value );
            key.reset();
            value.reset();
        }

        report.getEmitted().incr();

    }

    @Override
    public String toString() {
        return String.format( "%s:%s@%s", getClass().getName(), name, Integer.toHexString(hashCode()) );
    }

    @Override 
    public void close() throws IOException {
        super.close();
    }
    
}

