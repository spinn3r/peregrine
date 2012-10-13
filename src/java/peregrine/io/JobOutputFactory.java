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
package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.driver.*;
import peregrine.io.driver.blackhole.*;
import peregrine.io.driver.broadcast.*;
import peregrine.io.driver.file.*;
import peregrine.io.driver.shuffle.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;
import peregrine.task.*;

/**
 * Factory for obtaining job output from a given Output definition.  
 *
 */
public class JobOutputFactory {

    public static List<JobOutput> getJobOutput( Config config,
                                                Job job,
                                                Partition partition,
                                                Output output,
                                                Reporter reporter ) throws IOException {

        List<JobOutput> result = new ArrayList( output.getReferences().size() );

        for( OutputReference ref : output.getReferences() ) {

            IODriver driver = IODriverRegistry.getInstance( ref.getScheme() );
            
            // see if it is registered as a driver.
            if ( driver != null ) {

                JobOutput delegate = driver.getJobOutput( config, job, ref, new PartitionWorkReference( partition ) );

                result.add( delegate );
                //result.add( new ReportingJobOutput( delegate, reporter ) );

                continue;
            }

            throw new IOException( "ref not supported: " + ref.getClass().getName() );

        }

        return result;
        
    }
    
}

/**
 * Wrapper delegate for JobOutput so that we don't forget to keep track of our
 * emit rate.
 */
class ReportingJobOutput implements JobOutput {

    private JobOutput delegate;
    private Reporter reporter;
    
    public ReportingJobOutput( JobOutput delegate, Reporter reporter ) {
        this.delegate = delegate;
        this.reporter = reporter;
    }

    @Override
    public void emit( StructReader key , StructReader value ) {

        reporter.getEmitted().incr();
        
        delegate.emit( key, value );
        
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

}