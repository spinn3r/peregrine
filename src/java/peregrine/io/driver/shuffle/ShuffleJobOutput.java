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
package peregrine.io.driver.shuffle;

import java.io.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;
import peregrine.task.*;
import peregrine.util.*;

import com.spinn3r.log5j.Logger;

public class ShuffleJobOutput
    implements JobOutput, ChunkStreamListener, Closeable, Flushable {

    private static final Logger log = Logger.getLogger();

    public Config config;

    public String name;

    protected Partition partition;

    protected ShuffleJobOutputDelegate jobOutputDelegate;

    protected ChunkStreamListener chunkStreamListener;

    public Job job = null;
    
    private long started = System.currentTimeMillis();

    private int emits = 0;

    private Report report;
    
    public ShuffleJobOutput( Config config, Job job, Partition partition, Report report ) {
        this( config, job, "default", partition, report );
    }

    public ShuffleJobOutput( Config config, Job job, ShuffleOutputReference outputReference, Partition partition, Report report ) {
    	this(config, job, outputReference.getName(), partition, report );
    }
    
    public ShuffleJobOutput( Config config, Job job, String name, Partition partition, Report report ) {
    	
        this.config = config;
        this.job = job;
        this.name = name;
        this.partition = partition;
        this.report = report;
        
        jobOutputDelegate = new ShuffleJobOutputDirect( this );

        chunkStreamListener = (ChunkStreamListener) jobOutputDelegate;
        
    }

    @Override
    public void emit( StructReader key , StructReader value ) {

        Hashcode.assertKeyLength( key );
        
        jobOutputDelegate.emit( key, value );
        ++emits;

        report.getEmitted().incr();
        
    }

    public void emit( int to_partition, StructReader key , StructReader value ) {
        jobOutputDelegate.emit( to_partition, key, value );
        ++emits;
    }

    @Override 
    public void flush() throws IOException {
        jobOutputDelegate.flush();
    }

    @Override 
    public void close() throws IOException {

        jobOutputDelegate.close();

        long now = System.currentTimeMillis();

        long duration = (now-started);

        long throughput = -1;

        if ( duration > 0 ) {
            throughput = (long)(( length() / (double)duration) * 1000 );
        }

        log.info( "Shuffled %,d entries (%,d bytes) from %s in %s %,d ms with throughput %,d b/s",
                  emits, length(), partition, this, duration, throughput );

    }

    @Override 
    public void onChunk( ChunkReference chunkRef ) {
        chunkStreamListener.onChunk( chunkRef );
    }

    @Override 
    public void onChunkEnd( ChunkReference chunkRef ) {
        chunkStreamListener.onChunkEnd( chunkRef );
    }

    @Override
    public String toString() {
        return jobOutputDelegate.toString();
    }

    public long length() {
        return jobOutputDelegate.length();
    }

    public Report getReport() {
        return report;
    }

    public Partition getPartition() {
        return partition;
    }
    
}

