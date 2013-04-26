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
package peregrine.task;

import java.util.*;

import peregrine.config.*;
import peregrine.config.partitioner.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.util.*;
import peregrine.controller.*;

import com.spinn3r.log5j.Logger;

/**
 * A Report allows a job to report progress to the controller for tracking and
 * auditing purposes.
 */
public class Report implements Comparable<Report>, MessageSerializable {

    // the partition for which these reported stats belong
    protected Partition partition = new Partition(-1);
    
    // the number of consumed records. This should be ever increasing.  A job
    // MAY NOT actually be emitting anything but it DOES need to consume
    // records.
    protected Metric consumed = new Metric();
    
    // the number of emitted records for this job.
    protected Metric emitted = new Metric();

    // the total number of bytes we have emitted during this job.
    protected Metric emittedBytes = new Metric();
    
    // the progress of our job between 0 and 100.
    protected Metric progress = new Metric( 100 );

    public Report() {}

    public Report( Partition partition ) {
        this.partition = partition;
    }

    /**
     * Get the partition for this Report.
     */
    public Partition getPartition() {
        return partition;
    }
    
    public Metric getConsumed() {
        return consumed;
    }
    
    public Metric getEmitted() {
        return emitted;
    }

    public Metric getEmittedBytes() {
        return emittedBytes;
    }
    
    public Metric getProgress() {
        return progress;
    }

    @Override
    public int hashCode() {
        return partition.hashCode();
    }

    @Override
    public boolean equals( Object obj ) {

        if ( obj instanceof Report == false )
            return false;
        
        return partition.equals( ((Report)obj).partition );
        
    }

    @Override
    public int compareTo( Report r ) {
        return partition.compareTo( r.partition );
    }

    @Override
    public String toString() {
        return String.format( "consumed: %s, emitted: %s, emittedBytes: %s, progress: %s", consumed, emitted, emittedBytes, progress );
    }

    public Report plus( Report val ) {

        Report result = new Report();

        result.consumed.set( consumed.get() + val.consumed.get() );
        result.emitted.set( emitted.get() + val.emitted.get() );
        result.emittedBytes.set( emittedBytes.get() + val.emittedBytes.get() );
        result.progress.set( progress.get() + val.progress.get() );

        return result;

    }
    
    public Message toMessage() {

        Message message = new Message();
        message.put( "consumed"  ,     consumed.get() );
        message.put( "emitted"   ,     emitted.get() );
        message.put( "emittedBytes" ,  emittedBytes.get() );
        message.put( "progress"  ,     progress.get() );
        message.put( "partition" ,     partition.getId() );

        return message;
        
    }

    public void fromMessage( Message message ) {

        consumed.set( message.getLong( "consumed" ) );
        emitted.set( message.getLong( "emitted" ) );
        emittedBytes.set( message.getLong( "emittedBytes" ) );
        progress.set( message.getLong( "progress" ) );
        partition = new Partition( message.getInt( "partition" ) );
        
    }

    public class Metric {

        private long value;

        private long max = Long.MAX_VALUE;

        public Metric() {}

        public Metric( long max ) {
            this.max = max;
        }

        public void incr() {
            ++value;
        }

        public void incr( long delta ) {
            value += delta;
        }
        
        public long get() {
            return value;
        }

        public String getFormatted() {
            return Longs.format( get() );
        }

        public String getFormattedBytes() {
            return Longs.formatBytes( get() );
        }

        public void set( long value ) {
            this.value = value;
        }
        
        @Override
        public String toString() {
            return "" + value;
        }
        
    }
    
}
