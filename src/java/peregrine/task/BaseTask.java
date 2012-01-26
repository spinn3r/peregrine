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

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.driver.shuffle.*;
import peregrine.rpc.*;
import peregrine.shuffle.sender.*;
import peregrine.sysstat.*;
import peregrine.task.*;

import com.spinn3r.log5j.*;

/**
 * Generic code shared across all tasks.
 */
public abstract class BaseTask implements Task {

    private static final Logger log = Logger.getLogger();

    protected Host host;
    
    protected Output output = null;

    protected List<JobOutput> jobOutput = null;

    protected List<BroadcastInput> broadcastInput = new ArrayList();
    
    protected List<ShuffleJobOutput> shuffleJobOutput = new ArrayList();
    
    protected Partition partition = null;

    protected Config config = null;

    protected TaskStatus status = TaskStatus.UNKNOWN;

    protected Throwable cause = null;

    /**
     * The job we should be running.
     */
    protected Class delegate = null;

    protected Input input = null;
    
    /**
     * The given work for this task.
     */
    protected Work work = null;
    
    protected JobDelegate jobDelegate = null;

    private boolean killed = false;

    /**
     * The time this job was started, in milliseconds.
     */
    protected long started = -1;
    
    public void init( Config config, Work work, Class delegate ) throws IOException {
    	this.config      = config;
        this.host        = config.getHost();
        this.work        = work;
        this.delegate    = delegate;
        this.started     = System.currentTimeMillis();

        for ( WorkReference current : work.getReferences() ) {
            
            if ( current instanceof PartitionWorkReference ) {
                partition = ((PartitionWorkReference)current).getPartition();
            }
            
        }

        if ( partition == null ) {
            throw new IOException( "No partition defined for work: " + work );
        }

    }

    public List<BroadcastInput> getBroadcastInput() { 
        return this.broadcastInput;
    }
    
    public Input getInput() { 
        return this.input;
    }

    public void setInput( Input input ) { 
        this.input = input;
    }    
    
    public Output getOutput() { 
        return this.output;
    }

    public void setOutput( Output output ) { 
        this.output = output;
    }
    
    public Work getWork() { 
        return this.work;
    }

    public void setWork( Work work ) { 
        this.work = work;
    }
    
    public List<JobOutput> getJobOutput() {
        return this.jobOutput;
    }

    public void setJobOutput( List<JobOutput> jobOutput ) {
        this.jobOutput = jobOutput;
    }

    public void setStatus( TaskStatus status ) {
        this.status = status;
    }

    public void handleFailure( Logger log, Throwable cause ) {
        log.error( String.format( "Unable to run delegate %s on %s", delegate, partition ), cause );
        setCause( cause );
        setStatus( TaskStatus.FAILED );
    }
    
    public void setCause( Throwable cause ) {

        // for now we only care about the first cause.
        if ( this.cause != null )
            return;
        
        this.cause = cause;
    }
    
    public void setup() throws Exception {

        jobDelegate = (JobDelegate)delegate.newInstance();
    	    		
        if ( output == null || output.size() == 0 ) {
            throw new Exception( "No output specified. " );
        }
    	
        this.jobOutput = JobOutputFactory.getJobOutput( config, partition, output );
       
        for( JobOutput current : jobOutput ) {

            log.info( "Job output: %s" , current );
            
            if ( current instanceof ShuffleJobOutput ) {
                shuffleJobOutput.add( (ShuffleJobOutput)current );
            }       
            
        }
        
        broadcastInput = BroadcastInputFactory.getBroadcastInput( config, getInput(), partition );
        
    }
    
    public Object call() throws Exception {

        SystemProfiler profiler = config.getSystemProfiler();

        try {

            log.info( "Running %s on %s", delegate, partition );
            
            setup();
            jobDelegate.setBroadcastInput( getBroadcastInput() );
            jobDelegate.setPartition( partition );
            jobDelegate.setConfig( config );
            jobDelegate.init( getJobOutput() );

            try {
                doCall();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
            	jobDelegate.cleanup();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
                teardown();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            // if nothing above has failed, we are complete... 
            if ( status == TaskStatus.UNKNOWN )
                setStatus( TaskStatus.COMPLETE );
            	
        } catch ( Throwable t ) { 
            handleFailure( log, t );
        } finally {
            report();

            log.info( "Ran with profiler rate: \n%s", profiler.rate() );
            
        }
        
        return null;
        
    }
    
    /**
     * Perform delegate specific call execution.
     * 
     * @throws Exception on a failure.
     */
    protected abstract void doCall() throws Exception;

    public void teardown() throws IOException {

        //TODO: close ALL of these jobs even if one of them fails and then
        //throw ALL exceptions.  Also we need to gossip here if it is failing
        //talking to a remote host.

        for( JobOutput current : jobOutput ) {
            log.info( "Closing job output: %s" , current );
            current.close();
        }

        log.info( "Closing job output...done" );

    }

    public void report() throws IOException {

        if ( status == TaskStatus.UNKNOWN ) {

            // no failures happened in the delegate, cleanup, and teardown so we
            // are complete.

            setStatus( TaskStatus.COMPLETE );
        }

        log.info( "Task %s on %s is %s", delegate, partition, status );

        if ( status == TaskStatus.COMPLETE ) {
            sendCompleteToController();
        } else if ( status == TaskStatus.FAILED ) {
            sendFailedToController( cause );
        } else {
            String msg = String.format( "Wrong status code: %s" , status );
            log.error( msg );
            throw new RuntimeException( msg );
        }

        // now measure memory usage for debug purposes

        System.gc(); /* Running GC at the end of tasks isn't going to kill
                        performance.  We might want to make this a configurable
                        though. */
        
        Runtime runtime  = Runtime.getRuntime();
        long freeMemory  = runtime.freeMemory();
        long totalMemory = runtime.totalMemory();
        long usedMemory  = totalMemory - freeMemory;
        
        log.info( "Memory footprint: used=%,d , free=%,d , total=%,d", usedMemory, freeMemory, totalMemory );

    }

    /**
     * Mark this task as killed.
     */
    @Override
    public void setKilled( boolean killed ) {
        this.killed = killed;
    }

    @Override
    public boolean isKilled() {
        return killed;
    }

    /**
     * Assert that we are alive and have not been marked killed by the
     * controller.
     */
    @Override
    public void assertAlive() throws IOException {

        if ( killed ) {
            throw new IOException( "This task was killed for partition: %s" + partition );
        }

    }
    
    /**
     * Mark the partition for this task complete.  
     */
    protected void sendCompleteToController() throws IOException {

        Message message = new Message();

        message.put( "action" ,   "complete" );
        message.put( "killed",    killed );

        sendMessageToController( message );

    }

    /**
     * Tell the controller that we failed (be a good citizen if we can).
     */
    protected void sendFailedToController( Throwable cause ) throws IOException {

        Message message = new Message();
        
        message.put( "action" ,     "failed" );
        message.put( "stacktrace",  cause );
        
        sendMessageToController( message );

    }

    /**
     * Tell the controller about our progress so we can resume if we crash.
     */
    protected void sendProgressToController( String nonce, String pointer ) throws IOException {

        Message message = new Message();
        
        message.put( "action" ,     "progress" );
        message.put( "nonce" ,      nonce );
        message.put( "pointer" ,    pointer );

        sendMessageToController( message );

    }

    protected void sendMessageToController( Message message ) throws IOException {
    	
        log.info( "Sending %s message to controller%s", message.get( "action" ), message );
        
        message.put( "host",        config.getHost().toString() );
        message.put( "partition",   partition.getId() );
        message.put( "input",       input.getReferences() );
        message.put( "work",        work.getReferences() );

        new Client().invoke( config.getController(), "controller", message );
    	
    }
    
}
