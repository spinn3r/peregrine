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
package peregrine.controller;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.http.*;
import peregrine.io.*;
import peregrine.io.driver.shuffle.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.task.*;

import com.spinn3r.log5j.*;

/**
 * 
 * Main interface for running a map reduce job in Peregrine.  The controller
 * communicates with worker nodes which will then in turn will run jobs with
 * your specified {@link Mapper}, {@link Merger} and/or {@link Reducer}.
 * 
 */
public class Controller {

    private static final Logger log = Logger.getLogger();

    private static final int MAX_HISTORY = 200;
    
    private Config config = null;

    private ControllerDaemon daemon = null;
    
    protected ClusterState clusterState;

    /**
     * True if we are shutdown to avoid multiple shutdown attempts.
     */
    protected boolean shutdown = false;

    /**
     * By default, specify which jobs we should execute.
     */
    protected ExecutionRange executionRange = new ExecutionRange();

    /**
     * Keep track of executed items.  Note that unless this is cleared, if we
     * keep running jobs we will run out of memory.  We should probably have an
     * internal upper bound on the max number of items.
     */
    protected ConcurrentLinkedQueue<Batch> history = new ConcurrentLinkedQueue();

    /**
     * The currently executing batch.
     */
    protected Batch executing = null;

    /**
     * Batch jobs that are pending execution.
     */
    protected ConcurrentLinkedQueue<Batch> pending = new ConcurrentLinkedQueue();

    public Controller() {}
    
    public Controller( Config config ) {
    	
        this.config = config;

        // verify that we aren't running on the right host.  Starting up a
        // controller on the wrong machine doesn't make sense and the job won't
        // work.
        
        String hostname = config.getHost().getName();
        String controllername = config.getController().getName();

        if ( ! hostname.equals( controllername ) ) {
            throw new RuntimeException( String.format( "Starting controller on incorrect host( %s vs %s )",
                                                       hostname, controllername ) );
        }

        // make sure to set the host to the controller so we use the right port.
        config.setHost( config.getController() );

        log.info( "Starting controller: %s", config.getController() );

        this.clusterState = new ClusterState( config, this );

        this.daemon = new ControllerDaemon( this, config, clusterState );

    }

    public void setExecutionRange( int start, int end ) {

        this.executionRange = new ExecutionRange( start, end );

        log.info( "Using execution range: %s", this.executionRange );
        
    }

    public void setExecutionRange( String executionRange ) {

        if ( executionRange == null || "".equals( executionRange ) )
            return;

        String[] split = executionRange.split( ":" );

        int start = Integer.parseInt( split[0] );
        int end   = Integer.parseInt( split[1] );
        
        setExecutionRange( start, end );
        
    }

    public Config getConfig() {
        return config;
    }

    public Collection<Batch> getHistory() {
        return history;
    }

    public Queue<Batch> getPending() {
        return pending;
    }

    public Batch getExecuting() {
        return executing;
    }

    public void setExecuting( Batch executing ) {
        this.executing = executing;
    }
    
    public void map( Class mapper,
                     String... paths ) throws Exception {

        exec( new Batch().map( mapper, paths ) );

    }

    public void map( Class mapper,
                     Input input ) throws Exception {

        exec( new Batch().map( mapper, input ) );

    }
    
    public void map( Class delegate,
    		 		 Input input,
    				 Output output ) throws Exception {

        exec( new Batch().map( delegate, input, output ) );

    }
    
    /**
     * Run map jobs on all chunks on the given path.
     */
    public void map( Job job ) throws Exception {
        exec( new Batch().map( job ) );
    }
    
    public void merge( Class mapper,
                       String... paths ) throws Exception {

        merge( mapper, new Input( paths ) );

    }

    public  void merge( Class mapper,
                        Input input ) throws Exception {

        merge( mapper, input, null );

    }

    public void merge( Class delegate,
                       Input input,
                       Output output ) throws Exception {

        exec( new Batch().merge( delegate, input, output ) );

    }

    /**
     * 
     * Conceptually, <a href='http://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join'>
     * a full outer join</a> combines the effect of applying both left
     * and right outer joins. Where records in the FULL OUTER JOINed tables do not
     * match, the result set will have NULL values for every column of the table
     * that lacks a matching row. For those records that do match, a single row
     * will be produced in the result set (containing fields populated from both
     * tables)
     */
    public void merge( Job job ) throws Exception {
        exec( new Batch().merge( job ) );
    }
  
    public void reduce( Class delegate,
    				    Input input,
    				    Output output ) throws Exception {

        exec( new Batch().reduce( delegate, input, output ) );

    }

    public void touch( String output ) throws Exception {
        exec( new Batch().touch( output ) );
    }
    
    public void sort( String input, String output, Class comparator ) throws Exception {
        exec( new Batch().sort( input, output, comparator ) );
    }

    /**
     * Perform a reduce over the previous shuffle data (or broadcast data).
     */
    public void reduce( Job job ) throws Exception {
        exec( new Batch().reduce( job ) );
    }

    /**
     * Execute a batch of jobs.
     */
    public void exec( Batch batch ) throws Exception {

        if ( batch.getJobs().size() == 0 ) {
            throw new Exception( "Batch has no jobs" );
        }
        
        setExecuting( batch );

        try {
            
            for ( Job job : batch.getJobs() ) {
                exec( job );
            }

        } finally {

            // TODO: we should swap in executing and the history in one atomic
            // operation because technically it would be possible to do a read
            // and see that there is NO currently executing job and it isn't in
            // the history either.  We could do this with one ControllerState
            // that could a message too.  We would have to copy the entire
            // history each time but this is trivial and won't take very long.
            setExecuting( null );
            addHistory( batch );

        }

    }

    /**
     * Add a job to the pending queue for later execution.  If there are no jobs
     * executing or in the queue the batch will be executed immediately.
     */
    public void submit( Batch batch ) throws Exception {
        pending.add( batch );
    }

    /**
     * Run map jobs on all chunks on the given path.
     */
    public void exec( final Job job ) throws Exception {

        if ( job.getOperation().equals( JobOperation.REDUCE ) ) {

            if ( job.getInput() == null || job.getInput().getReferences().size() < 1 ) {
                throw new IOException( "Reducer requires at least one shuffle input." );
            }

        }
        
    	withScheduler( job, new Scheduler( job.getOperation(), job, config, clusterState ) {

    			@Override
                public void invoke( Host host, Work work ) throws Exception {
                    
                    Message message = createSchedulerMessage( "exec", job, work );
                    new Client( config ).invoke( host, job.getOperation(), message );
                    
                }
                
            } );

        if ( job.getOperation().equals( JobOperation.REDUCE ) ) {

            for( InputReference ref : job.getInput().getReferences() ) {

                if ( ref instanceof ShuffleInputReference ) {

                    ShuffleInputReference shuffle = (ShuffleInputReference)ref;

                    log.info( "Going to purge %s for job %s", shuffle.getName(), job );
                    purgeShuffleData( shuffle.getName() );

                }
                
            }

        }
        
    }

    private void withScheduler( Job job, Scheduler scheduler ) 
        throws Exception {

        try {
            
            // add this to the list of jobs that have been submitted so we can keep
            // track of what is happening with teh cluster state.
            String operation = scheduler.getOperation();

            job.setState( JobState.EXECUTING );
            
            String desc = String.format( "%s for delegate %s, named %s, with identifier %,d for input %s and output %s ",
                                         operation,
                                         job.getDelegate().getName(),
                                         job.getName(),
                                         job.getIdentifier(),
                                         job.getInput(),
                                         job.getOutput() );

            if ( job.getIdentifier() < executionRange.start || job.getIdentifier() > executionRange.end ) {
                log.info( "SKIP job due to execution range %s (%s)", executionRange, desc );
                return;
            }

            log.info( "STARTING %s", desc );

            long before = System.currentTimeMillis();
            
            daemon.setScheduler( scheduler );

            scheduler.waitForCompletion();

            daemon.setScheduler( null );

            // shufflers can be flushed after any stage even reduce as nothing will
            // happen other than a bit more latency.
            flushAllShufflers();

            // now reset the worker nodes between jobs.
            reset();
            
            long after = System.currentTimeMillis();

            long duration = after - before;

            job.setState( JobState.COMPLETED );

            log.info( "COMPLETED %s (duration %,d ms)", desc, duration );

        } catch ( Exception e ) {
            job.setState( JobState.FAILED );
            throw e;
        }

    }

    private void addHistory( Batch batch ) {

        if ( history.size() > MAX_HISTORY )
            history.poll();

        history.add( batch );
        
    }
    
    // TODO this probably should not be public.
    public void flushAllShufflers() throws Exception {

        Message message = new Message();
        message.put( "action", "flush" );

        callMethodOnCluster( "shuffler", message );
        
    }

    /**
     * Purge the shuffle data on disk so that it does not conflict with other jobs.
     * 
     * @throws Exception
     */
    public void purgeShuffleData( String name ) throws Exception {

        Message message = new Message();
        message.put( "action", "purge" );
        message.put( "name",   name );

        callMethodOnCluster( "shuffler", message );
        
    }

    /**
     * Reset cluster job state between jobs.
     */
    private void reset() throws Exception {

        Message message = new Message();
        message.put( "action", "reset" );

        callMethodOnCluster( "map",    message );
        callMethodOnCluster( "merge",  message );
        callMethodOnCluster( "reduce", message );
        
    }
    
    private void callMethodOnCluster( String service, Message message ) throws Exception {

        String desc = String.format( "CALL %s %,d hosts with message: %s" , message, config.getHosts().size(), message );
        
        log.info( "STARTING %s", desc );

        long before = System.currentTimeMillis();
        
        List<HttpClient> clients = new ArrayList();
        
        for ( Host host : config.getHosts() ) {
            clients.add( new Client( config ).invokeAsync( host, service, message ) );
        }

        for( HttpClient client : clients ) {
            client.shutdown();
        }

        for( HttpClient client : clients ) {
            client.close();
        }

        long after = System.currentTimeMillis();

        long duration = after - before;

        log.info( "COMPLETED %s (duration %,d ms)", desc, duration );

    }
    
    private Message createSchedulerMessage( String action,
                                            Job job,
                                            Work work ) {

        Message message = job.toMessage();
        
        message.put( "action", action );
    	message.put( "work" ,  work.getReferences() );

        return message;
        
    }

    public void shutdown() {

        if ( shutdown )
            return;

        daemon.shutdown();

        shutdown = true;
        
    }
    
}

interface CallableFactory {

    public Callable newCallable( Partition part, Host host );

}

/**
 * Represents which jobs we should be executing.
 */
class ExecutionRange {

    protected int start;

    protected int end;

    public ExecutionRange() {
        this( 0, Integer.MAX_VALUE );
    }

    public ExecutionRange( int start, int end ) {
        this.start = start;
        this.end   = end;
    }

    public String toString() {
        return String.format( "%s:%s", start, end );
    }
    
}
