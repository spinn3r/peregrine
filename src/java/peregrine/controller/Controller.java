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
import peregrine.util.*;

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

    private static final int BATCH_MAX_HISTORY = 200;

    private static final int BATCH_MAX_PENDING= 200;

    private Config config = null;

    private ControllerDaemon daemon = null;
    
    protected ClusterState clusterState;

    /**
     * True if we are shutdown to avoid multiple shutdown attempts.
     */
    protected boolean shutdown = false;

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

    protected long started = -1;
    
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

        this.started = System.currentTimeMillis();
        
        this.clusterState = new ClusterState( config, this );

        this.daemon = new ControllerDaemon( this, config, clusterState );

    }

    public Config getConfig() {
        return config;
    }

    public List<Batch> getHistory() {
        return new ArrayList( history );
    }

    public List<Batch> getPending() {
        return new ArrayList( pending );
    }

    public Batch getExecuting() {
        return executing;
    }

    public void setExecuting( Batch executing ) {
        this.executing = executing;
    }

    /**
     * Return the time that the controller was started.
     */
    public long getStarted() {
        return started;
    }
    
    public void map( Class mapper,
                     String... paths ) throws Exception {

        exec( new Batch( mapper ).map( mapper, paths ) );

    }

    public void map( Class mapper,
                     Input input ) throws Exception {

        exec( new Batch( mapper ).map( mapper, input ) );

    }
    
    public void map( Class delegate,
    		 		 Input input,
    				 Output output ) throws Exception {

        exec( new Batch( delegate ).map( delegate, input, output ) );

    }
    
    /**
     * Run map jobs on all chunks on the given path.
     */
    public void map( Job job ) throws Exception {
        exec( new Batch( job ).map( job ) );
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

        exec( new Batch( delegate ).merge( delegate, input, output ) );

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
        exec( new Batch( job ).merge( job ) );
    }
  
    public void reduce( Class delegate,
    				    Input input,
    				    Output output ) throws Exception {

        exec( new Batch(delegate ).reduce( delegate, input, output ) );

    }

    public void touch( String output ) throws Exception {
        exec( new Batch( output ).touch( output ) );
    }
    
    public void sort( String input, String output, Class comparator ) throws Exception {
        exec( new Batch( output ).sort( input, output, comparator ) );
    }

    /**
     * Perform a reduce over the previous shuffle data (or broadcast data).
     */
    public void reduce( Job job ) throws Exception {
        exec( new Batch( job ).reduce( job ) );
    }

    /**
     * Add a job to the pending queue for later execution.  If there are no jobs
     * executing or in the queue the batch will be executed immediately.
     */
    public Batch submit( Batch batch ) throws Exception {

        if ( pending.size() >= BATCH_MAX_PENDING) {
            throw new Exception( "Hit maximum submissions: " + BATCH_MAX_PENDING );
        }

        batch.assertExecutionViability();
        batch.setIdentifier( NonceFactory.newNonce() );
        
        pending.add( batch );

        return batch;
        
    }

    /**
     * Execute a batch of jobs.
     */
    public void exec( Batch batch ) throws Exception {

        try {

            batch.setState( JobState.EXECUTING )
                 .setStarted( System.currentTimeMillis() )
                ;
            
            setExecuting( batch );

            batch.assertExecutionViability();

            int id = -1;
            for ( Job job : batch.getJobs() ) {

                ++id;

                if ( id < batch.getStart() || id > batch.getEnd() ) {
                    log.info( "SKIP job %s due to start/end range %s (%s)", id, batch.getStart(), batch.getEnd() );
                    job.setState( JobState.SKIPPED );
                    continue;
                }

                exec( job );
                
            }

            batch.setState( JobState.COMPLETED );

        } catch ( Exception e ) {

            batch.setState( JobState.FAILED )
                 .setCause( e )
                ;
            throw e;
            
        } finally {

            batch.setDuration( System.currentTimeMillis() - batch.getStarted() );

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
     * Run map jobs on all chunks on the given path.
     */
    public void exec( final Job job ) throws Exception {

        try {

            job.setState( JobState.EXECUTING );
            job.setStarted( System.currentTimeMillis() );
            
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

            job.setState( JobState.COMPLETED );

        } catch ( Exception e ) {

            job.setState( JobState.FAILED )
               .setCause( e )
               ;

            throw e;
            
        } finally {
            job.setDuration( System.currentTimeMillis() - job.getStarted() );
        }
        
    }

    /**
     * Foreground batch submission handler that waits for batch jobs to be added
     * to the submission queue and then executes them.  This process ALL batch
     * requests including current and future requests.
     */
    public void processBatchSubmissions() {

        while( true ) {

            if ( processCurrentBatchSubmissions() == false ) {

                Threads.coma( 100L );

            }

        }

    }

    /**
     * Process only currently submitted batch jobs.  Return true if at least one
     * batch was executed and false if there were non executed.
     */
    public boolean processCurrentBatchSubmissions() {

        boolean result = false;
        
        while( true ) {

            try {
            
                Batch batch = pending.poll();
                
                // we have a job to execute ... go go go.
                if ( batch != null ) {
                    exec( batch );
                    result = true;
                    continue;
                }

                break;
                
            } catch ( Exception e ) {
                log.error( "Unable to exec batch job: ", e );
            }

        }

        return result;

    }
    
    private void withScheduler( Job job, Scheduler scheduler ) 
        throws Exception {

        // add this to the list of jobs that have been submitted so we can keep
        // track of what is happening with teh cluster state.
        String operation = scheduler.getOperation();

        String desc = String.format( "%s for delegate %s, named %s, with identifier %,d for input %s and output %s ",
                                     operation,
                                     job.getDelegate().getName(),
                                     job.getName(),
                                     job.getIdentifier(),
                                     job.getInput(),
                                     job.getOutput() );

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

        log.info( "COMPLETED %s (duration %,d ms)", desc, duration );

    }

    private void addHistory( Batch batch ) {

        if ( history.size() > BATCH_MAX_PENDING )
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
