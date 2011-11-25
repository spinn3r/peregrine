
package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.http.*;
import peregrine.io.*;
import peregrine.pfsd.*;
import peregrine.rpc.*;
import peregrine.task.*;

import com.spinn3r.log5j.*;

/**
 * 
 * Main interface for running a mapred job.  The controller communicated with PFS 
 * nodes which will then in turn run jobs with your specified Mapper, Merger 
 * and/or Reducer.
 * 
 * @author burton@spinn3r.com
 * 
 */
public class Controller {

    private static final Logger log = Logger.getLogger();

    private Config config = null;

    private ControllerDaemon daemon = null;
    
    public Controller( Config config ) {
    	
        this.config = config;

        // FIXME: we should not startup an ORDINARY daemon... it should be JUST
        // for RPC and minimum services.  Nothing else.

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
        
        this.daemon = new ControllerDaemon( this, config );

    }

    public void map( Class mapper,
                     String... paths ) throws Exception {
        map( mapper, new Input( paths ) );
    }

    public void map( final Class mapper,
                     final Input input ) throws Exception {
        map( mapper, input, null );
    }
    
    public void map( final Class delegate,
    		 		 final Input input,
    				 final Output output ) throws Exception {

    	map( new Job().setDelegate( delegate ) 
    			      .setInput( input )
    			      .setOutput( output ) );
    		
    }
    
    /**
     * Run map jobs on all chunks on the given path.
     */
    public void map( final Job job ) throws Exception {
    	
    	withScheduler( "map", job, new Scheduler( config ) {

                public void invoke( Host host, Partition part ) throws Exception {

                    Message message 
                        = createSchedulerMessage( "exec", job, part );

                    new Client().invoke( host, "mapper", message );
                    
                }
                
            } );

    }
    
    public void merge( Class mapper,
                       String... paths ) throws Exception {

        merge( mapper, new Input( paths ) );

    }

    public  void merge( final Class mapper,
                        final Input input ) throws Exception {

        merge( mapper, input, null );

    }

    public void merge( final Class delegate,
                       final Input input,
                       final Output output ) throws Exception {
    	
    	merge( new Job().setDelegate( delegate )
    			        .setInput( input )
    			        .setOutput( output ) );
        	
    }
    
    /**
     * http://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join
     * 
     * Conceptually, a full outer join combines the effect of applying both left
     * and right outer joins. Where records in the FULL OUTER JOINed tables do not
     * match, the result set will have NULL values for every column of the table
     * that lacks a matching row. For those records that do match, a single row
     * will be produced in the result set (containing fields populated from both
     * tables)
     */
    public void merge( final Job job ) throws Exception {
    	
        withScheduler( "merge", job, new Scheduler( config ) {

                public void invoke( Host host, Partition part ) throws Exception {

                    Message message = createSchedulerMessage( "exec", job, part );
                    new Client().invoke( host, "merger", message );
                    
                }
                
            } );
            
    }
  
    public void reduce( final Class delegate,
    				    final Input input,
    				    final Output output ) 
            		throws Exception {
    	
    	reduce( new Job().setDelegate( delegate )
		                 .setInput( input )
		                 .setOutput( output ) );    	
    }
    
    public void reduce( final Job job ) 
        throws Exception {

    	final Input input = job.getInput();
    	
        // we need to support reading input from the shuffler.  If the user
        // doesn't specify input, use the default shuffler.

        if ( input == null )
            throw new Exception( "Input may not be null" );
        
        if ( input.getReferences().size() == 0 ) {
            input.add( new ShuffleInputReference() );
        }

        if ( input.getReferences().size() < 1 ) {
            throw new IOException( "Reducer requires at least one shuffle input." );
        }
      
        withScheduler( "reduce", job, new Scheduler( config ) {

                public void invoke( Host host, Partition part ) throws Exception {

                    Message message = createSchedulerMessage( "exec", job, part );
                    new Client().invoke( host, "reducer", message );
                    
                }
                
            } );

    }

    private void withScheduler( String operation, Job job, Scheduler scheduler ) 
    		throws Exception {
    	
        String desc = String.format( "%s %s (%s) for input %s and output %s ",
                                     operation, job.getDelegate().getName(),
                                     job.getName(), job.getInput(), job.getOutput() );

        log.info( "STARTING %s", desc );

        long before = System.currentTimeMillis();
        
        daemon.setScheduler( scheduler );

        scheduler.waitForCompletion();

        daemon.setScheduler( null );

        // shufflers can be flushed after any stage even reduce as nothing will
        // happen

        if ( "map".equals( operation ) || "merge".equals( operation ) )
            flushAllShufflers();

        long after = System.currentTimeMillis();

        long duration = after - before;
        
        log.info( "COMPLETED %s (duration %,d ms)", desc, duration );

    }
    
    // TODO this probably should not be public.
    public void flushAllShufflers() throws Exception {

        Message message = new Message();
        message.put( "action", "flush" );

        String desc = String.format( "Flushing all %,d shufflers with message: %s" , config.getHosts().size(), message );
        
        log.info( "STARTING %s", desc );

        long before = System.currentTimeMillis();
        
        List<HttpClient> clients = new ArrayList();
        
        for ( Host host : config.getHosts() ) {
            clients.add( new Client().invokeAsync( host, "shuffler", message ) );
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
                                            Partition partition ) {

    	Class delegate = job.getDelegate();
    	Input input = job.getInput();
    	Output output = job.getOutput();
    	
        int idx;

        Message message = new Message();
        message.put( "action",     action );
        message.put( "partition",  partition.getId() );
        message.put( "delegate",   delegate.getName() );
        message.put( "job_id",     job.getId() );
        
        if ( input != null ) {
        
            idx = 0;
            for( InputReference ref : input.getReferences() ) {
                message.put( "input." + idx++, ref.toString() );
            }

        }

        if ( output != null ) {
            
            idx = 0;
            for( OutputReference ref : output.getReferences() ) {
                message.put( "output." + idx++, ref.toString() );
            }

        }

        return message;
        
    }

    public void shutdown() {
        daemon.shutdown();
    }
    
}

interface CallableFactory {

    public Callable newCallable( Partition part, Host host );

}

