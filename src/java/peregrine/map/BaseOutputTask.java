
package peregrine.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.map.*;
import peregrine.rpc.*;
import peregrine.pfs.*;
import peregrine.reduce.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.task.*;

import com.spinn3r.log5j.*;

public abstract class BaseOutputTask {

    private static final Logger log = Logger.getLogger();

    protected Output output = null;

    protected JobOutput[] jobOutput = null;

    protected Partition partition = null;

    protected Config config = null;

    protected TaskStatus status = TaskStatus.UNKNOWN;

    protected Throwable cause = null;
    
    protected void init( Partition partition ) {
        this.partition = partition;
    }

    public Output getOutput() { 
        return this.output;
    }

    public void setOutput( Output output ) { 
        this.output = output;
    }
    
    public JobOutput[] getJobOutput() {
        return this.jobOutput;
    }

    public void setJobOutput( JobOutput[] jobOutput ) {
        this.jobOutput = jobOutput;
    }

    public void setStatus( TaskStatus status ) {
        this.status = status;
    }

    public void setCause( Throwable cause ) {
        this.cause = cause;
    }
    
    public void setup() throws IOException {

        this.jobOutput = JobOutputFactory.getJobOutput( config, partition, output );

        for( int i = 0; i < jobOutput.length; ++i ) {
            log.info( "Job output %,d is %s" , i , jobOutput[i] );
        }
        
    }

    public void teardown() throws IOException {

        //FIXME: close ALL of these jobs even if one of them fails and then
        //throw ALL exceptions.  Also we need to gossip here if it is failing
        //talking to a remote host.

        for( JobOutput current : jobOutput ) {
            log.info( "Closing job output: %s" , current );
            current.close();
        }

        log.info( "Closing job output...done" );

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
     * Mark the partition for this task complete.  
     */
    protected void sendCompleteToController() throws IOException {

        Message message = new Message();

        message.put( "action" ,   "complete" );
        message.put( "host",      config.getHost().toString() );
        message.put( "partition", partition.getId() );

        log.info( "Sending complete message to controller: %s", message );
        
        new Client().invoke( config.getController(), "controller", message );

    }

    /**
     * Tell the controller tha twe failed (be a good citizen if we can).
     */
    protected void sendFailedToController( Throwable cause ) throws IOException {

        Message message = new Message();

        // include the full stack trace 
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        cause.printStackTrace( new PrintStream( out ) );

        String stacktrace = new String( out.toByteArray() );
        
        message.put( "action" ,     "failed" );
        message.put( "host",        config.getHost().toString() );
        message.put( "partition",   partition.getId() );
        message.put( "stacktrace",  stacktrace );

        log.info( "Sending failed message to controller: %s", message );
        
        new Client().invoke( config.getController(), "controller", message );

    }

}
