
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
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.values.*;

import com.spinn3r.log5j.*;

public abstract class BaseOutputTask {

    private static final Logger log = Logger.getLogger();

    protected Output output = null;

    protected JobOutput[] jobOutput = null;

    protected Partition partition = null;

    protected Config config = null;
    
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
    
    public void setup() throws IOException {

        this.jobOutput = JobOutputFactory.getJobOutput( config, partition, output );

    }

    public void teardown() throws IOException {

        //FIXME: close ALL of these even if one of them fails and then throw
        //ALL exceptions.
        
        for( JobOutput current : jobOutput ) {
            current.close();
        }
        
        sendCompleteToController();
        
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

        message.put( "action" ,   "failed" );
        message.put( "host",      config.getHost().toString() );
        message.put( "partition", partition.getId() );
        message.put( "cause",     cause.getMessage() );

        // TODO: consider including the full stack trace as 'trace'
        
        log.info( "Sending failed message to controller: %s", message );
        
        new Client().invoke( config.getController(), "controller", message );

    }

}
