
package peregrine.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.shuffle.*;

public abstract class BaseOutputTask {

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
        
    }
    
}
