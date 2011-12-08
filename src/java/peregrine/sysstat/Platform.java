
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public interface Platform {

    /**
     * Basic stat update request.  the first time this is called we show the
     * stats since the system was started (or stats have last rolled over).
     *
     * There is NO exception thrown by this method because we want
     * implementations to not have to worry about how to handle failure.
     * Internally we simply log that we failed.
     *
     * A null object pattern and empty StatMeta is returned which will not show
     * any logging information.
     */
    public StatMeta update();

    /**
     * Filter results.  If null all are returned.
     */
    public Set<String> getDisks();
    public void setDisks( Set<String> disks );    

    /**
     * Filter interface results.  If null all are returned.
     */
    public Set<String> getInterfaces();
    public void setInterfaces( Set<String> interfaces );

    /**
     * Filter processor results.  If null all are returned.
     */
    public Set<String> getProcessors();
    public void setProcessors( Set<String> processors );

    /**
     * The interval/rate you would like your throughput stats computed over.
     */
    public long getInterval();
    public void setInterval( long interval );
    
}