
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public abstract class BasePlatform implements Platform {
    
    private long interval = 1000L;
    
    private Set<String> disks = null;
    private Set<String> processors = null;
    private Set<String> interfaces = null;

    public Set<String> getInterfaces() { 
        return this.interfaces;
    }

    public void setInterfaces( Set<String> interfaces ) { 
        this.interfaces = interfaces;
    }

    public Set<String> getProcessors() { 
        return this.processors;
    }

    public void setProcessors( Set<String> processors ) { 
        this.processors = processors;
    }

    public Set<String> getDisks() { 
        return this.disks;
    }

    public void setDisks( Set<String> disks ) { 
        this.disks = disks;
    }

    public long getInterval() { 
        return this.interval;
    }

    public void setInterval( long interval ) { 
        this.interval = interval;
    }

}