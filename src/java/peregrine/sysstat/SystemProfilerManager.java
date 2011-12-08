package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class SystemProfilerManager {

    private static final Logger log = Logger.getLogger();

    public static SystemProfiler getInstance() {

        String os = System.getProperty("os.name").toLowerCase();
        
        if ( os.contains("linux") ) {
        	SystemProfiler profiler = new LinuxSystemProfiler();
        	profiler.update();
        	return profiler;
        }

        log.warn( "Unsupported platform: %s", System.getProperty("os.name") );
        
        return new UnsupportedPlatform();
        
    }

}

