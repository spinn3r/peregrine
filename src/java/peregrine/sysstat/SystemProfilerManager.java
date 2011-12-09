package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public class SystemProfilerManager {

    private static final Logger log = Logger.getLogger();

    public static SystemProfiler getInstance() {
        return getInstance( null, null, null );
    }

    public static SystemProfiler getInstance( Set<String> interfaces,
                                              Set<String> disks,
                                              Set<String> processors ) {

        String os = System.getProperty("os.name").toLowerCase();

        SystemProfiler profiler = null;
        
        if ( os.contains("linux") ) {

            try {
                profiler = new LinuxSystemProfiler( interfaces, disks, processors );
            } catch ( IOException e ) {
                log.warn( "Unable to create linux profiler: %s", e );
            }

        }

        if ( profiler != null ) {

        	profiler.update();

            return profiler;
            
        }
        
        log.warn( "Unsupported platform: %s", System.getProperty("os.name") );
        
        return new UnsupportedPlatform();
        
    }

}

