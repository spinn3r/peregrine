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

