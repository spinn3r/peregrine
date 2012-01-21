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
package peregrine.worker;

import java.io.*;
import java.util.*;
import peregrine.config.*;
import peregrine.worker.*;

import com.spinn3r.log5j.Logger;

/**
 * Setup an embedded daemon in the current process.  This way we can host a PFSd
 * daemon for any process.
 */
public class EmbeddedDaemon {

    private static final Logger log = Logger.getLogger();

    protected List<FSDaemon> daemons = new ArrayList();

    private String[] args = new String[0];
    
    public EmbeddedDaemon() { }

    public EmbeddedDaemon( String[] args ) {
        this.args = args;
    }

    /**
     * Startup all daemons.
     */
    public void start() throws IOException {

        Config config = ConfigParser.parse( args );

        for( Host host : config.getHosts() ) {

            config = ConfigParser.parse( args );
            config.setHost( host );
            config.init();

            daemons.add( new FSDaemon( config ) );

        }

        log.info( "Working with daemons %s" , daemons );

    }

    public void shutdown() {

        log.info( "Shutting down %,d daemons", daemons.size() );
        
        for( FSDaemon daemon : daemons ) {

            log.info( "Shutting down: %s", daemons );
            daemon.shutdown();
        }

    }
    
}
