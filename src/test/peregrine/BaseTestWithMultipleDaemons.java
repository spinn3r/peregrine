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
package peregrine;

import java.util.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.worker.*;

import com.spinn3r.log5j.Logger;

public abstract class BaseTestWithMultipleDaemons extends peregrine.BaseTest {

    private static final Logger log = Logger.getLogger();

    protected Host controller;

    protected Config config;

    protected List<WorkerDaemon> daemons = new ArrayList();

    protected List<Config> configs = new ArrayList();

    protected int concurrency = -1;
    protected int replicas    = -1;
    protected int nr_daemons  = -1;
    
    public BaseTestWithMultipleDaemons() {
        this( 1, 1, 2 );
    }

    public BaseTestWithMultipleDaemons( int concurrency,
                                        int replicas,
                                        int nr_daemons ) {
        this.concurrency = concurrency;
        this.replicas = replicas;
        this.nr_daemons = nr_daemons;
    }

    public void setUp() {

        super.setUp();

        controller = new Host( "localhost", 11111 );
        config = newConfig( "localhost", 11111 );

        for( int i = 0; i < nr_daemons; ++i ) {

            Config config = newConfig( "localhost", Host.DEFAULT_PORT + i );
            configs.add( config );
            
            daemons.add( new WorkerDaemon( config ) );

        }

        log.info( "Working with configs %s and daemons %s" , configs, daemons );
        
    }

    protected Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.setController( controller );
        config.setConcurrency( concurrency );
        config.setReplicas( replicas );

        for( int i = 0; i < nr_daemons; ++i ) {
            config.getHosts().add( new Host( "localhost", Host.DEFAULT_PORT + i ) );
        }
        
        config.init();
        
        return config;
        
    }

    public void tearDown() {

        for( WorkerDaemon daemon : daemons ) {
            daemon.shutdown();
        }
        
        super.tearDown();

    }

}
