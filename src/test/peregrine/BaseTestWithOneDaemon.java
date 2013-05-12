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

public abstract class BaseTestWithOneDaemon extends peregrine.BaseTest {

    protected Host controller;

    protected Config config;

    protected Config config0;

    protected List<WorkerDaemon> daemons = new ArrayList();
    
    public void setUp() {

        super.setUp();

        controller = new Host( "localhost", 11111 );

        config = newConfig( "localhost", 11111 );
        
        config0 = newConfig( "localhost", 11112 );

        daemons.add( new WorkerDaemon( config0 ) );

    }

    protected Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.setController( controller );

        config.getHosts().add( new Host( "localhost", 11112 ) );

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
