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

import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.worker.*;

public class PFSTest extends peregrine.BaseTest {

    protected WorkerDaemon daemon = null;
    
    public void setUp() {

        super.setUp();

        Config config = new Config();

        config.setHost( new Host( "localhost" ) );

        config.init();
        
        daemon = new WorkerDaemon( config );
        
    }

    public void tearDown() {

        daemon.shutdown();
        
        super.tearDown();

        //FIXME: remove this when I can shut down without having extra threads
        //lying around.  Rework log5j for this.
        //System.exit( 0 );

    }

}
