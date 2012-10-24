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
package peregrine.controller;

import java.net.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;
import peregrine.worker.*;

public class TestMarkOfflineTimer extends peregrine.BaseTestWithTwoDaemons {

    public void doTest() throws Exception {

        /*
        HeartbeatTimer.CONTROLLER_ONLINE_SLEEP_INTERVAL = 5000L;

        MarkOfflineTimer.FAILED_INTERVAL  = 10000L;

        MarkOfflineTimer.SCHEDULE_DELAY = 1000L;
        
        Controller controller = new Controller( config );;
        
        try {

            System.out.printf( "sleeping...\n" );

            Thread.sleep( 1000L );

            System.out.printf( "done\n" );

            shutdown();

            Thread.sleep( 11000L );

            // make sure our hosts are marked offline.

            Offline offline = controller.clusterState.getOffline();
            
            for( Host host : config.getHosts() ) {

                if ( ! offline.contains( host ) )
                    throw new RuntimeException( "host not offline: " + host );
                
            }

            System.out.printf( "WIN.  All hosts have been marked offline.\n" );
            
        } finally {
            controller.shutdown();
        }
        */
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
