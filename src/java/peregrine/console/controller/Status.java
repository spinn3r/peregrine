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
package peregrine.console.controller;

import com.spinn3r.log5j.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.util.*;
import peregrine.rpc.*;
import peregrine.controller.rpcd.delegate.*;

/**
 * Obtain and print the status of the controller.
 */
public class Status {

    public static void main( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );
        Client client = new Client( config );
        
        Message message = new Message();
        message.put( "action", "status" );
        
        Message result = client.invoke( config.getController(), "controller", message );

        ControllerStatusResponse response = new ControllerStatusResponse();
        response.fromMessage( result );

        System.out.printf( "Completed %,d batch jobs.\n" , response.getCompleted().size() );

    }
    
}