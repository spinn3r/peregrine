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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.http.*;
import peregrine.io.*;
import peregrine.io.driver.shuffle.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.task.*;
import peregrine.util.*;
import peregrine.controller.rpcd.delegate.*;
import peregrine.rpc.*;

import peregrine.config.*;

import com.spinn3r.log5j.*;

/**
 * 
 */
public class ControllerClient {

    private static final Logger log = Logger.getLogger();

    public static ControllerStatusResponse status( Config config ) throws Exception {

        ControllerStatusResponse response = new ControllerStatusResponse();

        Client client = new Client( config );
        
        Message message = new Message();
        message.put( "action", "status" );
        
        Message result = client.invoke( config.getController(), "controller", message );
        
        response.fromMessage( result );

        return response;
        
    }

    /**
     * Submit a job for execution and return the assigned identifier.
     */
    public static long submit( Config config, Batch batch ) throws Exception {

        log.info( "Going to submit batch: %s" , batch.getName() );

        batch.assertExecutionViability();
        
        Client client = new Client( config );
        
        Message message = new Message();
        message.put( "action", "submit" );
        message.put( "batch",  batch );
        
        Message result = client.invoke( config.getController(), "controller", message );

        long identifier = result.getLong( "identifier" );

        log.info( "Batch successfully submitted and now has identifier: %s", identifier );
        
        return identifier;
        
    }
    
}
