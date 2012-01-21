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

import peregrine.config.*;
import org.apache.log4j.xml.DOMConfigurator;

import com.spinn3r.log5j.Logger;

public class Main {
	
    private static final Logger log = Logger.getLogger();
        
    private static Config config = null;
        
    public static void main(String[] args ) throws Exception {

        DOMConfigurator.configure( "conf/log4j.xml" );
        config = ConfigParser.parse( args );

        log.info( "Starting on %s with controller: %s" , config.getHost(), config.getController() );
        
        new FSDaemon( config );
        
        Thread.sleep( Long.MAX_VALUE );
        
    }
   
}
    
