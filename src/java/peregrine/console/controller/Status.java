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

import java.util.*;
import java.lang.reflect.*;

import com.spinn3r.log5j.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.controller.rpcd.delegate.*;
import peregrine.rpc.*;
import peregrine.util.*;

/**
 * Obtain and print the status of the controller.
 */
public class Status {

    public static String toStatus( Batch batch ) {

        StringBuilder buff = new StringBuilder();
        
        for ( Job job : batch.getJobs() ) {

            buff.append( toStatus( job ) );

        }

        return buff.toString();
        
    }
    
    /**
     * Pretty print the given object so we can represent it on the console.
     */
    public static String toStatus( Object obj ) {

        StringBuilder buff = new StringBuilder();

        try {

            buff.append( String.format( "%s:\n", obj.getClass().getSimpleName() ) );
            
            Field[] fields = obj.getClass().getDeclaredFields();

            for( Field f : fields ) {

                if ( Modifier.isStatic( f.getModifiers() ) )
                    continue;

                f.setAccessible( true );
                Object value = f.get( obj );

                if ( value != null ) {
                    
                    if ( f.getType().equals( Class.class ) ) {
                        value = ((Class)value).getName();
                    }

                }

                buff.append( "    " );
                buff.append( f.getName() );
                buff.append( "=" );
                buff.append( value );
                buff.append( "\n" );
            }

            return buff.toString();

        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
            
    }
    
    public static void main( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );
        Client client = new Client( config );
        
        Message message = new Message();
        message.put( "action", "status" );
        
        Message result = client.invoke( config.getController(), "controller", message );

        ControllerStatusResponse response = new ControllerStatusResponse();
        response.fromMessage( result );

        System.out.printf( "Executed %,d batch jobs.\n" , response.getHistory().size() );

    }
    
}