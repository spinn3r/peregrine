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
package peregrine.io;

import java.util.*;

import peregrine.io.driver.*;
import peregrine.io.driver.blackhole.*;
import peregrine.io.driver.shuffle.*;

/**
 * Represents job output and constructs references to said output so that we can 
 * pass these to the JobOutputFactory. 
 *
 */
public final class Output {

    private List<OutputReference> references = new ArrayList();

    public Output() { }

    public Output( List<String> paths ) {

        for( String path : paths ) {

            if ( path.contains( ":" ) ) {

                String[] split = path.split( ":" );

                String scheme    = split[0];
                String arg       = null;
                
                if ( split.length >= 2 )
                    arg = split[1];
                
                if ( "broadcast".equals( scheme ) )
                    add( new BroadcastOutputReference( arg ) );
                
                if ( "file".equals( scheme ) ) {
                    boolean append = split[2].equals( "true" );
                    add( new FileOutputReference( arg, append ) );
                }

                IODriver driver = IODriverRegistry.getInstance( scheme );
                
                if ( driver != null )
                    add( driver.getOutputReference( path ) );
                
            } else {
                add( new FileOutputReference( path ) );
            }
                
        }

    }
    
    public Output( String... paths ) {
        this( Arrays.asList( paths ) );
    }

    public Output( OutputReference... refs ) {
        for( OutputReference ref : refs )
            add( ref );
    }
    
    public Output( OutputReference ref ) {
        add( ref );
    }

    public Output add( OutputReference ref ) {
        this.references.add( ref );
        return this;
    }

    public List<OutputReference> getReferences() {
        return references;
    }

    public int size() {
    	return references.size();    	
    }
    
    @Override
    public String toString() {
        return references.toString();
    }

}
