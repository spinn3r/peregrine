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

public final class Input {

    private List<InputReference> references = new ArrayList();

    public Input() { }

    public Input( List<String> paths ) {

        for( String path : paths ) {

            if ( path.contains( ":" ) ) {

                String[] split = path.split( ":" );

                if ( split.length < 2 )
                    throw new RuntimeException( "Unable to split: " + path );
                
                String type      = split[0];
                String arg       = split[1];

                if ( "broadcast".equals( type ) )
                    add( new BroadcastInputReference( arg ) );

                if ( "file".equals( type ) )
                    add( new FileInputReference( arg ) );

                if ( "shuffle".equals( type ) )
                    add( new ShuffleInputReference( arg ) );

            } else { 
                add( new FileInputReference( path ) );
            }

        }

    }
    
    public Input( String... paths ) {
        this( Arrays.asList( paths ) );
    }

    protected Input( InputReference... refs ) {
        for( InputReference ref : refs )
            add( ref );
    }

    protected Input( InputReference ref ) {
        add( ref );
    }
    
    public Input add( InputReference ref ) {
        this.references.add( ref );
        return this;
    }

    public List<InputReference> getReferences() {
        return references;
    }

    @Override
    public String toString() {
        return references.toString();
    }
    
}
