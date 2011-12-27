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

public final class Output {

    private List<OutputReference> references = new ArrayList();

    public Output() { }

    public Output( List<String> paths ) {

        for( String path : paths ) {

            if ( path.contains( ":" ) ) {

                String[] split = path.split( ":" );

                String type      = split[0];
                String arg       = null;

                if ( split.length >= 2 )
                    arg = split[1];

                if ( "broadcast".equals( type ) )
                    add( new BroadcastOutputReference( arg ) );

                if ( "file".equals( type ) ) {
                    boolean append = split[2].equals( "true" );
                    add( new FileOutputReference( arg, append ) );
                }

                if ( "shuffle".equals( type ) )
                    add( new ShuffleOutputReference( arg ) );

               if ( "blackhole".equals( type ) )
                    add( new BlackholeOutputReference() );

            } else {
                add( new FileOutputReference( path ) );
            }

        }

    }
    
    public Output( String... paths ) {
        this( Arrays.asList( paths ) );
    }

    protected Output( OutputReference... refs ) {
        for( OutputReference ref : refs )
            add( ref );
    }
    
    protected Output( OutputReference ref ) {
        add( ref );
    }

    protected Output add( OutputReference ref ) {
        this.references.add( ref );
        return this;
    }

    public List<OutputReference> getReferences() {
        return references;
    }

    @Override
    public String toString() {
        return references.toString();
    }

}
