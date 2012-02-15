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
package peregrine.util;

import java.util.*;

/**
 * Various utility methods for working with strings.
 */
public class Split {

    private String input = null;

    private String token = null;
    
    private String[] split = null;

    private int idx = 0;
    
    public Split( String input, String token ) {

        this.token = token;
        this.input = input;
        this.split = input.split( token );
        
    }

    public String readString() {

        if ( idx >= split.length ) {

            String message = String.format( "Field %s does not exist in split for data '%s' and token %s",
                                            idx,
                                            input,
                                            token );

            throw new RuntimeException( message );

        }
        
        return split[idx++];
    }

    public int readInt() {
        return Integer.parseInt( readString() );
    }
    
}
