/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.os.proc;

import java.io.*;
import java.util.*;

import peregrine.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Linux specific functions.
 */
public class ProcessListEntry {
    
    private int id = -1;
    
    private List<String> arguments = new ArrayList();

    public int getId() { 
        return this.id;
    }

    public void setId( int id ) { 
        this.id = id;
    }

    public void setArguments( List<String> arguments ) { 
        this.arguments = arguments;
    }

    public List<String> getArguments() { 
        return this.arguments;
    }

    public String toString() {
        return String.format( "%10s %s" , id , Strings.join( arguments, " " ) );
    }

}