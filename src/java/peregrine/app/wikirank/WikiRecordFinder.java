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
package peregrine.app.wikirank;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.nio.*;
import java.nio.channels.*;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.worker.*;
import peregrine.os.*;
import peregrine.split.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * Parse out the wikipedia sample data.
 */
public class WikiRecordFinder implements RecordFinder {

    @Override
    public long findRecord( InputFileReader reader, long pos ) throws IOException {

        while( true ) {

            if ( reader.read(pos - 0) == '(' &&
                 reader.read(pos - 1) == ',' &&
                 reader.read(pos - 2) == ')' ) {
                
                return pos - 1;
                
            }

            // go back one character now.
            --pos;
            
        }

    }

}