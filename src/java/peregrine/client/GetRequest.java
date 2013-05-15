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
package peregrine.client;

import java.util.*;
import java.io.*;
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;
import peregrine.io.chunk.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * Represents a request to the server for GETing keys.
 */
public class GetRequest {
    
    private boolean hashcode = false;
    
    private String source = null;
    
    private List<StructReader> keys = null;

    public void setKeys( List<StructReader> keys ) {
        this.keys = keys;
    }
    
    public List<StructReader> getKeys() { 
        return this.keys;
    }

    public String getSource() { 
        return this.source;
    }

    public void setSource( String source ) { 
        this.source = source;
    }

    /**
     * When true, the input keys need to be hashed before we send them in the
     * request.
     */
    public boolean getHashcode() { 
        return this.hashcode;
    }

    public void setHashcode( boolean hashcode ) { 
        this.hashcode = hashcode;
    }

}