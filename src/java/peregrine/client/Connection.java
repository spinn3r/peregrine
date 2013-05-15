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

import java.io.*;
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

import peregrine.*;

import com.spinn3r.log5j.*;

/**
 * Run a get request for the given keys.
 */
public class Connection {
    
    private String endpoint = null;

    public Connection( String endpoint ) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() { 
        return this.endpoint;
    }

    public void setEndpoint( String endpoint ) { 
        this.endpoint = endpoint;
    }

}