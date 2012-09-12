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

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.worker.*;

import org.jboss.netty.buffer.*;

/**
 * Callback used when we are parsing streams of wikipedia dumps.
 */
public class ParserListener {

    public void onEntry( String source, List<String> targets );
    
}