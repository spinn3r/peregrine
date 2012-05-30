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
package peregrine;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.app.pagerank.*;
import peregrine.app.wikirank.*;

public class TestWikirank extends peregrine.BaseTestWithMultipleProcesses {

    @Override
    public void doTest() throws Exception {

        Config config = getConfig();

        Wikirank wikirank = new Wikirank( config, 
                                          "corpus/wikirank/enwiki-20120502-page-sample.sql",
                                          "corpus/wikirank/enwiki-20120502-pagelinks-sample.sql" );

        wikirank.exec();
        
    }

    public static void main( String[] args ) throws Exception {

        setPropertyDefault( "peregrine.test.config", "1:1:1" ); 
        runTests();

    }

}
