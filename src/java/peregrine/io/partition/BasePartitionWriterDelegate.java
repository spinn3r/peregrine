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
package peregrine.io.partition;

import java.io.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.config.Partition;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public abstract class BasePartitionWriterDelegate implements PartitionWriterDelegate {

    protected Partition partition;

    protected Host host;

    protected String path;

    protected Config config;
    
    @Override
    public void init( Config config,
                      Partition partition,
                      Host host,
                      String path ) throws IOException {

        this.config = config;
        this.partition = partition;
        this.host = host;
        this.path = path;
        
    }

    @Override
    public String toString() {
        return path;
    }

    @Override
    public Host getHost() {
        return host;
    }
    
}
