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

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class DefaultThreadFactory implements ThreadFactory {

    public AtomicInteger idx = new AtomicInteger();;

    private String template;
    
    public DefaultThreadFactory( Class clazz ) {
        this( clazz.getName() );
    }

    public DefaultThreadFactory( String template ) {
        this.template = template;
    }

    public Thread newThread(Runnable r) {
        Thread thread = new Thread( r, template + ":" + idx.getAndIncrement() );
        thread.setDaemon( true );
        return thread;
    }
    
}
