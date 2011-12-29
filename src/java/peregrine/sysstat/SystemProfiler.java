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
package peregrine.sysstat;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

public interface SystemProfiler {

    /**
     * Basic stat update request.  the first time this is called we show the
     * stats since the system was started (or stats have last rolled over).
     *
     * There is NO exception thrown by this method because we want
     * implementations to not have to worry about how to handle failure.
     * Internally we simply log that we failed.
     *
     * A null object pattern and empty StatMeta is returned which will not show
     * any logging information.
     */
    public StatMeta update();

    /**
     * Filter results.  If null all are returned.
     */
    public Set<String> getDisks();
    public void setDisks( Set<String> disks );    

    /**
     * Filter interface results.  If null all are returned.
     */
    public Set<String> getInterfaces();
    public void setInterfaces( Set<String> interfaces );

    /**
     * Filter processor results.  If null all are returned.
     */
    public Set<String> getProcessors();
    public void setProcessors( Set<String> processors );

    /**
     * The interval/rate you would like your throughput stats computed over.
     */
    public long getInterval();
    public void setInterval( long interval );

    public StatMeta diff();

    public StatMeta rate();

}
