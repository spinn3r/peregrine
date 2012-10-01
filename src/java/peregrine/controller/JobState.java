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
package peregrine.controller;

import com.spinn3r.log5j.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.util.*;

/**
 * 
 * The current state of a Job in Peregrine.
 * 
 * <p>
 * Note that thsi should probably be an enum but serialization and deserialization 
 */
public class JobState {

    /**
     * A job has been handed out and is waiting for execution.
     */
    public static String SUBMITTED = "submitted";

    /**
     * A job has been taken off the queue and is currently executing.
     */
    public static String EXECUTING = "executing";

    /**
     * A given job has completed execution successfully.
     */
    public static String COMPLETED = "completed";

    /**
     * The job was executed but failed to complete successfully.
     */
    public static String FAILED = "failed";

    /**
     * The job was skipped due to batch settings on start/end.
     */
    public static String SKIPPED = "skipped";

    /**
     * The job was killed.
     */
    public static String KILLED = "killed";

}
