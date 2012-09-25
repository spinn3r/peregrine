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

import java.util.concurrent.atomic.*;

import com.spinn3r.log5j.Logger;

/**
 * A simple nonce system which tries to prevent identical message IDs from being
 * handed out in a distributed system.
 * 
 * <p> This algorithm will in practice never collide with a previous nonce but
 * our requirements are very low.  This isn't a security protocol so if a
 * message collides it is irrelevant and would just mean a job might need to be
 * re-executed.
 *
 * <p> The nonces that are handed out are always greater than the last one
 * handed out which means that we cand do quick comparisons to see if is already
 * used.
 *
 */
public class NonceFactory {

    private static final Logger log = Logger.getLogger();

    private static final long PADD = 1000000000;
    
    private static AtomicInteger local = new AtomicInteger();

    public static long newNonce() {

        int g = (int)(System.currentTimeMillis() / 1000);
        int l = local.getAndIncrement();

        return (g * PADD) + l;

    }
    
}