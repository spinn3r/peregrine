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

package peregrine.worker.clientd.requests;

/**
 * Used so that we can estimate the size of a request before we add an item
 * to the queue.  I'm aware that "sizeable" is not a word and the real english
 * word is "sizable" but we're exposing the "size()" method so I think it makes
 * sense to call this "sizeable".
 */
public interface RequestSizeable {

    /**
     * Return the number of items this request represents.  For some requests
     * this is an approximate size.  SCAN for example doesn't know ahead of time
     * how many entries it will match (it may be none).
     */
    public int size();

}
