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

import java.util.Iterator;
import java.util.List;

/**
 * An iterator which only returns items which are ready to perform IO.  It's
 * pointless to perform a request on a channel that isn't ready for the IO so
 * this iterator just skips them.
 */
public class ReadyBackendRequestIterator implements Iterator<BackendRequest> {

    private Iterator<BackendRequest> delegate;

    public ReadyBackendRequestIterator(List<BackendRequest> list) {
        this.delegate = list.iterator();
    }

    private BackendRequest next = null;

    @Override
    public boolean hasNext() {

        next = null;

        while( delegate.hasNext() ) {

            BackendRequest current = delegate.next();

            ClientBackendRequest client = current.getClient();

            if ( client.isSuspended() == false &&
                 client.isCancelled() == false ) {

                next = current;
                break;

            }

        }

        return next != null;

    }

    @Override
    public BackendRequest next() {
        return next;
    }

    @Override
    public void remove() {
        delegate.remove();
    }
}
