package peregrine.io.sstable;

import peregrine.StructReader;

/**
 * Represents a request to the backend to GET a key.  This represents an
 * individual request for an item and not ALL the items in a GET client
 * request which may be numerous.
 */
public class GetBackendRequest {

    private State state = State.STOP;

    private StructReader key;

    public GetBackendRequest(StructReader key) {
        this.key = key;
    }

    public StructReader getKey() {
        return key;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    /**
     * The state of this request depending on the client.  The state will be
     * GO when a client is ready to read over the network, and STOP if it is
     * idle and not reading results over the network.  If the client dropped
     * off (timeout, etc) then the request is in state CANCEL.
     */
    public enum State {
       STOP, GO, CANCEL;
    }

}
