package peregrine.io;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class BroadcastInputReference implements InputReference {

    private String path;
    
    public BroadcastInputReference( String path ) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

}
    
