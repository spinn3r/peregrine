package peregrine.io;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class BroadcastInputReference implements InputReference {

    private String name;
    
    public BroadcastInputReference() {}

    public BroadcastInputReference( String name ) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "broadcast:" + getName();
    }

}
    
