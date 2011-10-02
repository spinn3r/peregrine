package peregrine.io;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class BroadcastOutputReference implements OutputReference {

    private String name;
    
    public BroadcastOutputReference( String name ) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
    
