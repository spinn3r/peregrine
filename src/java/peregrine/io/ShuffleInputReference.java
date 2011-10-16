package peregrine.io;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class ShuffleInputReference implements InputReference {

    private String name;

    public ShuffleInputReference() {
        this( "default" );
    }

    public ShuffleInputReference( String name ) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "shuffle:" + getName();
    }
    
}
    
