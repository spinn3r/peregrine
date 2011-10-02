package peregrine.io;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class FileOutputReference implements OutputReference {

    private String path;
    
    public FileOutputReference( String path ) {
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    public String toString() {
        return getPath();
    }

}
    
