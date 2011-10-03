package peregrine.io;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class FileOutputReference implements OutputReference {
    
    private boolean append = false;

    private String path;

    public FileOutputReference( String path ) {
        this( path, false );
    }
    
    public FileOutputReference( String path, boolean append ) {
        this.path = path;
        this.append = append;
    }

    public String getPath() {
        return this.path;
    }

    public String toString() {
        return getPath();
    }

    public boolean getAppend() { 
        return this.append;
    }

}
    
