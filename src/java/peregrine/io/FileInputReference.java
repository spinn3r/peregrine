package peregrine.io;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class FileInputReference implements InputReference {

    private String path;
    
    public FileInputReference( String path ) {
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    @Override
    public String toString() {
        return "file:" + getPath();
    }

}
    
