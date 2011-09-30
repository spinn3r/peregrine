package maprunner.io;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;

public final class FileInputReference implements InputReference {

    private String path;
    
    public FileInputReference( String path ) {
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    public String toString() {
        return getPath();
    }
    
}
    
