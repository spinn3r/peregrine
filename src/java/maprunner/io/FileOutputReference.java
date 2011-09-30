package maprunner.io;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;

public final class FileOutputReference implements InputReference {

    private String path;
    
    public FileOutputReference( String path ) {
        this.path = path;
    }

}
    
