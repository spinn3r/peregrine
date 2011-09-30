package maprunner.io;

import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;

public final class Input {

    private List<InputReference> references = new ArrayList();
    
    public void add( InputReference ref ) {
        this.references.add( ref );
    }

    public List<InputReference> getReferences() {
        return references;
    }
    
    public static Input fromPaths( String[] paths ) {

        Input input = new Input();
        for( String path : paths ) {
            input.add( new FileInputReference( path ) );
        }

        return input;
        
    }
    
}