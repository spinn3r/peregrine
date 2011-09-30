package maprunner.io;

import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;

public final class Input {

    private List<InputReference> references = new ArrayList();

    public Input() { }

    public Input( String[] paths ) {
        for( String path : paths ) {
            add( new FileInputReference( path ) );
        }
    }
    
    public void add( InputReference ref ) {
        this.references.add( ref );
    }

    public List<InputReference> getReferences() {
        return references;
    }

}