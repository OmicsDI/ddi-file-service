package uk.ac.ebi.ddi.ddifileservice.type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class ConvertibleOutputStream extends ByteArrayOutputStream {

    //Create InputStream without actually copying the buffer and using up mem for that.
    public InputStream toInputStream(){
        return new ByteArrayInputStream(buf, 0, count);
    }

    public int getSize() {
        return count;
    }
}
