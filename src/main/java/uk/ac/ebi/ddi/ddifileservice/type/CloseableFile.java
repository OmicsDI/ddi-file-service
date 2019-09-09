package uk.ac.ebi.ddi.ddifileservice.type;

import java.io.Closeable;
import java.io.File;
import java.net.URI;

public abstract class CloseableFile extends File implements Closeable {
    public CloseableFile(String pathname) {
        super(pathname);
    }

    public CloseableFile(String parent, String child) {
        super(parent, child);
    }

    public CloseableFile(File parent, String child) {
        super(parent, child);
    }

    public CloseableFile(URI uri) {
        super(uri);
    }

    public CloseableFile(File file) {
        super(file.getAbsolutePath());
    }
}
