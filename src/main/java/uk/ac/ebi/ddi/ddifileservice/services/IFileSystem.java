package uk.ac.ebi.ddi.ddifileservice.services;

import uk.ac.ebi.ddi.ddifileservice.type.ConvertibleOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface IFileSystem {

    InputStream getInputStream(String filePath) throws FileNotFoundException;

    File getFile(String filePath) throws IOException;

    void saveFile(ConvertibleOutputStream outputStream, String filePath) throws IOException;

    void copyFile(File localFile, String destinationFile) throws IOException;

    List<String> listFilesFromFolder(String folderPath);

    void deleteFile(String filePath);

    boolean isFile(String filePath);

    void cleanDirectory(String dirPath);
}
