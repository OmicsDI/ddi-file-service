package uk.ac.ebi.ddi.ddifileservice.services;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import uk.ac.ebi.ddi.ddifileservice.type.ConvertibleOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@ConditionalOnProperty(
        value = "file.provider",
        havingValue = "local"
)
public class LocalFileSystem implements IFileSystem {

    @Override
    public InputStream getInputStream(String filePath) throws FileNotFoundException {
        return new FileInputStream(new File(filePath));
    }

    @Override
    public File getFile(String filePath) {
        return new File(filePath);
    }

    @Override
    public void saveFile(String filePath, ConvertibleOutputStream outputStream) throws IOException {
        try (OutputStream os = new FileOutputStream(filePath)) {
            outputStream.writeTo(os);
        }
    }

    @Override
    public void copyFile(File localFile, String destinationFile) throws IOException {
        File dest = new File(destinationFile);
        FileCopyUtils.copy(localFile, dest);
    }

    @Override
    public List<String> listFilesFromFolder(String folderPath) {
        try (Stream<Path> walk = Files.walk(Paths.get(folderPath))) {

            return walk.filter(Files::isRegularFile)
                    .map(x -> x.toAbsolutePath().toString()).collect(Collectors.toList());

        } catch (IOException e) {
            return new ArrayList<>();
        }
    }

    @Override
    public void deleteFile(String filePath) {
        File file = new File(filePath);
        file.delete();
    }
}
