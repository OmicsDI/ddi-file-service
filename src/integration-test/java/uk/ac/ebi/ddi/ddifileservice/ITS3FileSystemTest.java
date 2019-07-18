package uk.ac.ebi.ddi.ddifileservice;

import com.amazonaws.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ddi.ddifileservice.configuration.S3Properties;
import uk.ac.ebi.ddi.ddifileservice.services.IFileSystem;
import uk.ac.ebi.ddi.ddifileservice.type.ConvertibleOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest({
		"s3.endpoint_url=https://s3.embassy.ebi.ac.uk",
		"s3.bucket_name=caas-omicsdi",
		"s3.region=eu-west-2",
		"s3.env_auth=true"
})
public class ITS3FileSystemTest {

	@Autowired
	private IFileSystem fileSystem;

	@Autowired
	private S3Properties s3Properties;

	private String parentPath = "testing/s3-service";
	private String testFile1 = parentPath + "/sample-file.txt";
	private String testFile2 = parentPath + "/sample-file-2.txt";

	@Test
	public void testS3FileSystem() throws IOException {

		// Testing upload file
		File experiment = new File(getClass().getClassLoader().getResource("sample-file.txt").getFile());
		fileSystem.copyFile(experiment, testFile1);

		// Testing upload output stream
		try (InputStream in = getClass().getClassLoader().getResourceAsStream("sample-file-2.txt")) {
			ConvertibleOutputStream outputStream = new ConvertibleOutputStream();
			int b;
			while ((b = in.read()) != -1) {
				outputStream.write(b);
			}
			fileSystem.saveFile(outputStream, testFile2);
		}

		List<String> files = fileSystem.listFilesFromFolder(parentPath);
		Assert.assertEquals(2, files.size());

		try (InputStream f1 = fileSystem.getInputStream(testFile1)) {
			Assert.assertTrue(IOUtils.toString(f1).contains("This is a test file"));
		}


		try (InputStream f2 = fileSystem.getInputStream(testFile2)) {
			Assert.assertTrue(IOUtils.toString(f2).contains("this is the test file 2"));
		}

		File file = fileSystem.getFile(testFile1);
		String contents = new String(Files.readAllBytes(file.toPath()));
		Assert.assertTrue(contents.contains("This is a test file"));


		fileSystem.deleteFile(testFile1);
		fileSystem.deleteFile(testFile2);

		files = fileSystem.listFilesFromFolder(parentPath);

		Assert.assertEquals(0, files.size());
	}

}
