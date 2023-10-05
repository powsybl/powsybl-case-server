package com.powsybl.caseserver.datasource.util;

import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.commons.datasource.DataSource;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.*;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=file"})
@ContextConfigurationWithTestChannel
public class FsCaseDataSourceControllerTest extends AbstractCaseDataSourceControllerTest {

    FileSystem fileSystem = FileSystems.getDefault();

    @Before
    public void setUp() throws URISyntaxException, IOException {
        Path path = fileSystem.getPath(rootDirectory);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        Path caseDirectory = fileSystem.getPath(rootDirectory).resolve(CASE_UUID.toString());
        if (!Files.exists(caseDirectory)) {
            Files.createDirectories(caseDirectory);
        }

        caseService.setFileSystem(fileSystem);
        //insert a cgmes in the FS
        try (InputStream cgmesURL = getClass().getResourceAsStream("/" + cgmesName)) {
            Path cgmes = caseDirectory.resolve(cgmesName);
            Files.copy(cgmesURL, cgmes, StandardCopyOption.REPLACE_EXISTING);
        }
        dataSource = DataSource.fromPath(Paths.get(getClass().getResource("/" + cgmesName).toURI()));
    }
}
