package togos.ccouch3.repo;

import java.io.File;

public class SHA1FileRepositoryTest extends RepositoryTest
{
	@Override
	Repository createRepo() {
		return new SHA1FileRepository(new File("temp/test-repo/data"), "whatever");
	}
}
