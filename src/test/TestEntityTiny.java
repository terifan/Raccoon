package test;

import java.io.File;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonEntity;


public class TestEntityTiny
{
	public static void main(String... args)
	{
		try
		{
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				db.saveAll(new User("adam"), new User("eve"), new User("steve"));
				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, null))
			{
				db.listAll(User.class).forEach(System.out::println);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}


	@RaccoonEntity(collection = "users")
	static class User extends Document
	{
		public User()
		{
		}

		public User(String name)
		{
			put("name", name);
		}
	}
}
