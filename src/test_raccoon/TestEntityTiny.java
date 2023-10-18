package test_raccoon;

import java.nio.file.Paths;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonEntity;
import org.terifan.raccoon.document.Array;


public class TestEntityTiny
{
	public static void main(String... args)
	{
		try
		{
			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				db.saveEntity(new User("adam").add(new Email("adam@gmail.com")), new User("eve").add(new Email("eve@gmail.com")), new User("steve").add(new Email("steve@gmail.com")));
				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, null))
			{
				db.listEntity(User.class).forEach(System.out::println);
				db.listEntity(Email.class).forEach(System.out::println);
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
		@RaccoonEntity Array emails = new Array();

		public User()
		{
		}

		public User(String aName)
		{
			put("name", aName);
			put("emails", emails);
		}

		public User add(Email aEmail)
		{
			emails.add(aEmail);
			return this;
		}
	}


	@RaccoonEntity(collection = "email")
	static class Email extends Document
	{
		public Email()
		{
		}

		public Email(String aAddress)
		{
			put("address", aAddress);
		}
	}
}
