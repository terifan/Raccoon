package examples;

import java.io.FileWriter;
import java.io.IOException;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.testng.annotations.Test;


public class BasicSample_1
{
	@Test
	public void test() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW, new TableParam(8,1,0), CompressionParam.BEST_SPEED))
		{
			db.save(new MyEntity(1, "apple"));
			for (int i = 0; i < 100000;i++)
			{
				db.save(new MyEntity(2+i, "banana"+i));
			}
			db.commit();
		}

		try (Database db = Database.open(blockDevice, OpenOption.OPEN))
		{
			String scan = db.scan().sb.toString();

			System.out.println(scan);
			
			try (FileWriter fw = new FileWriter("d:\\test.html"))
			{
				fw.write(scan.toString());
			}
			
			db.list(MyEntity.class).forEach(System.out::println);
		}
	}

	static class MyEntity
	{
		@Key int id;
		String name;

		public MyEntity()
		{
		}

		public MyEntity(int aId, String aName)
		{
			this.id = aId;
			this.name = aName;
		}

		@Override
		public String toString()
		{
			return "X{" + "id=" + id + ", name=" + name + '}';
		}
	}
}