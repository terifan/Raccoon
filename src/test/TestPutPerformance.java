package test;

import java.io.File;
import java.util.Random;
import org.terifan.bundle.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestPutPerformance
{
	private final static Random rnd = new Random(1);


	public static void main(String... args)
	{
		try
		{
			int N = 1000_000;
			int M = 100;

//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			System.out.printf("%-20s ", "INSERT " + N*M);
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				for (int j = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++)
					{
						String s = value();
						db.getCollection("table").save(new Document().putString("key", s).putNumber("hash", s.hashCode()));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-20s ", "GET " + N*M);
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").get(new Document().putNumber("_id", k));
						assert doc.getString("key").hashCode() == doc.getInt("hash");
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-20s ", "REMOVE " + N*M);
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						db.getCollection("table").remove(new Document().putNumber("_id", k));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-20s ", "INSERT " + N+"x"+M);
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				for (int j = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++)
					{
						db.getCollection("table").save(new Document().putNumber("_id", i).putString("key", value()));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-20s ", "GET " + N+"x"+M);
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++)
					{
						Document doc = db.getCollection("table").get(new Document().putNumber("_id", i));
						assert doc.getString("key").hashCode() == doc.getInt("hash");
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-20s ", "REMOVE " + N);
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				long t = System.currentTimeMillis();
				for (int i = 0; i < N; i++)
				{
					db.getCollection("table").remove(new Document().putNumber("_id", i));
				}
				System.out.printf("%8d", System.currentTimeMillis() - t);
				db.commit();
			}
			System.out.println();
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static String value()
	{
		byte[] buf = new byte[rnd.nextInt(100)];
		for (int i = 0; i < buf.length; i++)
		{
			buf[i] = (byte)(32 + rnd.nextInt(128));
		}
		return new String(buf);
	}
}
