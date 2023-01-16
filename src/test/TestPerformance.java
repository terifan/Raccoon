package test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import org.terifan.bundle.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestPerformance
{
	private final static Random rnd = new Random(1);


	public static void main(String... args)
	{
		try
		{
			int N = 1000_000;
			int M = 10;

//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			System.out.printf("%-15s ", "INSERT FULL");
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

			System.out.printf("%-15s ", "GET FULL");
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

			System.out.printf("%-15s ", "REMOVE FULL");
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

			System.out.printf("%-15s ", "INSERT REPLACE");
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

			System.out.printf("%-15s ", "GET REPLACE");
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


			ArrayList<Integer> order = new ArrayList<>();
			for (int i = 0; i < N*M;i++)
			{
				order.add(i);
			}
			Collections.shuffle(order, rnd);


			System.out.printf("%-15s ", "INSERT RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						db.getCollection("table").save(new Document().putNumber("_id", order.get(k)).putString("key", s).putNumber("hash", s.hashCode()));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-15s ", "REPLACE RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						db.getCollection("table").save(new Document().putNumber("_id", order.get(k)).putString("key", s).putNumber("hash", s.hashCode()));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf("%-15s ", "GET RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").get(new Document().putNumber("_id", order.get(k)));
						assert doc.getString("key").hashCode() == doc.getInt("hash");
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-15s ", "REMOVE RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						db.getCollection("table").remove(new Document().putNumber("_id", order.get(k)));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
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
