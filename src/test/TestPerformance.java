package test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RuntimeDiagnostics;
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

			System.out.println(N + " x " + M);

			ArrayList<Integer> order = new ArrayList<>();
			for (int i = 1; i <= N*M;i++)
			{
				order.add(i);
			}

//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			System.out.printf("%-15s ", "SAVE SEQ");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				for (int j = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++)
					{
						String s = value();
						db.getCollection("table").save(new Document().put("value", s).put("hash", s.hashCode()));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-15s ", "SAVE ALL SEQ");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				for (int j = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					ArrayList<Document> documents = new ArrayList<>();
					for (int i = 0; i < N; i++)
					{
						String s = value();
						documents.add(new Document().put("value", s).put("hash", s.hashCode()));
					}
					db.getCollection("table").saveAll(documents.toArray(new Document[0]));
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-15s ", "SELECT SEQ");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").get(new Document().put("_id", k));
						assert doc.getString("value").hashCode() == doc.getInt("hash");
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-15s ", "UPDATE SEQ");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						db.getCollection("table").save(new Document().put("_id", k).put("value", s).put("hash", s.hashCode()));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-15s ", "SELECT SEQ");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
//				db.getCollection("table").stream().forEach(System.out::println);
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").get(new Document().put("_id", k));
						assert doc.getString("value").hashCode() == doc.getInt("hash");
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			System.out.printf("%-15s ", "DELETE SEQ");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						db.getCollection("table").delete(new Document().put("_id", k));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();




			Collections.shuffle(order, rnd);

			System.out.printf("%-15s ", "INSERT RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						db.getCollection("table").save(new Document().put("_id", order.get(k)).put("value", s).put("hash", s.hashCode()));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf("%-15s ", "SELECT RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").get(new Document().put("_id", order.get(k)));
						assert doc.getString("value").hashCode() == doc.getInt("hash");
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf("%-15s ", "UPDATE RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						db.getCollection("table").save(new Document().put("_id", order.get(k)).put("value", s).put("hash", s.hashCode()));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf("%-15s ", "SELECT RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").get(new Document().put("_id", order.get(k)));
						assert doc.getString("value").hashCode() == doc.getInt("hash");
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf("%-15s ", "REMOVE RANDOM");
			try (RaccoonDatabase db = new RaccoonDatabase(new File("c:\\temp\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						db.getCollection("table").delete(new Document().put("_id", order.get(k)));
					}
					System.out.printf("%8d", System.currentTimeMillis() - t);
					db.commit();
				}
			}
			System.out.println();

			RuntimeDiagnostics.print();
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
			buf[i] = (byte)(32 + rnd.nextInt(95));
		}
		return new String(buf);
	}
}
