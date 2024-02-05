package test_raccoon;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import static org.terifan.raccoon.blockdevice.util.ValueFormatter.formatDuration;


public class TestManyUpdates
{
	private final static Random rnd = new Random(1);


	public static void main(String... args)
	{
		try
		{
			String labelPattern = "%-15s ";
			String durationPattern = "%9s";

			int N = 1000;
			int M = 10;

			System.out.println(N + " x " + M);

			ArrayList<Integer> order = new ArrayList<>();
			for (int i = 1; i <= N * M; i++)
			{
				order.add(i);
			}

			RaccoonBuilder builder = new RaccoonBuilder().device("c:\\temp\\test.rdb");

			System.out.printf(labelPattern, "SAVE SEQ");
			try (RaccoonDatabase db = builder.get(DatabaseOpenOption.REPLACE))
			{
				for (int j = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++)
					{
						String s = value();
						db.getCollection("table").saveOne(new Document().put("value", s).put("hash", s.hashCode()));
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			System.out.printf(labelPattern, "SAVE ALL SEQ");
			try (RaccoonDatabase db = builder.get(DatabaseOpenOption.REPLACE))
			{
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					ArrayList<Document> documents = new ArrayList<>();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						documents.add(new Document().put("_id", k).put("value", s).put("hash", s.hashCode()));
					}
					db.getCollection("table").saveMany(documents.toArray(new Document[0]));
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			System.out.printf(labelPattern, "SELECT SEQ");
			try (RaccoonDatabase db = builder.get())
			{
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").findOne(new Document().put("_id", k));
						assert doc.getString("value").hashCode() == doc.getInt("hash");
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			System.out.printf(labelPattern, "UPDATE SEQ");
			try (RaccoonDatabase db = builder.get(DatabaseOpenOption.REPLACE))
			{
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						db.getCollection("table").saveOne(new Document().put("_id", k).put("value", s).put("hash", s.hashCode()));
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			System.out.printf(labelPattern, "SELECT SEQ");
			try (RaccoonDatabase db = builder.get())
			{
//				db.getCollection("table").stream().forEach(System.out::println);
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").findOne(new Document().put("_id", k));
						assert doc.getString("value").hashCode() == doc.getInt("hash");
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			System.out.printf(labelPattern, "DELETE SEQ");
			try (RaccoonDatabase db = builder.get())
			{
				for (int j = 0, k = 1; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						db.getCollection("table").deleteOne(new Document().put("_id", k));
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf(labelPattern, "INSERT RANDOM");
			try (RaccoonDatabase db = builder.get(DatabaseOpenOption.REPLACE))
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						db.getCollection("table").saveOne(new Document().put("_id", order.get(k)).put("value", s).put("hash", s.hashCode()));
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf(labelPattern, "SELECT RANDOM");
			try (RaccoonDatabase db = builder.get())
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").findOne(new Document().put("_id", order.get(k)));
						assert doc.getString("value").hashCode() == doc.getInt("hash");
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf(labelPattern, "UPDATE RANDOM");
			try (RaccoonDatabase db = builder.get())
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						String s = value();
						db.getCollection("table").saveOne(new Document().put("_id", order.get(k)).put("value", s).put("hash", s.hashCode()));
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf(labelPattern, "SELECT RANDOM");
			try (RaccoonDatabase db = builder.get())
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						Document doc = db.getCollection("table").findOne(new Document().put("_id", order.get(k)));
						assert doc.getString("value").hashCode() == doc.getInt("hash");
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

			Collections.shuffle(order, rnd);

			System.out.printf(labelPattern, "REMOVE RANDOM");
			try (RaccoonDatabase db = builder.get())
			{
				for (int j = 0, k = 0; j < M; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < N; i++, k++)
					{
						db.getCollection("table").deleteOne(new Document().put("_id", order.get(k)));
					}
					System.out.printf(durationPattern, formatDuration(-t));
					db.commit();
				}
			}
			System.out.println();

//			RuntimeDiagnostics.print();
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
