package test;

import java.util.ArrayList;
import java.util.HashSet;
import org.terifan.bundle.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestConcurrentPuts
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.REPLACE, ac);

			long time = System.currentTimeMillis();

			int loops = 10;
			ArrayList<String> sourceWords = _WordLists.list4342;

			try ( _FixedThreadExecutor exe = new _FixedThreadExecutor(1))
			{
				for (int worker = 0; worker < loops; worker++)
				{
					int _worker = worker;
					exe.submit(() ->
					{
						for (String word : sourceWords)
						{
							try
							{
								Document doc = new Document().putString("word", word + _worker);
								db.getCollection("words").save(doc);
							}
							catch (Exception e)
							{
								e.printStackTrace(System.out);
							}
						}
					});
				}
			}

			System.out.println(System.currentTimeMillis() - time);

			HashSet<String> words = new HashSet<>();

			for (int worker = 0; worker < loops; worker++)
			{
				for (int word = 0; word < sourceWords.size(); word++)
				{
					Document doc = new Document().putNumber("_id", 1 + word * loops + worker);
					if (db.getCollection("words").tryGet(doc))
					{
						words.add(doc.getString("word"));
					}
				}
			}

			System.out.println(words.size() + " == " + sourceWords.size() * loops);
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
