package test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.terifan.bundle.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestConcurrentInserts
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.REPLACE, ac);

			int loops = 10;
			ArrayList<String> sourceWords = _WordLists.list4342;

			try ( _FixedThreadExecutor exe = new _FixedThreadExecutor(2))
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
								db.getCollection("words").save(new Document().putString("word", word + _worker));
							}
							catch (Exception e)
							{
								e.printStackTrace(System.out);
							}
						}
					});
				}
			}

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
