package test;

import java.util.ArrayList;
import java.util.HashSet;
import org.terifan.bundle.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestConcurrentGets
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.REPLACE, ac);

			int loops = 1000;

			ArrayList<String> sourceWords = _WordLists.list4342;
			RaccoonCollection collection = db.getCollection("words");

			for (int workerIndex = 0; workerIndex < loops; workerIndex++)
			{
				for (String word : sourceWords)
				{
					try
					{
						collection.save(new Document().putString("word", word + workerIndex));
					}
					catch (Exception e)
					{
						e.printStackTrace(System.out);
					}
				}
			}

			HashSet<String> words = new HashSet<>();

			try ( _FixedThreadExecutor exe = new _FixedThreadExecutor(10))
			{
				for (int worker = 0; worker < loops; worker++)
				{
					int workerIndex = worker;
					exe.submit(() ->
					{
						for (int wordIndex = 0; wordIndex < sourceWords.size(); wordIndex++)
						{
							Document doc = new Document().putNumber("_id", 1 + wordIndex * loops + workerIndex);
							if (collection.tryGet(doc))
							{
								synchronized (words)
								{
									words.add(doc.getString("word"));
								}
							}
						}
					});
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
