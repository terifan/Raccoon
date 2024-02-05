package test_raccoon;

import java.util.ArrayList;
import java.util.HashSet;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;


public class TestConcurrentGets
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockStorage blockDevice = new MemoryBlockStorage(512);
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
						collection.saveOne(new Document().put("word", word + workerIndex));
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
							Document doc = new Document().put("_id", 1 + wordIndex * loops + workerIndex);
							if (collection.tryFindOne(doc))
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
