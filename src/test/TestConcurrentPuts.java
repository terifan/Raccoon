package test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import static test._Tools.showTree;


public class TestConcurrentPuts
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.REPLACE, ac);

			long time = System.currentTimeMillis();
			long seed = Math.abs(new Random().nextInt());

			System.out.println(seed);

			int loops = 231;
			List<String> sourceWords = _WordLists.list4342; //;.subList(0, 5);
			Collections.shuffle(sourceWords, new Random(seed));

			RaccoonCollection collection = db.getCollection("words");

			try ( _FixedThreadExecutor exe = new _FixedThreadExecutor(10))
			{
				for (int i = 0; i < loops; i++)
				{
					int _index = i;
					exe.submit(() ->
					{
						for (String word : sourceWords)
						{
							Document doc = new Document().putString("word", word + _index);
							collection.save(doc);
						}
					});
				}
			}

//			showTree(collection.getImplementation());

			System.out.println(System.currentTimeMillis() - time);

			HashSet<String> words = new HashSet<>();

			for (int i = 0; i < loops; i++)
			{
				for (int word = 0; word < sourceWords.size(); word++)
				{
					Document doc = new Document().putNumber("_id", 1 + word * loops + i);
					if (collection.tryGet(doc))
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
