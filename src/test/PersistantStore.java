package test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.document.Document;


public class PersistantStore
{
	private final static int CAPACITY = 1000;

	private RaccoonDatabase mDatabase;

	private final LinkedHashMap<Object, Document> mCache = new LinkedHashMap<>(CAPACITY + 1, 0.75f, true)
	{
		@Override
		protected boolean removeEldestEntry(Map.Entry<Object, Document> aEldest)
		{
			return size() > CAPACITY;
		}
	};


	public PersistantStore(String aApplicationName) throws IOException
	{
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run()
			{
				RaccoonDatabase db = mDatabase;
				mDatabase = null;

				try
				{
					db.commit();
				}
				catch (Exception | Error e)
				{
					e.printStackTrace(System.out);
				}
				try
				{
					db.close();
				}
				catch (Exception | Error e)
				{
					e.printStackTrace(System.out);
				}
			}
		});

		mDatabase = new RaccoonDatabase(getTempDirectory(aApplicationName).resolve("persistantstore.rdb"), DatabaseOpenOption.CREATE, null);

		new Timer(true).schedule(new TimerTask()
		{
			@Override
			public void run()
			{
				if (mDatabase == null)
				{
					cancel();
				}
				try
				{
					mDatabase.commit();
				}
				catch (Exception | Error e)
				{
					e.printStackTrace(System.out);
				}
			}
		}, 10000, 10000);
	}


	public synchronized void save(Document aDocument)
	{
		if (!aDocument.containsKey("_id"))
		{
			throw new IllegalArgumentException("An _id field must be provided for the document.");
		}

		mCache.put(aDocument.get("_id"), aDocument);

		mDatabase.getCollection("ps").save(aDocument);
	}


	public synchronized Document load(Document aDocument)
	{
		if (!aDocument.containsKey("_id"))
		{
			throw new IllegalArgumentException("An _id field must be provided for the document.");
		}

		Object key = aDocument.get("_id");
		Document doc = mCache.get(key);

		if (doc != null)
		{
			return doc;
		}

		if (mDatabase.getCollection("ps").tryGet(aDocument))
		{
			mCache.put(key, aDocument.clone());
		}

		return aDocument;
	}


	public synchronized void remove(Document aDocument)
	{
		if (!aDocument.containsKey("_id"))
		{
			throw new IllegalArgumentException("An _id field must be provided for the document.");
		}

		Object key = aDocument.get("_id");
		mCache.remove(key);
		mDatabase.getCollection("ps").delete(aDocument);
	}


	protected Path getTempDirectory(String aApplicationName) throws IOException
	{
		Path dir = Paths.get(System.getProperty("java.io.tmpdir"));
		if (!Files.exists(dir))
		{
			throw new IllegalStateException("No temporary directory exists in this environment.");
		}

		dir = dir.resolve(aApplicationName + "_" + Integer.toString(Math.abs(aApplicationName.hashCode()), 16));
		Files.createDirectories(dir);

		if (!Files.exists(dir))
		{
			throw new IllegalStateException("Failed to create temporary directory at " + dir);
		}

		return dir;
	}


	public static void main(String ... args)
	{
		try
		{
			PersistantStore ps = new PersistantStore("ApplicationName");
//			ps.save(Document.of("{_id:a,a:2}"));

			Document doc = ps.load(Document.of("{_id:a}"));
			System.out.println(doc);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
