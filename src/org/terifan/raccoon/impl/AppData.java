package org.terifan.raccoon.impl;

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
import org.terifan.raccoon.blockdevice.managed.UnsupportedVersionException;
import org.terifan.raccoon.document.Document;


/**
 * The AppData class is a simple wrapper for a Raccoon database allowing an application to use Raccoon as a persistent store without "thinking".
 *
 * AppData appdata = new AppData("my application name");
 * appdata.save(Document.of("{_id:a,a:2}"));
 * System.out.println(appdata.load(Document.of("{_id:a}")));
 */
public class AppData
{
	private final static int CAPACITY = 1000;

	private String mApplicationName;
	private RaccoonDatabase mDatabase;
	private TimerTask mTimerTask;

	private final LinkedHashMap<Object, Document> mCache = new LinkedHashMap<>(CAPACITY + 1, 0.75f, true)
	{
		@Override
		protected boolean removeEldestEntry(Map.Entry<Object, Document> aEldest)
		{
			return size() > CAPACITY;
		}
	};


	public AppData(String aApplicationName) throws IOException
	{
		mApplicationName = aApplicationName;

		final Thread shutdownHook = new Thread()
		{
			@Override
			public void run()
			{
				if (mTimerTask != null)
				{
					mTimerTask.cancel();
				}

				if (mDatabase != null)
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
			}
		};

		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}


	private synchronized void initialize()
	{
		if (mDatabase != null)
		{
			return;
		}

		mTimerTask = new TimerTask()
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
		};

		try
		{
			mDatabase = new RaccoonDatabase(getDatabaseFile(), DatabaseOpenOption.CREATE, null);
			mDatabase.setShutdownHookEnabled(false);
		}
		catch (IOException | UnsupportedVersionException e)
		{
			throw new IllegalStateException(e);
		}

		new Timer(true).schedule(mTimerTask, 10_000, 10_000);
	}


	/**
	 * Close the database and release any cached items. Calling this method is optional since the JVM shutdownhook will handle this.
	 */
	public synchronized void close()
	{
		if (mTimerTask != null)
		{
			mTimerTask.cancel();
		}
		if (mDatabase != null)
		{
			mDatabase.close();
			mDatabase = null;
		}

		mCache.clear();
	}


	/**
	 * Delete the database and release all cached items. Any calls to the save/load/delete methods will create a new empty database.
	 */
	public synchronized void destroy()
	{
		close();

		try
		{
			Files.deleteIfExists(getDatabaseFile());
		}
		catch (IOException e)
		{
		}
	}


	public synchronized void put(Document aDocument)
	{
		validate(aDocument);
		initialize();

		mCache.put(aDocument.get("_id"), aDocument);
		mDatabase.getCollection("ps").save(aDocument);
	}


	public synchronized Document get(Document aDocument)
	{
		validate(aDocument);
		initialize();

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


	public synchronized void delete(Document aDocument)
	{
		validate(aDocument);
		initialize();

		Object key = aDocument.get("_id");
		mCache.remove(key);
		mDatabase.getCollection("ps").delete(aDocument);
	}


	private void validate(Document aDocument) throws IllegalArgumentException
	{
		if (aDocument == null)
		{
			throw new IllegalArgumentException("aDocument is null.");
		}
		if (!aDocument.containsKey("_id"))
		{
			throw new IllegalArgumentException("An _id field must be provided for the document.");
		}
	}


	private Path getDatabaseFile() throws IOException
	{
		String workingDirectory;
		String OS = (System.getProperty("os.name")).toUpperCase();
		if (OS.contains("WIN"))
		{
			workingDirectory = System.getenv("AppData");
		}
		else
		{
			workingDirectory = System.getProperty("user.home");
			if (Files.isDirectory(Paths.get(workingDirectory, "/Library/Application Support")))
			{
				workingDirectory += "/Library/Application Support";
			}
		}

		Path path = Paths.get(workingDirectory, "AppData_" + mApplicationName.toLowerCase().replace(' ', '_') + "_" + Integer.toString(Math.abs(mApplicationName.hashCode()), 16) + ".rdb");
		return path;
	}


	public static void main(String ... args)
	{
		try
		{
			AppData appdata = new AppData("my application name");
			appdata.put(Document.of("{_id:a,a:2}"));

			System.out.println(appdata.get(Document.of("{_id:a}")));

			appdata.destroy();

			System.out.println(appdata.get(Document.of("{_id:a}")));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
