package org.terifan.raccoon;

import org.terifan.raccoon.exceptions.DatabaseException;
import org.terifan.raccoon.exceptions.DatabaseClosedException;
import org.terifan.raccoon.btree.BTree;
import org.terifan.raccoon.document.ObjectId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.RaccoonIOException;
import org.terifan.raccoon.blockdevice.lob.LobOpenOption;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.managed.UnsupportedVersionException;
import org.terifan.raccoon.blockdevice.storage.FileBlockStorage;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.blockdevice.lob.LobByteChannel;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.util.Assert;
import org.terifan.raccoon.util.ReadWriteLock;
import org.terifan.raccoon.util.ReadWriteLock.WriteLock;
import org.terifan.raccoon.util.DualMap;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;
import org.terifan.raccoon.btree.BTreeConfiguration;
import org.terifan.raccoon.util.FutureQueue;

// createCollection - samma som getcollection
// insert - skapar alltid ett nytt _id
// coll:
//  	mapReduce​(String mapFunction, String reduceFunction)

public final class RaccoonDatabase implements AutoCloseable
{
	private final static Logger log = Logger.getLogger();

	private final static String TENANT = "tenant";
	private final static String VERSION = "RaccoonDatabase.1";
	private final static String DIRECTORY = "dir";
	private final static String INTERNAL_PREFIX = "$";
	private final static String LOB_COLLECTION = "$lob.";
	private final static String INDEX_COLLECTION = "$indices";
	private final static String HEAP_COLLECTION = "$heaps";

//	private ExecutorService mExecutor;

	private ManagedBlockDevice mBlockDevice;
	private DatabaseRoot mDatabaseRoot;
	private DatabaseOpenOption mDatabaseOpenOption;
	private ConcurrentSkipListMap<String, RaccoonCollection> mCollectionInstances;
	private ConcurrentHashMap<String, RaccoonHeap> mHeapInstances;
	private ArrayList<DatabaseStatusListener> mDatabaseStatusListener;
	private final ReadWriteLock mLock;

	final DualMap<ObjectId, ObjectId, Document> mIndices;

	private boolean mModified;
	private boolean mReadOnly;
	private boolean mShutdownHookEnabled;
	private Thread mShutdownHook;
	private Timer mMaintenanceTimer;


	@Deprecated
	private RaccoonDatabase()
	{
		mShutdownHookEnabled = true;
		mIndices = new DualMap<>();
		mLock = new ReadWriteLock();
		mCollectionInstances = new ConcurrentSkipListMap<>();
		mHeapInstances = new ConcurrentHashMap<>();
		mDatabaseStatusListener = new ArrayList<>();
//		mExecutor = Executors.newFixedThreadPool(1);
	}


	public RaccoonDatabase(Path aPath, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		FileBlockStorage fileBlockDevice = null;

		try
		{
			if (Files.exists(aPath))
			{
				if (aOpenOptions == DatabaseOpenOption.REPLACE)
				{
					if (!Files.deleteIfExists(aPath))
					{
						throw new RaccoonIOException("Failed to delete existing file: " + aPath);
					}
				}
				else if ((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && Files.size(aPath) == 0)
				{
					throw new RaccoonIOException("File is empty.");
				}
			}
			else if (aOpenOptions == DatabaseOpenOption.OPEN || aOpenOptions == DatabaseOpenOption.READ_ONLY)
			{
				throw new RaccoonIOException("File not found: " + aPath);
			}

			boolean newFile = !Files.exists(aPath);

			fileBlockDevice = new FileBlockStorage(aPath, 4096, aOpenOptions == DatabaseOpenOption.READ_ONLY);

			init(fileBlockDevice, newFile, aOpenOptions, aAccessCredentials);
		}
		catch (DatabaseException | RaccoonIOException | DatabaseClosedException e)
		{
			if (fileBlockDevice != null)
			{
				try
				{
					fileBlockDevice.close();
				}
				catch (Exception ee)
				{
				}
			}

			throw e;
		}
		catch (Throwable e)
		{
			if (fileBlockDevice != null)
			{
				try
				{
					fileBlockDevice.close();
				}
				catch (Exception ee)
				{
				}
			}

			throw new DatabaseException(e);
		}
	}


	public RaccoonDatabase(BlockStorage aBlockDevice, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		Assert.assertFalse((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aBlockDevice.size() == 0, "Block device is empty.");

		boolean create = aBlockDevice.size() == 0 || aOpenOptions == DatabaseOpenOption.REPLACE;

		init(aBlockDevice, create, aOpenOptions, aAccessCredentials);
	}


	public RaccoonDatabase(ManagedBlockDevice aBlockDevice, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		Assert.assertFalse((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aBlockDevice.size() == 0, "Block device is empty.");

		boolean create = aBlockDevice.size() == 0 || aOpenOptions == DatabaseOpenOption.REPLACE;

		init(aBlockDevice, create, aOpenOptions, aAccessCredentials);
	}


	private void init(Object aBlockDevice, boolean aCreate, DatabaseOpenOption aOpenOption, AccessCredentials aAccessCredentials)
	{
		try
		{
			mDatabaseOpenOption = aOpenOption;

			ManagedBlockDevice blockDevice;

			if (aBlockDevice instanceof ManagedBlockDevice v)
			{
				if (aAccessCredentials != null)
				{
					throw new IllegalArgumentException("The BlockDevice provided cannot be secured.");
				}

				blockDevice = v;
			}
			else if (aBlockDevice instanceof BlockStorage v)
			{
				log.d("creating a block device");

				blockDevice = aAccessCredentials == null ? new ManagedBlockDevice(v) : new ManagedBlockDevice(new SecureBlockDevice(aAccessCredentials, v));
			}
			else
			{
				throw new IllegalStateException();
			}

			if (aCreate)
			{
				log.i("create database");
				log.inc();

				blockDevice.getMetadata().put(TENANT, VERSION);

				if (blockDevice.size() > 0)
				{
					blockDevice.clear();
					blockDevice.commit();
				}

				mModified = true;
				mBlockDevice = blockDevice;
				mBlockDevice.getMetadata().put(DIRECTORY, null);
				mDatabaseRoot = new DatabaseRoot(mBlockDevice, mBlockDevice.getMetadata().computeIfAbsent(DIRECTORY, e -> new BTreeConfiguration()));

				commit();

				log.dec();
			}
			else
			{
				log.i("open database");
				log.inc();

				if (!VERSION.equals(blockDevice.getMetadata().getString(TENANT)))
				{
					throw new DatabaseException("Not a Raccoon database file");
				}

				mBlockDevice = blockDevice;
				mDatabaseRoot = new DatabaseRoot(mBlockDevice, new BTreeConfiguration(mBlockDevice.getMetadata().getDocument(DIRECTORY)));
				mReadOnly = mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY;

				RaccoonCollection indices = getCollectionImpl(INDEX_COLLECTION, false);
				if (indices != null)
				{
					indices.forEach(indexConf ->
					{
						mIndices.put(indexConf.getArray("_id").getObjectId(0), indexConf.getArray("_id").getObjectId(1), indexConf);
					});
				}

				log.dec();
			}

			mShutdownHook = new Thread()
			{
				@Override
				public void run()
				{
					if (mShutdownHookEnabled)
					{
						log.i("shutdown hook executing");
						log.inc();

						mShutdownHook = null;

						try
						{
							close();
						}
						catch (Exception e)
						{
							log.e("{}", e);
						}

						log.dec();
					}
				}
			};

			Runtime.getRuntime().addShutdownHook(mShutdownHook);

			mMaintenanceTimer = new Timer(false);
			mMaintenanceTimer.schedule(mMaintenanceTimerTask, 1000, 1000);
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}


	private TimerTask mMaintenanceTimerTask = new TimerTask()
	{
		@Override
		public void run()
		{
//			for (RaccoonCollection instance : mCollectionInstances.values())
//			{
//				instance.flush();
//			}
			for (RaccoonHeap instance : mHeapInstances.values())
			{
				instance.getChannel().flush();
			}
		}
	};


	public ArrayList<String> listCollectionNames()
	{
		checkOpen();
		return new ArrayList<>(mDatabaseRoot.list().stream().filter(e -> !e.startsWith(INTERNAL_PREFIX)).collect(Collectors.toList()));
	}


	public ArrayList<String> listDirectoryNames()
	{
		checkOpen();
		return new ArrayList<>(mDatabaseRoot.list().stream().filter(e -> e.startsWith(LOB_COLLECTION)).collect(Collectors.toList()));
	}


	public synchronized boolean existsCollection(String aName)
	{
		checkOpen();
		return mCollectionInstances.containsKey(aName) || mDatabaseRoot.exists(aName);
	}


	public synchronized RaccoonCollection getIndex(String aName)
	{
		for (HashMap<ObjectId, Document> entry : mIndices.values())
		{
			for (Document conf : entry.values())
			{
				if (aName.equals(conf.getDocument("configuration").getString("name")))
				{
					return getCollection("index:" + conf.getObjectId("_id"));
				}
			}
		}
		return null;
	}


	public synchronized RaccoonCollection getCollection(String aName)
	{
		if (aName.startsWith(INTERNAL_PREFIX))
		{
			throw new IllegalArgumentException("Collection names cannot start with dollar sign.");
		}

		return getCollectionImpl(aName, true);
	}


	private synchronized RaccoonCollection getCollectionImpl(String aName, boolean aCreateMissing)
	{
		checkOpen();

		RaccoonCollection instance = mCollectionInstances.get(aName);
		if (instance != null)
		{
			return instance;
		}

		BTreeConfiguration conf = mDatabaseRoot.get(aName);
		if (conf != null)
		{
			instance = new RaccoonCollection(this, conf);
			mCollectionInstances.put(aName, instance);
			return instance;
		}

		if (!aCreateMissing)
		{
			return null;
		}

		if (mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY)
		{
			throw new DatabaseException("No such collection: " + aName);
		}

		log.i("create table {} with option {}", aName, mDatabaseOpenOption);
		log.inc();

		try
		{
			conf = new BTreeConfiguration().put("_id", ObjectId.randomId());
			instance = new RaccoonCollection(this, conf);
			mDatabaseRoot.put(aName, conf);
			mCollectionInstances.put(aName, instance);
		}
		finally
		{
			log.dec();
		}

		return instance;
	}


	void removeCollectionImpl(RaccoonCollection aCollection) throws IOException, InterruptedException, ExecutionException
	{
		for (Document indexConf : mIndices.values(aCollection.getCollectionId()))
		{
			aCollection.getIndexByConf(indexConf).drop();
		}

		for (Entry<String, RaccoonCollection> en : mCollectionInstances.entrySet())
		{
			if (en.getValue() == aCollection)
			{
				mDatabaseRoot.remove(en.getKey());
				mCollectionInstances.remove(en.getKey());
				break;
			}
		}
		mModified = true;
	}


	void removeDirectoryImpl(RaccoonDirectory aDirectory) throws IOException, InterruptedException, ExecutionException
	{
		removeCollectionImpl(aDirectory.getCollection());
	}


	public RaccoonDirectory getDirectory(String aName)
	{
		RaccoonCollection collection = getCollectionImpl(LOB_COLLECTION + aName, true);

		return new RaccoonDirectory(this, collection);
	}


	public RaccoonHeap getHeap(String aName) throws IOException, InterruptedException, ExecutionException
	{
		return getHeap(aName, null);
	}


	public RaccoonHeap getHeap(String aName, Document aOptions) throws IOException, InterruptedException, ExecutionException
	{
		if (mHeapInstances.containsKey(aName))
		{
			return mHeapInstances.get(aName);
		}

		RaccoonCollection collection = getCollectionImpl(HEAP_COLLECTION, true);

		Document header = new Document().put("_id", aName);
		collection.tryFindOne(header);

		header.putIfAbsent("heap", k -> new Document().put("record", 128));

		BlockAccessor blockAccessor = getBlockAccessor();

		LobByteChannel channel = new LobByteChannel(blockAccessor, header, mReadOnly ? LobOpenOption.READ : LobOpenOption.WRITE, aOptions).setCloseAction(ch -> {
			if (!mReadOnly)
			{
				collection.saveOne(header);
				mModified = true;
			}
		});

		RaccoonHeap heap = new RaccoonHeap(blockAccessor, channel, header.getDocument("heap").getInt("record"), he -> {
			mHeapInstances.remove(aName);
		});
		mHeapInstances.put(aName, heap);

		return heap;
	}


	private void checkOpen()
	{
		if (mDatabaseRoot == null)
		{
			throw new DatabaseClosedException("Database is closed");
		}
	}


//	public boolean isModified()
//	{
//		checkOpen();
//
//		for (RaccoonCollection instance : mCollectionInstances.values())
//		{
//			if (instance.isChanged())
//			{
//				return true;
//			}
//		}
//
//		return false;
//	}


	public boolean isOpen()
	{
		return mDatabaseRoot != null;
	}


//	public void flush()
//	{
//		try (WriteLock lock = mLock.writeLock())
//		{
//			checkOpen();
//
//			log.i("flush changes");
//			log.inc();
//
//			for (RaccoonCollection entry : mCollectionInstances.values())
//			{
//				entry.flush();
//			}
//
//			log.dec();
//		}
//	}


	/**
	 * Persists all pending changes. It's necessary to commit changes on a regular basis to avoid data loss.
	 */
	public void commit()
	{
		try (WriteLock lock = mLock.writeLock())
		{
			checkOpen();

			commitImpl();
		}
		finally
		{
			log.dec();
		}
	}


	/**
	 * Reverts all pending changes.
	 */
	public void rollback()
	{
		try (WriteLock lock = mLock.writeLock())
		{
			checkOpen();

			rollbackImpl();
		}
		finally
		{
			log.dec();
		}
	}


	private void commitImpl()
	{
		log.i("commit database");
		log.inc();

		try (FutureQueue queue = new FutureQueue()) // FutureQueue blocks until all futures have been executed
		{
			for (Entry<String, RaccoonCollection> entry : mCollectionInstances.entrySet())
			{
				Runnable onModified = () -> {
					log.i("table updated {}", entry.getKey());

					mDatabaseRoot.put(entry.getKey(), entry.getValue().getConfiguration());
					mModified = true;
				};

				queue.add(entry.getValue().commit(onModified));
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}

		if (mModified)
		{
			log.i("updating super block");
			log.inc();

			mDatabaseRoot.commit(mBlockDevice);

			mBlockDevice.getMetadata().put(DIRECTORY, mDatabaseRoot.getConfiguration());
			try
			{
				mBlockDevice.commit();
			}
			catch (Exception e)
			{
				e.printStackTrace(System.out);
			}
			mModified = false;

			assert integrityCheck() == null : integrityCheck();

			log.dec();
		}
	}


	private void rollbackImpl()
	{
		log.i("rollback");
		log.inc();

		try
		{
			try (FutureQueue queue = new FutureQueue()) // FutureQueue blocks until all futures have been executed
			{
				for (RaccoonCollection instance : mCollectionInstances.values())
				{
					queue.add(instance.rollback());
				}
			}

			mDatabaseRoot = new DatabaseRoot(mBlockDevice, new BTreeConfiguration(mBlockDevice.getMetadata().getDocument(DIRECTORY)));
			mBlockDevice.rollback();
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
		finally
		{
			log.dec();
		}
	}


	@Override
	public void close() throws Exception
	{
		if (mBlockDevice == null)
		{
			log.w("database already closed");
			return;
		}

		try (WriteLock lock = mLock.writeLock())
		{
			commitImpl();

			if (mMaintenanceTimer != null)
			{
				mMaintenanceTimer.cancel();
				mMaintenanceTimer = null;
			}

			if (mShutdownHook != null)
			{
				try
				{
					Runtime.getRuntime().removeShutdownHook(mShutdownHook);
				}
				catch (Exception e)
				{
					// ignore this
				}
			}

			if (mReadOnly)
			{
				return;
			}

			log.d("begin closing database");
			log.inc();

			for (RaccoonCollection instance : mCollectionInstances.values())
			{
				try
				{
					instance.close();
				}
				catch (Exception | Error e)
				{
					e.printStackTrace(System.out);
				}
			}

			mCollectionInstances.clear();
			mCollectionInstances = null;

			mDatabaseRoot.commit(mBlockDevice);
			mDatabaseRoot = null;

			if (mBlockDevice != null)
			{
				mBlockDevice.commit();
				mBlockDevice.close();
				mBlockDevice = null;
			}

			log.i("database was closed");
			log.dec();
		}
		catch (Exception e)
		{
			throw new IllegalStateException(e);
		}
	}


	ManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	public String integrityCheck()
	{
		for (RaccoonCollection instance : mCollectionInstances.values())
		{
			String s = instance._getImplementation().integrityCheck();
			if (s != null)
			{
				return s;
			}
		}

		return null;
	}


	private void reportStatus(Level aLevel, String aMessage, Throwable aThrowable)
	{
		System.out.printf("%-6s%s%n", aLevel, aMessage);

		for (DatabaseStatusListener listener : mDatabaseStatusListener)
		{
			listener.statusChanged(aLevel, aMessage, aThrowable);
		}
	}


	public void addStatusListener(DatabaseStatusListener aListener)
	{
		mDatabaseStatusListener.add(aListener);
	}


	BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(getBlockDevice(), true);
	}


	public boolean isShutdownHookEnabled()
	{
		return mShutdownHookEnabled;
	}


	/**
	 * Enables or disable the shutdown hook that will close the database and release resources on a JVM shutdown. Enabled by default.
	 */
	public RaccoonDatabase setShutdownHookEnabled(boolean aShutdownHookEnabled)
	{
		mShutdownHookEnabled = aShutdownHookEnabled;
		return this;
	}
}
