package org.terifan.raccoon;

import org.terifan.raccoon.document.ObjectId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.DeviceException;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.managed.UnsupportedVersionException;
import org.terifan.raccoon.blockdevice.physical.FileBlockDevice;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.blockdevice.util.LogLevel;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.util.Assert;
import org.terifan.raccoon.util.ReadWriteLock;
import org.terifan.raccoon.util.ReadWriteLock.WriteLock;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;
import org.terifan.raccoon.util.DualMap;

// createCollection - samma som getcollection
// insert - skapar alltid ett nytt _id
// coll:
//  	mapReduce​(String mapFunction, String reduceFunction)

public final class RaccoonDatabase implements AutoCloseable
{
	private final static String TENANT = "tenant";
	private final static String VERSION = "RaccoonDatabase.1";
	private final static String DIRECTORY = "0";
	private final static String INTERNAL_PREFIX = "$";
	private final static String LOB_COLLECTION = "$lob.";
	private final static String INDEX_COLLECTION = "$indices";
	private final static String HEAP_COLLECTION = "$heaps";

	private ManagedBlockDevice mBlockDevice;
	private DatabaseRoot mDatabaseRoot;
	private DatabaseOpenOption mDatabaseOpenOption;
	private final ConcurrentSkipListMap<String, RaccoonCollection> mCollectionInstances;
	private final ConcurrentHashMap<String, RaccoonHeap> mHeapInstances;
	private final ArrayList<DatabaseStatusListener> mDatabaseStatusListener;
	private final ReadWriteLock mLock;

	final DualMap<ObjectId, ObjectId, Document> mIndices;

	private boolean mModified;
	private boolean mCloseDeviceOnCloseDatabase;
	private boolean mReadOnly;
	private boolean mShutdownHookEnabled;
	private Thread mShutdownHook;


	private RaccoonDatabase()
	{
		mShutdownHookEnabled = true;
		mIndices = new DualMap<>();
		mLock = new ReadWriteLock();
		mCollectionInstances = new ConcurrentSkipListMap<>();
		mHeapInstances = new ConcurrentHashMap<>();
		mDatabaseStatusListener = new ArrayList<>();
	}


	public RaccoonDatabase(Path aPath, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		FileBlockDevice fileBlockDevice = null;

		try
		{
			if (Files.exists(aPath))
			{
				if (aOpenOptions == DatabaseOpenOption.REPLACE)
				{
					if (!Files.deleteIfExists(aPath))
					{
						throw new DeviceException("Failed to delete existing file: " + aPath);
					}
				}
				else if ((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && Files.size(aPath) == 0)
				{
					throw new DeviceException("File is empty.");
				}
			}
			else if (aOpenOptions == DatabaseOpenOption.OPEN || aOpenOptions == DatabaseOpenOption.READ_ONLY)
			{
				throw new DeviceException("File not found: " + aPath);
			}

			boolean newFile = !Files.exists(aPath);

			fileBlockDevice = new FileBlockDevice(aPath, 4096, aOpenOptions == DatabaseOpenOption.READ_ONLY);

			init(fileBlockDevice, newFile, true, aOpenOptions, aAccessCredentials);
		}
		catch (DatabaseException | DeviceException | DatabaseClosedException e)
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


	public RaccoonDatabase(PhysicalBlockDevice aBlockDevice, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		Assert.fail((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aBlockDevice.size() == 0, "Block device is empty.");

		boolean create = aBlockDevice.size() == 0 || aOpenOptions == DatabaseOpenOption.REPLACE;

		init(aBlockDevice, create, false, aOpenOptions, aAccessCredentials);
	}


	public RaccoonDatabase(ManagedBlockDevice aBlockDevice, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		Assert.fail((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aBlockDevice.size() == 0, "Block device is empty.");

		boolean create = aBlockDevice.size() == 0 || aOpenOptions == DatabaseOpenOption.REPLACE;

		init(aBlockDevice, create, false, aOpenOptions, aAccessCredentials);
	}


	private void init(Object aBlockDevice, boolean aCreate, boolean aCloseDeviceOnCloseDatabase, DatabaseOpenOption aOpenOption, AccessCredentials aAccessCredentials)
	{
		try
		{
			mDatabaseOpenOption = aOpenOption;

			ManagedBlockDevice blockDevice;

			if (aBlockDevice instanceof ManagedBlockDevice)
			{
				if (aAccessCredentials != null)
				{
					throw new IllegalArgumentException("The BlockDevice provided cannot be secured.");
				}

				blockDevice = (ManagedBlockDevice)aBlockDevice;
			}
			else if (aAccessCredentials == null)
			{
				Log.d("creating a managed block device");

				blockDevice = new ManagedBlockDevice((PhysicalBlockDevice)aBlockDevice);
			}
			else
			{
				Log.d("creating a secure block device");

				blockDevice = new ManagedBlockDevice(new SecureBlockDevice(aAccessCredentials, (PhysicalBlockDevice)aBlockDevice));
			}

			if (aCreate)
			{
				Log.i("create database");
				Log.inc();

				blockDevice.getMetadata().put(TENANT, VERSION);

				if (blockDevice.size() > 0)
				{
					blockDevice.clear();
					blockDevice.commit();
				}

				mModified = true;
				mBlockDevice = blockDevice;
				mBlockDevice.getMetadata().put(DIRECTORY, BTree.createDefaultConfig());
				mDatabaseRoot = new DatabaseRoot(mBlockDevice, mBlockDevice.getMetadata().getDocument(DIRECTORY));

				commit();

				Log.dec();
			}
			else
			{
				Log.i("open database");
				Log.inc();

				if (!VERSION.equals(blockDevice.getMetadata().getString(TENANT)))
				{
					throw new DatabaseException("Not a Raccoon database file");
				}

				mBlockDevice = blockDevice;
				mDatabaseRoot = new DatabaseRoot(mBlockDevice, mBlockDevice.getMetadata().getDocument(DIRECTORY));
				mReadOnly = mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY;

				RaccoonCollection indices = getCollectionImpl(INDEX_COLLECTION, false);
				if (indices != null)
				{
					for (Document indexConf : indices.listAll())
					{
						mIndices.put(indexConf.getArray("_id").getObjectId(0), indexConf.getArray("_id").getObjectId(1), indexConf);
					}
				}

				Log.dec();
			}

			mCloseDeviceOnCloseDatabase = aCloseDeviceOnCloseDatabase;

			mShutdownHook = new Thread()
			{
				@Override
				public void run()
				{
					if (mShutdownHookEnabled)
					{
						Log.i("shutdown hook executing");
						Log.inc();

						mShutdownHook = null;

						close();

						Log.dec();
					}
				}
			};

			Runtime.getRuntime().addShutdownHook(mShutdownHook);
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}


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

		Document conf = mDatabaseRoot.get(aName);
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

		Log.i("create table '%s' with option %s", aName, mDatabaseOpenOption);
		Log.inc();

		try
		{
			conf = createDefaultConfig();
			instance = new RaccoonCollection(this, conf);
			mDatabaseRoot.put(aName, conf);
			mCollectionInstances.put(aName, instance);
		}
		finally
		{
			Log.dec();
		}

		return instance;
	}


	public synchronized void removeCollection(RaccoonCollection aCollection)
	{
		checkOpen();

		aCollection.clear();

		removeCollectionImpl(aCollection);
	}


	public synchronized void removeDirectory(RaccoonDirectory aDirectory)
	{
		checkOpen();

		if (aDirectory.size() != 0)
		{
			throw new IllegalStateException("The RaccoonDirectory is not empty.");
		}

		removeCollectionImpl(aDirectory.getCollection());
	}


	private void removeCollectionImpl(RaccoonCollection aCollection)
	{
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


	public RaccoonDirectory getDirectory(String aName)
	{
		RaccoonCollection collection = getCollectionImpl(LOB_COLLECTION + aName, true);

		return new RaccoonDirectory(collection);
	}


	public RaccoonHeap getHeap(String aName) throws IOException
	{
		if (mHeapInstances.containsKey(aName))
		{
			return mHeapInstances.get(aName);
		}

		RaccoonCollection collection = getCollectionImpl(HEAP_COLLECTION, true);

		Document header = new Document().put("_id", aName);
		if (collection.tryGet(header))
		{
//			LobByteChannel channel = new LobByteChannel(getBlockAccessor(), header, LobOpenOption.WRITE, null);
//			return new RaccoonHeap(getBlockAccessor(), channel, 128);
		}

		BlockAccessor blockAccessor = getBlockAccessor();

		LobByteChannel channel = new LobByteChannel(blockAccessor, header, LobOpenOption.CREATE, ch -> {
			mModified = true;
		}, 1, false);

		RaccoonHeap heap = new RaccoonHeap(blockAccessor, channel, 128, he -> {
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


	public boolean isModified()
	{
		checkOpen();

		for (RaccoonCollection instance : mCollectionInstances.values())
		{
			if (instance.isModified())
			{
				return true;
			}
		}

		return false;
	}


	public boolean isOpen()
	{
		return mDatabaseRoot != null;
	}


	public long flush()
	{
		checkOpen();

		long nodesWritten = 0;

		try (WriteLock lock = mLock.writeLock())
		{
			Log.i("flush changes");
			Log.inc();

			for (RaccoonCollection entry : mCollectionInstances.values())
			{
				nodesWritten = entry.flush();
			}

			Log.dec();
		}

		return nodesWritten;
	}


	/**
	 * Persists all pending changes. It's necessary to commit changes on a regular basis to avoid data loss.
	 */
	public void commit()
	{
		try
		{
			checkOpen();

			Log.i("commit database");
			Log.inc();

			try (WriteLock lock = mLock.writeLock())
			{
				for (Entry<String, RaccoonCollection> entry : mCollectionInstances.entrySet())
				{
					if (entry.getValue().commit())
					{
						Log.i("table updated '%s'", entry.getKey());

						mDatabaseRoot.put(entry.getKey(), entry.getValue().getConfiguration());
						mModified = true;
					}
				}

				if (mModified)
				{
					Log.i("updating super block");
					Log.inc();

					mBlockDevice.getMetadata().put(DIRECTORY, mDatabaseRoot.commit(mBlockDevice));
					mBlockDevice.commit();
					mModified = false;

					assert integrityCheck() == null : integrityCheck();

					Log.dec();
				}
			}
			finally
			{
				Log.dec();
			}
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}


	/**
	 * Reverts all pending changes.
	 */
	public void rollback()
	{
		checkOpen();

		Log.i("rollback");
		Log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			for (RaccoonCollection instance : mCollectionInstances.values())
			{
				instance.rollback();
			}

			mDatabaseRoot = new DatabaseRoot(mBlockDevice, mBlockDevice.getMetadata().getDocument(DIRECTORY));
			mBlockDevice.rollback();
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
		finally
		{
			Log.dec();
		}
	}


	@Override
	public void close()
	{
		try (WriteLock lock = mLock.writeLock())
		{
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

			if (mBlockDevice == null)
			{
				Log.w("database already closed");
				return;
			}

			if (mReadOnly)
			{
				if (mModified)
				{
					reportStatus(LogLevel.WARN, "readonly database modified, changes are not committed", null);
				}
				return;
			}

			Log.d("begin closing database");
			Log.inc();

			if (!mModified)
			{
				for (RaccoonCollection entry : mCollectionInstances.values())
				{
					mModified |= entry.isModified();
				}
			}

			if (mModified)
			{
				Log.w("rollback on close");
				Log.inc();

				for (RaccoonCollection instance : mCollectionInstances.values())
				{
					instance.rollback();
				}

				mDatabaseRoot = new DatabaseRoot(mBlockDevice, mBlockDevice.getMetadata().getDocument(DIRECTORY));
				mBlockDevice.rollback();

				Log.dec();
			}

			if (mDatabaseRoot != null)
			{
				for (RaccoonCollection instance : mCollectionInstances.values())
				{
					instance.close();
				}

				mCollectionInstances.clear();

				mDatabaseRoot.commit(mBlockDevice);
				mDatabaseRoot = null;
			}

			if (mBlockDevice != null && mCloseDeviceOnCloseDatabase)
			{
				mBlockDevice.close();
			}

			mBlockDevice = null;

			Log.i("database finished closing");
			Log.dec();
		}
		catch (IOException e)
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


	private void reportStatus(LogLevel aLevel, String aMessage, Throwable aThrowable)
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


	private Document createDefaultConfig()
	{
		return new Document()
			.put("_id", ObjectId.randomId())
			.put(RaccoonCollection.CONFIGURATION, BTree.createDefaultConfig());
	}
}
