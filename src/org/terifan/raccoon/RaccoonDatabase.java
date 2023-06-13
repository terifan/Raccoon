package org.terifan.raccoon;

import org.terifan.raccoon.document.ObjectId;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
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
import org.terifan.raccoon.blockdevice.LobHeader;
import org.terifan.raccoon.util.DualMap;


public final class RaccoonDatabase implements AutoCloseable
{
	public final static String TENANT_NAME = "RaccoonDB";
	public final static int TENANT_VERSION = 1;

	private final ReadWriteLock mLock;

	private ManagedBlockDevice mBlockDevice;
	private DatabaseDirectory mDatabaseDirectory;
	private DatabaseOpenOption mDatabaseOpenOption;
	private final ConcurrentSkipListMap<String, RaccoonCollection> mCollectionInstancesName;
	private final ConcurrentSkipListMap<ObjectId, RaccoonCollection> mCollectionInstancesId;
	private final ArrayList<DatabaseStatusListener> mDatabaseStatusListener;

	final DualMap<ObjectId, ObjectId, Document> mIndices;

	private boolean mModified;
	private boolean mCloseDeviceOnCloseDatabase;
	private boolean mReadOnly;
	private Thread mShutdownHook;


	private RaccoonDatabase()
	{
		mIndices = new DualMap<>();
		mLock = new ReadWriteLock();
		mCollectionInstancesName = new ConcurrentSkipListMap<>();
		mCollectionInstancesId = new ConcurrentSkipListMap<>();
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

				blockDevice.getMetadata().put("tenantName", TENANT_NAME).put("tenantVersion", TENANT_VERSION);

				if (blockDevice.size() > 0)
				{
					blockDevice.clear();
					blockDevice.commit();
				}

				mModified = true;
				mBlockDevice = blockDevice;
				mDatabaseDirectory = new DatabaseDirectory(mBlockDevice);

				commit();

				Log.dec();
			}
			else
			{
				Log.i("open database");
				Log.inc();

				if (!TENANT_NAME.equals(blockDevice.getMetadata().getString("tenantName")))
				{
					throw new DatabaseException("Not a Raccoon database file");
				}
				if (blockDevice.getMetadata().get("tenantVersion", -1) != TENANT_VERSION)
				{
					throw new DatabaseException("Unsupported Raccoon database version");
				}

				mBlockDevice = blockDevice;
				mDatabaseDirectory = new DatabaseDirectory(mBlockDevice);
				mReadOnly = mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY;

				for (Document indexConf : getCollection("system:indices").listAll())
				{
					mIndices.put(indexConf.getArray("_id").getObjectId(0), indexConf.getArray("_id").getObjectId(1), indexConf);
				}

				Log.dec();
			}

			mCloseDeviceOnCloseDatabase = aCloseDeviceOnCloseDatabase;

			// remove this?
			mShutdownHook = new Thread()
			{
				@Override
				public void run()
				{
					Log.i("shutdown hook executing");
					Log.inc();

					mShutdownHook = null;

					close();

					Log.dec();
				}
			};

			Runtime.getRuntime().addShutdownHook(mShutdownHook);
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}


	public ArrayList<String> getCollectionNames()
	{
		checkOpen();

		return new ArrayList<>(mDatabaseDirectory.list());
	}


	public synchronized boolean existsCollection(String aName)
	{
		checkOpen();

		return mCollectionInstancesName.containsKey(aName) || mDatabaseDirectory.exists(aName);
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


	public synchronized RaccoonCollection getCollection(ObjectId aId)
	{
		RaccoonCollection instance = mCollectionInstancesId.get(aId);
		if (instance != null)
		{
			return instance;
		}

		Document conf = mDatabaseDirectory.get(aId);
		if (conf == null)
		{
			throw new DatabaseException("No such collection: " + aId);
		}

		instance = new RaccoonCollection(this, conf);
		mCollectionInstancesId.put(aId, instance);
		mCollectionInstancesName.put(conf.getString("name"), instance);
		return instance;
	}


	public synchronized RaccoonCollection getCollection(String aName)
	{
		checkOpen();

		RaccoonCollection instance = mCollectionInstancesName.get(aName);
		if (instance != null)
		{
			return instance;
		}

		Document conf = mDatabaseDirectory.get(aName);
		if (conf != null)
		{
			instance = new RaccoonCollection(this, conf);
			mCollectionInstancesId.put(conf.getObjectId("_id"), instance);
			mCollectionInstancesName.put(aName, instance);
			return instance;
		}

		if (mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY)
		{
			throw new DatabaseException("No such collection: " + aName);
		}

		Log.i("create table '%s' with option %s", aName, mDatabaseOpenOption);
		Log.inc();

		try
		{
			conf = createDefaultConfig(aName);
			instance = new RaccoonCollection(this, conf);
			mDatabaseDirectory.put(conf);
			mCollectionInstancesId.put(conf.getObjectId("_id"), instance);
			mCollectionInstancesName.put(aName, instance);
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
		mDatabaseDirectory.remove(aCollection.getName());
		mModified = true;

		mCollectionInstancesId.remove(aCollection.getCollectionId());
		mCollectionInstancesName.remove(aCollection.getName());
	}


	public LobByteChannel openLob(String aCollection, Object aId, LobOpenOption aLobOpenOption) throws IOException
	{
		LobByteChannel lob = tryOpenLob(aCollection, aId, aLobOpenOption);

		if (lob == null)
		{
			throw new FileNotFoundException("No LOB " + aId);
		}

		return lob;
	}


	public LobByteChannel tryOpenLob(String aCollection, Object aId, LobOpenOption aLobOpenOption) throws IOException
	{
		Document entry = new Document().put("_id", aId);

		RaccoonCollection collection = getCollection(aCollection);

		if (!collection.tryGet(entry) && aLobOpenOption == LobOpenOption.READ)
		{
			return null;
		}

		LobHeader header = new LobHeader(entry.get("header"));

		Runnable closeAction = () -> collection.save(entry.put("header", header.marshal()));

		return new LobByteChannel(getBlockAccessor(), header, aLobOpenOption, closeAction);
	}


	public void deleteLob(String aCollection, ObjectId aObjectId) throws IOException
	{
		Document entry = new Document().put("_id", aObjectId);

		RaccoonCollection collection = getCollection(aCollection);

		if (collection.tryGet(entry))
		{
			LobHeader header = new LobHeader(entry.get("header"));

			new LobByteChannel(getBlockAccessor(), header, LobOpenOption.APPEND).delete();

			collection.delete(entry);
		}
	}


	private void checkOpen()
	{
		if (mDatabaseDirectory == null)
		{
			throw new DatabaseClosedException("Database is closed");
		}
	}


	public boolean isModified()
	{
		checkOpen();

		for (RaccoonCollection instance : mCollectionInstancesName.values())
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
		return mDatabaseDirectory != null;
	}


	public long flush()
	{
		checkOpen();

		long nodesWritten = 0;

		try (WriteLock lock = mLock.writeLock())
		{
			Log.i("flush changes");
			Log.inc();

			for (RaccoonCollection entry : mCollectionInstancesName.values())
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
				for (RaccoonCollection collection : mCollectionInstancesName.values())
				{
					if (collection.commit())
					{
						Log.i("table updated '%s'", collection.getConfiguration().getString("name"));

						mDatabaseDirectory.put(collection.getConfiguration());
						mModified = true;
					}
				}

				if (mModified)
				{
					Log.i("updating super block");
					Log.inc();

					mDatabaseDirectory.commit(mBlockDevice);
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
			for (RaccoonCollection instance : mCollectionInstancesName.values())
			{
				instance.rollback();
			}

			mDatabaseDirectory = new DatabaseDirectory(mBlockDevice);
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
				for (RaccoonCollection entry : mCollectionInstancesName.values())
				{
					mModified |= entry.isModified();
				}
			}

			if (mModified)
			{
				Log.w("rollback on close");
				Log.inc();

				for (RaccoonCollection instance : mCollectionInstancesName.values())
				{
					instance.rollback();
				}

				mDatabaseDirectory = new DatabaseDirectory(mBlockDevice);
				mBlockDevice.rollback();

				Log.dec();
			}

			if (mDatabaseDirectory != null)
			{
				for (RaccoonCollection instance : mCollectionInstancesName.values())
				{
					instance.close();
				}

				mCollectionInstancesName.clear();

				mDatabaseDirectory.commit(mBlockDevice);
				mDatabaseDirectory = null;
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
		for (RaccoonCollection instance : mCollectionInstancesName.values())
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


	private Document createDefaultConfig(String aName)
	{
		return new Document()
			.put("_id", ObjectId.randomId())
			.put("name", aName)
			.put("btree", new Document()
				.put("intBlockSize", mBlockDevice.getBlockSize())
				.put("leafBlockSize", mBlockDevice.getBlockSize())
				.put("entrySizeLimit", mBlockDevice.getBlockSize() / 4)
			);
	}


	public RaccoonDatabase saveEntity(Document... aDocuments)
	{
		for (Document doc : aDocuments)
		{
			RaccoonEntity entity = doc.getClass().getAnnotation(RaccoonEntity.class);
			getCollection(entity.collection()).save(doc);
		}
		return this;
	}


	public <T extends Document> List<T> listEntity(Class<T> aType)
	{
		RaccoonEntity entity = aType.getAnnotation(RaccoonEntity.class);

		Supplier<T> supplier = () ->
		{
			try
			{
				Constructor<T> c = aType.getConstructor();
				c.setAccessible(true);
				return (T)c.newInstance();
			}
			catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException e)
			{
				throw new IllegalArgumentException("Failed to create instance. Ensure class has a public zero argument constructor: " + aType, e);
			}
		};

		return (List<T>)getCollection(entity.collection()).listAll(supplier);
	}


//	public void createIndex(String aIndexName, String aOnCollection, boolean aUnique, String... aFieldNames)
//	{
//		if (aFieldNames.length == 0)
//		{
//			throw new IllegalArgumentException();
//		}
//
//		Document indexConf = new Document()
//			.put("_id", aIndexName)
//			.put("onCollection", aOnCollection)
//			.put("unique", aUnique)
//			.put("fields", Array.of(aFieldNames));
//
//		getCollection("system:indices").save(indexConf);
//
//		mIndices.computeIfAbsent(aOnCollection, n -> new Array()).add(indexConf);
//	}
}
