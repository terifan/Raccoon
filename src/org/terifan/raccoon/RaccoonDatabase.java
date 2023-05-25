package org.terifan.raccoon;

import org.terifan.raccoon.document.ObjectId;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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


public final class RaccoonDatabase implements AutoCloseable
{
	final static String INDEX_COLLECTION = "::index";

	public final static String TENANT_NAME = "RaccoonDB";
	public final static int TENANT_VERSION = 1;

	private final ReadWriteLock mLock;

	private ManagedBlockDevice mBlockDevice;
	private DatabaseDirectory mDatabaseDirectory;
	private DatabaseOpenOption mDatabaseOpenOption;
	private final ConcurrentSkipListMap<String, RaccoonCollection> mCollectionInstances;
	private final ArrayList<DatabaseStatusListener> mDatabaseStatusListener;

	private boolean mModified;
	private boolean mCloseDeviceOnCloseDatabase;
	private boolean mReadOnly;
	private Thread mShutdownHook;


	private RaccoonDatabase()
	{
		mLock = new ReadWriteLock();
		mCollectionInstances = new ConcurrentSkipListMap<>();
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

		return mCollectionInstances.containsKey(aName) || mDatabaseDirectory.exists(aName);
	}


	public synchronized RaccoonCollection getCollection(String aName)
	{
		checkOpen();

		RaccoonCollection instance = mCollectionInstances.get(aName);
		if (instance != null)
		{
			return instance;
		}

		Document conf = mDatabaseDirectory.get(aName);
		if (conf != null)
		{
			instance = new RaccoonCollection(this, conf);
			mCollectionInstances.put(aName, instance);
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
			mDatabaseDirectory.put(aName, conf);
			mCollectionInstances.put(aName, instance);
		}
		finally
		{
			Log.dec();
		}

		return instance;
	}


	public synchronized boolean removeCollection(RaccoonCollection aCollection)
	{
		checkOpen();

		aCollection.clear();
		mDatabaseDirectory.remove(aCollection.getName());
		mModified = true;

		return mCollectionInstances.remove(aCollection.getName(), aCollection);
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

						mDatabaseDirectory.put(entry.getKey(), entry.getValue().getConfiguration());
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
			for (RaccoonCollection instance : mCollectionInstances.values())
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

				mDatabaseDirectory = new DatabaseDirectory(mBlockDevice);
				mBlockDevice.rollback();

				Log.dec();
			}

			if (mDatabaseDirectory != null)
			{
				for (RaccoonCollection instance : mCollectionInstances.values())
				{
					instance.close();
				}

				mCollectionInstances.clear();

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
}
