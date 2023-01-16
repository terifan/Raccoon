package org.terifan.raccoon;

import org.terifan.raccoon.io.DatabaseIOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.terifan.bundle.Document;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.io.managed.UnsupportedVersionException;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.physical.FileBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.util.Assert;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ReadWriteLock;
import org.terifan.raccoon.util.ReadWriteLock.WriteLock;


public final class RaccoonDatabase implements AutoCloseable
{
	public final static String TENANT_NAME = "RaccoonDB";
	public final static int TENANT_VERSION = 1;

	private final ReadWriteLock mLock;

	private IManagedBlockDevice mBlockDevice;
	private DatabaseRoot mDatabaseRoot;
	private CompressionParam mCompressionParam;
	private DatabaseOpenOption mDatabaseOpenOption;
	private final ConcurrentHashMap<String, RaccoonCollection> mCollections;
	private final ArrayList<DatabaseStatusListener> mDatabaseStatusListener;

	private boolean mModified;
	private boolean mCloseDeviceOnCloseDatabase;
	private boolean mReadOnly;
	private Thread mShutdownHook;


	private RaccoonDatabase()
	{
		mLock = new ReadWriteLock();
		mCollections = new ConcurrentHashMap<>();
		mDatabaseStatusListener = new ArrayList<>();
	}


	/**
	 * Create a new or open an existing database
	 *
	 * @param aFile the database file
	 * @param aOpenOptions OpenOption enum constant describing the options for creating the database instance
	 * @param aParameters parameters for the database
	 */
	public RaccoonDatabase(File aFile, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		FileBlockDevice fileBlockDevice = null;

		try
		{
			if (aFile.exists())
			{
				if (aOpenOptions == DatabaseOpenOption.REPLACE)
				{
					if (!aFile.delete())
					{
						throw new DatabaseIOException("Failed to delete existing file: " + aFile);
					}
				}
				else if ((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aFile.length() == 0)
				{
					throw new DatabaseIOException("File is empty.");
				}
			}
			else if (aOpenOptions == DatabaseOpenOption.OPEN || aOpenOptions == DatabaseOpenOption.READ_ONLY)
			{
				throw new DatabaseIOException("File not found: " + aFile);
			}

			boolean newFile = !aFile.exists();

			fileBlockDevice = new FileBlockDevice(aFile, 4096, aOpenOptions == DatabaseOpenOption.READ_ONLY);

			init(fileBlockDevice, newFile, true, aOpenOptions, aAccessCredentials);
		}
		catch (DatabaseException | DatabaseIOException | DatabaseClosedException e)
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


	/**
	 * Create a new or open an existing database
	 *
	 * @param aBlockDevice a block device containing a database
	 * @param aOpenOptions OpenOptions enum constant describing the options for creating the database instance
	 * @param aParameters parameters for the database
	 */
	public RaccoonDatabase(IPhysicalBlockDevice aBlockDevice, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		Assert.fail((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aBlockDevice.length() == 0, "Block device is empty.");

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == DatabaseOpenOption.REPLACE;

		init(aBlockDevice, create, false, aOpenOptions, aAccessCredentials);
	}


	/**
	 * Create a new or open an existing database
	 *
	 * @param aBlockDevice a block device containing a database
	 * @param aOpenOptions OpenOptions enum constant describing the options for creating the database instance
	 * @param aParameters parameters for the database
	 */
	public RaccoonDatabase(IManagedBlockDevice aBlockDevice, DatabaseOpenOption aOpenOptions, AccessCredentials aAccessCredentials) throws UnsupportedVersionException
	{
		this();

		Assert.fail((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aBlockDevice.length() == 0, "Block device is empty.");

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == DatabaseOpenOption.REPLACE;

		init(aBlockDevice, create, false, aOpenOptions, aAccessCredentials);
	}


	private void init(Object aBlockDevice, boolean aCreate, boolean aCloseDeviceOnCloseDatabase, DatabaseOpenOption aOpenOption, AccessCredentials aAccessCredentials)
	{
		mCompressionParam = CompressionParam.BEST_SPEED;
		mDatabaseOpenOption = aOpenOption;

		IManagedBlockDevice blockDevice;

		if (aBlockDevice instanceof IManagedBlockDevice)
		{
			if (aAccessCredentials != null)
			{
				throw new IllegalArgumentException("The BlockDevice provided cannot be secured.");
			}

			blockDevice = (IManagedBlockDevice)aBlockDevice;
		}
		else if (aAccessCredentials == null)
		{
			Log.d("creating a managed block device");

			blockDevice = new ManagedBlockDevice((IPhysicalBlockDevice)aBlockDevice);
		}
		else
		{
			Log.d("creating a secure block device");

			IPhysicalBlockDevice physicalDevice = (IPhysicalBlockDevice)aBlockDevice;
			SecureBlockDevice secureDevice;

			if (aCreate)
			{
				secureDevice = SecureBlockDevice.create(aAccessCredentials, physicalDevice);
			}
			else
			{
				secureDevice = SecureBlockDevice.open(aAccessCredentials, physicalDevice);
			}

			if (secureDevice == null)
			{
				throw new InvalidPasswordException("Incorrect password or not a secure BlockDevice");
			}

			blockDevice = new ManagedBlockDevice(secureDevice);
		}

		mDatabaseRoot = new DatabaseRoot();

		if (aCreate)
		{
			Log.i("create database");
			Log.inc();

			blockDevice.getApplicationMetadata().putString("tenantName", TENANT_NAME).putNumber("tenantVersion", TENANT_VERSION);

			if (blockDevice.length() > 0)
			{
				blockDevice.clear();
				blockDevice.commit();
			}

			mBlockDevice = blockDevice;
			mModified = true;

			commit();

			Log.dec();
		}
		else
		{
			Log.i("open database");
			Log.inc();

			if (!TENANT_NAME.equals(blockDevice.getApplicationMetadata().getString("tenantName")))
			{
				throw new DatabaseException("Not a Raccoon database file");
			}
			if (blockDevice.getApplicationMetadata().getInt("tenantVersion", -1) != TENANT_VERSION)
			{
				throw new DatabaseException("Unsupported Raccoon database version");
			}

			mBlockDevice = blockDevice;
			mDatabaseRoot.readFromDevice(mBlockDevice);
			mReadOnly = mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY;

			for (String name : mDatabaseRoot.listCollections())
			{
				Document conf = mDatabaseRoot.getCollection(name);

				if (conf == null)
				{
					conf = createDefaultConfig(name);
				}

				mCollections.put(name, new RaccoonCollection(this, conf));
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


	public List<RaccoonCollection> getCollections()
	{
		checkOpen();

		return new ArrayList<>(mCollections.values());
	}


	public synchronized RaccoonCollection getCollection(String aName)
	{
		checkOpen();

		RaccoonCollection instance = mCollections.get(aName);

		if (instance != null)
		{
			return instance;
		}

		if (mDatabaseOpenOption == DatabaseOpenOption.OPEN || mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY)
		{
			throw new DatabaseException("No such collection: " + aName);
		}

		Log.i("create table '%s' with option %s", aName, mDatabaseOpenOption);
		Log.inc();

		try
		{
			instance = new RaccoonCollection(this, createDefaultConfig(aName));

			mCollections.put(aName, instance);

//			if (mDatabaseOpenOption == DatabaseOpenOption.REPLACE)
//			{
//				instance.clear();
//			}
		}
		finally
		{
			Log.dec();
		}

		return instance;
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

		for (RaccoonCollection instance : mCollections.values())
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

			for (RaccoonCollection entry : mCollections.values())
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
	public boolean commit()
	{
		checkOpen();

		Log.i("commit database");
		Log.inc();

		try (WriteLock lock = mLock.writeLock())
		{
			for (Entry<String, RaccoonCollection> entry : mCollections.entrySet())
			{
				if (entry.getValue().commit())
				{
					Log.i("table updated '%s'", entry.getKey());

					mDatabaseRoot.putCollection(entry.getKey(), entry.getValue().getConfiguration());
					mModified = true;
				}
			}

			boolean returnModified = mModified;

			if (mModified)
			{
				Log.i("updating super block");
				Log.inc();

				mDatabaseRoot.nextTransaction();
				mDatabaseRoot.writeToDevice(mBlockDevice);
				mBlockDevice.commit();
				mModified = false;

				assert integrityCheck() == null : integrityCheck();

				Log.dec();
			}

			return returnModified;
		}
		finally
		{
			Log.dec();
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
			for (RaccoonCollection instance : mCollections.values())
			{
				instance.rollback();
			}

			mDatabaseRoot.readFromDevice(mBlockDevice);
			mBlockDevice.rollback();
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
				for (RaccoonCollection entry : mCollections.values())
				{
					mModified |= entry.isModified();
				}
			}

			if (mModified)
			{
				Log.w("rollback on close");
				Log.inc();

				for (RaccoonCollection instance : mCollections.values())
				{
					instance.rollback();
				}

				mDatabaseRoot.readFromDevice(mBlockDevice);
				mBlockDevice.rollback();

				Log.dec();
			}

			if (mDatabaseRoot != null)
			{
				for (RaccoonCollection instance : mCollections.values())
				{
					instance.close();
				}

				mCollections.clear();

				mDatabaseRoot.writeToDevice(mBlockDevice);
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
	}


	public IManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	public String integrityCheck()
	{
		for (RaccoonCollection instance : mCollections.values())
		{
			String s = instance.getImplementation().integrityCheck();
			if (s != null)
			{
				return s;
			}
		}

		return null;
	}


	protected synchronized void forceClose(Throwable aException)
	{
		if (mDatabaseRoot == null)
		{
			return;
		}

		reportStatus(LogLevel.FATAL, "an error was detected, forcefully closing block device to prevent damage, uncommitted changes were lost.", aException);

		mBlockDevice.forceClose();
		mDatabaseRoot = null;
	}


	private void reportStatus(LogLevel aLevel, String aMessage, Throwable aThrowable)
	{
		System.out.printf("%-6s%s%n", aLevel, aMessage);

		for (DatabaseStatusListener listener : mDatabaseStatusListener)
		{
			listener.statusChanged(aLevel, aMessage, aThrowable);
		}
	}


	public void addStatusListener(DatabaseStatusListener aErrorReportListener)
	{
		mDatabaseStatusListener.add(aErrorReportListener);
	}



	CompressionParam getCompressionParameter()
	{
		return mCompressionParam;
	}


	public BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(getBlockDevice(), mCompressionParam);
	}


	public long getTransaction()
	{
		return mDatabaseRoot.getTransactionId();
	}


	private Document createDefaultConfig(String aName)
	{
		return new Document()
			.putString("name", aName)
			.putNumber("indexSize", mBlockDevice.getBlockSize())
			.putNumber("leafSize", mBlockDevice.getBlockSize())
			.putNumber("entrySizeLimit", mBlockDevice.getBlockSize() / 4)
//			.putBundle("compression", CompressionParam.NO_COMPRESSION.marshal());
			.putBundle("compression", CompressionParam.BEST_SPEED.marshal());
	}
}
