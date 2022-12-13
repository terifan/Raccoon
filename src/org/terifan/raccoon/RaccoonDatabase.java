package org.terifan.raccoon;

import org.terifan.raccoon.io.DatabaseIOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.terifan.bundle.Document;
import org.terifan.raccoon.btree.BTreeStorage;
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


public final class RaccoonDatabase implements AutoCloseable
{
	private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();
//	private final Lock mReadLock = mReadWriteLock.readLock();
//	private final Lock mWriteLock = mReadWriteLock.writeLock();

	private IManagedBlockDevice mBlockDevice;
	private final ConcurrentHashMap<String, RaccoonCollection> mCollections;
	private final ArrayList<DatabaseStatusListener> mDatabaseStatusListener;
	private boolean mModified;
	private boolean mCloseDeviceOnCloseDatabase;
	private boolean mReadOnly;
	private Thread mShutdownHook;
	private ApplicationHeader mApplicationHeader;
	private CompressionParam mCompressionParam;
	private int mReadLocked;
	private DatabaseOpenOption mDatabaseOpenOption;


	private RaccoonDatabase()
	{
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
				if (aOpenOptions == DatabaseOpenOption.CREATE_NEW)
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

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == DatabaseOpenOption.CREATE_NEW;

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

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == DatabaseOpenOption.CREATE_NEW;

		init(aBlockDevice, create, false, aOpenOptions, aAccessCredentials);
	}


	private void init(Object aBlockDevice, boolean aCreate, boolean aCloseDeviceOnCloseDatabase, DatabaseOpenOption aOpenOption, AccessCredentials aAccessCredentials)
	{
		mCompressionParam = CompressionParam.NO_COMPRESSION; //getParameter(CompressionParam.class, aOpenParams, CompressionParam.BEST_SPEED);
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

		mApplicationHeader = new ApplicationHeader();

		if (aCreate)
		{
			Log.i("create database");
			Log.inc();

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

			mBlockDevice = blockDevice;
			mApplicationHeader.readFromDevice(mBlockDevice);
			mReadOnly = mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY;

			for (String name : mApplicationHeader.list())
			{
				Document conf = mApplicationHeader.get(name);

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


	public RaccoonCollection getCollection(String aName)
	{
		checkOpen();

		RaccoonCollection instance = mCollections.get(aName);

		if (instance != null)
		{
			return instance;
		}

		if (mDatabaseOpenOption == DatabaseOpenOption.OPEN || mDatabaseOpenOption == DatabaseOpenOption.READ_ONLY)
		{
			throw new IllegalStateException("Collection doesn't exist: " + aName);
		}

		Log.i("create table '%s' with option %s", aName, mDatabaseOpenOption);
		Log.inc();

		try
		{
			instance = new RaccoonCollection(this, createDefaultConfig(aName));

			mCollections.put(aName, instance);

			if (mDatabaseOpenOption == DatabaseOpenOption.CREATE_NEW)
			{
				instance.clear();
			}
		}
		finally
		{
			Log.dec();
		}

		return instance;
	}


	private void checkOpen()
	{
		if (mApplicationHeader == null)
		{
			throw new DatabaseClosedException("Database is closed");
		}
	}


	public boolean isModified()
	{
		checkOpen();

//		mReadLock.lock();
//		try
//		{
			for (RaccoonCollection instance : mCollections.values())
			{
				if (instance.isModified())
				{
					return true;
				}
			}
//		}
//		finally
//		{
//			mReadLock.unlock();
//		}

		return false;
	}


	public boolean isOpen()
	{
		return mApplicationHeader != null;
	}


	public long flush()
	{
		checkOpen();

		long nodesWritten = 0;

//		aquireWriteLock();
//		try
//		{
			Log.i("flush changes");
			Log.inc();

			for (RaccoonCollection entry : mCollections.values())
			{
				nodesWritten = entry.flush();
			}

			Log.dec();
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}

		return nodesWritten;
	}


	/**
	 * Persists all pending changes. It's necessary to commit changes on a regular basis to avoid data loss.
	 */
	public boolean commit()
	{
		checkOpen();

//		aquireWriteLock();
//
//		try
//		{
			Log.i("commit database");
			Log.inc();

			for (Entry<String, RaccoonCollection> entry : mCollections.entrySet())
			{
				if (entry.getValue().commit())
				{
					Log.i("table updated '%s'", entry.getKey());

					mApplicationHeader.put(entry.getKey(), entry.getValue().getConfiguration());
					mModified = true;

					System.out.println(entry.getValue().getConfiguration());
				}
			}

			boolean returnModified = mModified;

			if (mModified)
			{
				Log.i("updating super block");
				Log.inc();

				mApplicationHeader.nextTransaction();
				mApplicationHeader.writeToDevice(mBlockDevice);
				mBlockDevice.commit();
				mModified = false;

				assert integrityCheck() == null : integrityCheck();

				Log.dec();
			}

			Log.dec();

			return returnModified;
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}
	}


	/**
	 * Reverts all pending changes.
	 */
	public void rollback()
	{
		checkOpen();

//		aquireWriteLock();
//		try
//		{
			Log.i("rollback");
			Log.inc();

			for (RaccoonCollection instance : mCollections.values())
			{
				instance.rollback();
			}

			mApplicationHeader.readFromDevice(mBlockDevice);
			mBlockDevice.rollback();

			Log.dec();
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}
	}


	@Override
	public void close()
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

//		aquireWriteLock();
//
//		try
//		{
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

				mApplicationHeader.readFromDevice(mBlockDevice);
				mBlockDevice.rollback();

				Log.dec();
			}

			if (mApplicationHeader != null)
			{
				for (RaccoonCollection instance : mCollections.values())
				{
					instance.close();
				}

				mCollections.clear();

				mApplicationHeader.writeToDevice(mBlockDevice);
				mApplicationHeader = null;
			}

			if (mBlockDevice != null && mCloseDeviceOnCloseDatabase)
			{
				mBlockDevice.close();
			}

			mBlockDevice = null;

			Log.i("database finished closing");
			Log.dec();
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}
	}


	/**
	 * Saves an entity.
	 *
	 * @return true if this table did not already contain the specified entity
	 */
//	public boolean save(Document aDocument)
//	{
//		aquireWriteLock();
//		try
//		{
//			TableInstance instance = openTable(aDocument.getString("_collection"), DatabaseOpenOption.CREATE);
//			return instance.save(this, aDocument);
//		}
//		catch (DatabaseException e)
//		{
//			forceClose(e);
//			throw e;
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}
//	}
//
//
//	void aquireWriteLock()
//	{
//		if (mReadOnly)
//		{
//			throw new ReadOnlyDatabaseException();
//		}
//
//		mWriteLock.lock();
//	}
//
//
//	void releaseWriteLock()
//	{
//		mWriteLock.unlock();
//	}
//
//
//	/**
//	 * Attempts to retrieves an entity returning true if entity found and updating the provided instance.
//	 *
//	 * @param aDocument an entity with discriminator/key fields set
//	 * @return true if the entity was found.
//	 */
//	public boolean tryGet(Document aDocument)
//	{
//		mReadLock.lock();
//		try
//		{
//			TableInstance instance = openTable(aDocument.getString("_collection"), DatabaseOpenOption.OPEN);
//			if (instance == null)
//			{
//				return false;
//			}
//			return instance.get(this, aDocument);
//		}
//		catch (DatabaseException e)
//		{
//			forceClose(e);
//			throw e;
//		}
//		finally
//		{
//			mReadLock.unlock();
//		}
//	}
//
//
//	/**
//	 * Retrieves an entity throwing and exception if the entity wasn't found.
//	 *
//	 * @param aDocument an entity with discriminator/key fields set
//	 * @throws NoSuchEntityException if the entity cannot be found
//	 */
//	public Document get(Document aDocument) throws DatabaseException
//	{
//		Assert.notNull(aDocument, "Argument is null");
//
//		mReadLock.lock();
//		try
//		{
//			TableInstance instance = openTable(aDocument.getString("_collection"), DatabaseOpenOption.OPEN);
//
//			if (instance == null)
//			{
//				throw new NoSuchEntityException("No table exists matching type " + aDocument.getClass());
//			}
//			if (!instance.get(this, aDocument))
//			{
//				throw new NoSuchEntityException("No entity exists matching key");
//			}
//
//			return aDocument;
//		}
//		catch (DatabaseException e)
//		{
//			forceClose(e);
//			throw e;
//		}
//		finally
//		{
//			mReadLock.unlock();
//		}
//	}
//
//
//	/**
//	 * Removes the entity.
//	 *
//	 * @return true if the entity was removed.
//	 */
//	public boolean remove(Document aDocument)
//	{
//		aquireWriteLock();
//		try
//		{
//			TableInstance instance = openTable(aDocument.getString("_collection"), DatabaseOpenOption.OPEN);
//			if (instance == null)
//			{
//				return false;
//			}
//			return instance.remove(this, aDocument);
//		}
//		catch (DatabaseException e)
//		{
//			forceClose(e);
//			throw e;
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}
//	}
//
//
//	public void clear(String aCollection)
//	{
//		aquireWriteLock();
//		try
//		{
//			TableInstance instance = openTable(aCollection, DatabaseOpenOption.OPEN);
//			if (instance != null)
//			{
//				instance.clear(this);
//			}
//		}
//		catch (DatabaseException e)
//		{
//			forceClose(e);
//			throw e;
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}
//	}
//
//
//	/**
//	 * The contents of the stream is associated with the key found in the entity provided. The stream will persist the entity when it's
//	 * closed.
//	 */
//	public LobByteChannel openLob(Document aDocument, LobOpenOption aOpenOption)
//	{
//		TableInstance instance = openTable(aDocument.getString("_collection"), aOpenOption == LobOpenOption.READ ? DatabaseOpenOption.OPEN : DatabaseOpenOption.CREATE);
//
//		if (instance == null)
//		{
//			if (aOpenOption == LobOpenOption.READ)
//			{
//				return null;
//			}
//
//			throw new DatabaseException("Failed to create table");
//		}
//
//		return instance.openBlob(this, aDocument, aOpenOption);
//	}
//
//
//	public List<Document> list(Document aDocument)
//	{
//		List<Document> list = null;
//
//		Log.i("list items %s", aDocument.getString("_collection"));
//		Log.inc();
//
//		mReadLock.lock();
//		try
//		{
//			TableInstance instance = openTable(aDocument.getString("_collection"), DatabaseOpenOption.OPEN);
//
//			if (instance != null)
//			{
//				list = instance.list(this);
//			}
//		}
//		catch (DatabaseException e)
//		{
//			forceClose(e);
//			throw e;
//		}
//		finally
//		{
//			mReadLock.unlock();
//
//			System.setErr(System.out);
//
//			Log.dec();
//		}
//
//		return list != null ? list : new ArrayList<>();
//	}


	public IManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


//	public int size(String aCollection)
//	{
//		mReadLock.lock();
//		try
//		{
//			TableInstance instance = openTable(aCollection, DatabaseOpenOption.OPEN);
//			if (instance == null)
//			{
//				return 0;
//			}
//			return instance.size();
//		}
//		finally
//		{
//			mReadLock.unlock();
//		}
//	}


	public String integrityCheck()
	{
//		aquireWriteLock();
//		try
//		{
			for (RaccoonCollection instance : mCollections.values())
			{
				String s = instance.integrityCheck();
				if (s != null)
				{
					return s;
				}
			}

			return null;
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}
	}


	public List<RaccoonCollection> getCollections()
	{
		checkOpen();

//		mReadLock.lock();
//
//		try
//		{
			ArrayList<RaccoonCollection> list = new ArrayList<>();
			list.addAll(mCollections.values());
			return list;
//		}
//		finally
//		{
//			mReadLock.unlock();
//		}
	}


//	public ScanResult scan(ScanResult aScanResult)
//	{
//		mReadLock.lock();
//
//		try
//		{
//			if (aScanResult == null)
//			{
//				aScanResult = new ScanResult();
//			}
//
//			aScanResult.enterTable(mSystemTable);
//
//			mSystemTable.scan(aScanResult);
//
//			aScanResult.exitTable();
//
//			for (RaccoonCollection collection : getCollections())
//			{
//				boolean wasOpen = mOpenCollections.containsKey(collection.getName());
//
//				TableInstance instance = openTable(collection, DatabaseOpenOption.OPEN);
//
//				aScanResult.enterTable(instance);
//
//				instance.scan(aScanResult);
//
//				aScanResult.exitTable();
//
//				if (!wasOpen)
//				{
//					instance.close();
//					mOpenCollections.remove(collection.getName());
//				}
//			}
//
//			return aScanResult;
//		}
//		finally
//		{
//			mReadLock.unlock();
//		}
//	}


	protected synchronized void forceClose(Throwable aException)
	{
		if (mApplicationHeader == null)
		{
			return;
		}

		reportStatus(LogLevel.FATAL, "an error was detected, forcefully closing block device to prevent damage, uncommitted changes were lost.", aException);

		mBlockDevice.forceClose();
		mApplicationHeader = null;
	}


	/**
	 * Return true if the database is being read by a thread at this time.
	 */
	public boolean isReadLocked()
	{
		return mReadWriteLock.getReadHoldCount() > 0;
	}


	/**
	 * Return true if the database is being written to a thread at this time.
	 */
	public boolean isWriteLocked()
	{
		return mReadWriteLock.isWriteLocked();
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


//	Lock getReadLock()
//	{
//		return mReadLock;
//	}


	CompressionParam getCompressionParameter()
	{
		return mCompressionParam;
	}


//	public void execute(Consumer<Database> aConsumer)
//	{
//		mWriteLock.lock(); // note: allow this lock even on read-only databases
//
//		try
//		{
//			aConsumer.accept(this);
//		}
//		finally
//		{
//			mWriteLock.unlock();
//		}
//	}
//
//
//	public <T> void forEach(Class<T> aType, Consumer<T> aConsumer)
//	{
//		aquireReadLock();
//
//		TableInstance<T> table = openTable(getTable(aType), DatabaseOpenOption.OPEN);
//
//		try
//		{
//			for (Iterator<T> it = new DocumentIterator<>(this, table, table.getEntryIterator()); it.hasNext();)
//			{
//				aConsumer.accept(it.next());
//			}
//		}
//		finally
//		{
//			releaseReadLock();
//		}
//	}
//
//
//	public void forEachResultSet(Class aType, ResultSetConsumer aConsumer)
//	{
//		aquireReadLock();
//
//		TableInstance instance = openTable(getTable(aType), DatabaseOpenOption.OPEN);
//
//		try
//		{
//			ResultSet resultSet = new ResultSet(instance, instance.getEntryIterator());
//
//			while (resultSet.next())
//			{
//				aConsumer.handle(resultSet);
//			}
//		}
//		finally
//		{
//			releaseReadLock();
//		}
//	}
//	private synchronized void releaseReadLock()
//	{
//		mReadLock.unlock();
//		mReadLocked--;
//	}
//
//
//	private synchronized void aquireReadLock()
//	{
//		mReadLock.lock();
//		mReadLocked++;
//	}


	public BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(getBlockDevice(), getCompressionParameter());
	}


	public long getTransaction()
	{
		return mApplicationHeader.getTransaction();
	}


	private Document createDefaultConfig(String aName)
	{
		return new Document()
			.putString("name", aName)
			.putNumber("indexSize", mBlockDevice.getBlockSize())
			.putNumber("leafSize", mBlockDevice.getBlockSize())
			.putNumber("entrySizeLimit", mBlockDevice.getBlockSize() / 4)
			.putBundle("compression", CompressionParam.NO_COMPRESSION.marshal());
	}
}
