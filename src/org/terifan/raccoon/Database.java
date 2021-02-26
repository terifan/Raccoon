package org.terifan.raccoon;

import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.io.managed.DeviceHeader;
import java.io.File;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.io.managed.UnsupportedVersionException;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.physical.FileBlockDevice;
import org.terifan.raccoon.util.Assert;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class Database implements AutoCloseable
{
	private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();
	private final Lock mReadLock = mReadWriteLock.readLock();
	private final Lock mWriteLock = mReadWriteLock.writeLock();

	private IManagedBlockDevice mBlockDevice;
	private final HashMap<Class, Supplier> mFactories;
	private final HashMap<Class, Initializer> mInitializers;
	private final ConcurrentHashMap<Table, TableInstance> mOpenTables;
	private final TableMetadataProvider mTableMetadatas;
	private final ArrayList<DatabaseStatusListener> mDatabaseStatusListener;
	private TableInstance mSystemTable;
	private boolean mModified;
	private Table mSystemTableMetadata;
	private boolean mCloseDeviceOnCloseDatabase;
	private Thread mShutdownHook;
	private boolean mReadOnly;
	private CompressionParam mCompressionParam;
	private TableParam mTableParam;
	private PerformanceTool mPerformanceTool;


	private Database()
	{
		mOpenTables = new ConcurrentHashMap<>();
		mTableMetadatas = new TableMetadataProvider();
		mFactories = new HashMap<>();
		mInitializers = new HashMap<>();
		mDatabaseStatusListener = new ArrayList<>();
		mPerformanceTool = new PerformanceTool(null);
	}


	/**
	 * Create a new or open an existing database
	 *
	 * @param aFile
	 * the database file
	 * @param aOpenOptions
	 * OpenOption enum constant describing the options for creating the database instance
	 * @param aParameters
	 * parameters for the database
	 */
	public Database(File aFile, DatabaseOpenOption aOpenOptions, OpenParam... aParameters) throws UnsupportedVersionException
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

			BlockSizeParam blockSizeParam = getParameter(BlockSizeParam.class, aParameters, new BlockSizeParam(Constants.DEFAULT_BLOCK_SIZE));

			fileBlockDevice = new FileBlockDevice(aFile, blockSizeParam.getValue(), aOpenOptions == DatabaseOpenOption.READ_ONLY);

			init(fileBlockDevice, newFile, true, aOpenOptions, aParameters);
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
	 * @param aBlockDevice
	 * a block device containing a database
	 * @param aOpenOptions
	 * OpenOptions enum constant describing the options for creating the database instance
	 * @param aParameters
	 * parameters for the database
	 */
	public Database(IPhysicalBlockDevice aBlockDevice, DatabaseOpenOption aOpenOptions, OpenParam... aParameters) throws UnsupportedVersionException
	{
		this();

		Assert.fail((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aBlockDevice.length() == 0, "Block device is empty.");

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == DatabaseOpenOption.CREATE_NEW;

		init(aBlockDevice, create, false, aOpenOptions, aParameters);
	}


	/**
	 * Create a new or open an existing database
	 *
	 * @param aBlockDevice
	 * a block device containing a database
	 * @param aOpenOptions
	 * OpenOptions enum constant describing the options for creating the database instance
	 * @param aParameters
	 * parameters for the database
	 */
	public Database(IManagedBlockDevice aBlockDevice, DatabaseOpenOption aOpenOptions, OpenParam... aParameters) throws UnsupportedVersionException
	{
		this();

		Assert.fail((aOpenOptions == DatabaseOpenOption.READ_ONLY || aOpenOptions == DatabaseOpenOption.OPEN) && aBlockDevice.length() == 0, "Block device is empty.");

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == DatabaseOpenOption.CREATE_NEW;

		init(aBlockDevice, create, false, aOpenOptions, aParameters);
	}


	private void init(Object aBlockDevice, boolean aCreate, boolean aCloseDeviceOnCloseDatabase, DatabaseOpenOption aOpenOption, OpenParam[] aOpenParams)
	{
		mPerformanceTool = getParameter(PerformanceTool.class, aOpenParams, mPerformanceTool);

		assert mPerformanceTool.enter(this, "init", "init database");

		try
		{
			AccessCredentials accessCredentials = getParameter(AccessCredentials.class, aOpenParams, null);
			DeviceHeader tenantHeader = getParameter(DeviceHeader.class, aOpenParams, null);

			mCompressionParam = getParameter(CompressionParam.class, aOpenParams, CompressionParam.NO_COMPRESSION);
			mTableParam = getParameter(TableParam.class, aOpenParams, TableParam.DEFAULT);

			IManagedBlockDevice device;

			if (aBlockDevice instanceof IManagedBlockDevice)
			{
				if (accessCredentials != null)
				{
					throw new IllegalArgumentException("The BlockDevice provided cannot be secured, ensure that the BlockDevice it writes to is a secure BlockDevice.");
				}

				device = (IManagedBlockDevice)aBlockDevice;
			}
			else if (accessCredentials == null)
			{
				Log.d("creating a managed block device");

				device = new ManagedBlockDevice((IPhysicalBlockDevice)aBlockDevice);
			}
			else
			{
				Log.d("creating a secure block device");

				IPhysicalBlockDevice physicalDevice = (IPhysicalBlockDevice)aBlockDevice;
				SecureBlockDevice secureDevice;

				if (aCreate)
				{
					secureDevice = SecureBlockDevice.create(accessCredentials, physicalDevice);
				}
				else
				{
					secureDevice = SecureBlockDevice.open(accessCredentials, physicalDevice);
				}

				if (secureDevice == null)
				{
					throw new InvalidPasswordException("Incorrect password or not a secure BlockDevice");
				}

				device = new ManagedBlockDevice(secureDevice);
			}

			if (aCreate)
			{
				if (tenantHeader != null)
				{
					device.setTenantHeader(tenantHeader);
				}

				create(device, aOpenParams);
			}
			else
			{
				open(device, aOpenOption == DatabaseOpenOption.READ_ONLY, aOpenParams);

				if (tenantHeader != null && !tenantHeader.getLabel().equals(device.getTenantHeader().getLabel()))
				{
					throw new UnsupportedVersionException("Device tenant header labels don't match: expected: " + tenantHeader + ", actual:" + device.getTenantHeader());
				}
			}

			mCloseDeviceOnCloseDatabase = aCloseDeviceOnCloseDatabase;

			mShutdownHook = new Thread()
			{
				@Override
				public void run()
				{
					Log.i("shutdown hook executing");
					Log.inc();

					close();

					Log.dec();
				}
			};

			Runtime.getRuntime().addShutdownHook(mShutdownHook);
		}
		finally
		{
			assert mPerformanceTool.exit(this, "init");
		}
	}


	private void create(IManagedBlockDevice aBlockDevice, OpenParam[] aParameters)
	{
		Log.i("create database");
		Log.inc();

		if (aBlockDevice.length() > 0)
		{
			aBlockDevice.setApplicationPointer(new byte[0]);
			aBlockDevice.clear();
			aBlockDevice.commit();
		}

		mBlockDevice = aBlockDevice;
		mSystemTableMetadata = new Table(this, Table.class, null);
		mSystemTable = new TableInstance(this, mSystemTableMetadata, null);
		mModified = true;

		commit();

		Log.dec();
	}


	private void open(IManagedBlockDevice aBlockDevice, boolean aReadOnly, OpenParam[] aParameters) throws UnsupportedVersionException
	{
		Log.i("open database");
		Log.inc();

		DeviceHeader applicationHeader = aBlockDevice.getApplicationHeader();

		if (!Arrays.equals(applicationHeader.getSerialNumberBytes(), Constants.DEVICE_HEADER.getSerialNumberBytes()))
		{
			throw new UnsupportedVersionException("This block device does not contain a Raccoon database (serialno): " + applicationHeader);
		}

		if (applicationHeader.getMajorVersion() != Constants.DEVICE_HEADER.getMajorVersion() || applicationHeader.getMinorVersion() != Constants.DEVICE_HEADER.getMinorVersion())
		{
			throw new UnsupportedVersionException("Unsupported database version: " + applicationHeader);
		}

		byte[] tableHeader = aBlockDevice.getApplicationPointer();

		if (tableHeader.length < BlockPointer.SIZE)
		{
			throw new UnsupportedVersionException("The application pointer is too short: " + tableHeader.length);
		}

		mBlockDevice = aBlockDevice;
		mSystemTableMetadata = new Table(this, Table.class, null);
		mSystemTable = new TableInstance(this, mSystemTableMetadata, tableHeader);
		mReadOnly = aReadOnly;

		Log.dec();
	}


	protected TableInstance openTable(Class aType, DiscriminatorType aDiscriminator, DatabaseOpenOption aOptions)
	{
		checkOpen();

		Assert.notEquals(aType.getName(), Table.class.getName());

		return openTable(mTableMetadatas.getOrCreate(this, aType, aDiscriminator), aOptions);
	}


	protected TableInstance openTable(Table aTableMetadata, DatabaseOpenOption aOptions)
	{
		checkOpen();

		TableInstance table = mOpenTables.get(aTableMetadata);

		if (table != null)
		{
			return table;
		}

		synchronized (aTableMetadata)
		{
			checkOpen();

			table = mOpenTables.get(aTableMetadata);

			if (table != null)
			{
				return table;
			}

			aTableMetadata.initialize(this);

			Log.i("open table '%s' with option %s", aTableMetadata.getTypeName(), aOptions);
			Log.inc();

			try
			{
				boolean tableExists = mSystemTable.get(aTableMetadata);

				if (!tableExists && (aOptions == DatabaseOpenOption.OPEN || aOptions == DatabaseOpenOption.READ_ONLY))
				{
					return null;
				}

				table = new TableInstance(this, aTableMetadata, aTableMetadata.getTableHeader());

				if (!tableExists)
				{
					mSystemTable.save(aTableMetadata);
				}

				mOpenTables.put(aTableMetadata, table);

				if (aOptions == DatabaseOpenOption.CREATE_NEW)
				{
					table.clear();
				}
			}
			finally
			{
				Log.dec();
			}

			return table;
		}
	}


	private void checkOpen() throws IllegalStateException
	{
		if (mSystemTable == null)
		{
			throw new DatabaseClosedException("Database is closed");
		}
	}


	public boolean isModified()
	{
		checkOpen();

		mReadLock.lock();

		try
		{
			for (TableInstance table : mOpenTables.values())
			{
				if (table.isModified())
				{
					return true;
				}
			}
		}
		finally
		{
			mReadLock.unlock();
		}

		return false;
	}


	public boolean isOpen()
	{
		return mSystemTable != null;
	}


	/**
	 * Persists all pending changes. It's necessary to commit changes on a regular basis to avoid data loss.
	 */
	public boolean commit()
	{
		assert mPerformanceTool.enter(this, "commit", "commit changes");

		try
		{
			checkOpen();

			aquireWriteLock();

			try
			{
				Log.i("commit database");
				Log.inc();

				for (java.util.Map.Entry<Table, TableInstance> entry : mOpenTables.entrySet())
				{
					if (entry.getValue().commit())
					{
						Log.i("table updated '%s'", entry.getKey());

						mSystemTable.save(entry.getKey());

						mModified = true;
					}
				}

				boolean returnModified = mModified;

				if (mModified)
				{
					mSystemTable.commit();

					updateSuperBlock();

					mBlockDevice.commit();

					mModified = false;

					assert integrityCheck() == null : integrityCheck();
				}

				Log.dec();

				return returnModified;
			}
			finally
			{
				mWriteLock.unlock();
			}
		}
		finally
		{
			assert mPerformanceTool.exit(this, "commit");
		}
	}


	/**
	 * Reverts all pending changes.
	 */
	public void rollback()
	{
		checkOpen();

		aquireWriteLock();
		try
		{
			Log.i("rollback");
			Log.inc();

			for (TableInstance table : mOpenTables.values())
			{
				table.rollback();
			}

			mSystemTable.rollback();
			mBlockDevice.rollback();

			Log.dec();
		}
		finally
		{
			mWriteLock.unlock();
		}
	}


	private void updateSuperBlock()
	{
		Log.i("updating super block");
		Log.inc();

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(IManagedBlockDevice.APPLICATION_POINTER_MAX_SIZE);
		if (mSystemTableMetadata.getTableHeader() != null)
		{
			buffer.write(mSystemTableMetadata.getTableHeader());
		}
		buffer.trim();

		mBlockDevice.setApplicationPointer(buffer.array());
		mBlockDevice.setApplicationHeader(Constants.DEVICE_HEADER);

		Log.dec();
	}


	@Override
	public void close()
	{
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

		aquireWriteLock();

		try
		{
			Log.d("begin closing database");
			Log.inc();

			if (!mModified)
			{
				for (java.util.Map.Entry<Table, TableInstance> entry : mOpenTables.entrySet())
				{
					mModified |= entry.getValue().isModified();
				}
			}

			if (mModified)
			{
				Log.w("rollback on close");
				Log.inc();

				for (TableInstance table : mOpenTables.values())
				{
					table.rollback();
				}

				if (mSystemTable != null)
				{
					mSystemTable.rollback();
				}

				mBlockDevice.rollback();

				Log.dec();
			}

			if (mSystemTable != null)
			{
				for (TableInstance tableInstance : mOpenTables.values())
				{
					tableInstance.close();
				}

				mOpenTables.clear();

				mSystemTable.close();
				mSystemTable = null;
			}

			if (mBlockDevice != null && mCloseDeviceOnCloseDatabase)
			{
				mBlockDevice.close();
			}

			mBlockDevice = null;

			Log.i("database finished closing");
			Log.dec();
		}
		finally
		{
			mWriteLock.unlock();
		}
	}


	/**
	 * Saves an entity.
	 *
	 * @return
	 * true if this table did not already contain the specified entity
	 */
	public boolean save(Object aEntity)
	{
		assert mPerformanceTool.enter(this, "save", "save");

		try
		{
			aquireWriteLock();
			try
			{
				TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), DatabaseOpenOption.CREATE);
				return table.save(aEntity);
			}
			catch (DatabaseException e)
			{
				forceClose(e);
				throw e;
			}
			finally
			{
				mWriteLock.unlock();
			}
		}
		finally
		{
			assert mPerformanceTool.exit(this, "save");
		}
	}


	void aquireWriteLock()
	{
		if (mReadOnly)
		{
			throw new ReadOnlyDatabaseException();
		}

		mWriteLock.lock();
	}


	void releaseWriteLock()
	{
		mWriteLock.unlock();
	}


	/**
	 * Retrieves an entity.
	 *
	 * @return
	 * true if the entity was found.
	 */
	public boolean tryGet(Object aEntity)
	{
		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), DatabaseOpenOption.OPEN);
			if (table == null)
			{
				return false;
			}
			return table.get(aEntity);
		}
		catch (DatabaseException e)
		{
			forceClose(e);
			throw e;
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	/**
	 *
	 * @throws NoSuchEntityException
	 * if the entity cannot be found
	 */
	public <T> T get(T aEntity) throws DatabaseException
	{
		Assert.notNull(aEntity, "Argument is null");

		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), DatabaseOpenOption.OPEN);

			if (table == null)
			{
				throw new NoSuchEntityException("No table exists matching type " + aEntity.getClass());
			}
			if (!table.get(aEntity))
			{
				throw new NoSuchEntityException("No entity exists matching key");
			}

			return aEntity;
		}
		catch (DatabaseException e)
		{
			forceClose(e);
			throw e;
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	/**
	 * Removes the entity.
	 *
	 * @return
	 * true if the entity was removed.
	 */
	public boolean remove(Object aEntity)
	{
		aquireWriteLock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), DatabaseOpenOption.OPEN);
			if (table == null)
			{
				return false;
			}
			return table.remove(aEntity);
		}
		catch (DatabaseException e)
		{
			forceClose(e);
			throw e;
		}
		finally
		{
			mWriteLock.unlock();
		}
	}


	/**
	 * Remove all entries of the entities type.
	 */
	public void clear(Class aType)
	{
		clearImpl(aType, null);
	}


	/**
	 * Remove all entries of the entities type and possible it's discriminator.
	 */
	public void clear(Object aEntity)
	{
		clearImpl(aEntity.getClass(), aEntity);
	}


	private void clearImpl(Class aType, Object aEntity)
	{
		aquireWriteLock();
		try
		{
			TableInstance table = openTable(aType, new DiscriminatorType(aEntity), DatabaseOpenOption.OPEN);
			if (table != null)
			{
				table.clear();
			}
		}
		catch (DatabaseException e)
		{
			forceClose(e);
			throw e;
		}
		finally
		{
			mWriteLock.unlock();
		}
	}


	/**
	 * The contents of the stream is associated with the key found in the entity provided. The stream will persist the entity when it's closed.
	 */
	public LobByteChannel openLob(Object aEntity, LobOpenOption aOpenOption)
	{
		TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), aOpenOption == LobOpenOption.READ ? DatabaseOpenOption.OPEN : DatabaseOpenOption.CREATE);

		if (table == null)
		{
			if (aOpenOption == LobOpenOption.READ)
			{
				return null;
			}

			throw new DatabaseException("Failed to create table");
		}

		return table.openBlob(aEntity, aOpenOption);
	}


	/**
	 * List entities matching the provided discriminator.
	 */
	public <T> List<T> list(T aDiscriminator)
	{
		return list(aDiscriminator.getClass(), getDiscriminator(aDiscriminator));
	}


	public <T> List<T> list(Class<T> aType)
	{
		return listFirst(aType, (T)null, -1);
	}


	public <T> List<T> list(Class<T> aType, DiscriminatorType<T> aDiscriminator)
	{
		return listFirst(aType, aDiscriminator.getInstance(), -1);
	}


	public <T> List<T> list(Class<T> aType, T aEntity)
	{
		return listFirst(aType, aEntity, -1);
	}


	public <T> List<T> listFirst(Class<T> aType, T aEntity, int aLimit)
	{
		List<T> list = null;

		Log.i("list items %s", aType);
		Log.inc();

		mReadLock.lock();
		try
		{
			TableInstance tableInstance = openTable(aType, new DiscriminatorType(aEntity), DatabaseOpenOption.OPEN);

			if (tableInstance != null)
			{
				list = tableInstance.list(aType, aLimit <= 0 ? Integer.MAX_VALUE : aLimit);

				Log.i("list %s %s", aType.getSimpleName(), tableInstance.getCost().toString());
			}
		}
		catch (DatabaseException e)
		{
			forceClose(e);
			throw e;
		}
		finally
		{
			mReadLock.unlock();

			System.setErr(System.out);
			if (list != null && list.size() == 3886)
			{
				Thread.dumpStack();
			}

			Log.dec();
		}

		return list != null ? list : new ArrayList<>();
	}


	/**
	 * Sets a Supplier associated with the specified type. The Supplier is used to create instances of specified types.
	 *
	 * E.g:
	 * mDatabase.setSupplier(Photo.class, ()-&gt;new Photo(PhotoAlbum.this));
	 */
	public <T> void setSupplier(Class<T> aType, Supplier<T> aSupplier)
	{
		mFactories.put(aType, aSupplier);
	}


	<T> Supplier<T> getSupplier(Class<T> aType)
	{
		return mFactories.get(aType);
	}


	public <T> void setInitializer(Class<T> aType, Initializer<T> aSupplier)
	{
		mInitializers.put(aType, aSupplier);
	}


	<T> Initializer<T> getInitializer(Class<T> aType)
	{
		return mInitializers.get(aType);
	}


	public IManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	public TransactionGroup getTransactionId()
	{
		return new TransactionGroup(mBlockDevice.getTransactionId());
	}


	int size(Class aType)
	{
		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aType, null, DatabaseOpenOption.OPEN);
			if (table == null)
			{
				return 0;
			}
			return table.size();
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	int size(DiscriminatorType aDiscriminator)
	{
		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aDiscriminator.getType(), aDiscriminator, DatabaseOpenOption.OPEN);
			if (table == null)
			{
				return 0;
			}
			return table.size();
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public String integrityCheck()
	{
		aquireWriteLock();
		try
		{
			for (TableInstance table : mOpenTables.values())
			{
				String s = table.integrityCheck();
				if (s != null)
				{
					return s;
				}
			}

			return null;
		}
		finally
		{
			mWriteLock.unlock();
		}
	}


	private static <T extends OpenParam> T getParameter(Class<T> aType, OpenParam[] aParameters, T aDefaultValue)
	{
		if (aParameters != null)
		{
			for (OpenParam param : aParameters)
			{
				if (param != null && aType.isAssignableFrom(param.getClass()))
				{
					return (T)param;
				}
			}
		}

		return aDefaultValue;
	}


	public List<Table> getTables()
	{
		checkOpen();

		mReadLock.lock();

		try
		{
			return mSystemTable.list(Table.class, Integer.MAX_VALUE);
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public <T> Table<T> getTable(T aObject)
	{
		TableInstance table = openTable(aObject.getClass(), getDiscriminator(aObject), DatabaseOpenOption.OPEN);

		if (table == null)
		{
			return null;
		}

		return table.getTable();
	}


	public <T> Table<T> getTable(Class<T> aType)
	{
		TableInstance table = openTable(aType, null, DatabaseOpenOption.OPEN);

		if (table == null)
		{
			return null;
		}

		return table.getTable();
	}


	public <T> Table<T> getTable(Class<T> aType, DiscriminatorType aDiscriminator)
	{
		TableInstance table = openTable(aType, aDiscriminator, DatabaseOpenOption.OPEN);

		if (table == null)
		{
			return null;
		}

		return table.getTable();
	}


	public Table getTable(String aTypeName)
	{
		checkOpen();

		mReadLock.lock();

		try
		{
			return (Table)mSystemTable.list(Table.class, Integer.MAX_VALUE).stream().filter(e ->
			{
				String tm = ((Table)e).getTypeName();
				return tm.equals(aTypeName) || tm.endsWith("." + aTypeName);
			}).findFirst().orElse(null);
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public List<Table> getTables(String aTypeName)
	{
		checkOpen();

		mReadLock.lock();

		try
		{
			return (List<Table>)mSystemTable.list(Table.class, Integer.MAX_VALUE).stream().filter(e ->
			{
				String tm = ((Table)e).getTypeName();
				return tm.equals(aTypeName) || tm.endsWith("." + aTypeName);
			}).collect(Collectors.toList());
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public <T> List<Table<T>> getTables(Class<T> aType)
	{
		checkOpen();

		mReadLock.lock();

		try
		{
			return (List<Table<T>>)mSystemTable.list(Table.class, Integer.MAX_VALUE).stream().filter(e -> e == aType).collect(Collectors.toList());
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public DiscriminatorType getDiscriminator(Object aType)
	{
		return new DiscriminatorType(aType);
	}


	public synchronized <T> List<DiscriminatorType<T>> getDiscriminators(Class<T> aType)
	{
		mReadLock.lock();

		try
		{
			Log.i("get discriminators %s", aType);
			Log.inc();

			ArrayList<DiscriminatorType<T>> result = new ArrayList<>();

			String name = aType.getName();

			for (Table tableMetadata : (List<Table>)mSystemTable.list(Table.class, Integer.MAX_VALUE))
			{
				if (name.equals(tableMetadata.getTypeName()))
				{
					try
					{
						T instance = (T)aType.newInstance();
						tableMetadata.getMarshaller().unmarshal(ByteArrayBuffer.wrap(tableMetadata.getDiscriminatorKey()), instance, Table.FIELD_CATEGORY_DISCRIMINATOR);
						result.add(new DiscriminatorType<>(instance));
					}
					catch (InstantiationException | IllegalAccessException e)
					{
						throw new IllegalArgumentException("Failed to instantiate object: " + aType, e);
					}
				}
			}

			return result;
		}
		finally
		{
			mReadLock.unlock();

			Log.dec();
		}
	}


	public ScanResult scan(ScanResult aScanResult)
	{
		mReadLock.lock();

		try
		{
			if (aScanResult == null)
			{
				aScanResult = new ScanResult();
			}

			aScanResult.enterTable(mSystemTable);

			mSystemTable.scan(aScanResult);

			aScanResult.exitTable();

			for (Table tableMetadata : getTables())
			{
				boolean wasOpen = mOpenTables.containsKey(tableMetadata);

				TableInstance table = openTable(tableMetadata, DatabaseOpenOption.OPEN);

				aScanResult.enterTable(table);

				table.scan(aScanResult);

				aScanResult.exitTable();

				if (!wasOpen)
				{
					table.close();
					mOpenTables.remove(tableMetadata);
				}
			}

			return aScanResult;
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	protected synchronized void forceClose(Throwable aException)
	{
		if (mSystemTable == null)
		{
			return;
		}

		reportStatus(LogLevel.FATAL, "an error was detected, forcefully closing block device to prevent damage, uncommitted changes were lost.", aException);

		mBlockDevice.forceClose();
		mSystemTable = null;
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


	Lock getReadLock()
	{
		return mReadLock;
	}


	CompressionParam getCompressionParameter()
	{
		return mCompressionParam;
	}


	TableParam getTableParameter()
	{
		return mTableParam;
	}


	public void execute(Consumer<Database> aConsumer)
	{
		mWriteLock.lock(); // note: allow this lock even on read-only databases

		try
		{
			aConsumer.accept(this);
		}
		finally
		{
			mWriteLock.unlock();
		}
	}


	public PerformanceTool getPerformanceTool()
	{
		return mPerformanceTool;
	}
}
