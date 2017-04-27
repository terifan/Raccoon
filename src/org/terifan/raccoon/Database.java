package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.io.managed.UnsupportedVersionException;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.storage.BlobOutputStream;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.io.physical.FileBlockDevice;
import org.terifan.raccoon.util.Assert;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


public final class Database implements AutoCloseable
{
    private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();
    private final Lock mReadLock = mReadWriteLock.readLock();
    private final Lock mWriteLock = mReadWriteLock.writeLock();

	private IManagedBlockDevice mBlockDevice;
	private final HashMap<Class,Supplier> mFactories;
	private final HashMap<Class,Initializer> mInitializers;
	private final ConcurrentHashMap<Table,TableInstance> mOpenTables;
	private final TableMetadataProvider mTableMetadatas;
	private final TransactionCounter mTransactionId;
	private final ArrayList<DatabaseStatusListener> mDatabaseStatusListener;
	private TableInstance mSystemTable;
	private boolean mModified;
	private Object[] mProperties;
	private Table mSystemTableMetadata;
	private boolean mCloseDeviceOnCloseDatabase;
	private Thread mShutdownHook;
	private boolean mReadOnly;


	private Database()
	{
		mOpenTables = new ConcurrentHashMap<>();
		mTableMetadatas = new TableMetadataProvider();
		mFactories = new HashMap<>();
		mTransactionId = new TransactionCounter(0);
		mInitializers = new HashMap<>();
		mDatabaseStatusListener = new ArrayList<>();
	}


	/**
	 *
	 * @param aParameters
	 *   supports: AccessCredentials, DeviceLabel
	 */
	public static Database open(File aFile, OpenOption aOpenOptions, Object... aParameters) throws IOException, UnsupportedVersionException
	{
		FileBlockDevice fileBlockDevice = null;

		try
		{
			if (aFile.exists())
			{
				if (aOpenOptions == OpenOption.CREATE_NEW)
				{
					if (!aFile.delete())
					{
						throw new IOException("Failed to delete existing file: " + aFile);
					}
				}
				else if ((aOpenOptions == OpenOption.READ_ONLY || aOpenOptions == OpenOption.OPEN) && aFile.length() == 0)
				{
					throw new IOException("File is empty.");
				}
			}
			else if (aOpenOptions == OpenOption.OPEN || aOpenOptions == OpenOption.READ_ONLY)
			{
				throw new FileNotFoundException("File not found: " + aFile);
			}

			boolean newFile = !aFile.exists();

			BlockSizeParam blockSizeParam = getParameter(BlockSizeParam.class, aParameters, new BlockSizeParam(Constants.DEFAULT_BLOCK_SIZE));

			fileBlockDevice = new FileBlockDevice(aFile, blockSizeParam.getValue(), aOpenOptions == OpenOption.READ_ONLY);

			return init(fileBlockDevice, newFile, true, aParameters, aOpenOptions);
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

			throw e;
		}
	}


	/**
	 *
	 * @param aParameters
	 *   supports: AccessCredentials, DeviceLabel
	 */
	public static Database open(IPhysicalBlockDevice aBlockDevice, OpenOption aOpenOptions, Object... aParameters) throws IOException, UnsupportedVersionException
	{
		Assert.fail((aOpenOptions == OpenOption.READ_ONLY || aOpenOptions == OpenOption.OPEN) && aBlockDevice.length() == 0, "Block device is empty.");

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == OpenOption.CREATE_NEW;

		return init(aBlockDevice, create, false, aParameters, aOpenOptions);
	}


	/**
	 *
	 * @param aParameters
	 *   supports: AccessCredentials, DeviceLabel
	 */
	public static Database open(IManagedBlockDevice aBlockDevice, OpenOption aOpenOptions, Object... aParameters) throws IOException, UnsupportedVersionException
	{
		Assert.fail((aOpenOptions == OpenOption.READ_ONLY || aOpenOptions == OpenOption.OPEN) && aBlockDevice.length() == 0, "Block device is empty.");

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == OpenOption.CREATE_NEW;

		return init(aBlockDevice, create, false, aParameters, aOpenOptions);
	}


	private static Database init(Object aBlockDevice, boolean aCreate, boolean aCloseDeviceOnCloseDatabase, Object[] aParameters, OpenOption aOpenOptions) throws IOException
	{
		AccessCredentials accessCredentials = getParameter(AccessCredentials.class, aParameters, null);

		IManagedBlockDevice device;
		String label = null;

		for (Object o : aParameters)
		{
			if (o instanceof DeviceLabel)
			{
				label = o.toString();
			}
		}

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

			device = new ManagedBlockDevice((IPhysicalBlockDevice)aBlockDevice, label, 512);
		}
		else
		{
			Log.d("creating a secure block device");

			device = new ManagedBlockDevice(new SecureBlockDevice((IPhysicalBlockDevice)aBlockDevice, accessCredentials), label, 512);
		}

		Database db;

		if (aCreate)
		{
			db = create(device, aParameters);
		}
		else
		{
			db = open(device, aParameters, aOpenOptions);
		}

		db.mCloseDeviceOnCloseDatabase = aCloseDeviceOnCloseDatabase;

		db.mShutdownHook = new Thread()
		{
			@Override
			public void run()
			{
				Log.i("shutdown hook executing");
				Log.inc();

				try
				{
					db.close();
				}
				catch (IOException e)
				{
					throw new IllegalStateException(e);
				}

				Log.dec();
			}
		};

		Runtime.getRuntime().addShutdownHook(db.mShutdownHook);

		return db;
	}


	private static Database create(IManagedBlockDevice aBlockDevice, Object[] aParameters) throws IOException
	{
		Log.i("create database");
		Log.inc();

		if (aBlockDevice.length() > 0)
		{
			aBlockDevice.setExtraData(null);
			aBlockDevice.clear();
			aBlockDevice.commit();
		}

		Database db = new Database();

		db.mProperties = aParameters;
		db.mBlockDevice = aBlockDevice;
		db.mSystemTableMetadata = new Table(db, Table.class, null);
		db.mSystemTable = new TableInstance(db, db.mSystemTableMetadata, null);
		db.mModified = true;

		db.updateSuperBlock();

		db.commit();

		Log.dec();

		return db;
	}


	private static Database open(IManagedBlockDevice aBlockDevice, Object[] aParameters, OpenOption aOpenOptions) throws IOException, UnsupportedVersionException
	{
		Database db = new Database();

		Log.i("open database");
		Log.inc();

		byte[] extraData = aBlockDevice.getExtraData();

		if (extraData == null || extraData.length < 20)
		{
			throw new UnsupportedVersionException("This block device does not contain a Raccoon database (bad extra data length) ("+(extraData==null?null:extraData.length)+")");
		}

		ByteArrayBuffer buffer = new ByteArrayBuffer(extraData);

		if (MurmurHash3.hash_x86_32(buffer.array(), 4, buffer.capacity()-4, Constants.EXTRA_DATA_CHECKSUM_SEED) != buffer.readInt32())
		{
			throw new UnsupportedVersionException("This block device does not contain a Raccoon database (bad extra checksum)");
		}

		long identity = buffer.readInt64();
		int version = buffer.readInt32();

		if (identity != Constants.RACCOON_DB_IDENTITY)
		{
			throw new UnsupportedVersionException("This block device does not contain a Raccoon database (bad extra identity)");
		}
		if (version != Constants.RACCOON_FILE_FORMAT_VERSION)
		{
			throw new UnsupportedVersionException("Unsupported database version: provided: " + version + ", expected: " + Constants.RACCOON_FILE_FORMAT_VERSION);
		}

		db.mTransactionId.set(buffer.readInt64());

		db.mProperties = aParameters;
		db.mBlockDevice = aBlockDevice;
		db.mSystemTableMetadata = new Table(db, Table.class, null);
		db.mSystemTable = new TableInstance(db, db.mSystemTableMetadata, buffer.crop().array());
		db.mReadOnly = aOpenOptions == OpenOption.READ_ONLY;

		Log.dec();

		return db;
	}


	protected TableInstance openTable(Class aType, DiscriminatorType aDiscriminator, OpenOption aOptions)
	{
		checkOpen();

		Assert.notEquals(aType.getName(), Table.class.getName());

		return openTable(mTableMetadatas.getOrCreate(this, aType, aDiscriminator), aOptions);
	}


	protected TableInstance openTable(Table aTableMetadata, OpenOption aOptions)
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

			boolean tableExists = mSystemTable.get(aTableMetadata);

			if (!tableExists && (aOptions == OpenOption.OPEN || aOptions == OpenOption.READ_ONLY))
			{
				return null;
			}

			table = new TableInstance(this, aTableMetadata, aTableMetadata.getPointer());

			if (!tableExists)
			{
				mSystemTable.save(aTableMetadata);
			}

			mOpenTables.put(aTableMetadata, table);

			Log.dec();

			if (aOptions == OpenOption.CREATE_NEW)
			{
				table.clear();
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


	/**
	 * Persists all pending changes. It's necessary to commit changes on a regular basis to avoid data loss.
	 */
	public boolean commit()
	{
		checkOpen();

		aquireWriteLock();

		try
		{
			Log.i("commit database");
			Log.inc();

			for (java.util.Map.Entry<Table,TableInstance> entry : mOpenTables.entrySet())
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

				try
				{
					mBlockDevice.commit();
				}
				catch (IOException e)
				{
					throw new DatabaseIOException(e);
				}

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


	/**
	 * Reverts all pending changes.
	 */
	public void rollback() throws IOException
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

		mTransactionId.increment();

		ByteArrayBuffer buffer = new ByteArrayBuffer(4 + 8 + 4 + 8 + BlockPointer.SIZE);
		buffer.writeInt32(0); // leave space for checksum
		buffer.writeInt64(Constants.RACCOON_DB_IDENTITY);
		buffer.writeInt32(Constants.RACCOON_FILE_FORMAT_VERSION);
		buffer.writeInt64(mTransactionId.get());
		if (mSystemTableMetadata.getPointer() != null)
		{
			buffer.write(mSystemTableMetadata.getPointer());
		}
		buffer.trim();

		buffer.position(0).writeInt32(MurmurHash3.hash_x86_32(buffer.array(), 4, buffer.capacity() - 4, Constants.EXTRA_DATA_CHECKSUM_SEED));

		mBlockDevice.setExtraData(buffer.array());

		Log.dec();
	}


	@Override
	public void close() throws IOException
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
				for (java.util.Map.Entry<Table,TableInstance> entry : mOpenTables.entrySet())
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

				mSystemTable.rollback();

				mBlockDevice.rollback();

				Log.dec();
			}

			if (mSystemTable != null)
			{
				for (TableInstance table : mOpenTables.values())
				{
					table.close();
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
	 *   true if the entity didn't previously existed.
	 */
	public boolean save(Object aEntity)
	{
		aquireWriteLock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), OpenOption.CREATE);
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


	private void aquireWriteLock()
	{
		if (mReadOnly)
		{
			throw new ReadOnlyDatabaseException();
		}

		mWriteLock.lock();
	}


	/**
	 * Retrieves an entity.
	 *
	 * @return
	 *   true if the entity was found.
	 */
	public boolean tryGet(Object aEntity)
	{
		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), OpenOption.OPEN);
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
	 *   if the entity cannot be found
	 */
	public <T> T get(T aEntity) throws DatabaseException
	{
		Assert.notNull(aEntity, "Argument is null");

		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), OpenOption.OPEN);

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
	 *   true if the entity was removed.
	 */
	public boolean remove(Object aEntity)
	{
		aquireWriteLock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), OpenOption.OPEN);
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
			TableInstance table = openTable(aType, new DiscriminatorType(aEntity), OpenOption.OPEN);
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
	public BlobOutputStream saveBlob(Object aEntity)
	{
		aquireWriteLock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), OpenOption.CREATE);
			return table.saveBlob(aEntity);
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
	 * Save the contents of the stream with the key defined by the entity provided.
	 */
	public boolean save(Object aKeyEntity, InputStream aInputStream)
	{
		aquireWriteLock();
		try
		{
			TableInstance table = openTable(aKeyEntity.getClass(), new DiscriminatorType(aKeyEntity), OpenOption.CREATE);
			return table.save(aKeyEntity, aInputStream);
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
	 * Return an InputStream to the value associated to the key defined by the entity provided.
	 */
	public InputStream read(Object aEntity)
	{
		if ((aEntity instanceof String) || (aEntity instanceof Number))
		{
			throw new IllegalArgumentException("Provided object must be an Entity instance!");
		}

		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aEntity.getClass(), new DiscriminatorType(aEntity), OpenOption.OPEN);
			if (table == null)
			{
				return null;
			}

			try (InputStream in = table.read(aEntity))
			{
				if (in == null)
				{
					return null;
				}

				return new ByteArrayInputStream(Streams.readAll(in));
			}
		}
		catch (DatabaseException e)
		{
			forceClose(e);
			throw e;
		}
		catch (IOException e)
		{
			forceClose(e);
			throw new DatabaseIOException(e);
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public <T> List<T> list(Class<T> aType)
	{
		return list(aType, (T)null);
	}


	public <T> List<T> list(Class<T> aType, DiscriminatorType<T> aDiscriminator)
	{
		return list(aType, aDiscriminator.getInstance());
	}


	public <T> List<T> list(Class<T> aType, T aEntity)
	{
		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aType, new DiscriminatorType(aEntity), OpenOption.OPEN);
			if (table == null)
			{
				return new ArrayList<>();
			}
			return table.list(aType);
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


//	public <T> Stream<T> stream(Class<T> aType, T aEntity)
//	{
//		mReadLock.lock();
//		try
//		{
//			Table table = openTable(aType, aEntity, OpenOption.OPEN);
//
//			if (table == null)
//			{
//				mReadLock.unlock();
//
//				return new ArrayList<T>().stream();
//			}
//
//			return table.stream(mReadLock);
//		}
//		catch (Throwable e)
//		{
//			mReadLock.unlock();
//
//			forceClose(e);
//			throw e;
//		}
//	}


	/**
	 * Sets a Supplier associated with the specified type. The Supplier is used to create instances of specified types.
	 *
	 * E.g:
	 * 	 mDatabase.setSupplier(Photo.class, ()-&gt;new Photo(PhotoAlbum.this));
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


	public TransactionCounter getTransactionId()
	{
		return mTransactionId;
	}


	int size(Class aType)
	{
		mReadLock.lock();
		try
		{
			TableInstance table = openTable(aType, null, OpenOption.OPEN);
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
			TableInstance table = openTable(aDiscriminator.getType(), aDiscriminator, OpenOption.OPEN);
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


	private static <E> E getParameter(Class aType, Object[] aParameters, E aDefaultValue)
	{
		if (aParameters != null)
		{
			for (Object param : aParameters)
			{
				if (param != null && aType.isAssignableFrom(param.getClass()))
				{
					return (E)param;
				}
			}
		}

		return aDefaultValue;
	}


	<E> E getParameter(Class<E> aType, E aDefaultParam)
	{
		return getParameter(aType, mProperties, aDefaultParam);
	}


	public List<Table> getTables()
	{
		checkOpen();

		mReadLock.lock();

		try
		{
			return mSystemTable.list(Table.class);
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public <T> Table<T> getTable(T aObject)
	{
		TableInstance table = openTable(aObject.getClass(), getDiscriminator(aObject), OpenOption.OPEN);

		if (table == null)
		{
			return null;
		}

		return table.getTable();
	}


	public <T> Table<T> getTable(Class<T> aType)
	{
		TableInstance table = openTable(aType, null, OpenOption.OPEN);

		if (table == null)
		{
			return null;
		}

		return table.getTable();
	}


	public <T> Table<T> getTable(Class<T> aType, DiscriminatorType aDiscriminator)
	{
		TableInstance table = openTable(aType, aDiscriminator, OpenOption.OPEN);

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
			return (Table)mSystemTable.list(Table.class).stream().filter(e->
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
			return (List<Table>)mSystemTable.list(Table.class).stream().filter(e->
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
			return (List<Table<T>>)mSystemTable.list(Table.class).stream().filter(e->e == aType).collect(Collectors.toList());
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
			ArrayList<DiscriminatorType<T>> result = new ArrayList<>();

			String name = aType.getName();

			for (Table tableMetadata : (List<Table>)mSystemTable.list(Table.class))
			{
				if (name.equals(tableMetadata.getTypeName()))
				{
					try
					{
						T instance = (T)aType.newInstance();
						tableMetadata.getMarshaller().unmarshal(new ByteArrayBuffer(tableMetadata.getDiscriminatorKey()), instance, Table.FIELD_CATEGORY_DISCRIMINATOR);
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
		}
	}


	public ScanResult scan() throws IOException
	{
		ScanResult scanResult = new ScanResult();

		mSystemTable.scan(scanResult);

		for (Table tableMetadata : getTables())
		{
			try (TableInstance table = openTable(tableMetadata, OpenOption.OPEN))
			{
				table.scan(scanResult);
			}
		}

		return scanResult;
	}


	protected synchronized void forceClose(Throwable aException)
	{
		if (mSystemTable == null)
		{
			return;
		}

		reportStatus(LogLevel.FATAL, "an error was detected, forcefully closing block device to prevent damage, uncommitted changes were lost.", aException);

		try
		{
			mBlockDevice.forceClose();
			mSystemTable = null;
		}
		catch (IOException e)
		{
			throw new DatabaseException(e);
		}
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
		System.err.printf("%-6s%s%n", aLevel, aMessage);

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
}
