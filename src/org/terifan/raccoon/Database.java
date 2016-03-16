package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.IPhysicalBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.SecureBlockDevice;
import org.terifan.raccoon.io.UnsupportedVersionException;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.io.BlobOutputStream;
import org.terifan.raccoon.io.FileBlockDevice;
import org.terifan.raccoon.util.Assert;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


public class Database implements AutoCloseable
{
	private final static int EXTRA_DATA_CHECKSUM_SEED = 0xf49209b1;
	private final static long RACCOON_DB_IDENTITY = 0x726163636f6f6e00L; // 'raccoon\0'
	private final static BlockSizeParam DEFAULT_BLOCK_SIZE = new BlockSizeParam(4096);
	private final static int RACCOON_DB_VERSION = 1;

    private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();
    private final Lock mReadLock = mReadWriteLock.readLock();
    private final Lock mWriteLock = mReadWriteLock.writeLock();

	private IManagedBlockDevice mBlockDevice;
	private final HashMap<Class,Factory> mFactories;
	private final HashMap<Class,Initializer> mInitializers;
	private final HashMap<TableMetadata,Table> mOpenTables;
	private final TableMetadataMap mTableMetadatas;
	private final TransactionCounter mTransactionId;
	private Table mSystemTable;
	private boolean mChanged;
	private Object[] mProperties;
	private TableMetadata mSystemTableMetadata;
	private boolean mCloseDeviceOnCloseDatabase;


	private Database()
	{
		mOpenTables = new HashMap<>();
		mTableMetadatas = new TableMetadataMap();
		mFactories = new HashMap<>();
		mTransactionId = new TransactionCounter();
		mInitializers = new HashMap<>();
	}


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
				throw new IOException("File not found: " + aFile);
			}

			boolean newFile = !aFile.exists();

			BlockSizeParam blockSizeParam = getParameter(BlockSizeParam.class, aParameters, DEFAULT_BLOCK_SIZE);

			fileBlockDevice = new FileBlockDevice(aFile, blockSizeParam.getValue(), aOpenOptions == OpenOption.READ_ONLY);

			return init(fileBlockDevice, newFile, true, aParameters);
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
	 *   AccessCredentials
	 */
	public static Database open(IPhysicalBlockDevice aBlockDevice, OpenOption aOpenOptions, Object... aParameters) throws IOException, UnsupportedVersionException
	{
		if ((aOpenOptions == OpenOption.READ_ONLY || aOpenOptions == OpenOption.OPEN) && aBlockDevice.length() == 0)
		{
			throw new IOException("Block device is empty.");
		}

		boolean create = aBlockDevice.length() == 0 || aOpenOptions == OpenOption.CREATE_NEW;

		return init(aBlockDevice, create, false, aParameters);
	}


	private static Database init(IPhysicalBlockDevice aBlockDevice, boolean aCreate, boolean aCloseDeviceOnCloseDatabase, Object[] aParameters) throws IOException
	{
		AccessCredentials accessCredentials = getParameter(AccessCredentials.class, aParameters, null);

		ManagedBlockDevice device;

		if (aBlockDevice instanceof IManagedBlockDevice)
		{
			if (accessCredentials != null)
			{
				throw new IllegalArgumentException("The BlockDevice provided cannot be secured, ensure that the BlockDevice it writes to is a secure BlockDevice.");
			}

			device = (ManagedBlockDevice)aBlockDevice;
		}
		else if (accessCredentials == null)
		{
			Log.d("creating a managed block device");

			device = new ManagedBlockDevice(aBlockDevice);
		}
		else
		{
			Log.d("creating a secure block device");

			device = new ManagedBlockDevice(new SecureBlockDevice(aBlockDevice, accessCredentials));
		}

		Database db;

		if (aCreate)
		{
			db = create(device, aParameters);
		}
		else
		{
			db = open(device, aParameters);
		}

		db.mCloseDeviceOnCloseDatabase = aCloseDeviceOnCloseDatabase;

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
		db.mSystemTableMetadata = new TableMetadata().create(TableMetadata.class, null);
		db.mSystemTable = new Table(db, db.mSystemTableMetadata, null);
		db.mChanged = true;

		db.updateSuperBlock();

		db.commit();

		Log.dec();

		return db;
	}


	private static Database open(IManagedBlockDevice aBlockDevice, Object[] aParameters) throws IOException, UnsupportedVersionException
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

		if (MurmurHash3.hash_x86_32(buffer.array(), 4, buffer.capacity()-4, EXTRA_DATA_CHECKSUM_SEED) != buffer.readInt32())
		{
			throw new UnsupportedVersionException("This block device does not contain a Raccoon database (bad extra checksum)");
		}

		long identity = buffer.readInt64();
		int version = buffer.readInt32();

		if (identity != RACCOON_DB_IDENTITY)
		{
			throw new UnsupportedVersionException("This block device does not contain a Raccoon database (bad extra identity)");
		}
		if (version != RACCOON_DB_VERSION)
		{
			throw new UnsupportedVersionException("Unsupported database version: provided: " + version + ", expected: " + RACCOON_DB_VERSION);
		}

		db.mTransactionId.set(buffer.readInt64());

		db.mProperties = aParameters;
		db.mBlockDevice = aBlockDevice;
		db.mSystemTableMetadata = new TableMetadata().create(TableMetadata.class, null);
		db.mSystemTable = new Table(db, db.mSystemTableMetadata, buffer.crop().array());

		Log.dec();

		return db;
	}


	private Table openTable(Class aType, Object aDiscriminator, OpenOption aOptions)
	{
		checkOpen();

		synchronized (this)
		{
			TableMetadata tableMetadata = mTableMetadatas.get(aType, aDiscriminator);

			if (tableMetadata == null)
			{
				tableMetadata = new TableMetadata().create(aType, aDiscriminator);

				mTableMetadatas.add(tableMetadata);
			}

			return openTable(tableMetadata, aOptions);
		}
	}


	private Table openTable(TableMetadata aTableMetadata, OpenOption aOptions)
	{
		Table table = mOpenTables.get(aTableMetadata);

		if (table == null)
		{
			Log.i("open table '%s' with option %s", aTableMetadata.getTypeName(), aOptions);
			Log.inc();

			boolean tableExists = mSystemTable.get(aTableMetadata);

			if (!tableExists && (aOptions == OpenOption.OPEN || aOptions == OpenOption.READ_ONLY))
			{
				return null;
			}

			aTableMetadata.initialize();

			table = new Table(this, aTableMetadata, aTableMetadata.getPointer());

			if (!tableExists)
			{
				mSystemTable.save(aTableMetadata);
			}

			mOpenTables.put(aTableMetadata, table);

			Log.dec();
		}

		if (aOptions == OpenOption.CREATE_NEW)
		{
			table.clear();
		}

		return table;
	}


	private void checkOpen() throws IllegalStateException
	{
		if (mSystemTable == null)
		{
			throw new IllegalStateException("Database is closed");
		}
	}


	public boolean isChanged()
	{
		checkOpen();

		for (Table table : mOpenTables.values())
		{
			if (table.isChanged())
			{
				return true;
			}
		}

		return false;
	}


	/**
	 * Persists all pending changes. It's necessary to commit changes on a regular basis to avoid data loss.
	 */
	public boolean commit()
	{
		checkOpen();

		mWriteLock.lock();

		try
		{
			Log.i("commit database");
			Log.inc();

			for (java.util.Map.Entry<TableMetadata,Table> entry : mOpenTables.entrySet())
			{
				if (entry.getValue().commit())
				{
					Log.i("table updated '%s'", entry.getKey());

					mSystemTable.save(entry.getKey());

					mChanged = true;
				}
			}
			
			boolean changed = mChanged;

			if (mChanged)
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

				mChanged = false;

				assert integrityCheck() == null : integrityCheck();
			}

			Log.dec();

			return changed;
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

		mWriteLock.lock();
		try
		{
			Log.i("rollback");
			Log.inc();

			for (Table table : mOpenTables.values())
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
		Log.i("update SuperBlock");

		mTransactionId.increment();

		ByteArrayBuffer buffer = new ByteArrayBuffer(100);
		buffer.writeInt32(0); // leave space for checksum
		buffer.writeInt64(RACCOON_DB_IDENTITY);
		buffer.writeInt32(RACCOON_DB_VERSION);
		buffer.writeInt64(mTransactionId.get());
		if (mSystemTableMetadata.getPointer()!=null) buffer.write(mSystemTableMetadata.getPointer());
		buffer.trim();

		int checksum = MurmurHash3.hash_x86_32(buffer.array(), 4, buffer.position() - 4, EXTRA_DATA_CHECKSUM_SEED);
		buffer.position(0).writeInt32(checksum);

		mBlockDevice.setExtraData(buffer.array());
	}


	@Override
	public void close() throws IOException
	{
		if (mBlockDevice == null)
		{
			return;
		}

		mWriteLock.lock();

		try
		{
			if (mSystemTable != null)
			{
				Log.i("close database");

				for (Table table : mOpenTables.values())
				{
					table.close();
				}

				mOpenTables.clear();

				mSystemTable.close();
				mSystemTable = null;
			}

			if (mCloseDeviceOnCloseDatabase)
			{
				mBlockDevice.close();
			}

			mBlockDevice = null;
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
		mWriteLock.lock();
		try
		{
			Table table = openTable(aEntity.getClass(), aEntity, OpenOption.CREATE);
			return table.save(aEntity);
		}
		finally
		{
			mWriteLock.unlock();
		}
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
			Table table = openTable(aEntity.getClass(), aEntity, OpenOption.OPEN);
			if (table == null)
			{
				return false;
			}
			return table.get(aEntity);
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
			Table table = openTable(aEntity.getClass(), aEntity, OpenOption.OPEN);

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
		mWriteLock.lock();
		try
		{
			Table table = openTable(aEntity.getClass(), aEntity, OpenOption.OPEN);
			if (table == null)
			{
				return false;
			}
			return table.remove(aEntity);
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
		mWriteLock.lock();
		try
		{
			Table table = openTable(aType, aEntity, OpenOption.OPEN);
			if (table != null)
			{
				table.clear();
			}
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
		return openTable(aEntity.getClass(), aEntity, OpenOption.CREATE).saveBlob(aEntity);
	}


	/**
	 * Save the contents of the stream with the key defined by the entity provided.
	 */
	public boolean save(Object aKeyEntity, InputStream aInputStream)
	{
		mWriteLock.lock();
		try
		{
			Table table = openTable(aKeyEntity.getClass(), aKeyEntity, OpenOption.CREATE);
			return table.save(aKeyEntity, aInputStream);
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
		mReadLock.lock();
		try
		{
			Table table = openTable(aEntity.getClass(), aEntity, OpenOption.OPEN);
			if (table == null)
			{
				return null;
			}

			byte[] buffer;
			try (InputStream in = table.read(aEntity))
			{
				if (in == null)
				{
					return null;
				}

				buffer = Streams.readAll(in);
			}

			return new ByteArrayInputStream(buffer);
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public <T> Iterable<T> iterable(Class<T> aType)
	{
		return iterable(aType, null);
	}


	public <T> Iterable<T> iterable(Class<T> aType, T aDiscriminator)
	{
		Table table = openTable(aType, aDiscriminator, OpenOption.OPEN);
		if (table == null)
		{
			return null;
		}

		return ()->table.iterator();
	}


	public <T> Iterator<T> iterator(Class<T> aType)
	{
		return iterator(aType, null);
	}


	public <T> Iterator<T> iterator(Class<T> aType, T aDiscriminator)
	{
		mReadLock.lock();
		try
		{
			Table table = openTable(aType, aDiscriminator, OpenOption.OPEN);

			if (table == null)
			{
				return null;
			}

			return table.iterator();
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public <T> List<T> list(Class<T> aType)
	{
		return list(aType, null);
	}


	public <T> List<T> list(Class<T> aType, T aEntity)
	{
		mReadLock.lock();
		try
		{
			Table table = openTable(aType, aEntity, OpenOption.OPEN);
			if (table == null)
			{
				return new ArrayList<>();
			}
			return table.list(aType);
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public <T> Stream<T> stream(Class<T> aType)
	{
		return stream(aType, null);
	}


	public <T> Stream<T> stream(Class<T> aType, T aEntity)
	{
		mReadLock.lock();
		try
		{
			Table table = openTable(aType, aEntity, OpenOption.OPEN);

			// return empty stream when table not found
			if (table == null)
			{
				return StreamSupport.stream(new AbstractSpliterator<T>(0, Spliterator.SIZED)
				{
					@Override
					public boolean tryAdvance(Consumer<? super T> aConsumer)
					{
						return false;
					}
				}, false);
			}

			ArrayList<T> list = new ArrayList<>();
			table.iterator().forEachRemaining(e->list.add((T)e));

			return StreamSupport.stream(new AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
			{
				int i;
				@Override
				public boolean tryAdvance(Consumer<? super T> aConsumer)
				{
					if (i == list.size())
					{
						return false;
					}
					aConsumer.accept(list.get(i++));
					return true;
				}
			}, false);
		}
		finally
		{
			mReadLock.unlock();
		}
	}


//	public ResultSet list(String aType)
//	{
//		return list(aType, null);
//	}
//
//
//	public ResultSet list(String aType, EntityMap aEntity)
//	{
//		mReadLock.lock();
//		try
//		{
////			Table table = openTable(aType, aEntity, OpenOption.OPEN);
////			if (table == null)
////			{
////				return new ResultSet();
////			}
////			return table.list(aType);
//			return null;
//		}
//		finally
//		{
//			mReadLock.unlock();
//		}
//	}


	/**
	 * Sets a Factory associated with the specified type. The Factory is used to create instances of specified types.
	 *
	 * E.g:
	 * 	 mDatabase.setFactory(Photo.class, ()->new Photo(PhotoAlbum.this));
	 */
	public <T> void setFactory(Class<T> aType, Factory<T> aFactory)
	{
		mFactories.put(aType, aFactory);
	}


	<T> Factory<T> getFactory(Class<T> aType)
	{
		return mFactories.get(aType);
	}


	public <T> void setInitializer(Class<T> aType, Initializer<T> aFactory)
	{
		mInitializers.put(aType, aFactory);
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


	public int size(Class aType)
	{
		return openTable(aType, null, OpenOption.OPEN).size();
	}


	public int size(Object aDiscriminator)
	{
		return openTable(aDiscriminator.getClass(), aDiscriminator, OpenOption.OPEN).size();
	}


	public String integrityCheck()
	{
		mWriteLock.lock();
		try
		{
			for (Table table : mOpenTables.values())
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
		mReadLock.lock();

		try
		{
			ArrayList<Table> tables = new ArrayList<>();
			mSystemTable.list(TableMetadata.class).stream().forEach(e->tables.add(openTable((TableMetadata)e, OpenOption.OPEN)));
			return tables;
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public synchronized <T> List<T> getDiscriminators(Factory<T> aFactory)
	{
		mReadLock.lock();

		try
		{
			ArrayList<T> result = new ArrayList<>();

			String name = aFactory.newInstance().getClass().getName();

			for (TableMetadata tableMetadata : (List<TableMetadata>)mSystemTable.list(TableMetadata.class))
			{
				if (name.equals(tableMetadata.getTypeName()))
				{
					T instance = aFactory.newInstance();
					tableMetadata.getMarshaller().unmarshalDiscriminators(new ByteArrayBuffer(tableMetadata.getDiscriminatorKey()), instance);
					result.add(instance);
				}
			}

			return result;
		}
		finally
		{
			mReadLock.unlock();
		}
	}


	public void scan()
	{
		Log.out.println(mSystemTable);
		mSystemTable.scan();

		for (Table table : getTables())
		{
			Log.out.println(table);
			table.scan();
		}
	}


	private static void dummy()
	{
		// this exists to ensure MemoryBlockDevice is included in distributions
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
	}
}