package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.terifan.raccoon.io.FileBlockDevice;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.IPhysicalBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.SecureBlockDevice;
import org.terifan.raccoon.io.UnsupportedVersionException;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.io.BlobOutputStream;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class Database implements AutoCloseable
{
	private final static int EXTRA_DATA_CHECKSUM_SEED = 0xf49209b1;
	private final static long IDENTITY = 0x726163636f6f6e00L; // 'raccoon\0'
	private final static BlockSizeParam DEFAULT_BLOCK_SIZE = new BlockSizeParam(4096);
	private final static int VERSION = 1;

    private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();
    private final Lock mReadLock = mReadWriteLock.readLock();
    private final Lock mWriteLock = mReadWriteLock.writeLock();

	private IManagedBlockDevice mBlockDevice;
	private byte[] mSystemTableHeader;
	private final HashMap<Class,Initializer> mInitializers;
	private final HashMap<TableType,Table> mOpenTables;
	private final TableTypeMap mTableTypes;
	private final TransactionId mTransactionId;
	private Table mSystemTable;
	private boolean mChanged;
	private Object[] mProperties;


	private Database()
	{
		mOpenTables = new HashMap<>();
		mTableTypes = new TableTypeMap();
		mInitializers = new HashMap<>();
		mTransactionId = new TransactionId();

		setInitializer(Table.class, (e)->{
			try
			{
				e.init(this, new TableType(Table.class, null), null);
			}
			catch (Exception ex)
			{
				throw new IllegalStateException(ex);
			}
		});
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
			String mode = aOpenOptions == OpenOption.READ_ONLY ? "r" : "rw";

			BlockSizeParam blockSizeParam = getParameter(BlockSizeParam.class, aParameters, DEFAULT_BLOCK_SIZE);

			RandomAccessFile file = new RandomAccessFile(aFile, mode);
			fileBlockDevice = new FileBlockDevice(file, blockSizeParam.getValue());

			return init(fileBlockDevice, newFile, aParameters);
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

		return init(aBlockDevice, create, aParameters);
	}


	private static Database init(IPhysicalBlockDevice aBlockDevice, boolean aCreate, Object[] aParameters) throws IOException
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

		if (aCreate)
		{
			return create(device, aParameters);
		}
		else
		{
			return open(device, aParameters);
		}
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

		TableType systemTableType = new TableType(Table.class, null);

		db.mBlockDevice = aBlockDevice;
		db.mSystemTable = new Table(db, systemTableType, null).open(null);
		db.mChanged = true;
		db.mProperties = aParameters;

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

		if (identity != IDENTITY)
		{
			throw new UnsupportedVersionException("This block device does not contain a Raccoon database (bad extra identity)");
		}
		if (version != VERSION)
		{
			throw new UnsupportedVersionException("Unsupported database version: provided: " + version + ", expected: " + VERSION);
		}

		db.mTransactionId.set(buffer.readInt64());

		TableType systemTableType = new TableType(Table.class, null);

		db.mBlockDevice = aBlockDevice;
		db.mSystemTable = new Table(db, systemTableType, null).open(buffer);
		db.mSystemTableHeader = db.mSystemTable.getTableHeader();
		db.mProperties = aParameters;

		Log.dec();

		return db;
	}


	private Table openTable(Class aType, Object aDiscriminator, OpenOption aOptions) throws IOException
	{
		checkOpen();

		TableType tableType;

		synchronized (this)
		{
			tableType = mTableTypes.get(aType, aDiscriminator);

			if (tableType == null)
			{
				tableType = new TableType(aType, aDiscriminator);

				mTableTypes.add(tableType);
			}

			return openTable(tableType, aDiscriminator, aOptions);
		}
	}


	private Table openTable(TableType aTableType, Object aDiscriminator, OpenOption aOptions) throws IOException
	{
		Table table = mOpenTables.get(aTableType);

		if (table == null)
		{
			table = new Table(this, aTableType, aDiscriminator);

			Log.i("open table '%s' with option %s", table, aOptions);
			Log.inc();

			boolean tableExists = mSystemTable.get(table);

			if (!tableExists && (aOptions == OpenOption.OPEN || aOptions == OpenOption.READ_ONLY))
			{
				return null;
			}

			table.open(null);

			if (!tableExists)
			{
				mSystemTable.save(table);
			}

			mOpenTables.put(aTableType, table);

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
	public void commit() throws IOException
	{
		checkOpen();

		mWriteLock.lock();

		try
		{
			Log.i("commit database");
			Log.inc();

			for (Table table : mOpenTables.values())
			{
				if (table.commit())
				{
					Log.i("table updated '%s'", table);

					mSystemTable.save(table);

					mChanged = true;
				}
			}

			if (mChanged)
			{
				mSystemTable.commit();

				updateSuperBlock();

				mBlockDevice.commit();

				mSystemTableHeader = mSystemTable.getTableHeader();
				mChanged = false;

				assert integrityCheck() == null : integrityCheck();
			}

			Log.dec();
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
		buffer.writeInt64(IDENTITY);
		buffer.writeInt32(VERSION);
		buffer.writeInt64(mTransactionId.get());
		buffer.write(mSystemTable.getTableHeader());
		buffer.trim();

		int checksum = MurmurHash3.hash_x86_32(buffer.array(), 4, buffer.position()-4, EXTRA_DATA_CHECKSUM_SEED);
		buffer.position(0).writeInt32(checksum);

		mBlockDevice.setExtraData(buffer.array());
	}


	@Override
	public void close() throws IOException
	{
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
	public boolean save(Object aEntity) throws IOException
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
	public boolean get(Object aEntity) throws IOException
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
	 * Retrieves an entity.
	 *
	 * @return
	 *   true if the entity was found.
	 */
	public boolean contains(Object aEntity) throws IOException
	{
		mReadLock.lock();
		try
		{
			Table table = openTable(aEntity.getClass(), aEntity, OpenOption.OPEN);
			if (table == null)
			{
				return false;
			}
			return table.contains(aEntity);
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
	public boolean remove(Object aEntity) throws IOException
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
	public void clear(Class aType) throws IOException
	{
		clearImpl(aType, null);
	}


	/**
	 * Remove all entries of the entities type and possible it's discriminator.
	 */
	public void clear(Object aEntity) throws IOException
	{
		clearImpl(aEntity.getClass(), aEntity);
	}


	private void clearImpl(Class aType, Object aEntity) throws IOException
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
	public BlobOutputStream saveBlob(Object aEntity) throws IOException
	{
		return openTable(aEntity.getClass(), aEntity, OpenOption.CREATE).saveBlob(aEntity);
	}


	/**
	 * Save the contents of the stream with the key defined by the entity provided.
	 */
	public boolean save(Object aKeyEntity, InputStream aInputStream) throws IOException
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
	public InputStream read(Object aEntity) throws IOException
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

				buffer = Streams.fetch(in);
			}

			return new ByteArrayInputStream(buffer);
		}
		finally
		{
			mReadLock.unlock();
		}

//		mReadLock.lock();
//		try
//		{
//			Table table = openTable(aEntity.getClass(), aEntity, OpenOption.OPEN);
//			if (table == null)
//			{
//				return null;
//			}
//
//			InputStream in = table.read(aEntity);
//
//			return new InputStream()
//			{
//				@Override
//				public int read() throws IOException
//				{
//					return in.read();
//				}
//
//				@Override
//				public void close() throws IOException
//				{
//					try
//					{
//						in.close();
//					}
//					finally
//					{
//						mReadLock.unlock();
//					}
//				}
//			};
//		}
//		finally
//		{
////			mReadLock.unlock();
//		}
	}


	public <T> Iterable<T> iterable(Class<T> aType)
	{
		return iterable(aType, null);
	}


	public <T> Iterable<T> iterable(Class<T> aType, T aDiscriminator)
	{
		Table table;
		try
		{
			table = openTable(aType, aDiscriminator, OpenOption.OPEN);
			if (table == null)
			{
				return null;
			}
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}

		return () ->
		{
			return table.iterator();
		};
	}


	public <T> Iterator<T> iterator(Class<T> aType) throws IOException
	{
		return iterator(aType, null);
	}


	public <T> Iterator<T> iterator(Class<T> aType, T aDiscriminator) throws IOException
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


//	public Iterator<Entry> iteratorRaw(Table aTable) throws IOException
//	{
//		mReadLock.lock();
//		try
//		{
//			return aTable.iteratorRaw();
//		}
//		finally
//		{
//			mReadLock.unlock();
//		}
//	}


//	public <T> Stream<? extends T> stream(Class<T> aType) throws IOException
//	{
//		return stream(aType, null);
//	}
//
//
//	public <T> Stream<? extends T> stream(Class<T> aType, T aDiscriminator) throws IOException
//	{
//		Table table = openTable(aType, aDiscriminator, OpenOption.OPEN);
//
//		if (table == null)
//		{
//			throw new Error();
//		}
//
//		return StreamSupport.stream(Spliterators.spliterator(table.iterator(), table.size(), Spliterator.IMMUTABLE), true);
//	}


	public <T> List<T> list(Class<T> aType) throws IOException
	{
		return list(aType, null);
	}


	public <T> List<T> list(Class<T> aType, T aEntity) throws IOException
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


	/**
	 * Sets the Initializer associated with the specified type. The Initializer is called for each entity created by the database of specified type.
	 */
	public <T> void setInitializer(Class<T> aType, Initializer<T> aInitializer)
	{
		mInitializers.put(aType, aInitializer);
	}


	<T> Initializer<T> getInitializer(Class<T> aType)
	{
		return mInitializers.get(aType);
	}


	public IManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	public TransactionId getTransactionId()
	{
		return mTransactionId;
	}


	public int size(Class aType) throws IOException
	{
		return openTable(aType, null, OpenOption.OPEN).size();
	}


	public int size(Object aDiscriminator) throws IOException
	{
		return openTable(aDiscriminator.getClass(), aDiscriminator, OpenOption.OPEN).size();
	}


	public String integrityCheck()
	{
		mWriteLock.lock();
		try
		{
			// this exists to ensure MemoryBlockDevice is included in distributions
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

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
				if (aType.isAssignableFrom(param.getClass()))
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


	public List<Schema> getSchemas() throws IOException
	{
		ArrayList<Schema> schemas = new ArrayList<>();

		for (Table table : (List<Table>)mSystemTable.list(Table.class))
		{
			schemas.add(new Schema(table));
		}

		return schemas;
	}
}
