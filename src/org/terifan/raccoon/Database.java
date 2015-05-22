package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.terifan.raccoon.io.FileBlockDevice;
import org.terifan.raccoon.io.IBlockDevice;
import org.terifan.raccoon.io.IPhysicalBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.SecureBlockDevice;
import org.terifan.raccoon.io.UnsupportedVersionException;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.hashtable.BlockPointer;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.Log;


public class Database implements AutoCloseable
{
	private final static int VERSION = 1;

	private IBlockDevice mBlockDevice;
	private BlockPointer mSystemRootBlockPointer;
	private HashMap<Class,Initializer> mInitializers;
	private HashMap<TableType,Table> mOpenTables;
	private HashMap<Class<?>,TableType> mTableTypes;
	private Table mSystemTable;
	private long mTransactionId;
    private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();
    private final Lock mReadLock = mReadWriteLock.readLock();
    private final Lock mWriteLock = mReadWriteLock.writeLock();


	private Database()
	{
		mOpenTables = new HashMap<>();
		mTableTypes = new HashMap<>();
		mInitializers = new HashMap<>();
	}


	public synchronized static Database open(File aFile, OpenOption aOptions, Object... aParameters) throws IOException, UnsupportedVersionException
	{
		if (aFile.exists())
		{
			if (aOptions == OpenOption.CREATE_NEW)
			{
				if (!aFile.delete())
				{
					throw new IOException("Failed to delete existing file: " + aFile);
				}
			}
			else if ((aOptions == OpenOption.READ_ONLY || aOptions == OpenOption.OPEN) && aFile.length() == 0)
			{
				throw new IOException("File is empty.");
			}
		}
		else if (aOptions == OpenOption.OPEN || aOptions == OpenOption.READ_ONLY)
		{
			throw new IOException("File not found: " + aFile);
		}

		boolean newFile = !aFile.exists();
		String mode = aOptions == OpenOption.READ_ONLY ? "r" : "rw";

		RandomAccessFile file = new RandomAccessFile(aFile, mode);
		FileBlockDevice fileBlockDevice = new FileBlockDevice(file, 4096);

		return init(fileBlockDevice, newFile, aParameters);
	}


	public synchronized static Database open(IPhysicalBlockDevice aBlockDevice, OpenOption aOptions, Object... aParameters) throws IOException, UnsupportedVersionException
	{
		if ((aOptions == OpenOption.READ_ONLY || aOptions == OpenOption.OPEN) && aBlockDevice.length() == 0)
		{
			throw new IOException("Block device is empty.");
		}

		boolean newFile = aBlockDevice.length() == 0 || aOptions == OpenOption.CREATE_NEW;

		return init(aBlockDevice, newFile, aParameters);
	}


	private static Database init(IPhysicalBlockDevice aBlockDevice, boolean aNewFile, Object[] aParameters) throws IOException
	{
		AccessCredentials accessCredentials = null;
		for (Object param : aParameters)
		{
			if (param instanceof AccessCredentials)
			{
				accessCredentials = (AccessCredentials)param;
			}
		}

		ManagedBlockDevice device;

		if (accessCredentials == null)
		{
			device = new ManagedBlockDevice(aBlockDevice);
		}
		else
		{
			device = new ManagedBlockDevice(new SecureBlockDevice(aBlockDevice, accessCredentials));
		}

		if (aNewFile)
		{
			return create(device);
		}
		else
		{
			return open(device);
		}
	}


	private static Database create(IBlockDevice aBlockDevice) throws IOException
	{
		Database db = new Database();

		Log.i("create database");
		Log.inc();

		TableType systemTableType = new TableType(Table.class);

		db.mBlockDevice = aBlockDevice;
		db.mSystemTable = new Table(db, systemTableType, null).open(null);

		db.updateSuperBlock();

		db.commit();

		Log.dec();

		return db;
	}


	private static Database open(IBlockDevice aBlockDevice) throws IOException, UnsupportedVersionException
	{
		Database db = new Database();

		Log.i("open database");
		Log.inc();

		if (aBlockDevice instanceof ManagedBlockDevice)
		{
			ManagedBlockDevice device = (ManagedBlockDevice)aBlockDevice;

			byte[] ptr = new byte[BlockPointer.SIZE];
			ByteBuffer bb = ByteBuffer.wrap(device.getExtraData());

			int version = bb.getInt();

			if (version != VERSION)
			{
				throw new UnsupportedVersionException("Unsupported database version: " + version);
			}

			db.mTransactionId = bb.getLong();
			bb.get(ptr);

			BlockPointer rootPointer = new BlockPointer().decode(ptr, 0);

			TableType systemTableType = new TableType(Table.class);

			db.mBlockDevice = aBlockDevice;
			db.mSystemTable = new Table(db, systemTableType, null).open(rootPointer);
		}
		else
		{
			throw new UnsupportedOperationException();
//			db.mBlockDevice = aBlockDevice;
//			db.mSystemTable = new HashTable(aBlockDevice, 2L);
		}

		db.mSystemRootBlockPointer = db.mSystemTable.getRootBlockPointer();

		Log.dec();

		return db;
	}


//	public List<Schema> getSchemas()
//	{
//		ArrayList<Schema> list = new ArrayList<>();
//		for (Table t : (List<Table>)mSystemTable.list(Table.class))
//		{
//			list.add(Schema.decode(t.getName()));
//		}
//		return list;
//	}


	private Table openTable(Class aType, Object aDiscriminator, OpenOption aOptions) throws IOException
	{
		TableType tableType;

		synchronized (this)
		{
			tableType = mTableTypes.get(aType);

			if (tableType == null)
			{
				tableType = new TableType(aType);

				mTableTypes.put(aType, tableType);
			}
		}

		return openTable(tableType, aDiscriminator, aOptions);
	}


	private Table openTable(TableType aTableType, Object aDiscriminator, OpenOption aOptions) throws IOException
	{
		synchronized (this)
		{
			Table table = mOpenTables.get(aTableType);

			if (table == null)
			{
				table = new Table(this, aTableType, aDiscriminator);

				Log.i("open table '" + table + "' with option " + aOptions);
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
	}


	/**
	 * Persists all pending changes. It's necessary to commit changes on a regular basis to avoid data loss.
	 */
	public void commit() throws IOException
	{
		mWriteLock.lock();

		boolean changed = false;

		try
		{
			Log.i("commit database");
			Log.inc();

			for (Table table : mOpenTables.values())
			{
				if (table.commit())
				{
					Log.i("table updated '" + table + "'");

					changed = true;

					mSystemTable.save(table);
				}
			}

			if (changed)
			{
				mSystemTable.commit();

				updateSuperBlock();

				mBlockDevice.commit();

				mSystemRootBlockPointer = mSystemTable.getRootBlockPointer();
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
		mWriteLock.lock();
		try
		{
			Log.i("rollback");
			Log.inc();

			for (Table table : mOpenTables.values())
			{
				table.rollback();

//				mSystemTable.save(table);
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

		mTransactionId++;

		if (mBlockDevice instanceof ManagedBlockDevice)
		{
			if (mSystemTable.getRootBlockPointer() != mSystemRootBlockPointer)
			{
				byte[] extra = new byte[4 + 8 + BlockPointer.SIZE];

				ByteBuffer bb = ByteBuffer.wrap(extra);
				bb.putInt(VERSION);
				bb.putLong(mTransactionId);
				bb.put(mSystemTable.getRootBlockPointer().encode(new byte[BlockPointer.SIZE], 0));

				ManagedBlockDevice device = (ManagedBlockDevice)mBlockDevice;
				device.setExtraData(extra);
			}
		}
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
	 *   true if the entity was a new entity / false if an existing entity was updated.
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
//	public OutputStream write(Object aEntity) throws IOException
//	{
//		return openTable(aEntity.getClass(), aEntity, OpenOption.CREATE).push(aEntity);
//	}


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
				return null;
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


	public IBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	public long getTransactionId()
	{
		return mTransactionId;
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
}
