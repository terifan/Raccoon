package org.terifan.raccoon.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;


public final class ReadWriteLock
{
	private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();


	public ReadLock readLock()
	{
		return new ReadLock();
	}


	public WriteLock writeLock()
	{
		return new WriteLock();
	}


	public boolean isWriteLocked()
	{
		return mReadWriteLock.isWriteLocked();
	}


	public final class ReadLock implements AutoCloseable
	{
		ReadLock()
		{
			mReadWriteLock.readLock().lock();
		}

		@Override
		public void close()
		{
			mReadWriteLock.readLock().unlock();
		}
	}


	public final class WriteLock implements AutoCloseable
	{
		WriteLock()
		{
			mReadWriteLock.writeLock().lock();
		}

		@Override
		public void close()
		{
			mReadWriteLock.writeLock().unlock();
		}
	}
}
