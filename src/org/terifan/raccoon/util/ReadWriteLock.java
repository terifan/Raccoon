package org.terifan.raccoon.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;


public final class ReadWriteLock
{
	private ReentrantReadWriteLock mReentrantLock = new ReentrantReadWriteLock();


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
		return mReentrantLock.isWriteLocked();
	}


	public final class ReadLock implements AutoCloseable
	{
		ReadLock()
		{
			mReentrantLock.readLock().lock();
		}

		@Override
		public void close()
		{
			mReentrantLock.readLock().unlock();
		}
	}


	public final class WriteLock implements AutoCloseable
	{
		WriteLock()
		{
			mReentrantLock.writeLock().lock();
		}

		@Override
		public void close()
		{
			mReentrantLock.writeLock().unlock();
		}
	}
}
