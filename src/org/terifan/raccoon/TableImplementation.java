package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;


public abstract class TableImplementation implements Iterable<ArrayMapEntry>, AutoCloseable
{
	protected final String mTableName;
	protected final TransactionGroup mTransactionGroup;
	protected final boolean mCommitChangesToBlockDevice;
	protected BlockAccessor mBlockAccessor;


	public TableImplementation(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionGroup, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName)
	{
		mTableName = aTableName;
		mTransactionGroup = aTransactionGroup;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam);
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
	}


	abstract void openOrCreateTable(byte[] aTableHeader);


	abstract boolean get(ArrayMapEntry aEntry);


	abstract ArrayMapEntry put(ArrayMapEntry aEntry);


	abstract ArrayMapEntry remove(ArrayMapEntry aEntry);


	abstract ArrayList<ArrayMapEntry> list();


	abstract void removeAll(Consumer<ArrayMapEntry> aConsumer);


	abstract int size();


	abstract boolean isChanged();


	@Override
	public abstract void close();


	public abstract long flush();


	/**
	 *
	 * @param oChanged
	 *   can be null, returns true if the table was changed.
	 * @return
	 *   the TableHeader for this table
	 */
	abstract byte[] commit(AtomicBoolean oChanged);


	abstract void rollback();


	abstract String integrityCheck();


	abstract void scan(ScanResult aScanResult);


	abstract int getEntrySizeLimit();
}
