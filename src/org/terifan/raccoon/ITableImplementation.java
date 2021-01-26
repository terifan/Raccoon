package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;


public interface ITableImplementation extends Iterable<ArrayMapEntry>, AutoCloseable
{
	void create(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost, PerformanceTool aPerformanceTool);


	void open(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost, PerformanceTool aPerformanceTool, byte[] aTableHeader);


	boolean get(ArrayMapEntry aEntry);


	ArrayMapEntry put(ArrayMapEntry aEntry);


	ArrayMapEntry remove(ArrayMapEntry aEntry);


	ArrayList<ArrayMapEntry> list();


	Iterator<ArrayMapEntry> iterator();


	void removeAll();


	@Override
	void close();


	int size();


	boolean isChanged();


	/**
	 *
	 * @param oChanged
	 *   can be null, returns true if the table was changed.
	 * @return
	 *   the TableHeader for this table
	 */
	byte[] commit(AtomicBoolean oChanged);


	void rollback();


	String integrityCheck();


	void scan(ScanResult aScanResult);


	int getEntryMaximumLength();
}
