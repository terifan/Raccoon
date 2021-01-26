package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;


public interface ITableImplementation extends Iterable<ArrayMapEntry>
{
	void create(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost, PerformanceTool aPerformanceTool);


	void open(byte[] aTableHeader, IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost, PerformanceTool aPerformanceTool);


	boolean get(ArrayMapEntry aEntry);


	ArrayMapEntry put(ArrayMapEntry aEntry);


	ArrayMapEntry remove(ArrayMapEntry aEntry);


	ArrayList<ArrayMapEntry> list();


	Iterator<ArrayMapEntry> iterator();


	void clear();


	void close();


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


	boolean isChanged();


	void scan(ScanResult aScanResult);


	int size();
}
