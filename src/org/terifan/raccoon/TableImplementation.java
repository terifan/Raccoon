package org.terifan.raccoon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.RecordEntry;
import org.terifan.raccoon.ScanResult;


public interface TableImplementation extends AutoCloseable, Iterable<RecordEntry>
{
	boolean get(RecordEntry aEntry);


	boolean put(RecordEntry aEntry);


	boolean remove(RecordEntry aEntry);


	int size();


	void clear();


	ArrayList<RecordEntry> list();


	@Override
	void close();


	boolean commit() throws IOException;


	void rollback() throws IOException;


	int getEntryMaximumLength();


	boolean isChanged();


	byte[] marshalHeader();


	@Override
	Iterator<RecordEntry> iterator();


	void scan(ScanResult aScanResult);


	String integrityCheck();
}
