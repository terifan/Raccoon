package org.terifan.raccoon;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;


public abstract class TableImplementation implements Closeable, Iterable<RecordEntry>
{
	abstract public boolean get(RecordEntry aEntry);


	abstract public boolean put(RecordEntry aEntry);


	abstract public boolean remove(RecordEntry aEntry);


	abstract public int size();


	abstract public void clear();


	abstract public ArrayList<RecordEntry> list();


	abstract public boolean commit() throws IOException;


	abstract public void rollback() throws IOException;


	abstract public int getEntryMaximumLength();


	abstract public boolean isChanged();


	abstract public byte[] marshalHeader();


	abstract public void scan(ScanResult aScanResult);


	abstract public String integrityCheck();
}
