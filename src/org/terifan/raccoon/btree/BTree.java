package org.terifan.raccoon.btree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.RecordEntry;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.TableImplementation;


public class BTree implements TableImplementation
{
	@Override
	public boolean get(RecordEntry aEntry)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public boolean put(RecordEntry aEntry)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public boolean remove(RecordEntry aEntry)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public int size()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void clear()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public ArrayList<RecordEntry> list()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void close()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public boolean commit() throws IOException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void rollback() throws IOException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public int getEntryMaximumLength()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public boolean isChanged()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public byte[] marshalHeader()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public Iterator<RecordEntry> iterator()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public String integrityCheck()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
}
