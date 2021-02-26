package org.terifan.raccoon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.channels.SeekableByteChannel;


public class Parcel
{
	public SeekableByteChannel openChannel(LobOpenOption aOption) throws IOException
	{
		throw new UnsupportedOperationException();
	}


	public byte[] readAllBytes() throws IOException
	{
		throw new UnsupportedOperationException();
	}


	public LobByteChannel readAllBytes(OutputStream aDst) throws IOException
	{
		throw new UnsupportedOperationException();
	}


	public LobByteChannel readAllBytes(Buffer aDst) throws IOException
	{
		throw new UnsupportedOperationException();
	}


	public LobByteChannel writeAllBytes(byte[] aSrc) throws IOException
	{
		throw new UnsupportedOperationException();
	}


	public LobByteChannel writeAllBytes(InputStream aSrc) throws IOException
	{
		throw new UnsupportedOperationException();
	}


	public LobByteChannel writeAllBytes(Buffer aSrc) throws IOException
	{
		throw new UnsupportedOperationException();
	}


	public InputStream newInputStream()
	{
		throw new UnsupportedOperationException();
	}


	public OutputStream newOutputStream()
	{
		throw new UnsupportedOperationException();
	}


	public void delete()
	{
		throw new UnsupportedOperationException();
	}


	public long size()
	{
		throw new UnsupportedOperationException();
	}
}
