package org.terifan.raccoon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import org.terifan.raccoon.util.Listener;


public interface LobByteChannel extends SeekableByteChannel
{
	byte[] readAllBytes() throws IOException;


	LobByteChannel readAllBytes(OutputStream aDst) throws IOException;


	LobByteChannel writeAllBytes(byte[] aSrc) throws IOException;


	LobByteChannel writeAllBytes(InputStream aSrc) throws IOException;


	InputStream newInputStream();


	OutputStream newOutputStream();


	LobByteChannel setCloseListener(Listener<LobByteChannel> aListener);


	boolean isModified();
}
