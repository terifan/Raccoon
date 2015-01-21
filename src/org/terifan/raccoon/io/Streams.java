package org.terifan.raccoon.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.util.Arrays;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;


/**
 * Utility class that simplifies stream handling. The transfer methods accept:
 *
 * Input:
 * <ol>
 *  <li>java.io.InputStream</li>
 *  <li>java.io.File</li>
 *  <li>java.lang.String (file path)</li>
 *  <li>java.net.URL</li>
 *  <li>org.terifan.v1.net.filesystem.File</li>
 *  <li>byte array</li>
 * </ol>
 *
 * Output:
 * <ol>
 *  <li>java.io.OutputStream</li>
 *  <li>java.io.File</li>
 *  <li>java.lang.String (file path)</li>
 *  <li>java.net.URL</li>
 *  <li>org.terifan.v1.data.ByteBuffer</li>
 * </ol>
 *
 * This sample will copy a file:
 *
 * <pre>
 * Streams.transfer("myfile.txt", "copy of myfile.txt"));
 * </pre>
 */
public final class Streams
{
	private Streams()
	{
	}


	/**
	 * Transfers bytes from the InputStream to the OutputStreams until end of
	 * stream is reached. Streams are closed when the transfer has completed.
	 *
	 * @param aInput
	 *   an input to read from
	 * @param aOutput
	 *   one or more outputs to write to
	 * @return
	 *   number of bytes actually transfered.
	 */
	public static long transfer(Object aInput, Object ... aOutput) throws IOException
	{
		return transfer(Long.MAX_VALUE, true, true, aInput, aOutput);
	}


	/**
	 * Transfers bytes from the InputStream to the OutputStreams until end of
	 * stream is reached.
	 *
	 * @param aClose
	 *   if true, calls the close method when transfer is complete.
	 * @return
	 *   number of bytes actually transfered.
	 */
	public static long transfer(Boolean aClose, Object aInput, Object ... aOutput) throws IOException
	{
		return transfer(Long.MAX_VALUE, aClose, aClose, aInput, aOutput);
	}


	/**
	 * Transfers bytes from the InputStream to the OutputStreams until end of
	 * stream is reached.
	 *
	 * @param aCloseInput
	 *   if true, calls the closeInput method when transfer is complete.
	 * @param aCloseOutput
	 *   if true, calls the closeOutput method when transfer is complete.
	 * @return
	 *   number of bytes actually transfered.
	 */
	public static long transfer(Boolean aCloseInput, Boolean aCloseOutput, Object aInput, Object ... aOutput) throws IOException
	{
		return transfer(Long.MAX_VALUE, aCloseInput, aCloseOutput, aInput, aOutput);
	}


	/**
	 * Transfers a limited number of bytes from the InputStream to all the
	 * OutputStreams. Streams are closed when the transfer has completed.
	 *
	 * @param aLimit
	 *   number of bytes to transfer. A negative value indicates no limit.
	 * @return
	 *   number of bytes actually transfered.
	 */
	public static long transfer(Long aLimit, Object aInput, Object ... aOutput) throws IOException
	{
		return transfer(aLimit, true, true, aInput, aOutput);
	}


	/**
	 * Transfers a limited number of bytes from the InputStream to all the
	 * OutputStreams.
	 *
	 * @param aLimit
	 *   number of bytes to transfer. A negative value indicates no limit.
	 * @param aCloseInput
	 *   if true, calls the closeInput method when transfer is complete.
	 * @param aCloseOutput
	 *   if true, calls the closeOutput method when transfer is complete.
	 * @return
	 *   number of bytes actually transfered.
	 */
	public static long transfer(Long aLimit, Boolean aCloseInput, Boolean aCloseOutput, Object aInput, Object ... aOutput) throws IOException
	{
		boolean doCloseInput = true;
		boolean [] doCloseOutput = null;
		InputStream inputStream = null;
		OutputStream [] outputStreams = null;

		try
		{
			outputStreams = new OutputStream[aOutput.length];
			doCloseOutput = new boolean[aOutput.length];
			Arrays.fill(doCloseOutput, true);

			// setup input

			doCloseInput = aCloseInput;
			if (aInput instanceof InputStream)
			{
				inputStream = (InputStream)aInput;
			}
			else if (aInput instanceof URL) inputStream = ((URL)aInput).openStream();
			else if (aInput instanceof File) inputStream = new BufferedInputStream(new FileInputStream((File)aInput));
			else if (aInput instanceof String) inputStream = new FileInputStream((String)aInput);
			else if (aInput instanceof CharSequence) inputStream = new ByteArrayInputStream(((CharSequence)aInput).toString().getBytes());
			else if (aInput instanceof RandomAccessFile) inputStream = new RandomAccessFileInputStream((RandomAccessFile)aInput);
//			else if (aInput instanceof java.nio.ByteBuffer) inputStream = new ByteBufferInputStream((java.nio.ByteBuffer)aInput);
			else if (aInput instanceof byte[]) inputStream = new ByteArrayInputStream((byte[])aInput);
			else if (aInput == null) throw new IOException("Unsupported input type: null");
			else throw new IOException("Unsupported input type: " + aInput.getClass());

			// setup output

			for (int i = 0; i < aOutput.length; i++)
			{
				Object o = aOutput[i];
				doCloseOutput[i] = aCloseOutput;
				if (o instanceof OutputStream)
				{
					outputStreams[i] = (OutputStream)o;
				}
				else if (o instanceof File) outputStreams[i] = new BufferedOutputStream(new FileOutputStream((File)o));
				else if (o instanceof String) outputStreams[i] = new FileOutputStream((String)o);
//				else if (o instanceof ByteBuffer) outputStreams[i] = new ByteBufferOutputStream((ByteBuffer)o);
				else if (o instanceof Document) outputStreams[i] = new ByteArrayOutputStream();
				else if (o instanceof byte[]) outputStreams[i] = new MyByteArrayOutputStream((byte[])o);
				else if (o == null) throw new IOException("Unsupported output type: null");
				else throw new IOException("Unsupported output type: " + o.getClass());
			}

			// perform transfer

			long total = 0;
			byte [] buffer = new byte[4096];

			while (aLimit > 0)
			{
				int len = inputStream.read(buffer, 0, (int)Math.min(buffer.length, aLimit));

				if (len <= 0)
				{
					break;
				}

				for (OutputStream outputStream : outputStreams)
				{
					outputStream.write(buffer, 0, len);
				}

				total += len;
				aLimit -= len;
			}

			// post processing of output data

			for (int i = 0; i < aOutput.length; i++)
			{
				if (aOutput[i] instanceof Node)
				{
					try
					{
						Document sourceDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(((ByteArrayOutputStream)outputStreams[i]).toByteArray()));
						Document targetDoc = (Document)aOutput[i];

						Transformer tx   = TransformerFactory.newInstance().newTransformer();
						DOMSource source = new DOMSource(sourceDoc);
						DOMResult result = new DOMResult(targetDoc);
						tx.transform(source,result);
					}
					catch (Exception e)
					{
						throw new IOException(e);
					}
				}
			}

			return total;
		}
		catch (java.io.IOException e)
		{
			throw new IOException(e);
		}
		finally
		{
			// ensure all streams are closed

			if (doCloseInput && inputStream != null)
			{
				try
				{
					inputStream.close();
				}
				catch (Throwable e)
				{
				}
			}
			for (int i = 0; outputStreams != null && i < outputStreams.length; i++)
			{
				if (doCloseOutput[i])
				{
					try
					{
						outputStreams[i].close();
					}
					catch (Throwable e)
					{
					}
				}
			}
		}
	}


//	private static class ByteBufferOutputStream extends OutputStream
//	{
//		private ByteBuffer mBuffer;
//
//		public ByteBufferOutputStream(ByteBuffer aBuffer)
//		{
//			mBuffer = aBuffer;
//		}
//
//		@Override
//		public void write(int b) throws IOException
//		{
//			if (mBuffer.position() >= mBuffer.capacity()-1)
//			{
//				mBuffer.ensureCapacity(1+mBuffer.capacity()*3/2);
//			}
//			mBuffer.put(b);
//		}
//	}


	/**
	 * Transfers bytes from the InputStream to the OutputStreams until end of
	 * stream is reached. Streams are closed when the transfer has completed.
	 *
	 * @param aInput
	 *   an input to read from
	 * @return
	 *   number of bytes actually transfered.
	 */
	public static byte [] fetch(Object aInput) throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		transfer(aInput, baos);
		return baos.toByteArray();
	}


	private static class MyByteArrayOutputStream extends OutputStream
	{
		private byte [] mBuffer;
		private int mOffset;

		public MyByteArrayOutputStream(byte [] aBuffer)
		{
			mBuffer = aBuffer;
		}

		@Override
		public void write(int b) throws java.io.IOException
		{
			if (mOffset < mBuffer.length)
			{
				mBuffer[mOffset++] = (byte)b;
			}
		}
	}


	private static class RandomAccessFileInputStream extends InputStream
	{
		private RandomAccessFile mFile;


		public RandomAccessFileInputStream(RandomAccessFile aFile)
		{
			mFile = aFile;
		}


		@Override
		public int read(byte[] b, int off, int len) throws java.io.IOException
		{
			return mFile.read(b, off, len);
		}


		@Override
		public int read() throws java.io.IOException
		{
			return mFile.read();
		}


		@Override
		public void close() throws java.io.IOException
		{
			mFile.close();
		}
	}
}