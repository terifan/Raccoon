package org.terifan.v1.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;


/**
 * A ByteBuffer that wraps a primitive byte array and allow methods to read
 * and write different types to it.
 *
 * <code>
 * byte [] bytes = ByteBuffer.allocate(13).putLong(64561654964L).putInt(65464).put(37).array();
 * </code>
 */
public class ByteBuffer
{
	private ByteOrder mByteOrder;
	private int mMark;
	private byte[] mBuffer;
	private int mPosition;


	/**
	 * Use either of the createBigEndian and createLittleEndian methods to
	 * create an instance of this object.
	 */
	private ByteBuffer(byte[] aBuffer)
	{
		mBuffer = aBuffer;
		mByteOrder = ByteOrder.BIG_ENDIAN;
	}


	/**
	 * Creates a big endian byte buffer. Java uses Big Endian as it's default
	 * method of storing variables.
	 *
	 * @param aLength
	 *    size of the buffer.
	 * @return
	 *    a new ByteBuffer.BigEndian instance with the size specified.
	 */
	public static ByteBuffer allocate(int aLength)
	{
		return new ByteBuffer(new byte[aLength]);
	}


	/**
	 * Creates a big endian byte buffer. Java uses Big Endian as it's default
	 * method of storing variables.
	 *
	 * @param aByteBuffer
	 *    the byte array this object wraps.
	 * @return
	 *    a new ByteBuffer.BigEndian instance wrapping the byte array provided.
	 */
	public static ByteBuffer wrap(byte[] aByteBuffer)
	{
		return new ByteBuffer(aByteBuffer);
	}


	/**
	 * Returns a reference to the internal byte array this ByteBuffer wraps.<p>
	 *
	 * If this ByteBuffer was created with one of the create methods that take
	 * a byte array and the ByteBuffer hasn't been resized then this method will
	 * return the byte array originally provided.
	 *
	 * @return
	 *    The internal byte buffer.
	 */
	public final byte[] array()
	{
		return mBuffer;
	}


	/**
	 * Set the internal byte array to the byte array specified and resets
	 * position to zero.
	 *
	 * @param aByteArray
	 *    The new internal byte array that this ByteArray wraps.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer array(byte[] aByteBuffer)
	{
		mBuffer = aByteBuffer;
		mPosition = 0;
		mMark = 0;
		return this;
	}


	/**
	 * Copy data in the internal buffer.
	 */
	public final void copy(int aFromIndex, int aToIndex, int aLength)
	{
		System.arraycopy(mBuffer, aFromIndex, mBuffer, aToIndex, aLength);
	}


	/**
	 * Clears this buffer. The position is set to zero and the mark is discarded.
	 *
	 * This method fills the buffer with zero bytes.
	 */
	public final ByteBuffer clear()
	{
		fill((byte)0);
		mPosition = 0;
		mMark = -1;
		return this;
	}


	/**
	 * Rewinds this buffer. The position is set to zero and the mark is discarded.
	 * @return
	 *   This buffer
	 */
	public final ByteBuffer rewind()
	{
		mPosition = 0;
		mMark = -1;
		return this;
	}


	public final ByteBuffer order(ByteOrder aByteOrder)
	{
		mByteOrder = aByteOrder;
		return this;
	}


	public final ByteOrder order()
	{
		return mByteOrder;
	}


	/**
	 * Constructs a ByteBuffer with the contents of the File specified. Only
	 * the first 2 GiB will be loaded.
	 *
	 * @param aFile
	 *    this File to be loaded.
	 * @return
	 *    a ByteBuffer.
	 */
	public static ByteBuffer load(File aFile) throws IOException
	{
		return ByteBuffer.allocate(0).read(aFile);
	}


	/**
	 * Constructs a ByteBuffer with the contents of the File specified. Only
	 * the first 2 GiB will be loaded.
	 *
	 * @param aFile
	 *    this File to be loaded.
	 * @return
	 *    a ByteBuffer.
	 */
	public static ByteBuffer load(String aFile) throws IOException
	{
		return load(new File(aFile));
	}


	/**
	 * Read the contents from the File specified. Only the first 2 GiB will be
	 * loaded.
	 *
	 * @param aFile
	 *    this File to be loaded.
	 * @return
	 *    this ByteBuffer.
	 */
	public final ByteBuffer read(File aFile) throws IOException
	{
		if (aFile == null)
		{
			throw new IllegalArgumentException("aFile is null");
		}
		if (!aFile.exists())
		{
			throw new IllegalArgumentException("File not found: " + aFile);
		}

		int position = mPosition;

		ensureCapacity(mPosition + (int) aFile.length());

		InputStream in = new FileInputStream(aFile);
		try
		{
			read(in);
		}
		finally
		{
			in.close();
		}

		position(position);

		return this;
	}


	/**
	 * Read all contents from the InputStream specified. The ByteBuffer will
	 * grow to accommodate the read data. Only the first 2 GiB will be loaded.
	 *
	 * @param aInputStream
	 *    read from this stream
	 * @return
	 *    this ByteBuffer.
	 */
	public final ByteBuffer read(InputStream aInputStream) throws IOException
	{
		return read(aInputStream, Integer.MAX_VALUE);
	}


	/**
	 * Read at most aLimit bytes from the InputStream specified. The ByteBuffer
	 * will grow to accommodate the read data.
	 *
	 * @param aInputStream
	 *    read from this stream
	 * @return
	 *    this ByteBuffer.
	 */
	public final ByteBuffer read(InputStream aInputStream, int aLimit) throws IOException
	{
		if (aInputStream == null)
		{
			throw new IllegalArgumentException("aInputStream is null");
		}

		byte[] buffer = new byte[4096];
		for (int remaining = aLimit; remaining > 0;)
		{
			int len = aInputStream.read(buffer, 0, Math.min(remaining, buffer.length));
			if (len == -1)
			{
				break;
			}
			ensureCapacity(mPosition+len);
			put(buffer, 0, len);
			remaining -= len;
		}

		return this;
	}


	/**
	 * Writes the contents of this ByteBuffer to the File specified. Writing
	 * will start at ByteBuffers current position.
	 *
	 * @param aFile
	 *   destination File.
	 * @return
	 *   this ByteBuffer
	 */
	public final ByteBuffer write(File aFile) throws IOException
	{
		if (aFile == null)
		{
			throw new IllegalArgumentException("aFile is null");
		}

		OutputStream out = new FileOutputStream(aFile);
		try
		{
			write(out);
		}
		finally
		{
			out.close();
		}

		return this;
	}


	/**
	 * Writes the contents of this ByteBuffer to the Stream specified. Writing
	 * will start at ByteBuffers current position.
	 *
	 * @param aStream
	 *   destination Stream.
	 * @return
	 *   this ByteBuffer
	 */
	public final ByteBuffer write(OutputStream aOutputStream) throws IOException
	{
		if (aOutputStream == null)
		{
			throw new IllegalArgumentException("aOutputStream is null");
		}

		aOutputStream.write(mBuffer, mPosition, mBuffer.length - mPosition);

		return this;
	}


	/**
	 * Sets this buffer's position. If the mark is defined and larger than the
	 * new position then it is discarded.
	 *
	 * @param aPosition
	 *    The new position value; must be non-negative and no larger than the
	 *    buffer length
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer position(int aPosition)
	{
		mPosition = aPosition;
		return this;
	}


	/**
	 * Returns this buffer's position.
	 *
	 * @return
	 *    The position of this buffer.
	 */
	public final int position()
	{
		return mPosition;
	}


	/**
	 * Skips a number of bytes.
	 *
	 * @param aCount
	 *    number of bytes to skip.
	 */
	public final ByteBuffer skip(int aCount)
	{
		mPosition += aCount;
		return this;
	}


	/**
	 * Truncates the ByteBuffer to the current position.
	 */
	public final ByteBuffer truncate()
	{
		capacity(mPosition);
		return this;
	}


	/**
	 * Sets this buffer's mark at its position.
	 *
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer mark()
	{
		mMark = mPosition;
		return this;
	}


	/**
	 * Resets this buffer's position to the previously-marked position.<p>
	 * Invoking this method neither changes nor discards the mark's value.
	 *
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer reset()
	{
		mPosition = mMark;
		return this;
	}


	/**
	 * Reverse the order of all bytes.
	 *
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer reverse()
	{
		return reverse(0, mBuffer.length);
	}


	/**
	 * Reverse the order of some bytes.
	 *
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer reverse(int aIndex, int aLength)
	{
		for (int i = aIndex, j = aIndex + aLength - 1; i < j; i++, j--)
		{
			byte t = mBuffer[i];
			mBuffer[i] = mBuffer[j];
			mBuffer[j] = t;
		}
		return this;
	}


	/**
	 * Returns this buffer's length.
	 *
	 * @return
	 *    The length of this buffer.
	 */
	public final int capacity()
	{
		return mBuffer.length;
	}


	/**
	 * Sets this buffer's length. If the mark is defined and larger than the new
	 * length then it is discarded.
	 *
	 * @param aLength
	 *    The new size of this buffer.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer capacity(int aLength)
	{
		mBuffer = Arrays.copyOf(mBuffer, aLength);
		if (mMark < aLength)
		{
			mMark = aLength;
		}
		return this;
	}


	/**
	 * Return number of remaining bytes in this buffer.
	 *
	 * @return
	 *   remaining bytes
	 */
	public final int remaining()
	{
		return mBuffer.length-mPosition;
	}


	/**
	 * True if more bytes exist in the buffer
	 *
	 * @return
	 *   remaining bytes
	 */
	public final boolean hasRemaining()
	{
		return mPosition < mBuffer.length;
	}


	public final ByteBuffer insert(int aLength)
	{
		return insert(mPosition, aLength);
	}


	/**
	 * Inserts a number of bytes at the current position. The internal buffer
	 * is resized and content beyond current position is adjusted aLength bytes.
	 * The position is unchanged.
	 *
	 * @param aIndex
	 *   The index at which the bytes will be written
	 * @param aLength
	 *   number of bytes to insert
	 * @return
	 *   This buffer
	 */
	public final ByteBuffer insert(int aIndex, int aLength)
	{
		capacity(mBuffer.length + aLength);
		System.arraycopy(mBuffer, aIndex, mBuffer, aIndex + aLength, mBuffer.length - (aIndex + aLength));
		return this;
	}


	public final ByteBuffer remove(int aRemoveFromBegining)
	{
		System.arraycopy(mBuffer, aRemoveFromBegining, mBuffer, 0, mBuffer.length-aRemoveFromBegining);
		Arrays.fill(mBuffer, mBuffer.length-aRemoveFromBegining, mBuffer.length, (byte)0);
		return this;
	}


	/**
	 * Ensures this buffer has a certain capacity. If the current capacity is
	 * shorter than the length provided, then this buffer is resized.
	 *
	 * @param aLength
	 *    The new size of this buffer.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer ensureCapacity(int aLength)
	{
		if (aLength > mBuffer.length)
		{
			capacity(aLength);
		}
		return this;
	}


	/**
	 * Fills this buffer with the byte value specified.
	 *
	 * @param aValue
	 *    The buffer is filled with this value.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer fill(byte aValue)
	{
		java.util.Arrays.fill(mBuffer, aValue);
		return this;
	}


	/**
	 * Fills this buffer with the byte value specified.
	 *
	 * @param aIndex
	 *    The index at which the bytes will be written
	 * @param aLength
	 *    Number of bytes to fill
	 * @param aValue
	 *    The buffer is filled with this value.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer fill(int aIndex, int aLength, byte aValue)
	{
		java.util.Arrays.fill(mBuffer, aIndex, aIndex+aLength, aValue);
		return this;
	}


	public final byte get()
	{
		return mBuffer[mPosition++];
	}


	/**
	 * Get a byte value without altering the position.
	 */
	public final byte peek()
	{
		return mBuffer[mPosition];
	}


	/**
	 * Reads the byte at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The byte at the buffer's current position.
	 */
	public final byte get(int aIndex)
	{
		return mBuffer[aIndex];
	}


	public final ByteBuffer get(byte[] aDest)
	{
		get(aDest, 0, aDest.length);
		return this;
	}


	/**
	 * This method transfers bytes from this buffer into the given destination
	 * array.
	 *
	 * @param aDest
	 *    The array into which bytes are to be written
	 * @param aOffset
	 *    The offset within the array of the first byte to be written; must be
	 *    non-negative and no larger than dst.length
	 * @param aLength
	 *    The maximum number of bytes to be written to the given array; must be
	 *    non-negative and no larger than dst.length - offset
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer get(byte[] aDest, int aOffset, int aLength)
	{
		System.arraycopy(mBuffer, mPosition, aDest, aOffset, aLength);
		mPosition += aLength;
		return this;
	}


	public final byte [] getBytes(int aLength)
	{
		byte [] buf = new byte[aLength];
		System.arraycopy(mBuffer, mPosition, buf, 0, aLength);
		mPosition += aLength;
		return buf;
	}


	public final char [] getChars(int aLength)
	{
		char [] buf = new char[aLength];
		for (int i = 0; i < aLength; i++)
		{
			buf[i] = getChar();
		}
		return buf;
	}


	/**
	 * Writes the bytes of the String specified.
	 *
	 * @param aValue
	 *    The String value to be written.
	 * @return
	 *    This buffer.
	 */
	public final ByteBuffer putBytes(CharSequence aValue)
	{
		for (int i = 0; i < aValue.length(); i++)
		{
			put((byte) aValue.charAt(i));
		}

		return this;
	}


	/**
	 * Writes the chars of the String specified.
	 *
	 * @param aValue
	 *    The String value to be written.
	 * @return
	 *    This buffer.
	 */
	public final ByteBuffer putChars(CharSequence aValue)
	{
		for (int i = 0; i < aValue.length(); i++)
		{
			putChar(aValue.charAt(i));
		}

		return this;
	}


	public final ByteBuffer put(byte[] aValues)
	{
		System.arraycopy(aValues, 0, mBuffer, mPosition, aValues.length);
		mPosition += aValues.length;
		return this;
	}


	/**
	 * This method copies the content of the given source byte array to this
	 * buffer and then increment the position of the ByteBuffer.
	 *
	 * @param aValues
	 *    The source buffer.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer put(int aIndex, byte[] aValues)
	{
		System.arraycopy(aValues, 0, mBuffer, aIndex, aValues.length);
		mPosition += aValues.length;
		return this;
	}


	/**
	 * This method copies the content of the given source byte array to this
	 * buffer and then increment the position of the ByteBuffer.
	 *
	 * @param aValues
	 *    The source buffer.
	 * @param aOffset
	 *    Offset where to copy from
	 * @param aLength
	 *    number of bytes to copy
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer put(byte[] aValues, int aOffset, int aLength)
	{
		if (aValues.length < aOffset+aLength)
		{
			throw new IllegalArgumentException("Buffer too short: buffer length: "+aValues.length+", offset: "+aOffset+", length: "+aLength);
		}

		System.arraycopy(aValues, aOffset, mBuffer, mPosition, aLength);
		mPosition += aLength;
		return this;
	}


	/**
	 * This method transfers part of the content of the given source byte array
	 * into this buffer.
	 *
	 * @param aOffset
	 *    The offset within the array of the first byte to be read; must be
	 *    non-negative and no larger than aValues.length
	 * @param aLength
	 *    The maximum number of bytes to be written from the given array; must be
	 *    non-negative and no larger than aValues.length - offset
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer put(int aIndex, byte[] aValues, int aOffset, int aLength)
	{
		System.arraycopy(aValues, aOffset, mBuffer, aIndex, aLength);
		return this;
	}


	public final ByteBuffer put(byte aValue)
	{
		return put(mPosition++, aValue);
	}


	/**
	 * Writes the given byte into this buffer at the current position, and then
	 * increments the position.
	 *
	 * @param aValues
	 *    The byte to be written
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer put(int aIndex, byte aValue)
	{
		mBuffer[aIndex] = aValue;
		return this;
	}


	public final ByteBuffer put(int aValue)
	{
		mBuffer[mPosition++] = (byte)aValue;
		return this;
	}


	/**
	 * Writes the given byte into this buffer at the current position, and then
	 * increments the position.
	 *
	 * @param aValue
	 *    The byte to be written
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer put(int aIndex, int aValue)
	{
		mBuffer[aIndex] = (byte)aValue;
		return this;
	}


	public final float getFloat()
	{
		float v = getFloat(mPosition);
		mPosition += 4;
		return v;
	}


	/**
	 * Reads the float at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The float at the buffer's current position.
	 */
	public final float getFloat(int aIndex)
	{
		return Float.intBitsToFloat(getInt(aIndex));
	}


	public final ByteBuffer putFloat(float aValue)
	{
		putInt(Float.floatToRawIntBits(aValue));
		return this;
	}


	/**
	 * Writes the given float into this buffer at the current position, and
	 * then increments the position.
	 *
	 * @param aValue
	 *    The float value to be written.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putFloat(int aIndex, float aValue)
	{
		putInt(aIndex, Float.floatToRawIntBits(aValue));
		return this;
	}


	public final ByteBuffer putFloat(double aValue)
	{
		putInt(Float.floatToRawIntBits((float)aValue));
		return this;
	}


	/**
	 * Writes the given float into this buffer at the current position, and
	 * then increments the position.
	 *
	 * @param aValue
	 *    The float value to be written. The value provided is casted to a float.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putFloat(int aIndex, double aValue)
	{
		putInt(aIndex, Float.floatToRawIntBits((float) aValue));
		return this;
	}


	public final double getDouble()
	{
		double v = getDouble(mPosition);
		mPosition += 8;
		return v;
	}


	/**
	 * Reads the double at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The double at the buffer's current position.
	 */
	public final double getDouble(int aIndex)
	{
		return Double.longBitsToDouble(getLong(aIndex));
	}


	public final ByteBuffer putDouble(double aValue)
	{
		putLong(Double.doubleToRawLongBits(aValue));
		return this;
	}


	/**
	 * Writes the given double into this buffer at the current position, and
	 * then increments the position.
	 *
	 * @param aValue
	 *    The double value to be written.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putDouble(int aIndex, double aValue)
	{
		putLong(aIndex, Double.doubleToRawLongBits(aValue));
		return this;
	}


	public String getUTF()
	{
		int utflen = getUnsignedShort();
		byte[] bytearr;
		char[] chararr;
		bytearr = new byte[utflen];
		chararr = new char[utflen];

		int c, char2, char3;
		int count = 0;
		int chararr_count = 0;

		get(bytearr, 0, utflen);

		while (count < utflen)
		{
			c = (int) bytearr[count] & 0xff;
			if (c > 127)
			{
				break;
			}
			count++;
			chararr[chararr_count++] = (char) c;
		}

		while (count < utflen)
		{
			c = (int) bytearr[count] & 0xff;
			switch (c >> 4)
			{
				case 0:
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
					/* 0xxxxxxx*/
					count++;
					chararr[chararr_count++] = (char) c;
					break;
				case 12:
				case 13:
					/* 110x xxxx   10xx xxxx*/
					count += 2;
					if (count > utflen)
					{
						throw new RuntimeException(
								"malformed input: partial character at end");
					}
					char2 = (int) bytearr[count - 1];
					if ((char2 & 0xC0) != 0x80)
					{
						throw new RuntimeException(
								"malformed input around byte " + count);
					}
					chararr[chararr_count++] = (char) (((c & 0x1F) << 6)
							| (char2 & 0x3F));
					break;
				case 14:
					/* 1110 xxxx  10xx xxxx  10xx xxxx */
					count += 3;
					if (count > utflen)
					{
						throw new RuntimeException(
								"malformed input: partial character at end");
					}
					char2 = (int) bytearr[count - 2];
					char3 = (int) bytearr[count - 1];
					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
					{
						throw new RuntimeException(
								"malformed input around byte " + (count - 1));
					}
					chararr[chararr_count++] = (char) (((c & 0x0F) << 12)
							| ((char2 & 0x3F) << 6)
							| ((char3 & 0x3F)     ));
					break;
				default:
					/* 10xx xxxx,  1111 xxxx */
					throw new RuntimeException(
							"malformed input around byte " + count);
			}
		}
		// The number of chars produced may be less than utflen
		return new String(chararr, 0, chararr_count);
	}


	/**
	 * Writes a string to the underlying output stream using
	 * <a href="DataInput.html#modified-utf-8">modified UTF-8</a>
	 * encoding in a machine-independent manner.
	 * <p>
	 * First, two bytes are written to the output stream as if by the
	 * <code>putShort</code> method giving the number of bytes to
	 * follow. This value is the number of bytes actually written out,
	 * not the length of the string. Following the length, each character
	 * of the string is output, in sequence, using the modified UTF-8 encoding
	 * for the character. If no exception is thrown, the counter
	 * <code>written</code> is incremented by the total number of
	 * bytes written to the output stream. This will be at least two
	 * plus the length of <code>aValue</code>, and at most two plus
	 * thrice the length of <code>aValue</code>.
	 *
	 * @param aValue
	 *    The String value to be written.
	 * @return
	 *    This buffer.
	 * @see
	 *   java.io.DataOutputStream.writeUTF
	 */
	public final ByteBuffer putUTF(CharSequence str)
	{
		int strlen = str.length();
		int utflen = 0;
		int c, count = 0;

		/* use charAt instead of copying String to char array */
		for (int i = 0; i < strlen; i++)
		{
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F))
			{
				utflen++;
			}
			else if (c > 0x07FF)
			{
				utflen += 3;
			}
			else
			{
				utflen += 2;
			}
		}

		if (utflen > 65535)
		{
			throw new IllegalArgumentException("encoded string too long: " + utflen + " bytes");
		}

		byte[] bytearr;
		bytearr = new byte[utflen + 2];

		bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
		bytearr[count++] = (byte) ((utflen      ) & 0xFF);

		int i;
		for (i = 0; i < strlen; i++)
		{
			c = str.charAt(i);
			if (!((c >= 0x0001) && (c <= 0x007F)))
			{
				break;
			}
			bytearr[count++] = (byte) c;
		}

		for (; i < strlen; i++)
		{
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F))
			{
				bytearr[count++] = (byte) c;

			}
			else if (c > 0x07FF)
			{
				bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
				bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
				bytearr[count++] = (byte) (0x80 | ((c     ) & 0x3F));
			}
			else
			{
				bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
				bytearr[count++] = (byte) (0x80 | ((c     ) & 0x3F));
			}
		}
		put(bytearr, 0, utflen + 2);
		return this;
	}


	/**
	 * Reads the variable length number at this buffer's
	 * current position and then increments the position.
	 *
	 * @return
	 *    The variable length long at the buffer's current position.
	 */
	public final long getVLC()
	{
		int header = (mBuffer[mPosition++] & 255);

		long value;

		if ((header & 0x40) != 0)
		{
			value = header & 0x3f;
		}
		else if ((header & 0x20) != 0)
		{
			value = (header & 0x1f) << 8;
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 0x10) != 0)
		{
			value = (header & 0x0f) << 16;
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 0x08) != 0)
		{
			value = (header & 0x07) << 24;
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 0x04) != 0)
		{
			value = (long) (header & 0x03) << 32;
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 3) == 0)
		{
			value = ((long) (mBuffer[mPosition++] & 255) << 32);
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 3) == 1)
		{
			value = ((long) (mBuffer[mPosition++] & 255) << 40);
			value += ((long) (mBuffer[mPosition++] & 255) << 32);
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 3) == 2)
		{
			value = ((long) (mBuffer[mPosition++] & 255) << 48);
			value += ((long) (mBuffer[mPosition++] & 255) << 40);
			value += ((long) (mBuffer[mPosition++] & 255) << 32);
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else
		{
			value = ((long) (mBuffer[mPosition++] & 255) << 56);
			value += ((long) (mBuffer[mPosition++] & 255) << 48);
			value += ((long) (mBuffer[mPosition++] & 255) << 40);
			value += ((long) (mBuffer[mPosition++] & 255) << 32);
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}

		return (header & 0x80) != 0 ? -value - 1 : value;
	}


	/**
	 * Writes a variable length number into this buffer at the current position
	 * and then increments the position.
	 *
	 * @param aValue
	 *    The variable length long value to be written.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putVLC(long aValue)
	{
		// 6     s1......
		// 5+8   s01..... ........
		// 4+16  s001.... ........ ........
		// 3+24  s0001... ........ ........ ........
		// 2+32  s00001.. ........ ........ ........ ........
		// 0+40  s0000000 ........ ........ ........ ........ ........
		// 0+48  s0000001 ........ ........ ........ ........ ........ ........
		// 0+56  s0000010 ........ ........ ........ ........ ........ ........ ........
		// 0+64  s0000011 ........ ........ ........ ........ ........ ........ ........ ........

		int sign = 0;

		if (aValue < 0L)
		{
			aValue = -(aValue + 1);
			sign = 0x80;
		}

		if (aValue < (1L << 6))
		{
			mBuffer[mPosition++] = (byte) (sign | 0x40 | aValue);
		}
		else if (aValue < (1L << 13))
		{
			mBuffer[mPosition++] = (byte) (sign | 0x20 | (aValue >>> 8));
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 20))
		{
			mBuffer[mPosition++] = (byte) (sign | 0x10 | (aValue >>> 16));
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 27))
		{
			mBuffer[mPosition++] = (byte) (sign | 0x08 | (aValue >>> 24));
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 34))
		{
			mBuffer[mPosition++] = (byte) (sign | 0x04 | (aValue >>> 32));
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 40))
		{
			mBuffer[mPosition++] = (byte) (sign       );
			mBuffer[mPosition++] = (byte) (aValue >> 32);
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 48))
		{
			mBuffer[mPosition++] = (byte) (sign | 0x01);
			mBuffer[mPosition++] = (byte) (aValue >> 40);
			mBuffer[mPosition++] = (byte) (aValue >> 32);
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 56))
		{
			mBuffer[mPosition++] = (byte) (sign | 0x02);
			mBuffer[mPosition++] = (byte) (aValue >> 48);
			mBuffer[mPosition++] = (byte) (aValue >> 40);
			mBuffer[mPosition++] = (byte) (aValue >> 32);
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else
		{
			mBuffer[mPosition++] = (byte) (sign | 0x03);
			mBuffer[mPosition++] = (byte) (aValue >> 56);
			mBuffer[mPosition++] = (byte) (aValue >> 48);
			mBuffer[mPosition++] = (byte) (aValue >> 40);
			mBuffer[mPosition++] = (byte) (aValue >> 32);
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}

		return this;
	}


	/**
	 * Reads the variable length number at this buffer's current position and
	 * then increments the position.
	 *
	 * @return
	 *    The variable length long at the buffer's current position.
	 */
	public final long getUVLC()
	{
		int header = (mBuffer[mPosition++] & 255);

		long value;

		if ((header & 0x80) != 0)
		{
			value = header & 0x7f;
		}
		else if ((header & 0x40) != 0)
		{
			value = (header & 0x3f) << 8;
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 0x20) != 0)
		{
			value = (header & 0x1f) << 16;
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 0x10) != 0)
		{
			value = (header & 0x0f) << 24;
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 0x08) != 0)
		{
			value = (long) (header & 0x07) << 32;
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 0x04) != 0)
		{
			value = (long) (header & 0x03) << 40;
			value += ((long) (mBuffer[mPosition++] & 255) << 32);
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 0x02) != 0)
		{
			value = (long) (header & 0x01) << 48;
			value += ((long) (mBuffer[mPosition++] & 255) << 40);
			value += ((long) (mBuffer[mPosition++] & 255) << 32);
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else if ((header & 1) == 0)
		{
			value = ((long) (mBuffer[mPosition++] & 255) << 48);
			value += ((long) (mBuffer[mPosition++] & 255) << 40);
			value += ((long) (mBuffer[mPosition++] & 255) << 32);
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}
		else
		{
			value = ((long) (mBuffer[mPosition++] & 255) << 56);
			value += ((long) (mBuffer[mPosition++] & 255) << 48);
			value += ((long) (mBuffer[mPosition++] & 255) << 40);
			value += ((long) (mBuffer[mPosition++] & 255) << 32);
			value += ((long) (mBuffer[mPosition++] & 255) << 24);
			value += ((mBuffer[mPosition++] & 255) << 16);
			value += ((mBuffer[mPosition++] & 255) << 8);
			value += (mBuffer[mPosition++] & 255);
		}

		return value;
	}


	/**
	 * Writes a variable length number into this buffer at the current position
	 * and then increments the position.
	 *
	 * @param aValue
	 *    The variable length long value to be written.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putUVLC(long aValue)
	{
		// 7     1.......
		// 6+8   01...... ........
		// 5+16  001..... ........ ........
		// 4+24  0001.... ........ ........ ........
		// 3+32  00001... ........ ........ ........ ........
		// 2+40  000001.. ........ ........ ........ ........ ........
		// 1+48  0000001. ........ ........ ........ ........ ........ ........
		// 0+56  00000000 ........ ........ ........ ........ ........ ........ ........
		// 0+64  00000001 ........ ........ ........ ........ ........ ........ ........ ........

		if (aValue < 0L)
		{
			throw new IllegalArgumentException("Provided value is negative.");
		}

		if (aValue < (1L << 7))
		{
			mBuffer[mPosition++] = (byte) (0x80 | aValue);
		}
		else if (aValue < (1L << 14))
		{
			mBuffer[mPosition++] = (byte) (0x40 | (aValue >>> 8));
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 21))
		{
			mBuffer[mPosition++] = (byte) (0x20 | (aValue >>> 16));
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 28))
		{
			mBuffer[mPosition++] = (byte) (0x10 | (aValue >>> 24));
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 35))
		{
			mBuffer[mPosition++] = (byte) (0x08 | (aValue >>> 32));
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 42))
		{
			mBuffer[mPosition++] = (byte) (0x04 | (aValue >>> 40));
			mBuffer[mPosition++] = (byte) (aValue >> 32);
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 49))
		{
			mBuffer[mPosition++] = (byte) (0x02 | (aValue >>> 48));
			mBuffer[mPosition++] = (byte) (aValue >> 40);
			mBuffer[mPosition++] = (byte) (aValue >> 32);
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else if (aValue < (1L << 56))
		{
			mBuffer[mPosition++] = (byte) (0x00);
			mBuffer[mPosition++] = (byte) (aValue >> 48);
			mBuffer[mPosition++] = (byte) (aValue >> 40);
			mBuffer[mPosition++] = (byte) (aValue >> 32);
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}
		else
		{
			mBuffer[mPosition++] = (byte) (0x01);
			mBuffer[mPosition++] = (byte) (aValue >> 56);
			mBuffer[mPosition++] = (byte) (aValue >> 48);
			mBuffer[mPosition++] = (byte) (aValue >> 40);
			mBuffer[mPosition++] = (byte) (aValue >> 32);
			mBuffer[mPosition++] = (byte) (aValue >> 24);
			mBuffer[mPosition++] = (byte) (aValue >> 16);
			mBuffer[mPosition++] = (byte) (aValue >> 8);
			mBuffer[mPosition++] = (byte) (aValue);
		}

		return this;
	}


	/**
	 * Reads a VarChar String from the buffer.
	 *
	 * @param aValue
	 *    The String to write
	 * @return
	 *    The String
	 */
	public final String getVarChar()
	{
		int len = (int)getUVLC();

		char [] chararr = new char[len];

		for (int i = 0; i < len; i++)
		{
			chararr[i] = (char)getUnsignedByte();
		}

		for (int i = 0; i < len;)
		{
			int b = getUnsignedByte();
			if (b == 0)
			{
				b = getUnsignedByte();
//				System.out.println("++"+b+"++");
				chararr[i++] |= b << 8;
			}
			else if (b < 128)
			{
//				System.out.println("##"+(b-1)+"##");
				chararr[i++] |= (b-1) << 8;
			}
			else
			{
				b -= 127;
//				System.out.println("**"+b+"**");
				int s = i == 0 ? 0 : chararr[i-1] & 0xFF00;
				for (int j = 0; j < b; j++)
				{
					chararr[i++] |= s;
				}
			}
		}

		return new String(chararr);
	}


	/**
	 * Writes a String to the buffer. Characters are encoded either as bytes or
	 * UVLC codes if any existing character is unicode 256 or higher.
	 *
	 * @param aValue
	 *    The String to write
	 * @return
	 *     This buffer
	 */
	public final ByteBuffer putVarChar(String aValue)
	{
		int len = aValue.length();

		putUVLC(len);

		putBytes(aValue);

		for (int i = 0, prev = 0, counter = 0; i <= len; i++)
		{
			if (counter == 127 || (i == len && counter > 0))
			{
				put(127+counter);
//				System.out.println("**"+counter+"**");
				counter = 0;
			}

			if (i == len)
			{
				break;
			}

			int b = aValue.charAt(i) >> 8;

			if (b != prev && counter > 0)
			{
				put(127+counter);
//				System.out.println("**"+counter+"**");
				counter = 0;
			}

			if (b == prev)
			{
				counter++;
			}
			else if (b >= 127)
			{
//				System.out.println("++"+b+"++");
				put(0);
				put(b);
			}
			else
			{
//				System.out.println("##"+b+"##");
				put(b+1);
			}
			prev = b;
		}

//		System.out.println("");

		return this;
	}


	public final char getChar()
	{
		char v = getChar(mPosition);
		mPosition += 2;
		return v;
	}


	/**
	 * Reads the char at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The char at the buffer's current position.
	 */
	public final char getChar(int aIndex)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			return (char) ((getUnsignedByte(aIndex) << 8)
			             | (getUnsignedByte(aIndex+1)));
		}
		else
		{
			return (char) ((getUnsignedByte(aIndex))
			             | (getUnsignedByte(aIndex+1) << 8));
		}
	}


	public final ByteBuffer putChar(int aValue)
	{
		putChar(mPosition, aValue);
		mPosition += 2;
		return this;
	}


	/**
	 * Writes the given char into this buffer at the current position, and
	 * then increments the position.
	 *
	 * @param aValue
	 *    The char value to be written.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putChar(int aIndex, int aValue)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			mBuffer[aIndex++] = (byte) (aValue >> 8);
			mBuffer[aIndex++] = (byte) (aValue);
		}
		else
		{
			mBuffer[aIndex++] = (byte) (aValue);
			mBuffer[aIndex++] = (byte) (aValue >> 8);
		}
		return this;
	}


	public final short getShort()
	{
		short v = getShort(mPosition);
		mPosition += 2;
		return v;
	}


	/**
	 * Reads the short at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The short at the buffer's current position.
	 */
	public final short getShort(int aIndex)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			return (short) (((mBuffer[aIndex] & 255) << 8)
			              | ((mBuffer[aIndex+1] & 255)));
		}
		else
		{
			return (short) ((mBuffer[aIndex] & 255)
			             | ((mBuffer[aIndex+1] & 255) << 8));
		}
	}


	public final ByteBuffer putShort(short aValue)
	{
		putShort(mPosition, (int)aValue);
		mPosition += 2;
		return this;
	}


	public final ByteBuffer putShort(int aIndex, short aValue)
	{
		putShort(aIndex, (int)aValue);
		return this;
	}


	public final ByteBuffer putShort(int aValue)
	{
		putShort(mPosition, aValue);
		mPosition += 2;
		return this;
	}


	/**
	 * Writes the given short into this buffer at the current position, and
	 * then increments the position.
	 *
	 * @param aValue
	 *    The short value to be written.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putShort(int aIndex, int aValue)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			mBuffer[aIndex+0] = (byte) (aValue >> 8);
			mBuffer[aIndex+1] = (byte) (aValue);
		}
		else
		{
			mBuffer[aIndex+0] = (byte) (aValue);
			mBuffer[aIndex+1] = (byte) (aValue >> 8);
		}
		return this;
	}


	public final int getInt()
	{
		int v = getInt(mPosition);
		mPosition += 4;
		return v;
	}


	/**
	 * Reads the int at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The int at the buffer's current position.
	 */
	public final int getInt(int aIndex)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			return ((mBuffer[aIndex+0] & 255) << 24)
			     | ((mBuffer[aIndex+1] & 255) << 16)
				 | ((mBuffer[aIndex+2] & 255) << 8)
				 | ((mBuffer[aIndex+3] & 255));
		}
		else
		{
			return ((mBuffer[aIndex+0] & 255))
			     | ((mBuffer[aIndex+1] & 255) << 8)
				 | ((mBuffer[aIndex+2] & 255) << 16)
				 | ((mBuffer[aIndex+3] & 255) << 24);
		}
	}


	public final ByteBuffer putInt(int aValue)
	{
		putInt(mPosition, aValue);
		mPosition += 4;
		return this;
	}


	/**
	 * Writes the given int into this buffer at the current position, and
	 * then increments the position.
	 *
	 * @param aValue
	 *    The int value to be written.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putInt(int aIndex, int aValue)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			mBuffer[aIndex+0] = (byte) (aValue >> 24);
			mBuffer[aIndex+1] = (byte) (aValue >> 16);
			mBuffer[aIndex+2] = (byte) (aValue >> 8);
			mBuffer[aIndex+3] = (byte) (aValue     );
		}
		else
		{
			mBuffer[aIndex+0] = (byte) (aValue     );
			mBuffer[aIndex+1] = (byte) (aValue >> 8);
			mBuffer[aIndex+2] = (byte) (aValue >> 16);
			mBuffer[aIndex+3] = (byte) (aValue >> 24);
		}
		return this;
	}


	public final long getLong()
	{
		long v = getLong(mPosition);
		mPosition += 8;
		return v;
	}


	/**
	 * Reads the long at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The long at the buffer's current position.
	 */
	public final long getLong(int aIndex)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			return ((long) mBuffer[aIndex+0] << 56)
			     | ((long) (mBuffer[aIndex+1] & 255) << 48)
				 | ((long) (mBuffer[aIndex+2] & 255) << 40)
				 | ((long) (mBuffer[aIndex+3] & 255) << 32)
				 | ((long) (mBuffer[aIndex+4] & 255) << 24)
				 | ( (mBuffer[aIndex+5] & 255) << 16)
				 | ( (mBuffer[aIndex+6] & 255) << 8)
				 | ( (mBuffer[aIndex+7] & 255));
		}
		else
		{
			return ( (mBuffer[aIndex+0] & 255))
			     | ( (mBuffer[aIndex+1] & 255) << 8)
				 | ( (mBuffer[aIndex+2] & 255) << 16)
				 | ((long) (mBuffer[aIndex+3] & 255) << 24)
				 | ((long) (mBuffer[aIndex+4] & 255) << 32)
				 | ((long) (mBuffer[aIndex+5] & 255) << 40)
				 | ((long) (mBuffer[aIndex+6] & 255) << 48)
				 | ((long) mBuffer[aIndex+7] << 56);
		}
	}


	public final ByteBuffer putLong(long aValue)
	{
		putLong(mPosition, aValue);
		mPosition += 8;
		return this;
	}


	/**
	 * Writes the given long into this buffer at the current position, and
	 * then increments the position.
	 *
	 * @param aValue
	 *    The long value to be written.
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putLong(int aIndex, long aValue)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			mBuffer[aIndex+0] = (byte) (aValue >> 56);
			mBuffer[aIndex+1] = (byte) (aValue >> 48);
			mBuffer[aIndex+2] = (byte) (aValue >> 40);
			mBuffer[aIndex+3] = (byte) (aValue >> 32);
			mBuffer[aIndex+4] = (byte) (aValue >> 24);
			mBuffer[aIndex+5] = (byte) (aValue >> 16);
			mBuffer[aIndex+6] = (byte) (aValue >> 8);
			mBuffer[aIndex+7] = (byte) (aValue     );
		}
		else
		{
			mBuffer[aIndex+0] = (byte) (aValue     );
			mBuffer[aIndex+1] = (byte) (aValue >> 8);
			mBuffer[aIndex+2] = (byte) (aValue >> 16);
			mBuffer[aIndex+3] = (byte) (aValue >> 24);
			mBuffer[aIndex+4] = (byte) (aValue >> 32);
			mBuffer[aIndex+5] = (byte) (aValue >> 40);
			mBuffer[aIndex+6] = (byte) (aValue >> 48);
			mBuffer[aIndex+7] = (byte) (aValue >> 56);
		}
		return this;
	}


	public final int getUnsignedByte()
	{
		return mBuffer[mPosition++] & 255;
	}


	/**
	 * Reads the unsigned byte at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The unsigned byte at the buffer's current position.
	 */
	public final int getUnsignedByte(int aIndex)
	{
		return mBuffer[aIndex] & 255;
	}


	public final int getUnsignedShort()
	{
		return getShort() & 65535;
	}


	/**
	 * Reads the unsigned short at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The unsigned short at the buffer's current position.
	 */
	public final int getUnsignedShort(int aIndex)
	{
		return getShort(aIndex) & 65535;
	}


	public final long getUnsignedInt()
	{
		return getInt() & 0xFFFFFFFFL;
	}


	/**
	 * Reads the unsigned int at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return
	 *    The unsigned int at the buffer's current position.
	 */
	public final long getUnsignedInt(int aIndex)
	{
		return getInt(aIndex) & 0xFFFFFFFFL;
	}


	/**
	 * Writes the given variable length long into this buffer at the current
	 * position, and then increments the position.
	 *
	 * @param aValue
	 *    The variable length long value to be written.
	 * @param aLength
	 *    Number of bytes to write
	 * @return
	 *    This buffer
	 */
	public final ByteBuffer putNumber(long aValue, int aLength)
	{
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			int o = 8 * (aLength - 1);
			while (aLength-- > 0)
			{
				mBuffer[mPosition++] = (byte) (aValue >>> o);
				o -= 8;
			}
		}
		else
		{
			int o = 0;
			while (aLength-- > 0)
			{
				mBuffer[mPosition++] = (byte) (aValue >>> o);
				o += 8;
			}
		}
		return this;
	}


	/**
	 * Reads the variable length long at this buffer's current position,
	 * and then increments the position. This method returns the value of the specified number of bytes.
	 *
	 * @param aLength
	 *    Number of bytes to read
	 * @return
	 *    The variable length long at the buffer's current position.
	 */
	public final long getNumber(int aLength)
	{
		long v = 0;
		if (mByteOrder == ByteOrder.BIG_ENDIAN)
		{
			while (aLength-- > 0)
			{
				v <<= 8;
				v |= getUnsignedByte(mPosition++);
			}
		}
		else
		{
			int o = 0;

			while (aLength-- > 0)
			{
				v |= (long) getUnsignedByte(mPosition++) << o;
				o += 8;
			}
		}

		return v;
	}


	/**
	 * Reads a String from at this buffer's current position,
	 * and then increments the position.
	 *
	 * @param aLength
	 *    max number of bytes in the string.
	 * @param aBreakAtZero
	 *    if true, the string will end if a zero bytes is encountered.
	 * @param aSkipRemaining
	 *    if ture and aBreakAtZero is also true, if a zero byte was encountered
	 *    then the string will end and any remaining bytes will be discarded.
	 * @return
	 *    The String at the buffer's current position.
	 */
	public final String getString(int aLength, boolean aBreakAtZero, boolean aSkipRemaining)
	{
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < aLength; i++)
		{
			int c = get();
			if (c == 0 && aBreakAtZero)
			{
				if (aSkipRemaining)
				{
					skip(aLength - i - 1);
				}
				break;
			}
			builder.append((char) c);
		}

		return builder.toString();
	}
	

//	public static void main(String... args)
//	{
//		try
//		{
//			ByteBuffer bb = ByteBuffer.allocate(100);
//			bb.putLong(1).putInt(2).putShort(3).putChar(4).put(5).putFloat(6f).putDouble(7f).putUVLC(10000000000L).putVLC(-10000000000L);
//			System.out.println(bb.position());
//			bb.position(0);
//			//Debug.hexDump(bb.array());
//			long v = bb.getLong() + bb.getInt() + bb.getShort() + bb.getChar() + bb.get() + (long)bb.getFloat() + (long)bb.getDouble() + bb.getUVLC() + bb.getVLC();
//			System.out.println(bb.position());
//			System.out.println(v==28);
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}


	public static void main(String... args)
	{
		try
		{
			Random r = new Random(1);

			char [] c1 = new char[256];
			for (int i = 0; i < c1.length; i++)
			{
				c1[i] = (char)r.nextInt(256);
			}
			char [] c2 = new char[256];
			for (int i = 0; i < c2.length; i++)
			{
				c2[i] = (char)(32+r.nextInt(64));
			}
			char [] c3 = new char[256];
			for (int i = 0; i < c3.length; i++)
			{
				c3[i] = (char)r.nextInt(65536);
			}
			char [] c4 = new char[256];
			for (int i = 0; i < c4.length; i++)
			{
				if (r.nextDouble() > 0.1)
				{
					c4[i] = (char)r.nextInt(256);
				}
				else
				{
					c4[i] = (char)r.nextInt(65536);
				}
			}
			char [] c5 = new char[256];
			for (int i = 0; i < c5.length; i++)
			{
				if (r.nextDouble() > 0.5)
				{
					c5[i] = (char)(32+r.nextInt(64));
				}
				else
				{
					c5[i] = (char)r.nextInt(65536);
				}
			}
			char [] c6 = new char[256];
			for (int i = 0; i < c6.length; i++)
			{
				if (r.nextDouble() > 0.9)
				{
					c6[i] = (char)(32+r.nextInt(64));
				}
				else
				{
					c6[i] = (char)r.nextInt(65536);
				}
			}
			String s4 = "Embeddings of graphs have been of interest to theoreticians for some time, in particular those of planar graphs and graphs that are close to being planar.  One definition of a planar graph is one that can be drawn in the plane with no edge crossings.";
			String [][] testStrings = {
				{s4, "Natural text"},
				{new String(c1), "Random 8 bit chars"},
				{new String(c2), "Random 6 bit chars"},
				{new String(c3), "Random 16 bit chars"},
				{new String(c4), "90% Random 6 bit chars + 10% Random 16 bit chars"},
				{new String(c5), "50% Random 6 bit chars + 50% Random 16 bit chars"},
				{new String(c6), "10% Random 6 bit chars + 90% Random 16 bit chars"}
			};

			for (int stringIndex = 0; stringIndex < testStrings.length; stringIndex++)
			{
				ByteBuffer bb = allocate(1024*1024);
				String s = testStrings[stringIndex][0];

				bb.position(0).putUTF(s);
				if (!bb.position(0).getUTF().equals(s))
				{
					throw new RuntimeException();
				}

				bb.position(0).putVarChar(s);
				if (!bb.position(0).getVarChar().equals(s))
				{
					throw new RuntimeException();
				}
			}


			ByteBuffer bb1 = allocate(1024*1024*10);
			ByteBuffer bb2 = allocate(1024*1024*10);
			ByteBuffer bb3 = allocate(1024*1024*10);
			for (int test = 0; test < 1; test++)
			{
				for (int stringIndex = 0; stringIndex < testStrings.length; stringIndex++)
				{
					System.out.println(stringIndex+" - "+testStrings[stringIndex][1]);
					String data = testStrings[stringIndex][0];
					System.out.println("  Encoding");
					{
						long t = System.nanoTime();
						for (int j = 0; j < 100; j++)
						{
							bb1.position(0);
							for (int i = 0; i < 10000; i++)
							{
								bb1.putVarChar(data);
							}
						}
						System.out.printf("    %4d %4d %s\n", bb1.position()/10000, (System.nanoTime()-t)/1000000, "VarChar");
					}
					{
						long t = System.nanoTime();
						for (int j = 0; j < 100; j++)
						{
							bb2.position(0);
							for (int i = 0; i < 10000; i++)
							{
								bb2.putUTF(data);
							}
						}
						System.out.printf("    %4d %4d %s\n", bb2.position()/10000, (System.nanoTime()-t)/1000000, "UTF8");
					}
					{
						long t = System.nanoTime();
						for (int j = 0; j < 100; j++)
						{
							bb3.position(0);
							for (int i = 0; i < 10000; i++)
							{
								bb3.putChar(data.length());
								bb3.putChars(data);
							}
						}
						System.out.printf("    %4d %4d %s\n", bb3.position()/10000, (System.nanoTime()-t)/1000000, "Char");
					}
					System.out.println("  Decoding");
					{
						long t = System.nanoTime();
						for (int j = 0; j < 100; j++)
						{
							bb1.position(0);
							for (int i = 0; i < 10000; i++)
							{
								bb1.getVarChar();
							}
						}
						System.out.printf("    %4d %4d %s\n", bb1.position()/10000, (System.nanoTime()-t)/1000000, "VarChar");
					}
					{
						long t = System.nanoTime();
						for (int j = 0; j < 100; j++)
						{
							bb2.position(0);
							for (int i = 0; i < 10000; i++)
							{
								bb2.getUTF();
							}
						}
						System.out.printf("    %4d %4d %s\n", bb2.position()/10000, (System.nanoTime()-t)/1000000, "UTF8");
					}
					{
						long t = System.nanoTime();
						for (int j = 0; j < 100; j++)
						{
							bb3.position(0);
							for (int i = 0; i < 10000; i++)
							{
								bb3.getChars(bb3.getChar());
							}
						}
						System.out.printf("    %4d %4d %s\n", bb3.position()/10000, (System.nanoTime()-t)/1000000, "Char");
					}
				}
				System.out.println("");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}