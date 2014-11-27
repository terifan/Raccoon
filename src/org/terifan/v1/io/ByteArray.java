package org.terifan.v1.io;


/**
 * ByteArray wrapps around a primitive byte array and allows for simple
 * reading and writing of Java primitives such as ints and longs. This
 * implementation is more lightweight then the ByteBuffer class. ALl methods
 * operate on a user specified byte array. No internal state/pointer is keept.<p>
 *
 * A LittleEndian and a BigEndian implementation exists. These are accessed
 * through the public static members LE and BE.<p>
 *
 * Sample 1:<p>
 * <pre>
 *    byte [] arr = new byte[100];
 *    ByteArray.LE.putLong(arr, 0, 123456789L);
 *    ByteArray.LE.putInt(arr, 8, 65464);
 *    ByteArray.LE.putShort(arr, 12, 48);
 * </pre><p>
 *
 * Sample 2:<p>
 * <pre>
 *    byte [] arr = new byte[100];
 *    ByteArray ba = ByteArray.LE;
 *    ba.putLong(arr, 0, 123456789L);
 *    ba.putInt(arr, 8, 65464);
 *    ba.putShort(arr, 12, 48);
 * </pre><p>
 *
 * Sample 3:<p>
 * <pre>
 *    byte [] arr = new byte[100];
 *    ByteArray.LE.putLong(arr, 0, 123456789L)
 *                .putInt(arr, 8, 65464)
 *                .putShort(arr, 12, 48);
 * </pre><p>
 *
 * Sample 4 - static methods:<p>
 * <pre>
 *    System.out.println(ByteArray.toString(ByteArray.decode("a1 b2 c3")));
 *    // prints "A1B2C3"
 * </pre>
 *
 * TODO:
 * <ul>
 * <li>int indexOf(byte [], byte [], int offset)
 * <li>int lastIndexOf(byte [], byte [], int offset)
 * <li>int putUTF(byte [], int offset, String s)
 * <li>String getUTF(byte [], int offset)
 * </ul>
 */
public abstract class ByteArray
{
	/** Performs self-test. */
	public static void main(String ... args)
	{
		System.out.println(new String(ByteArray.xor("a".getBytes(), "b".getBytes(), "a".getBytes())));
		System.out.println(new String(ByteArray.xor("aa".getBytes(), "ba".getBytes(), "aa".getBytes())));
		System.out.println(new String(ByteArray.xor("aa".getBytes(), "ba".getBytes(), "b".getBytes())));

//		try
//		{
//			java.util.Random rnd = new java.util.Random();
//			byte [] buffer = new byte[49];
//			String err = "";
//long t = System.nanoTime();
//			for (int j = 0; j < 2; j++)
//			{
//				ByteArray ba = j == 0 ? ByteArray.LE : ByteArray.BE;
//
//				for (int i = 0; i < 1000000; i++)
//				{
//					long _long = rnd.nextLong();
//					int _int = rnd.nextInt();
//					short _short = (short)rnd.nextInt();
//					char _char = (char)rnd.nextInt();
//					float _float = rnd.nextFloat();
//					double _double = rnd.nextDouble();
//					byte _byte = (byte)rnd.nextInt();
//					long _number = rnd.nextLong()&0x00FFFFFFFFFFFFFFL;
//
//					ba.putLong(buffer, 4, _long)
//					  .putInt(buffer, 0, _int)
//					  .putShort(buffer, 12, _short)
//					  .putChar(buffer, 15, _char)
//					  .put(buffer, 14, _byte)
//					  .putFloat(buffer, 25, _float)
//					  .putDouble(buffer, 17, _double)
////					  .put(buffer, 29, "hej".getBytes())
//					  .putNumber(buffer, 32, _number, 7);
//
//					if (ba.getLong(buffer, 4) != _long) err+="Error: long\n";
//					if (ba.getInt(buffer, 0) != _int) err+="Error: int\n";
//					if (ba.getShort(buffer, 12) != _short) err+="Error: short\n";
//					if (ba.getChar(buffer, 15) != _char) err+="Error: char\n";
//					if (ba.get(buffer, 14) != _byte) err+="Error: byte\n";
//					if (ba.getFloat(buffer, 25) != _float) err+="Error: float\n";
//					if (ba.getDouble(buffer, 17) != _double) err+="Error: double\n";
//					if (ba.getNumber(buffer, 32, 7) != _number) err+="Error: number\n";
//
//					if (err.length() > 0)
//					{
//						System.out.println(ba.getClass());
//						System.out.println(err);
//
//						System.out.println(ByteArray.toString(buffer));
//
//						System.out.println(_long +"\t\t"+ ba.getLong(buffer, 4));
//						System.out.println(_int +"\t\t"+ ba.getInt(buffer, 0));
//						System.out.println(_short +"\t\t"+ ba.getShort(buffer, 12));
//						System.out.println(_char +"\t\t"+ ba.getChar(buffer, 15));
//						System.out.println(_byte +"\t\t"+ ba.get(buffer, 14));
//						System.out.println(_float +"\t\t"+ ba.getFloat(buffer, 25));
//						System.out.println(_double +"\t\t"+ ba.getDouble(buffer, 17));
//						System.out.println(new String(ba.get(buffer, 29, new byte[3])));
//						System.out.println(_number +"\t\t"+ ba.getNumber(buffer, 32, 7));
//
//						throw new RuntimeException();
//					}
//				}
//			}
//System.out.println((System.nanoTime()-t)/1000000);
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
	}


	/**
	 * LittleEndian implementation of the ByteArray class.
	 */
	public final static LittleEndian LE = new LittleEndian();

	/**
	 * BigEndian implementation of the ByteArray class (default Java-endian).
	 */
	public final static BigEndian BE = new BigEndian();


	public static int indexOf(byte[] aBuffer, byte[] aPattern)
	{
		for (int i = 0; i < aBuffer.length - aPattern.length; i++)
		{
			int len = 0;
			for (int j = 0; j < aPattern.length; j++)
			{
				if (aBuffer[i+j] != aPattern[j])
				{
					break;
				}
				len++;
			}
			if (len == aPattern.length)
			{
				return i;
			}
		}

		return -1;
	}


	ByteArray()
	{
	}


	/**
	 * This method gets a byte from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the byte value is returned.
	 */
	public byte get(byte [] aBuffer, int aPosition)
	{
		return aBuffer[aPosition];
	}


	/**
	 * This method gets an unsigned byte from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the byte value is returned as an int.
	 */
	public int getUnsignedByte(byte [] aBuffer, int aPosition)
	{
		return aBuffer[aPosition] & 255;
	}


	/**
	 * This method copies bytes from the source buffer into the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aDestBuffer
	 *    The destination buffer.
	 * @param aDestPosition
	 *    The destination position.
	 * @param aLength
	 *    The number of bytes to copy.
	 * @return
	 *    the byte value is returned.
	 */
	public byte [] get(byte [] aBuffer, int aPosition, byte [] aDestBuffer, int aDestPosition, int aLength)
	{
		System.arraycopy(aBuffer, aPosition, aDestBuffer, aDestPosition, aLength);
		return aDestBuffer;
	}


	/**
	 * This method copies bytes from the source buffer into the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aDestBuffer
	 *    The destination buffer.
	 * @return
	 *    aDestBuffer is returned
	 */
	public byte [] get(byte [] aBuffer, int aPosition, byte [] aDestBuffer)
	{
		System.arraycopy(aBuffer, aPosition, aDestBuffer, 0, aDestBuffer.length);
		return aDestBuffer;
	}


	/**
	 * This method gets a signed short from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the short value is returned.
	 */
	public abstract short getShort(byte [] aBuffer, int aPosition);


	/**
	 * This method gets an unsigned short from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the short value is returned as an int.
	 */
	public int getUnsignedShort(byte [] aBuffer, int aPosition)
	{
		return getShort(aBuffer, aPosition) & 65535;
	}


	/**
	 * This method gets a char from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the char value is returned.
	 */
	public char getChar(byte [] aBuffer, int aPosition)
	{
		return (char)getShort(aBuffer, aPosition);
	}


	/**
	 * This method gets an array of chars from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aChars
	 *   a destination char array
	 * @return
	 *    this instance
	 */
	public ByteArray getChars(byte [] aBuffer, int aPosition, char [] aChars)
	{
		return getChars(aBuffer, aPosition, aChars, 0, aChars.length);
	}


	/**
	 * This method gets an array of chars from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aChars
	 *   a destination char array
	 * @param aOffset
	 *   offset in the chars array where to store the read chars
	 * @param aLength
	 *   number of chars to get
	 * @return
	 *    this instance
	 */
	public ByteArray getChars(byte [] aBuffer, int aPosition, char [] aChars, int aOffset, int aLength)
	{
		for (int i = 0; i < aLength; i++)
		{
			aChars[aOffset + i] = (char)getShort(aBuffer, aPosition++);
		}
		return this;
	}


	/**
	 * This method gets an int from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the int value is returned.
	 */
	public abstract int getInt(byte [] aBuffer, int aPosition);


	/**
	 * This method copies ints from the byte buffer into the int buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aSourceOffset
	 *    The source position.
	 * @param aDestBuffer
	 *    The destination buffer.
	 * @param aDestPosition
	 *    The position in the destination buffer were to put the ints.
	 * @param aNumInts
	 *    Number of ints to copy.
	 * @return
	 *    the destination int buffer.
	 */
	public abstract int [] getInts(byte [] aSourceBuffer, int aSourceOffset, int [] aDestBuffer, int aDestPosition, int aNumInts);


	/**
	 * This method gets an unsigned int from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the byte value is returned as a long.
	 */
	public long getUnsignedInt(byte [] aBuffer, int aPosition)
	{
		return getInt(aBuffer, aPosition) & 0xFFFFFFFFL;
	}


	/**
	 * This method gets a long from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the long value is returned.
	 */
	public abstract long getLong(byte [] aBuffer, int aPosition);


	/**
	 * This method gets a float from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the float value is returned.
	 */
	public float getFloat(byte [] aBuffer, int aPosition)
	{
		return Float.intBitsToFloat(getInt(aBuffer, aPosition));
	}


	/**
	 * This method gets a double from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @return
	 *    the double value is returned.
	 */
	public double getDouble(byte [] aBuffer, int aPosition)
	{
		return Double.longBitsToDouble(getLong(aBuffer, aPosition));
	}


	/**
	 * This method gets a variable length long from the source buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aLength
	 *    The number of bytes to read.
	 * @return
	 *    the double value is returned.
	 */
	public abstract long getNumber(byte [] aBuffer, int aPosition, int aLength);


	/**
	 * This method puts a byte in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public ByteArray put(byte [] aBuffer, int aPosition, byte aValue)
	{
		aBuffer[aPosition] = aValue;
		return this;
	}


	/**
	 * This method puts a byte in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public ByteArray put(byte [] aBuffer, int aPosition, int aValue)
	{
		aBuffer[aPosition] = (byte)aValue;
		return this;
	}


	/**
	 * This method copies bytes from the source buffer into the destination buffer.
	 *
	 * @param aBuffer
	 *    The destination buffer.
	 * @param aPosition
	 *    The destination position.
	 * @param aSrcBuffer
	 *    The source buffer.
	 * @param aSrcPosition
	 *    The source position.
	 * @param aSrcLength
	 *    The number of bytes to copy.
	 * @return
	 *    this is returned.
	 */
	public ByteArray put(byte [] aBuffer, int aPosition, byte [] aSrcBuffer, int aSrcPosition, int aSrcLength)
	{
		if (aSrcPosition < 0 || aSrcLength < 0 || aSrcPosition + aSrcLength > aSrcBuffer.length)
		{
			throw new IllegalArgumentException("Reading beyond end of source buffer: " + aSrcPosition+" + "+aSrcLength+" > "+aSrcBuffer.length);
		}
		if (aPosition < 0 || aPosition + aSrcLength > aBuffer.length)
		{
			throw new IllegalArgumentException("Writing beyond end of destination buffer.");
		}

		System.arraycopy(aSrcBuffer, aSrcPosition, aBuffer, aPosition, aSrcLength);
		return this;
	}


	/**
	 * This method copies bytes from the source buffer into the destination buffer.
	 *
	 * @param aBuffer
	 *    The destination buffer.
	 * @param aPosition
	 *    The destination position.
	 * @param aSrcBuffer
	 *    The source buffer.
	 * @return
	 *    this is returned.
	 */
	public ByteArray put(byte [] aBuffer, int aPosition, byte [] aSrcBuffer)
	{
		System.arraycopy(aSrcBuffer, 0, aBuffer, aPosition, aSrcBuffer.length);
		return this;
	}


	/**
	 * This method puts a short in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public abstract ByteArray putShort(byte [] aBuffer, int aPosition, int aValue);


	/**
	 * This method puts a char in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public ByteArray putChar(byte [] aBuffer, int aPosition, int aValue)
	{
		putShort(aBuffer, aPosition, aValue);
		return this;
	}


	/**
	 * This method puts a int in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public abstract ByteArray putInt(byte [] aBuffer, int aPosition, int aValue);


	/**
	 * This method puts an int array in the destination buffer.
	 *
	 * @param aSourceBuffer
	 *    The source buffer.
	 * @param aStartOffset
	 *    The source position.
	 * @param aDestBuffer
	 *    The destination buffer.
	 * @param aDestOffset
	 *    The destination position.
	 * @param aNumInts
	 *    Number of ints to put.
	 * @return
	 *    this is returned.
	 */
	public abstract ByteArray putInts(int [] aSourceBuffer, int aStartOffset, byte [] aDestBuffer, int aDestOffset, int aNumInts);


	/**
	 * This method puts a long in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public abstract ByteArray putLong(byte [] aBuffer, int aPosition, long aValue);


	/**
	 * This method puts a float in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public ByteArray putFloat(byte [] aBuffer, int aPosition, float aValue)
	{
		putInt(aBuffer, aPosition, Float.floatToRawIntBits(aValue));
		return this;
	}


	/**
	 * This method puts a float in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public ByteArray putFloat(byte [] aBuffer, int aPosition, double aValue)
	{
		putInt(aBuffer, aPosition, Float.floatToRawIntBits((float)aValue));
		return this;
	}


	/**
	 * This method puts a double in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @return
	 *    this is returned.
	 */
	public ByteArray putDouble(byte [] aBuffer, int aPosition, double aValue)
	{
		putLong(aBuffer, aPosition, Double.doubleToRawLongBits(aValue));
		return this;
	}


	/**
	 * This method puts a variable length long in the destination buffer.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aPosition
	 *    The source position.
	 * @param aValue
	 *    The value to put.
	 * @param aLength
	 *    The number of bytes to put.
	 * @return
	 *    this is returned.
	 */
	public abstract ByteArray putNumber(byte [] aBuffer, int aPosition, long aValue, int aLength);


	/**
	 * Returns the hexadecimal String of the contents of the array.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @return
	 *    A hexadecimal String representation of the array.
	 */
	public static String toString(byte [] aBuffer)
	{
		StringBuilder s = new StringBuilder();
		for (byte b : aBuffer)
		{
			s.append((b&255)<16?"0":"").append(Integer.toString(b&255,16));
		}
		return s.toString().toUpperCase();
	}


	/**
	 * Returns the hexadecimal String of the contents of the array.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aOffset
	 *    The start offset.
	 * @param aLength
	 *    The number of bytes to convert.
	 * @return
	 *    A hexadecimal String representation of the array.
	 */
	public static String toString(byte [] aBuffer, int aOffset, int aLength)
	{
		StringBuilder s = new StringBuilder();
		while (aLength-- > 0)
		{
			byte b = aBuffer[aOffset++];
			s.append((b&255)<16?"0":"").append(Integer.toString(b&255,16));
		}
		return s.toString().toUpperCase();
	}


	/**
	 * Decodes the hexadecimal string provided and returns it as a byte array.
	 * The string may contains space characters.
	 *
	 * @param aHexString
	 *    A hexadecimal String with two characters per byte.
	 * @return
	 *    A byte array.
	 */
	public static byte [] decode(String aHexString)
	{
		if (aHexString.length() % 2 != 0)
		{
			throw new IllegalArgumentException("aHexString must have an even length.");
		}

		if (aHexString.indexOf(" ") != -1)
		{
			aHexString = aHexString.replace(" ", "");
		}

		byte [] buf = new byte[aHexString.length() / 2];
		for (int i = 0; i < aHexString.length(); i+=2)
		{
			buf[i/2] = (byte)Integer.parseInt(aHexString.substring(i,i+2), 16);
		}

		return buf;
	}


	/**
	 * Resizes the array specified. This method will copy data from the old
	 * array to a new array which is then returned.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aLength
	 *    New length of the array.
	 * @return
	 *    The new array.
	 */
	public static byte [] resize(byte [] aBuffer, int aLength)
	{
		byte [] temp = new byte[aLength];
		System.arraycopy(aBuffer, 0, temp, 0, Math.min(aBuffer.length, aLength));
		return temp;
	}


	/**
	 * Fills the specified array with a byte value.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aValue
	 *    Value used to fill the array.
	 * @return
	 *    The source buffer.
	 */
	public static byte [] fill(byte [] aBuffer, byte aValue)
	{
		for (int i = aBuffer.length; --i >= 0;) aBuffer[i] = aValue;
		return aBuffer;
	}


	/**
	 * Fills the specified array with a byte value.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aValue
	 *    Value used to fill the array.
	 * @return
	 *    The source buffer.
	 */
	public static byte [] fill(byte [] aBuffer, int aValue)
	{
		byte b = (byte)aValue;
		for (int i = aBuffer.length; --i >= 0;) aBuffer[i] = b;
		return aBuffer;
	}


	/**
	 * Fills the specified array with a byte value.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aOffset
	 *    The source buffer offset.
	 * @param aLength
	 *    The source buffer length.
	 * @param aValue
	 *    Value used to fill the array.
	 * @return
	 *    The source buffer.
	 */
	public static byte [] fill(byte [] aBuffer, int aOffset, int aLength, byte aValue)
	{
		while (aLength-- > 0) aBuffer[aOffset++] = aValue;
		return aBuffer;
	}


	/**
	 * Fills the specified array with a byte value.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aOffset
	 *    The source buffer offset.
	 * @param aLength
	 *    The source buffer length.
	 * @param aValue
	 *    Value used to fill the array.
	 * @return
	 *    The source buffer.
	 */
	public static byte [] fill(byte [] aBuffer, int aOffset, int aLength, int aValue)
	{
		byte b = (byte)aValue;
		while (aLength-- > 0) aBuffer[aOffset++] = b;
		return aBuffer;
	}


	/**
	 * Copies the specified range of the specified array into a new array.
	 *
	 * @param aBuffer
	 *    The source buffer.
	 * @param aOffset
	 *    Start offset to copy from.
	 * @param aLength
	 *    Number of bytes to copy.
	 * @return
	 *    The new buffer.
	 */
	public static byte [] copy(byte [] aBuffer, int aOffset, int aLength)
	{
		byte [] b = new byte[aLength];
		System.arraycopy(aBuffer, aOffset, b, 0, aLength);
		return b;
	}


	/**
	 * Reverse the order of all bytes.
	 *
	 * @param aBuffer
	 *   A byte buffer.
	 * @return
	 *   The input byte buffer.
	 */
	public static byte [] reverse(byte [] aBuffer)
	{
		return reverse(aBuffer, 0, aBuffer.length);
	}


	/**
	 * Reverse the order of some bytes.
	 *
	 * @param aBuffer
	 *   A byte buffer.
	 * @param aOffset
	 *    Start offset to reverse from.
	 * @param aLength
	 *    Number of bytes to reverse.
	 * @return
	 *   The input byte buffer.
	 */
	public static byte [] reverse(byte [] aBuffer, int aOffset, int aLength)
	{
		for (int i = aOffset, j = aOffset+aLength-1; i < j; i++, j--)
		{
			byte t = aBuffer[i];
			aBuffer[i] = aBuffer[j];
			aBuffer[j] = t;
		}

		return aBuffer;
	}


	public static boolean equals(byte[] aArrayA, byte[] aArrayB)
	{
		if (aArrayA.length != aArrayB.length)
		{
			return false;
		}
		for (int i = 0; i < aArrayA.length; i++)
		{
			if (aArrayA[i] != aArrayB[i])
			{
				return false;
			}
		}
		return true;
	}


	public static boolean equals(byte[] aArrayA, int aOffsetA, byte[] aArrayB, int aOffsetB, int aLength)
	{
		if (aArrayA.length < aOffsetA + aLength)
		{
			return false;
		}
		if (aArrayB.length < aOffsetB + aLength)
		{
			return false;
		}
		for (int i = 0; i < aLength; i++)
		{
			if (aArrayA[aOffsetA++] != aArrayB[aOffsetB++])
			{
				return false;
			}
		}
		return true;
	}


	/**
	 * Return a single bit from the byte array.
	 *
	 * @param aBuffer
	 *   the byte buffer
	 * @param aIndex
	 *   the bit index in the byte buffer. The index may address any byte in the
	 *   byte array; index 8 will be the first (lowest) bit in the second byte.
	 * @return
	 *   true if the bit has value 1
	 */
	public static boolean getBit(byte [] aBuffer, int aIndex)
	{
		return (aBuffer[aIndex >> 3] & (1 << (aIndex & 7))) != 0;
	}


	public static byte [] join(byte[] ... aBuffers)
	{
		int length = 0;
		for (int i = 0; i < aBuffers.length; i++)
		{
			if (aBuffers[i] != null)
			{
				length += aBuffers[i].length;
			}
		}
		byte [] temp = new byte[length];
		for (int i = 0, offset = 0; i < aBuffers.length; i++)
		{
			if (aBuffers[i] != null)
			{
				System.arraycopy(aBuffers[i], 0, temp, offset, aBuffers[i].length);
				offset += aBuffers[i].length;
			}
		}
		return temp;
	}


	/**
	 * XORs the first buffer with all remaining buffers in the provided array.
	 *
	 * @param aBuffers
	 *   two or more arrays of bytes
	 * @return
	 *   an array of same length as the first provided array
	 */
	public static byte [] xor(byte [] ... aBuffers)
	{
		byte [] output = aBuffers[0].clone();

		for (int i = 1; i < aBuffers.length; i++)
		{
			byte [] buffer = aBuffers[i];
			for (int j = 0, sz = Math.min(buffer.length, output.length); j < sz; j++)
			{
				output[j] ^= buffer[j];
			}
		}

		return output;
	}


	/** This class should not be instanced. Use the final static member ByteArray.LE */
	public static class LittleEndian extends ByteArray
	{
		protected LittleEndian()
		{
		}

		@Override
		public short getShort(byte [] aBuffer, int aPosition)
		{
			return (short)(((aBuffer[aPosition++] & 255)      )
			             + ((aBuffer[aPosition  ] & 255) <<  8));
		}

		@Override
		public int getInt(byte [] aBuffer, int aPosition)
		{
			return (int)(((aBuffer[aPosition++] & 255)      )
			           + ((aBuffer[aPosition++] & 255) <<  8)
			           + ((aBuffer[aPosition++] & 255) << 16)
			           + ((aBuffer[aPosition  ] & 255) << 24));
		}

		@Override
		public int [] getInts(byte [] aBuffer, int aPosition, int [] aDestBuffer, int aDestPosition, int aNumInts)
		{
			for (int i = 0; i < aNumInts; i++)
			{
				aDestBuffer[aDestPosition+i] = (int)(((aBuffer[aPosition++] & 255)      )
				                             +       ((aBuffer[aPosition++] & 255) <<  8)
				                             +       ((aBuffer[aPosition++] & 255) << 16)
				                             +       ((aBuffer[aPosition++] & 255) << 24));
			}

			return aDestBuffer;
		}

		@Override
		public long getLong(byte [] aBuffer, int aPosition)
		{
			return (((long)(aBuffer[aPosition + 7]      ) << 56)
			      + ((long)(aBuffer[aPosition + 6] & 255) << 48)
			      + ((long)(aBuffer[aPosition + 5] & 255) << 40)
			      + ((long)(aBuffer[aPosition + 4] & 255) << 32)
			      + ((long)(aBuffer[aPosition + 3] & 255) << 24)
			      + ((      aBuffer[aPosition + 2] & 255) << 16)
			      + ((      aBuffer[aPosition + 1] & 255) <<  8)
			      + ((      aBuffer[aPosition    ] & 255       )));
		}

		@Override
		public long getNumber(byte [] aBuffer, int aPosition, int aLength)
		{
			long v = 0;
			int o = 0;

			while (aLength-- > 0)
			{
				v |= (long)(aBuffer[aPosition++] & 255) << o;
				o += 8;
			}

			return v;
		}

		@Override
		public ByteArray putShort(byte [] aBuffer, int aPosition, int aValue)
		{
			aBuffer[aPosition++] = (byte)(aValue       );
			aBuffer[aPosition  ] = (byte)(aValue >>   8);
			return this;
		}

		@Override
		public ByteArray putInt(byte [] aBuffer, int aPosition, int aValue)
		{
			aBuffer[aPosition++] = (byte)(aValue       );
			aBuffer[aPosition++] = (byte)(aValue >>   8);
			aBuffer[aPosition++] = (byte)(aValue >>  16);
			aBuffer[aPosition  ] = (byte)(aValue >>> 24);
			return this;
		}

		@Override
		public ByteArray putInts(int [] aSourceBuffer, int aSourceOffset, byte [] aDestBuffer, int aDestOffset, int aNumInts)
		{
			for (int i = 0; i < aNumInts; i++)
			{
				int v = aSourceBuffer[aSourceOffset++];
				aDestBuffer[aDestOffset++] = (byte)(v       );
				aDestBuffer[aDestOffset++] = (byte)(v >>   8);
				aDestBuffer[aDestOffset++] = (byte)(v >>  16);
				aDestBuffer[aDestOffset++] = (byte)(v >>> 24);
			}
			return this;
		}

		@Override
		public ByteArray putLong(byte [] aBuffer, int aPosition, long aValue)
		{
			aBuffer[aPosition++] = (byte)(aValue       );
			aBuffer[aPosition++] = (byte)(aValue >>   8);
			aBuffer[aPosition++] = (byte)(aValue >>  16);
			aBuffer[aPosition++] = (byte)(aValue >>  24);
			aBuffer[aPosition++] = (byte)(aValue >>  32);
			aBuffer[aPosition++] = (byte)(aValue >>  40);
			aBuffer[aPosition++] = (byte)(aValue >>  48);
			aBuffer[aPosition  ] = (byte)(aValue >>> 56);
			return this;
		}

		@Override
		public ByteArray putNumber(byte [] aBuffer, int aPosition, long aValue, int aLength)
		{
			for (int o = 0; aLength-- > 0; o += 8)
			{
				aBuffer[aPosition++] = (byte)(aValue >>> o);
			}
			return this;
		}
	}




	/** This class should not be instanced. Use the final static member ByteArray.BE */
	public static class BigEndian extends ByteArray
	{
		protected BigEndian()
		{
		}

		@Override
		public short getShort(byte [] aBuffer, int aPosition)
		{
			return (short)(((aBuffer[aPosition++]      ) <<  8)
			             + ((aBuffer[aPosition  ] & 255)      ));
		}

		@Override
		public int getInt(byte [] aBuffer, int aPosition)
		{
			return (int)(((aBuffer[aPosition++] & 255) << 24)
			           + ((aBuffer[aPosition++] & 255) << 16)
			           + ((aBuffer[aPosition++] & 255) <<  8)
			           + ((aBuffer[aPosition  ] & 255)      ));
		}

		@Override
		public int [] getInts(byte [] aBuffer, int aPosition, int [] aDestBuffer, int aDestPosition, int aNumInts)
		{
			for (int i = 0; i < aNumInts; i++)
			{
				aDestBuffer[aDestPosition+i] = (int)(((aBuffer[aPosition++] & 255) << 24)
				                             +       ((aBuffer[aPosition++] & 255) << 16)
				                             +       ((aBuffer[aPosition++] & 255) <<  8)
				                             +       ((aBuffer[aPosition++] & 255)      ));
			}

			return aDestBuffer;
		}

		@Override
		public long getLong(byte [] aBuffer, int aPosition)
		{
			return (((long)(aBuffer[aPosition++]      ) << 56)
			      + ((long)(aBuffer[aPosition++] & 255) << 48)
			      + ((long)(aBuffer[aPosition++] & 255) << 40)
			      + ((long)(aBuffer[aPosition++] & 255) << 32)
			      + ((long)(aBuffer[aPosition++] & 255) << 24)
			      + (      (aBuffer[aPosition++] & 255) << 16)
			      + (      (aBuffer[aPosition++] & 255) <<  8)
			      + (      (aBuffer[aPosition  ] & 255)      ));
		}

		@Override
		public long getNumber(byte [] aBuffer, int aPosition, int aLength)
		{
			long v = 0;

			while (aLength-- > 0)
			{
				v <<= 8;
				v |= (aBuffer[aPosition++] & 255);
			}

			return v;
		}

		@Override
		public ByteArray putShort(byte [] aBuffer, int aPosition, int aValue)
		{
			aBuffer[aPosition++] = (byte)(aValue >>   8);
			aBuffer[aPosition  ] = (byte)(aValue       );
			return this;
		}

		@Override
		public ByteArray putInt(byte [] aBuffer, int aPosition, int aValue)
		{
			aBuffer[aPosition++] = (byte)(aValue >>> 24);
			aBuffer[aPosition++] = (byte)(aValue >>  16);
			aBuffer[aPosition++] = (byte)(aValue >>   8);
			aBuffer[aPosition  ] = (byte)(aValue       );
			return this;
		}

		@Override
		public ByteArray putInts(int [] aSourceBuffer, int aSourceOffset, byte [] aDestBuffer, int aDestOffset, int aNumInts)
		{
			for (int i = 0; i < aNumInts; i++)
			{
				int v = aSourceBuffer[aSourceOffset++];
				aDestBuffer[aDestOffset++] = (byte)(v >>> 24);
				aDestBuffer[aDestOffset++] = (byte)(v >>  16);
				aDestBuffer[aDestOffset++] = (byte)(v >>   8);
				aDestBuffer[aDestOffset++] = (byte)(v       );
			}
			return this;
		}

		@Override
		public ByteArray putLong(byte [] aBuffer, int aPosition, long aValue)
		{
			aBuffer[aPosition++] = (byte)(aValue >>> 56);
			aBuffer[aPosition++] = (byte)(aValue >>  48);
			aBuffer[aPosition++] = (byte)(aValue >>  40);
			aBuffer[aPosition++] = (byte)(aValue >>  32);
			aBuffer[aPosition++] = (byte)(aValue >>  24);
			aBuffer[aPosition++] = (byte)(aValue >>  16);
			aBuffer[aPosition++] = (byte)(aValue >>   8);
			aBuffer[aPosition  ] = (byte)(aValue       );
			return this;
		}

		@Override
		public ByteArray putNumber(byte [] aBuffer, int aPosition, long aValue, int aLength)
		{
			for (int o = 8 * (aLength - 1); --aLength >= 0; o -= 8)
			{
				aBuffer[aPosition++] = (byte)(aValue >>> o);
			}
			return this;
		}
	}
}