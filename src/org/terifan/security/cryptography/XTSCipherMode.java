package org.terifan.security.cryptography;


/**
 * This is an implementation of the XTS cipher mode with a modified IV initialization.
 * XTS source code is ported from TrueCrypt 7.0.
 */
public final class XTSCipherMode extends CipherMode
{
	private final static int BYTES_PER_BLOCK = 16;


	public XTSCipherMode()
	{
	}


	@Override
	public void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey)
	{
		assert (aUnitSize & -aUnitSize) == aUnitSize;
		assert (aLength & (BYTES_PER_BLOCK - 1)) == 0;
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;

		byte[] whiteningValue = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, aTweak, aBlockKey, whiteningValue, BYTES_PER_BLOCK);

			for (int block = 0; block < numBlocks; block++, offset += BYTES_PER_BLOCK)
			{
				xor(aBuffer, offset, BYTES_PER_BLOCK, whiteningValue, 0);

				aCipher.engineEncryptBlock(aBuffer, offset, aBuffer, offset);

				xor(aBuffer, offset, BYTES_PER_BLOCK, whiteningValue, 0);

				int finalCarry = ((whiteningValue[8 + 7] & 0x80) != 0) ? 135 : 0;

				putLong(whiteningValue, 8, getLong(whiteningValue, 8) << 1);

				if ((whiteningValue[7] & 0x80) != 0)
				{
					whiteningValue[8] |= 0x01;
				}

				putLong(whiteningValue, 0, getLong(whiteningValue, 0) << 1);

				whiteningValue[0] ^= finalCarry;
			}
		}
	}


	@Override
	public void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey)
	{
		assert (aUnitSize & -aUnitSize) == aUnitSize;
		assert (aLength & (BYTES_PER_BLOCK - 1)) == 0;
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;

		byte[] whiteningValue = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, aTweak, aBlockKey, whiteningValue, BYTES_PER_BLOCK);

			for (int block = 0; block < numBlocks; block++, offset += BYTES_PER_BLOCK)
			{
				xor(aBuffer, offset, BYTES_PER_BLOCK, whiteningValue, 0);

				aCipher.engineDecryptBlock(aBuffer, offset, aBuffer, offset);

				xor(aBuffer, offset, BYTES_PER_BLOCK, whiteningValue, 0);

				int finalCarry = (whiteningValue[8 + 7] & 0x80) != 0 ? 135 : 0;

				putLong(whiteningValue, 8, getLong(whiteningValue, 8) << 1);

				if ((whiteningValue[7] & 0x80) != 0)
				{
					whiteningValue[8] |= 0x01;
				}

				putLong(whiteningValue, 0, getLong(whiteningValue, 0) << 1);

				whiteningValue[0] ^= finalCarry;
			}
		}
	}


	// little endian
	private static void putLong(byte[] aBuffer, int aOffset, long aValue)
	{
		aBuffer[aOffset + 0] = (byte)(aValue >>> 0);
		aBuffer[aOffset + 1] = (byte)(aValue >>> 8);
		aBuffer[aOffset + 2] = (byte)(aValue >>> 16);
		aBuffer[aOffset + 3] = (byte)(aValue >>> 24);
		aBuffer[aOffset + 4] = (byte)(aValue >>> 32);
		aBuffer[aOffset + 5] = (byte)(aValue >>> 40);
		aBuffer[aOffset + 6] = (byte)(aValue >>> 48);
		aBuffer[aOffset + 7] = (byte)(aValue >>> 56);
	}


	// little endian
	private static long getLong(byte[] aBuffer, int aOffset)
	{
		return ((255 & aBuffer[aOffset]))
			+ ((255 & aBuffer[aOffset + 1]) << 8)
			+ ((255 & aBuffer[aOffset + 2]) << 16)
			+ ((long)(255 & aBuffer[aOffset + 3]) << 24)
			+ ((long)(255 & aBuffer[aOffset + 4]) << 32)
			+ ((long)(255 & aBuffer[aOffset + 5]) << 40)
			+ ((long)(255 & aBuffer[aOffset + 6]) << 48)
			+ ((long)(255 & aBuffer[aOffset + 7]) << 56);
	}
}
