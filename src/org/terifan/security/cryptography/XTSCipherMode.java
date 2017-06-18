package org.terifan.security.cryptography;


/**
 * This is an implementation of the XTS cipher mode with a modified IV initialization.
 * XTS source code is ported from TrueCrypt 7.0.
 */
public class XTSCipherMode implements CipherMode
{
	private final static int BYTES_PER_BLOCK = 16;


	public XTSCipherMode()
	{
	}


	@Override
	public void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey)
	{
		assert aUnitSize == 256 || aUnitSize == 512 || aUnitSize == 4096;
		assert (aLength & 15) == 0;

		byte[] whiteningValue = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, aTweak, aBlockKey, whiteningValue);

			for (int block = 0; block < numBlocks; block++, offset += BYTES_PER_BLOCK)
			{
				xor(aBuffer, offset, whiteningValue, 0);

				aCipher.engineEncryptBlock(aBuffer, offset, aBuffer, offset);

				xor(aBuffer, offset, whiteningValue, 0);

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
		assert aUnitSize == 256 || aUnitSize == 512 || aUnitSize == 4096;
		assert (aLength & 15) == 0;

		byte[] whiteningValue = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, aTweak, aBlockKey, whiteningValue);

			for (int block = 0; block < numBlocks; block++, offset += BYTES_PER_BLOCK)
			{
				xor(aBuffer, offset, whiteningValue, 0);

				aCipher.engineDecryptBlock(aBuffer, offset, aBuffer, offset);

				xor(aBuffer, offset, whiteningValue, 0);

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


	private static void xor(byte[] aBuffer, int aOffset, byte[] aMask, int aMaskOffset)
	{
		for (int i = 0; i < BYTES_PER_BLOCK; i++)
		{
			aBuffer[aOffset + i] ^= aMask[aMaskOffset + i];
		}
	}


	private static void putLong(byte[] aBuffer, int aOffset, long aValue)
	{
		aBuffer[aOffset++] = (byte)(aValue);
		aBuffer[aOffset++] = (byte)(aValue >> 8);
		aBuffer[aOffset++] = (byte)(aValue >> 16);
		aBuffer[aOffset++] = (byte)(aValue >> 24);
		aBuffer[aOffset++] = (byte)(aValue >> 32);
		aBuffer[aOffset++] = (byte)(aValue >> 40);
		aBuffer[aOffset++] = (byte)(aValue >> 48);
		aBuffer[aOffset] = (byte)(aValue >>> 56);
	}


	private static long getLong(byte[] aBuffer, int aOffset)
	{
		return ((255 & aBuffer[aOffset++]))
			+ ((255 & aBuffer[aOffset++]) << 8)
			+ ((255 & aBuffer[aOffset++]) << 16)
			+ ((long)(255 & aBuffer[aOffset++]) << 24)
			+ ((long)(255 & aBuffer[aOffset++]) << 32)
			+ ((long)(255 & aBuffer[aOffset++]) << 40)
			+ ((long)(255 & aBuffer[aOffset++]) << 48)
			+ ((long)(255 & aBuffer[aOffset]) << 56);
	}


	private static void prepareIV(long aDataUnitNo, byte[] aInputIV, BlockCipher aTweak, long aBlockKey, byte[] aOutputIV)
	{
		System.arraycopy(aInputIV, 0, aOutputIV, 0, BYTES_PER_BLOCK);

		aOutputIV[0] ^= (byte)(aBlockKey >>> 56);
		aOutputIV[1] ^= (byte)(aBlockKey >> 48);
		aOutputIV[2] ^= (byte)(aBlockKey >> 40);
		aOutputIV[3] ^= (byte)(aBlockKey >> 32);
		aOutputIV[4] ^= (byte)(aBlockKey >> 24);
		aOutputIV[5] ^= (byte)(aBlockKey >> 16);
		aOutputIV[6] ^= (byte)(aBlockKey >> 8);
		aOutputIV[7] ^= (byte)(aBlockKey);

		aOutputIV[8] ^= (byte)(aDataUnitNo >>> 56);
		aOutputIV[9] ^= (byte)(aDataUnitNo >> 48);
		aOutputIV[10] ^= (byte)(aDataUnitNo >> 40);
		aOutputIV[11] ^= (byte)(aDataUnitNo >> 32);
		aOutputIV[12] ^= (byte)(aDataUnitNo >> 24);
		aOutputIV[13] ^= (byte)(aDataUnitNo >> 16);
		aOutputIV[14] ^= (byte)(aDataUnitNo >> 8);
		aOutputIV[15] ^= (byte)(aDataUnitNo);

		aTweak.engineEncryptBlock(aOutputIV, 0, aOutputIV, 0);
	}
}
