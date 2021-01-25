package org.terifan.security.cryptography;

import static org.terifan.raccoon.util.ByteArrayUtil.*;


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
	public void encrypt(final byte[] aBuffer, int aOffset, final int aLength, final BlockCipher aCipher, long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long[] aBlockIV, BlockCipher aTweakCipher)
	{
		assert (aUnitSize & (BYTES_PER_BLOCK - 1)) == 0;
		assert (aLength & (BYTES_PER_BLOCK - 1)) == 0;
		assert aLength >= aUnitSize : aLength+" >= "+aUnitSize;
		assert (aLength % aUnitSize) == 0;

		byte[] whiteningValue = new byte[BYTES_PER_BLOCK];
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;
		int numUnits = aLength / aUnitSize;

		for (int unitIndex = 0; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aMasterIV, aBlockIV, aStartDataUnitNo++, whiteningValue, aTweakCipher);

			for (int block = 0; block < numBlocks; block++, aOffset += BYTES_PER_BLOCK)
			{
				xor(aBuffer, aOffset, BYTES_PER_BLOCK, whiteningValue, 0);

				aCipher.engineEncryptBlock(aBuffer, aOffset, aBuffer, aOffset);

				xor(aBuffer, aOffset, BYTES_PER_BLOCK, whiteningValue, 0);

				int finalCarry = ((whiteningValue[8 + 7] & 0x80) != 0) ? 135 : 0;

				putInt64LE(whiteningValue, 8, getInt64LE(whiteningValue, 8) << 1);

				if ((whiteningValue[7] & 0x80) != 0)
				{
					whiteningValue[8] |= 0x01;
				}

				putInt64LE(whiteningValue, 0, getInt64LE(whiteningValue, 0) << 1);

				whiteningValue[0] ^= finalCarry;
			}
		}
	}


	@Override
	public void decrypt(final byte[] aBuffer, int aOffset, final int aLength, final BlockCipher aCipher, long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long[] aBlockIV, BlockCipher aTweakCipher)
	{
		assert (aUnitSize & (BYTES_PER_BLOCK - 1)) == 0;
		assert (aLength & (BYTES_PER_BLOCK - 1)) == 0;
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;

		byte[] whiteningValue = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aMasterIV, aBlockIV, aStartDataUnitNo++, whiteningValue, aTweakCipher);

			for (int block = 0; block < numBlocks; block++, aOffset += BYTES_PER_BLOCK)
			{
				xor(aBuffer, aOffset, BYTES_PER_BLOCK, whiteningValue, 0);

				aCipher.engineDecryptBlock(aBuffer, aOffset, aBuffer, aOffset);

				xor(aBuffer, aOffset, BYTES_PER_BLOCK, whiteningValue, 0);

				int finalCarry = (whiteningValue[8 + 7] & 0x80) != 0 ? 135 : 0;

				putInt64LE(whiteningValue, 8, getInt64LE(whiteningValue, 8) << 1);

				if ((whiteningValue[7] & 0x80) != 0)
				{
					whiteningValue[8] |= 0x01;
				}

				putInt64LE(whiteningValue, 0, getInt64LE(whiteningValue, 0) << 1);

				whiteningValue[0] ^= finalCarry;
			}
		}
	}
}
