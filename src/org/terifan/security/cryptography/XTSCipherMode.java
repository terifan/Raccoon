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
	public void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long[] aBlockIV)
	{
		assert (aUnitSize & (BYTES_PER_BLOCK - 1)) == 0;
		assert (aLength & (BYTES_PER_BLOCK - 1)) == 0;
		assert aLength >= aUnitSize : aLength+" >= "+aUnitSize;
		assert (aLength % aUnitSize) == 0;

		byte[] whiteningValue = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aMasterIV, aBlockIV, unitIndex, whiteningValue);

			for (int block = 0; block < numBlocks; block++, offset += BYTES_PER_BLOCK)
			{
				xor(aBuffer, offset, BYTES_PER_BLOCK, whiteningValue, 0);

				aCipher.engineEncryptBlock(aBuffer, offset, aBuffer, offset);

				xor(aBuffer, offset, BYTES_PER_BLOCK, whiteningValue, 0);

				int finalCarry = ((whiteningValue[8 + 7] & 0x80) != 0) ? 135 : 0;

				putLongLE(whiteningValue, 8, getLongLE(whiteningValue, 8) << 1);

				if ((whiteningValue[7] & 0x80) != 0)
				{
					whiteningValue[8] |= 0x01;
				}

				putLongLE(whiteningValue, 0, getLongLE(whiteningValue, 0) << 1);

				whiteningValue[0] ^= finalCarry;
			}
		}
	}


	@Override
	public void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long[] aBlockIV)
	{
		assert (aUnitSize & (BYTES_PER_BLOCK - 1)) == 0;
		assert (aLength & (BYTES_PER_BLOCK - 1)) == 0;
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;

		byte[] whiteningValue = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aMasterIV, aBlockIV, unitIndex, whiteningValue);

			for (int block = 0; block < numBlocks; block++, offset += BYTES_PER_BLOCK)
			{
				xor(aBuffer, offset, BYTES_PER_BLOCK, whiteningValue, 0);

				aCipher.engineDecryptBlock(aBuffer, offset, aBuffer, offset);

				xor(aBuffer, offset, BYTES_PER_BLOCK, whiteningValue, 0);

				int finalCarry = (whiteningValue[8 + 7] & 0x80) != 0 ? 135 : 0;

				putLongLE(whiteningValue, 8, getLongLE(whiteningValue, 8) << 1);

				if ((whiteningValue[7] & 0x80) != 0)
				{
					whiteningValue[8] |= 0x01;
				}

				putLongLE(whiteningValue, 0, getLongLE(whiteningValue, 0) << 1);

				whiteningValue[0] ^= finalCarry;
			}
		}
	}
}
