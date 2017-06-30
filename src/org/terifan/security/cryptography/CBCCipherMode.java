package org.terifan.security.cryptography;

import static org.terifan.raccoon.util.ByteArrayUtil.*;


public final class CBCCipherMode extends CipherMode
{
	private final static int BYTES_PER_BLOCK = 16;


	public CBCCipherMode()
	{
	}


	@Override
	public void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long[] aBlockIV, BlockCipher aTweakCipher)
	{
		assert (aUnitSize & (BYTES_PER_BLOCK - 1)) == 0;
		assert (aLength & (BYTES_PER_BLOCK - 1)) == 0;
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;

		byte[] iv = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, bufferOffset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aMasterIV, aBlockIV, bufferOffset, iv, aTweakCipher);

			for (int block = 0; block < numBlocks; block++, bufferOffset += BYTES_PER_BLOCK)
			{
				xor(iv, 0, BYTES_PER_BLOCK, aBuffer, bufferOffset);

				aCipher.engineEncryptBlock(iv, 0, aBuffer, bufferOffset);

				System.arraycopy(aBuffer, bufferOffset, iv, 0, BYTES_PER_BLOCK);
			}
		}
	}


	@Override
	public void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long[] aBlockIV, BlockCipher aTweakCipher)
	{
		assert (aUnitSize & (BYTES_PER_BLOCK - 1)) == 0;
		assert (aLength & (BYTES_PER_BLOCK - 1)) == 0;
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;

		byte[] iv = new byte[BYTES_PER_BLOCK + BYTES_PER_BLOCK]; // IV + next IV
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, bufferOffset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aMasterIV, aBlockIV, bufferOffset, iv, aTweakCipher);

			for (int block = 0, ivOffset = 0; block < numBlocks; block++, ivOffset = BYTES_PER_BLOCK - ivOffset, bufferOffset += BYTES_PER_BLOCK)
			{
				System.arraycopy(aBuffer, bufferOffset, iv, BYTES_PER_BLOCK - ivOffset, BYTES_PER_BLOCK);

				aCipher.engineDecryptBlock(aBuffer, bufferOffset, aBuffer, bufferOffset);

				xor(aBuffer, bufferOffset, BYTES_PER_BLOCK, iv, ivOffset);
			}
		}
	}
}