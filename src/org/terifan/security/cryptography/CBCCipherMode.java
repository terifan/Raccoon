package org.terifan.security.cryptography;


public final class CBCCipherMode implements CipherMode
{
	private final static int BYTES_PER_BLOCK = 16;
	
	
	public CBCCipherMode()
	{
	}


	@Override
	public void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey)
	{
		assert (aUnitSize & -aUnitSize) == aUnitSize;
		assert (aLength & 15) == 0;

		byte[] iv = new byte[BYTES_PER_BLOCK];
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, bufferOffset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, aTweak, aBlockKey, iv);

			for (int block = 0; block < numBlocks; block++, bufferOffset += BYTES_PER_BLOCK)
			{
				xor(iv, 0, aBuffer, bufferOffset);

				aCipher.engineEncryptBlock(iv, 0, aBuffer, bufferOffset);

				System.arraycopy(aBuffer, bufferOffset, iv, 0, BYTES_PER_BLOCK);
			}
		}
	}


	@Override
	public void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey)
	{
		assert (aUnitSize & -aUnitSize) == aUnitSize;
		assert (aLength & 15) == 0;

		byte[] iv = new byte[BYTES_PER_BLOCK + BYTES_PER_BLOCK]; // IV + next IV
		int numUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize / BYTES_PER_BLOCK;

		for (int unitIndex = 0, bufferOffset = aOffset; unitIndex < numUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, aTweak, aBlockKey, iv);

			for (int block = 0, ivOffset = 0; block < numBlocks; block++, ivOffset = BYTES_PER_BLOCK - ivOffset, bufferOffset += BYTES_PER_BLOCK)
			{
				System.arraycopy(aBuffer, bufferOffset, iv, BYTES_PER_BLOCK - ivOffset, BYTES_PER_BLOCK);

				aCipher.engineDecryptBlock(aBuffer, bufferOffset, aBuffer, bufferOffset);

				xor(aBuffer, bufferOffset, iv, ivOffset);
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