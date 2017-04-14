package org.terifan.security.cryptography;


public final class CBCCipherMode
{
	public CBCCipherMode()
	{
	}


	public void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final byte[] aIV, final long aStartDataUnitNo, final long aBlockKey, final BlockCipher aTweakCipher, final int aUnitSize)
	{
		assert aUnitSize >= 16;
		assert (aUnitSize & -aUnitSize) == aUnitSize;

		byte[] iv = new byte[16];
		int numDataUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize >> 4;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numDataUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, aTweakCipher, aBlockKey, iv);

			for (int i = 0; i < numBlocks; i++, offset += 16)
			{
				for (int j = 0; j < 16; j++)
				{
					iv[j] ^= aBuffer[offset + j];
				}

				aCipher.engineEncryptBlock(iv, 0, aBuffer, offset);

				System.arraycopy(aBuffer, offset, iv, 0, 16);
			}
		}
	}


	public void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final byte[] aIV, final long aStartDataUnitNo, final long aBlockKey, final BlockCipher aTweakCipher, final int aUnitSize)
	{
		assert aUnitSize >= 16;
		assert (aUnitSize & -aUnitSize) == aUnitSize;

		byte[] iv = new byte[16 + 16]; // IV + next IV
		int numDataUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize >> 4;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numDataUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, aTweakCipher, aBlockKey, iv);

			for (int i = 0, x = 0; i < numBlocks; i++, x = 16 - x, offset += 16)
			{
				System.arraycopy(aBuffer, offset, iv, 16 - x, 16);

				aCipher.engineDecryptBlock(aBuffer, offset, aBuffer, offset);

				for (int j = 0; j < 16; j++)
				{
					aBuffer[offset + j] ^= iv[j + x];
				}
			}
		}
	}


	private static void prepareIV(long aDataUnitNo, byte[] aInputIV, BlockCipher aTweakCipher, long aBlockKey, byte[] aOutputIV)
	{
		System.arraycopy(aInputIV, 0, aOutputIV, 0, 16);

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

		aTweakCipher.engineEncryptBlock(aOutputIV, 0, aOutputIV, 0);
	}
}