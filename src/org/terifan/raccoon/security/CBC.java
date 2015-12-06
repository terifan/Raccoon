package org.terifan.raccoon.security;


public final class CBC
{
	private transient int mUnitSize;
	private transient Cipher mTweakCipher;


	public CBC(int aUnitSize, Cipher aTweakCipher)
	{
		assert aUnitSize >= 16;
		assert (aUnitSize & -aUnitSize) == aUnitSize;
		
		mUnitSize = aUnitSize;
		mTweakCipher = aTweakCipher;
	}


	public void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final long aStartDataUnitNo, final byte[] aIV, final Cipher aCipher, final long aBlockKey)
	{
		byte[] iv = new byte[16];
		int numDataUnits = aLength / mUnitSize;
		int numBlocks = mUnitSize >> 4;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numDataUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo, unitIndex, aIV, mTweakCipher, aBlockKey, iv);

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


	public void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final long aStartDataUnitNo, final byte[] aIV, final Cipher aCipher, final long aBlockKey)
	{
		byte[] iv = new byte[16 + 16]; // IV + next IV
		int numDataUnits = aLength / mUnitSize;
		int numBlocks = mUnitSize >> 4;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numDataUnits; unitIndex++)
		{
			prepareIV(aStartDataUnitNo, unitIndex, aIV, mTweakCipher, aBlockKey, iv);

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


	private static void prepareIV(long aDataUnitNo, int aUnitIndex, byte[] aInputIV, Cipher aTweakCipher, long aBlockKey, byte[] aOutputIV)
	{
		long dataUnitNo = aDataUnitNo ^ Long.reverseBytes(aUnitIndex);

		System.arraycopy(aInputIV, 0, aOutputIV, 0, 16);

		aOutputIV[0] ^= (byte)(aBlockKey >>> 56);
		aOutputIV[1] ^= (byte)(aBlockKey >> 48);
		aOutputIV[2] ^= (byte)(aBlockKey >> 40);
		aOutputIV[3] ^= (byte)(aBlockKey >> 32);
		aOutputIV[4] ^= (byte)(aBlockKey >> 24);
		aOutputIV[5] ^= (byte)(aBlockKey >> 16);
		aOutputIV[6] ^= (byte)(aBlockKey >> 8);
		aOutputIV[7] ^= (byte)(aBlockKey);

		aOutputIV[8] ^= (byte)(dataUnitNo >>> 56);
		aOutputIV[9] ^= (byte)(dataUnitNo >> 48);
		aOutputIV[10] ^= (byte)(dataUnitNo >> 40);
		aOutputIV[11] ^= (byte)(dataUnitNo >> 32);
		aOutputIV[12] ^= (byte)(dataUnitNo >> 24);
		aOutputIV[13] ^= (byte)(dataUnitNo >> 16);
		aOutputIV[14] ^= (byte)(dataUnitNo >> 8);
		aOutputIV[15] ^= (byte)(dataUnitNo);

		aTweakCipher.engineEncryptBlock(aOutputIV, 0, aOutputIV, 0);
	}
}