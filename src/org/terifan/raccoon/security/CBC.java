package org.terifan.raccoon.security;

import org.terifan.raccoon.util.Log;

public final class CBC
{
	public CBC()
	{
	}


	public void encrypt(int aUnitSize, byte[] aInput, byte[] aOutput, int aOffset, int aLength, long aStartDataUnitNo, byte[] aIV, Cipher aCipher, Cipher aTweakCipher, long aBlockKey)
	{
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;
		assert aIV.length == 16;
		assert aInput.length >= aOffset + aLength;
		assert aInput.length == aOutput.length;

		byte [] iv = new byte[16];

		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / aUnitSize; unitIndex < numDataUnits; unitIndex++, offset += aUnitSize)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, iv, aTweakCipher, aBlockKey);

			for (int i = 0; i < aUnitSize; i += 16)
			{
				for (int j = 0; j < 16; j++)
				{
					iv[j] ^= aInput[offset + i + j];
				}

				aCipher.engineEncryptBlock(iv, 0, iv, 0);

				System.arraycopy(iv, 0, aOutput, offset + i, 16);
			}
		}
	}


	public void decrypt(int aUnitSize, byte[] aInput, byte[] aOutput, int aOffset, int aLength, long aStartDataUnitNo, byte[] aIV, Cipher aCipher, Cipher aTweakCipher, long aBlockKey)
	{
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;
		assert aIV.length == 16;
		assert aInput.length >= aOffset + aLength;
		assert aInput.length == aOutput.length;

		byte [] iv = new byte[16 + 16]; // stores IV and next IV

		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / aUnitSize; unitIndex < numDataUnits; unitIndex++, offset += aUnitSize)
		{
			prepareIV(aStartDataUnitNo + unitIndex, aIV, iv, aTweakCipher, aBlockKey);

			for (int i = 0; i < aUnitSize; i += 16)
			{
				System.arraycopy(aInput, offset + i, iv, 16, 16);

				aCipher.engineDecryptBlock(aInput, offset + i, aOutput, offset + i);

				for (int j = 0; j < 16; j++)
				{
					aOutput[offset + i + j] ^= iv[j];
				}

				System.arraycopy(iv, 16, iv, 0, 16);
			}
		}
	}


	private static void prepareIV(long aDataUnitNo, byte [] aInputIV, byte [] aOutputIV, Cipher aTweakCipher, long aBlockKey)
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