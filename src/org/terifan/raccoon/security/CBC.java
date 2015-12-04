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
		assert aInput != aOutput;

		byte [] iv = new byte[16];
		int numDataUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize >> 4;
		int ivSeed = (int)aBlockKey;
		int blkSrc;
		int blkDst;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numDataUnits; unitIndex++, offset += aUnitSize)
		{
			aBlockKey = mix(aBlockKey);
			blkSrc = (int)(aBlockKey >>> 32) & (numBlocks - 1);
			blkDst = (int)(aBlockKey >>> 48) & (numBlocks - 1);

			prepareIV(aStartDataUnitNo + unitIndex, aIV, iv, aTweakCipher, ivSeed);

			for (int i = 0; i < numBlocks; i++)
			{
				int s = (i ^ blkSrc) << 4;
				int t = (i ^ blkDst) << 4;
				Log.out.printf("%3d-%-3d  ",s,t);

				for (int j = 0; j < 16; j++)
				{
					iv[j] ^= aInput[offset + s + j];
				}

				aCipher.engineEncryptBlock(iv, 0, iv, 0);

				System.arraycopy(iv, 0, aOutput, offset + t, 16);
			}
			Log.out.println();
		}
	}


	public void decrypt(int aUnitSize, byte[] aInput, byte[] aOutput, int aOffset, int aLength, long aStartDataUnitNo, byte[] aIV, Cipher aCipher, Cipher aTweakCipher, long aBlockKey)
	{
		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;
		assert aIV.length == 16;
		assert aInput.length >= aOffset + aLength;
		assert aInput.length == aOutput.length;
		assert aInput != aOutput;

		byte [] iv = new byte[16 + 16]; // stores IV and next IV
		int numDataUnits = aLength / aUnitSize;
		int numBlocks = aUnitSize >> 4;
		int ivSeed = (int)aBlockKey;
		int blkSrc;
		int blkDst;

		for (int unitIndex = 0, offset = aOffset; unitIndex < numDataUnits; unitIndex++, offset += aUnitSize)
		{
			aBlockKey = mix(aBlockKey);
			blkSrc = (int)(aBlockKey >>> 32) & (numBlocks - 1);
			blkDst = (int)(aBlockKey >>> 48) & (numBlocks - 1);

			prepareIV(aStartDataUnitNo + unitIndex, aIV, iv, aTweakCipher, ivSeed);

			for (int i = 0; i < numBlocks; i++)
			{
				int s = (i ^ blkSrc) << 4;
				int t = (i ^ blkDst) << 4;

				System.arraycopy(aInput, offset + t, iv, 16, 16);

				aCipher.engineDecryptBlock(aInput, offset + t, aOutput, offset + s);

				for (int j = 0; j < 16; j++)
				{
					aOutput[offset + s + j] ^= iv[j];
				}

				System.arraycopy(iv, 16, iv, 0, 16);
			}
		}
	}


	private static void prepareIV(long aDataUnitNo, byte [] aInputIV, byte [] aOutputIV, Cipher aTweakCipher, int aBlockKey)
	{
		System.arraycopy(aInputIV, 0, aOutputIV, 0, 16);

		aOutputIV[0] ^= (byte)(aBlockKey >>> 24);
		aOutputIV[1] ^= (byte)(aBlockKey >> 16);
		aOutputIV[2] ^= (byte)(aBlockKey >> 8);
		aOutputIV[3] ^= (byte)(aBlockKey);

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


	private static long mix(long v)
	{
		v ^= v << 21;
		v ^= v >>> 35;
		v ^= v << 4;
		return v;
	}
}