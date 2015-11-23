package org.terifan.raccoon.security;

import java.util.Arrays;

/*
x[((i + shift) ^ trans) & sz] ^ xor

12+12+8
*/
public final class CBC implements Crypto
{
	private final static int WORDS_PER_CIPHER_BLOCK = 4;


	public CBC()
	{
	}


	@Override
	public void reset()
	{
	}


	@Override
	public void encrypt(int aUnitSize, byte[] aInput, byte[] aOutput, int aOffset, int aLength, long aStartDataUnitNo, int[] aIV, Cipher aCipher, Cipher aTweakCipher, int[] aTweakKey, long aExtraTweak)
	{
		int wordsPerUnit = aUnitSize / 4;
		int [] words = new int[wordsPerUnit];
		int [] iv = new int[WORDS_PER_CIPHER_BLOCK];

		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;
		assert aIV.length == 4;
		assert aInput.length >= aOffset + aLength;
		assert aInput.length == aOutput.length;

		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / aUnitSize; unitIndex < numDataUnits; unitIndex++, offset += aUnitSize)
		{
			toInts(aInput, offset, words, wordsPerUnit);

			prepareIV(aStartDataUnitNo + unitIndex, aIV, iv, aTweakCipher);

			for (int i = 0; i < wordsPerUnit; i += WORDS_PER_CIPHER_BLOCK)
			{
				for (int j = 0; j < WORDS_PER_CIPHER_BLOCK; j++)
				{
					iv[j] ^= words[i + j];
				}

				aCipher.engineEncryptBlock(iv, 0, iv, 0);

				System.arraycopy(iv, 0, words, i, WORDS_PER_CIPHER_BLOCK);
			}

			toBytes(words, offset, aOutput, wordsPerUnit);
		}
	}


	@Override
	public void decrypt(int aUnitSize, byte[] aInput, byte[] aOutput, int aOffset, int aLength, long aStartDataUnitNo, int[] aIV, Cipher aCipher, Cipher aTweakCipher, int[] aTweakKey, long aExtraTweak)
	{
		int wordsPerUnit = aUnitSize / 4;
		int [] words = new int[wordsPerUnit];
		int [] iv = new int[WORDS_PER_CIPHER_BLOCK];
		int [] temp = new int[WORDS_PER_CIPHER_BLOCK];

		assert aLength >= aUnitSize;
		assert (aLength % aUnitSize) == 0;
		assert aIV.length == 4;
		assert aInput.length >= aOffset + aLength;
		assert aInput.length == aOutput.length;

		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / aUnitSize; unitIndex < numDataUnits; unitIndex++, offset += aUnitSize)
		{
			toInts(aInput, offset, words, wordsPerUnit);

			prepareIV(aStartDataUnitNo + unitIndex, aIV, iv, aTweakCipher);

			for (int i = 0; i < wordsPerUnit; i += WORDS_PER_CIPHER_BLOCK)
			{
				System.arraycopy(words, i, temp, 0, WORDS_PER_CIPHER_BLOCK);

				aCipher.engineDecryptBlock(words, i, words, i);

				for (int j = 0; j < WORDS_PER_CIPHER_BLOCK; j++)
				{
					words[i+j] ^= iv[j];
				}

				System.arraycopy(temp, 0, iv, 0, WORDS_PER_CIPHER_BLOCK);
			}

			toBytes(words, offset, aOutput, wordsPerUnit);
		}
	}


	private static void prepareIV(long aDataUnitNo, int [] aInputIV, int [] aOutputIV, Cipher aTweakCipher)
	{
		aOutputIV[0] = aInputIV[0];
		aOutputIV[1] = aInputIV[1];
		aOutputIV[2] = aInputIV[2] + (int)(aDataUnitNo >>> 32);
		aOutputIV[3] = aInputIV[3] + (int)(aDataUnitNo);

		aTweakCipher.engineEncryptBlock(aOutputIV, 0, aOutputIV, 0);
	}


	private static void toInts(byte [] aInput, int aOffset, int [] aOutput, int aNumWords)
	{
		for (int i = 0; i < aNumWords; i++)
		{
			aOutput[i] = ((255 & aInput[aOffset++]) << 24)
					   + ((255 & aInput[aOffset++]) << 16)
					   + ((255 & aInput[aOffset++]) <<  8)
					   + ((255 & aInput[aOffset++])      );
		}
	}


	private static void toBytes(int [] aInput, int aOffset, byte [] aOutput, int aNumWords)
	{
		for (int i = 0; i < aNumWords; i++)
		{
			int v = aInput[i];
			aOutput[aOffset++] = (byte)(v >>> 24);
			aOutput[aOffset++] = (byte)(v >>  16);
			aOutput[aOffset++] = (byte)(v >>   8);
			aOutput[aOffset++] = (byte)(v       );
		}
	}
}