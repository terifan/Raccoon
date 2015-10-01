package org.terifan.raccoon.security;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.DigestException;
import java.util.Arrays;


/**
 * The Whirlpool hashing function.
 *
 * The Whirlpool algorithm was developed by
 * <a href="mailto:pbarreto@scopus.com.br">Paulo S. L. M. Barreto</a> and
 * <a href="mailto:vincent.rijmen@cryptomathic.com">Vincent Rijmen</a>.
 *
 * See
 *	  P.S.L.M. Barreto, V. Rijmen,
 *	  ``The Whirlpool hashing function,''
 *	  First NESSIE workshop, 2000 (tweaked version, 2003),
 *	  https://www.cosic.esat.kuleuven.ac.be/nessie/workshop/submissions/whirlpool.zip
 *
 * @author	Paulo S.L.M. Barreto
 * @author	Vincent Rijmen.
 */
public final class Whirlpool extends MessageDigest implements Cloneable
{
	private final static int R = 10;
	private final static long[][] C = new long[8][256];
	private final static long[] RC = new long[R + 1];
	private final static String sbox =
		"\u1823\uc6E8\u87B8\u014F\u36A6\ud2F5\u796F\u9152" +
		"\u60Bc\u9B8E\uA30c\u7B35\u1dE0\ud7c2\u2E4B\uFE57" +
		"\u1577\u37E5\u9FF0\u4AdA\u58c9\u290A\uB1A0\u6B85" +
		"\uBd5d\u10F4\ucB3E\u0567\uE427\u418B\uA77d\u95d8" +
		"\uFBEE\u7c66\udd17\u479E\ucA2d\uBF07\uAd5A\u8333" +
		"\u6302\uAA71\uc819\u49d9\uF2E3\u5B88\u9A26\u32B0" +
		"\uE90F\ud580\uBEcd\u3448\uFF7A\u905F\u2068\u1AAE" +
		"\uB454\u9322\u64F1\u7312\u4008\uc3Ec\udBA1\u8d3d" +
		"\u9700\ucF2B\u7682\ud61B\uB5AF\u6A50\u45F3\u30EF" +
		"\u3F55\uA2EA\u65BA\u2Fc0\udE1c\uFd4d\u9275\u068A" +
		"\uB2E6\u0E1F\u62d4\uA896\uF9c5\u2559\u8472\u394c" +
		"\u5E78\u388c\ud1A5\uE261\uB321\u9c1E\u43c7\uFc04" +
		"\u5199\u6d0d\uFAdF\u7E24\u3BAB\ucE11\u8F4E\uB7EB" +
		"\u3c81\u94F7\uB913\u2cd3\uE76E\uc403\u5644\u7FA9" +
		"\u2ABB\uc153\udc0B\u9d6c\u3174\uF646\uAc89\u14E1" +
		"\u163A\u6909\u70B6\ud0Ed\ucc42\u98A4\u285c\uF886";

	static
	{
		for (int x = 0; x < 256; x++)
		{
			char c = sbox.charAt(x/2);
			long v1 = ((x & 1) == 0) ? c >>> 8 : c & 0xff;
			long v2 = v1 << 1;
			if (v2 >= 0x100L)
			{
				v2 ^= 0x11dL;
			}
			long v4 = v2 << 1;
			if (v4 >= 0x100L)
			{
				v4 ^= 0x11dL;
			}
			long v5 = v4 ^ v1;
			long v8 = v4 << 1;
			if (v8 >= 0x100L)
			{
				v8 ^= 0x11dL;
			}
			long v9 = v8 ^ v1;
			C[0][x] = (v1 << 56) | (v1 << 48) | (v4 << 40) | (v1 << 32) | (v8 << 24) | (v5 << 16) | (v2 <<  8) | (v9	  );
			for (int t = 1; t < 8; t++)
			{
				C[t][x] = (C[t - 1][x] >>> 8) | ((C[t - 1][x] << 56));
			}
		}
		RC[0] = 0L;
		for (int r = 1; r <= R; r++)
		{
			int i = 8*(r - 1);
			RC[r] =
				(C[0][i	+ 0] & 0xff00000000000000L) ^
				(C[1][i + 1] & 0x00ff000000000000L) ^
				(C[2][i + 2] & 0x0000ff0000000000L) ^
				(C[3][i + 3] & 0x000000ff00000000L) ^
				(C[4][i + 4] & 0x00000000ff000000L) ^
				(C[5][i + 5] & 0x0000000000ff0000L) ^
				(C[6][i + 6] & 0x000000000000ff00L) ^
				(C[7][i + 7] & 0x00000000000000ffL);
		}
	}

	private transient byte[] bitLength = new byte[32];
	private transient byte[] buffer = new byte[64];
	private transient int bufferBytes = 0;
	private transient int bufferPos = 0;
	private transient long [] hash  = new long[8];
	private transient long [] K	 = new long[8];
	private transient long [] L	 = new long[8];
	private transient long [] block = new long[8];
	private transient long [] state = new long[8];
	private transient final byte [] dummyBuffer = new byte[1];


	public Whirlpool()
	{
		super("whirlpool");

		engineReset();
	}


	@Override
	protected byte [] engineDigest()
	{
		byte [] output = new byte[64];
		finalize(output, 0);
		engineReset();
		return output;
	}


	@Override
	protected int engineDigest(byte[] buf, int offset, int len) throws DigestException
	{
		if (len < 64)
		{
			throw new DigestException("Buffer too short.");
		}

		finalize(buf, offset);
		engineReset();

		return 64;
	}


	@Override
	protected int engineGetDigestLength()
	{
		return 64;
	}


	@Override
	protected void engineReset()
	{
		Arrays.fill(bitLength, (byte)0);
		bufferBytes = bufferPos = 0;
		buffer[0] = 0;
		Arrays.fill(hash, 0L);
	}


	@Override
	protected void engineUpdate(byte input)
	{
		dummyBuffer[0] = input;
		add(dummyBuffer, 0, 8L);
	}


	@Override
	protected void engineUpdate(byte[] input, int offset, int len)
	{
		add(input, offset, 8L * len);
	}


	@Override
	protected void engineUpdate(ByteBuffer input)
	{
		while (input.hasRemaining())
		{
			engineUpdate(input.get());
		}
	}


	/**
	 * The core Whirlpool transform.
	 */
	protected void processBuffer()
	{
		for (int i = 0, j = 0; i < 8; i++, j += 8)
		{
			block[i] =
				(((long)buffer[j	]        ) << 56) ^
				(((long)buffer[j + 1] & 0xffL) << 48) ^
				(((long)buffer[j + 2] & 0xffL) << 40) ^
				(((long)buffer[j + 3] & 0xffL) << 32) ^
				(((long)buffer[j + 4] & 0xffL) << 24) ^
				(((long)buffer[j + 5] & 0xffL) << 16) ^
				(((long)buffer[j + 6] & 0xffL) <<  8) ^
				(((long)buffer[j + 7] & 0xffL)	  );
		}

		for (int i = 0; i < 8; i++)
		{
			state[i] = block[i] ^ (K[i] = hash[i]);
		}

		for (int r = 1; r <= R; r++)
		{
			for (int i = 0; i < 8; i++)
			{
				L[i] = 0L;
				for (int t = 0, s = 56; t < 8; t++, s -= 8)
				{
					L[i] ^= C[t][(int)(K[(i - t) & 7] >>> s) & 0xff];
				}
			}
			System.arraycopy(L, 0, K, 0, 8);
			K[0] ^= RC[r];

			for (int i = 0; i < 8; i++)
			{
				L[i] = K[i];
				for (int t = 0, s = 56; t < 8; t++, s -= 8)
				{
					L[i] ^= C[t][(int)(state[(i - t) & 7] >>> s) & 0xff];
				}
			}
			System.arraycopy(L, 0, state, 0, 8);
		}

		for (int i = 0; i < 8; i++)
		{
			hash[i] ^= state[i] ^ block[i];
		}
	}


	/**
	 * Delivers input data to the hashing algorithm.
	 *
	 * @param	source		plaintext data to hash.
	 * @param	sourceBits	how many bits of plaintext to process.
	 *
	 * This method maintains the invariant: bufferBytes < 64
	 */
	private void add(byte [] source, int offset, long sourceBits)
	{
		assert (sourceBits % 8) == 0;

		int sourcePos = offset;
		int b;

		long value = sourceBits;
		for (int i = 31, carry = 0; i >= 0; i--)
		{
			carry += (bitLength[i] & 0xff) + ((int)value & 0xff);
			bitLength[i] = (byte)carry;
			carry >>>= 8;
			value >>>= 8;
		}

		while (sourceBits >= 8)
		{
			b = source[sourcePos] & 0xff;

			buffer[bufferPos++] |= b;
			bufferBytes++;
			if (bufferBytes == 64)
			{
				processBuffer();
				bufferBytes = bufferPos = 0;
			}
			buffer[bufferPos] = (byte)((b << 8) & 0xff);

			sourceBits -= 8;
			sourcePos++;
		}
	}

	/**
	 * Get the hash value from the hashing state.
	 */
	private void finalize(byte [] digest, int offset)
	{
		buffer[bufferPos] |= 0x80;
		bufferPos++;

		if (bufferPos > 32)
		{
			while (bufferPos < 64)
			{
				buffer[bufferPos++] = 0;
			}
			processBuffer();
			bufferPos = 0;
		}
		while (bufferPos < 32)
		{
			buffer[bufferPos++] = 0;
		}
		System.arraycopy(bitLength, 0, buffer, 32, 32);
		processBuffer();
		for (int i = 0, j = offset; i < 8; i++, j += 8)
		{
			long h = hash[i];
			digest[j	] = (byte)(h >>> 56);
			digest[j + 1] = (byte)(h >>> 48);
			digest[j + 2] = (byte)(h >>> 40);
			digest[j + 3] = (byte)(h >>> 32);
			digest[j + 4] = (byte)(h >>> 24);
			digest[j + 5] = (byte)(h >>> 16);
			digest[j + 6] = (byte)(h >>>  8);
			digest[j + 7] = (byte)(h	   );
		}
	}


	@Override
	public String toString()
	{
		return "Whirlpool";
	}


	@Override
	public Whirlpool clone()
	{
		Whirlpool instance = new Whirlpool();

		instance.K = this.K.clone();
		instance.L = this.L.clone();
		instance.bitLength = this.bitLength.clone();
		instance.block = this.block.clone();
		instance.buffer = this.buffer.clone();
		instance.bufferBytes = this.bufferBytes;
		instance.bufferPos = this.bufferPos;
		instance.hash = this.hash.clone();
		instance.state = this.state.clone();

		return instance;
	}
}