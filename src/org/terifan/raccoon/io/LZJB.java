package org.terifan.raccoon.io;


public class LZJB implements Compressor
{
	private final static int NBBY = 8;
	private final static int MATCH_BITS	= 6;
	private final static int MATCH_MIN = 3;
	private final static int MATCH_MAX = ((1 << MATCH_BITS) + (MATCH_MIN - 1));
	private final static int OFFSET_MASK = ((1 << (16 - MATCH_BITS)) - 1);
	private final static int WINDOW_SIZE = 1024 - 1;

	
	LZJB()
	{
	}
	
	
	@Override
	public int compress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLimit)
	{
		int src = aInputOffset;
		int dst = aOutputOffset;
		int cpy, copymap = 0;
		int copymask = 1 << (NBBY - 1);
		int mlen, offset, hash;
		int hp;
		int [] refs = new int[WINDOW_SIZE+1];

		while (src < aInputLength)
		{
			if ((copymask <<= 1) == (1 << NBBY))
			{
				if (dst >= aOutputLimit - 1 - 2 * NBBY)
				{
					return Compressor.COMPRESSION_FAILED; //aInputLength;
				}
				copymask = 1;
				copymap = dst;
				aOutput[dst++] = 0;
			}
			if (src > aInputLength - MATCH_MAX)
			{
				aOutput[dst++] = aInput[src++];
				continue;
			}
			hash = (aInput[src] << 16) + (aInput[src+1] << 8) + aInput[src+2];
			hash += hash >> 9;
			hash += hash >> 5;
			hp = hash & WINDOW_SIZE;
			offset = (src - refs[hp]) & OFFSET_MASK;
			refs[hp] = src;
			cpy = src - offset;
			if (cpy >= 0 && cpy != src && aInput[src] == aInput[cpy] && aInput[src+1] == aInput[cpy+1] && aInput[src+2] == aInput[cpy+2])
			{
				aOutput[copymap] |= copymask;
				for (mlen = MATCH_MIN; mlen < MATCH_MAX; mlen++)
				{
					if (aInput[src+mlen] != aInput[cpy+mlen])
					{
						break;
					}
				}
				aOutput[dst++] = (byte)(((mlen - MATCH_MIN) << (NBBY - MATCH_BITS)) | (offset >> NBBY));
				aOutput[dst++] = (byte)offset;
				src += mlen;
			}
			else
			{
				aOutput[dst++] = aInput[src++];
			}
		}
		return dst;
	}


	@Override
	public void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength)
	{
		int src = aInputOffset;
		int dst = aOutputOffset;
		int d_end = aOutputLength;
		int copymap = 0;
		int copymask = 1 << (NBBY - 1);

		while (dst < d_end)
		{
			if ((copymask <<= 1) == (1 << NBBY))
			{
				copymask = 1;
				copymap = 255 & aInput[src++];
			}
			if ((copymap & copymask) != 0)
			{
				int mlen = ((255 & aInput[src]) >> (NBBY - MATCH_BITS)) + MATCH_MIN;
				int offset = (((255 & aInput[src]) << NBBY) | (255 & aInput[src+1])) & OFFSET_MASK;
				src += 2;
				int cpy = dst - offset;
				if (cpy < 0)
				{
					throw new IllegalStateException();
				}
				while (--mlen >= 0 && dst < d_end)
				{
					aOutput[dst++] = aOutput[cpy++];
				}
			}
			else
			{
				aOutput[dst++] = aInput[src++];
			}
		}

		if (dst != aOutputOffset + aOutputLength)
		{
			throw new IllegalStateException("Failed to decompress data.");
		}
	}


	@Override
	public String toString()
	{
		return "LZJB";
	}
}