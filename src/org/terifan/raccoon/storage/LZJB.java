package org.terifan.raccoon.storage;

import java.io.IOException;
import org.terifan.raccoon.util.ByteBlockOutputStream;


public class LZJB implements Compressor
{
	private final static int NBBY = 8;
	private final static int MATCH_BITS = 6;
	private final static int MATCH_MIN = 3;
	private final static int MATCH_MAX = ((1 << MATCH_BITS) + (MATCH_MIN - 1));
	private final static int OFFSET_MASK = ((1 << (16 - MATCH_BITS)) - 1);
	private final static int WINDOW_SIZE = 1024 - 1;


	public LZJB()
	{
	}


	@Override
	public boolean compress(byte[] aInput, int aInputOffset, int aInputLength, ByteBlockOutputStream aOutputStream)
	{
		int src = 0;
		int dst = 0;
		int cpy, copymap = 0;
		int copymask = 1 << (NBBY - 1);
		int mlen, offset, hash;
		int hp;
		int[] refs = new int[WINDOW_SIZE + 1];

		while (src < aInputLength)
		{
			if ((copymask <<= 1) == (1 << NBBY))
			{
				if (dst >= aInput.length - 1 - 2 * NBBY)
				{
					return false;
				}
				copymask = 1;
				copymap = dst;
				aOutputStream.write(0);
				dst++;
			}
			if (src > aInputLength - MATCH_MAX)
			{
				aOutputStream.write(aInput[src++]);
				dst++;
				continue;
			}
			hash = (aInput[src] << 16) + (aInput[src + 1] << 8) + aInput[src + 2];
			hash += hash >> 9;
			hash += hash >> 5;
			hp = hash & WINDOW_SIZE;
			offset = (src - refs[hp]) & OFFSET_MASK;
			refs[hp] = src;
			cpy = src - offset;
			if (cpy >= 0 && cpy != src && aInput[src] == aInput[cpy] && aInput[src + 1] == aInput[cpy + 1] && aInput[src + 2] == aInput[cpy + 2])
			{
				aOutputStream.getBuffer()[copymap] |= copymask;
				for (mlen = MATCH_MIN; mlen < MATCH_MAX; mlen++)
				{
					if (aInput[src + mlen] != aInput[cpy + mlen])
					{
						break;
					}
				}
				aOutputStream.write(((mlen - MATCH_MIN) << (NBBY - MATCH_BITS)) | (offset >> NBBY));
				aOutputStream.write(offset);
				dst += 2;
				src += mlen;
			}
			else
			{
				aOutputStream.write(aInput[src++]);
				dst++;
			}
		}

		return true;
	}


	@Override
	public void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength) throws IOException
	{
		int src = aInputOffset;
		int dst = aOutputOffset;
		int dstend = dst + aOutputLength;
		int copymap = 0;
		int copymask = 1 << (NBBY - 1);

		while (dst < dstend)
		{
			if ((copymask <<= 1) == (1 << NBBY))
			{
				copymask = 1;
				copymap = 255 & aInput[src++];
			}
			if ((copymap & copymask) != 0)
			{
				int mlen = ((255 & aInput[src]) >> (NBBY - MATCH_BITS)) + MATCH_MIN;
				int offset = (((255 & aInput[src]) << NBBY) | (255 & aInput[src + 1])) & OFFSET_MASK;
				src += 2;
				int cpy = dst - offset;
				if (cpy < 0)
				{
					throw new RuntimeException();
				}
				while (--mlen >= 0 && dst < dstend)
				{
					aOutput[dst++] = aOutput[cpy++];
				}
			}
			else
			{
				aOutput[dst++] = aInput[src++];
			}
		}
	}


//	public static void main(String... args)
//	{
//		try
//		{
//			ArrayList<Tuple<String, byte[]>> files = new ArrayList<>();
//
////			for (File f : new File("D:\\Google Drive\\Source code\\jpeg-8c").listFiles(e -> e.getName().endsWith(".c") && e.length() > 8192))
////			for (File f : new File("D:\\Pictures\\Wallpapers High Quality").listFiles(e -> e.getName().endsWith(".jpg") && e.length() > 65536))
//			for (File f : new File("D:\\Resources\\game resources\\half-life 2 content\\maps").listFiles(e -> e.getName().endsWith(".bsp")))
//			{
//				if (f.isDirectory())
//				{
//					continue;
//				}
//
//				byte[] src = new byte[16384];
//
//				try (FileInputStream in = new FileInputStream(f))
//				{
//					in.read(src);
//				}
//
//				files.add(new Tuple<>(f.toString(), src));
//			}
//
//			System.out.printf("\t   input   output   ratio    co-mi    de-mi  co-ns/b  de-ns/b match file\n");
//
//			LZJB compressor = new LZJB();
//			int totalZipLen = 0;
//			int totalZipEnc = 0;
//			int totalZipDec = 0;
//			int totalLzjbLen = 0;
//			int totalLzjbEnc = 0;
//			int totalLzjbDec = 0;
//
//			for (Tuple<String, byte[]> file : files)
//			{
//				byte[] src = file.second;
//				byte[] unpack = new byte[src.length];
//
//				long t1 = System.nanoTime();
//				ByteBlockOutputStream buf = new ByteBlockOutputStream(4096);
//				boolean success = compressor.compress(src, 0, src.length, buf);
//				t1 = System.nanoTime() - t1;
//
//				if (success)
//				{
//					long t2 = System.nanoTime();
//					compressor.decompress(buf.getBuffer(), 0, buf.size(), unpack, 0, src.length);
//					t2 = System.nanoTime() - t2;
//
//					if (!Arrays.equals(src, unpack))
//					{
//						throw new IllegalStateException("lzjb failed decompress");
//					}
//
//					System.out.printf("lzjb\t%8d %8d %8.3f %8d %8d  %s\n", src.length, buf.size(), 100.0 * buf.size() / src.length, t1 / 1000, t2 / 1000, file.first);
//
//					totalLzjbDec += t2;
//				}
//				else
//				{
//					System.out.printf("lzjb\tfailed%n");
//				}
//
//				totalLzjbLen += buf.size();
//				totalLzjbEnc += t1;
//
//				t1 = System.nanoTime();
//				ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 + src.length);
//				try (DeflaterOutputStream dos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_SPEED, true)))
//				{
//					dos.write(src);
//				}
//				t1 = System.nanoTime() - t1;
//				byte[] tmp = baos.toByteArray();
//
//				long t2 = System.nanoTime();
//				baos = new ByteArrayOutputStream(1024 + src.length);
//				try (InflaterOutputStream ios = new InflaterOutputStream(baos, new Inflater(true)))
//				{
//					ios.write(tmp);
//				}
//				t2 = System.nanoTime() - t2;
//				unpack = baos.toByteArray();
//
//				if (!Arrays.equals(src, unpack))
//				{
//					throw new IllegalStateException("zip failed decompress");
//				}
//
//				totalZipLen += tmp.length;
//				totalZipEnc += t1;
//				totalZipDec += t2;
//
//				System.out.printf("zip\t%8d %8d %8.3f %8d %8d\n", src.length, tmp.length, 100.0 * tmp.length / src.length, t1 / 1000, t2 / 1000);
//				System.out.println("");
//			}
//
//			System.out.printf("lzjb             %8d          %8d %8d%n", totalLzjbLen, totalLzjbEnc, totalLzjbDec);
//			System.out.printf("zip              %8d          %8d %8d%n", totalZipLen, totalZipEnc, totalZipDec);
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
//
//	private static class Tuple<F,S>
//	{
//		F first;
//		S second;
//
//		public Tuple(F aFirst, S aSecond)
//		{
//			this.first = aFirst;
//			this.second = aSecond;
//		}
//	}
}