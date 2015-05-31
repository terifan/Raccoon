package org.terifan.raccoon.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.util.ByteArray;


class RangeMap
{
	private TreeMap<Integer,Integer> mMap;
	private int mSpace;


	public RangeMap()
	{
		mMap = new TreeMap<>();
		mSpace = 0;
	}


	public synchronized void add(int aOffset, int aSize)
	{
		if (aOffset < 0 || aSize <= 0)
		{
			throw new IllegalArgumentException("Illegal range: offset: " + aOffset + ", size: " + aSize);
		}

		int start = aOffset;
		int end = aOffset + aSize;

		assert end > start : end+" > "+start;

		Integer before = mMap.floorKey(start);
		Integer after = mMap.ceilingKey(start);

		Integer v1 = mMap.lowerKey(end);
		Integer v2 = mMap.lowerKey(start);

		if (v1 != null && v1 >= start)
		{
			throw new IllegalArgumentException("Offset overlap an existing region (1): offset: " + aOffset + ", size: " + aSize + ", existing start: " + v1 + ", existing end: " + mMap.get(v1));
		}
		if (v2 != null && v2 < start && mMap.get(v2) > start)
		{
			throw new IllegalArgumentException("Offset overlap an existing region (2): offset: " + aOffset + ", size: " + aSize + ", existing start: " + v1 + ", existing end: " + mMap.get(v1));
		}

		boolean mergeBefore = (before != null && mMap.get(before) == start);
		boolean mergeAfter = (after != null && after == end);

		if (mergeBefore && mergeAfter)
		{
			mMap.put(before, mMap.remove(after));
		}
		else if (mergeBefore)
		{
			mMap.put(before, end);
		}
		else if (mergeAfter)
		{
			mMap.put(start, mMap.remove(after));
		}
		else
		{
			mMap.put(start, end);
		}

		mSpace += aSize;
	}


	public synchronized void remove(int aOffset, int aSize)
	{
		if (aSize <= 0)
		{
			throw new IllegalArgumentException("Size is zero or negative: size: " + aSize);
		}
		if (aOffset < 0)
		{
			throw new IllegalArgumentException("Offset is negative: offset: " + aOffset);
		}

		int start = aOffset;
		int end = aOffset + aSize;

		Integer blockStart = mMap.floorKey(start);

		if (blockStart == null)
		{
			throw new IllegalArgumentException("No free block at offset: offset: " + start);
		}

		int blockEnd = mMap.get(blockStart);

		if (end > blockEnd)
		{
			throw new IllegalArgumentException("Block size shorter than requested size: remove: " + start + "-" + end + ", from block: " + blockStart + "-" + blockEnd);
		}

		boolean leftOver = start != blockStart;
		boolean rightOver = end != blockEnd;

		if (leftOver && rightOver)
		{
			mMap.put(blockStart, start);
			mMap.put(end, blockEnd);
		}
		else if (leftOver)
		{
			mMap.put(blockStart, start);
		}
		else if (rightOver)
		{
			mMap.remove(blockStart);
			mMap.put(end, blockEnd);
		}
		else
		{
			mMap.remove(blockStart);
		}

		mSpace -= aSize;
	}


	public synchronized int next(int aSize)
	{
		Entry<Integer, Integer> entry = mMap.firstEntry();

		for (;;)
		{
			if (entry == null)
			{
				return -1;
			}

			int offset = entry.getKey();

			if (entry.getValue() - offset >= aSize)
			{
				remove(offset, aSize);

				return offset;
			}

			entry = mMap.higherEntry(offset);
		}
	}


	public synchronized int getFreeSpace()
	{
		return mSpace;
	}


	public synchronized int getUsedSpace()
	{
		return mMap.lastEntry().getValue() - mSpace;
	}


	public synchronized boolean isFree(int aOffset, int aSize)
	{
		Integer blockStart = mMap.floorKey(aOffset);

		if (blockStart != null)
		{
			int blockEnd = mMap.get(blockStart) - 1;

			if (blockEnd >= aOffset + aSize || blockEnd >= aOffset)
			{
				return false;
			}
		}

		return true;
	}


	public synchronized void clear()
	{
		mMap.clear();
		mSpace = 0;
	}


	@Override
	public synchronized RangeMap clone()
	{
		RangeMap map;
		try
		{
			map = (RangeMap)super.clone();
		}
		catch (CloneNotSupportedException e)
		{
			map = new RangeMap();
		}

		map.mSpace = this.mSpace;
		map.mMap = (TreeMap<Integer,Integer>)this.mMap.clone();

		return map;
	}


	@Override
	public synchronized String toString()
	{
		StringBuilder sb = new StringBuilder("{");
		for (Integer key : mMap.keySet())
		{
			if (sb.length() > 1)
			{
				sb.append(", ");
			}
			sb.append(key + "-" + (mMap.get(key) - 1));
		}
		sb.append("}");
		return sb.toString();
	}


	public synchronized void read(DataInput aDataInput) throws IOException
	{
		int size = ByteArray.readVarInt(aDataInput);

		for (int i = 0, prev = 0; i < size; i++)
		{
			int count = ByteArray.readVarInt(aDataInput);

			prev += ByteArray.readVarInt(aDataInput);

			add(prev, count);
		}
	}


	public synchronized void write(DataOutput aDataOutput) throws IOException
	{
		int prev = 0;

		ByteArray.writeVarInt(aDataOutput, mMap.size());

		for (Entry<Integer,Integer> entry : mMap.entrySet())
		{
			int index = entry.getKey();

			ByteArray.writeVarInt(aDataOutput, entry.getValue() - index);
			ByteArray.writeVarInt(aDataOutput, index - prev);

			prev = index;
		}
	}


//	public static void main(String... args)
//	{
//		try
//		{
//			final RangeMap map = new RangeMap();
//
//			JFrame frame = new JFrame();
//			frame.add(new JPanel()
//			{
//				@Override
//				protected void paintComponent(Graphics g)
//				{
//					g.setColor(Color.WHITE);
//					g.fillRect(0,0,getWidth(),getHeight());
//					Color freeColor = new Color(240,240,240);
//					Color usedColor = new Color(240,0,0);
//
//					int s = 20;
//					int wi = getWidth()/s;
//					int hi = getHeight()/s;
//
//					for (int y = 0, i = 0; y < hi; y++)
//					{
//						for (int x = 0; x < wi; x++, i++)
//						{
//							g.setColor(map.contains(i,1) ? usedColor : freeColor);
//							g.fillRect(s*x+1, s*y+1, s-1, s-1);
//						}
//					}
//				}
//			});
//			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//			frame.setSize(1024, 768);
//			frame.setLocationRelativeTo(null);
//			frame.setVisible(true);
//
//			/* -x----*/ map.add(1, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* xx----*/ map.add(0, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* xxx---*/ map.add(2, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* xxx-x-*/ map.add(4, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* xxxxx-*/ map.add(3, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* -xxxx-*/ map.remove(0, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* -x-xx-*/ map.remove(2, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* ---xx-*/ map.remove(1, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* ---x--*/ map.remove(4, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* ------*/ map.remove(3, 1); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//			/* xxxxxx*/ map.add(0, 10000); frame.repaint(); Thread.sleep(1000); System.out.println(map.mMap);
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}