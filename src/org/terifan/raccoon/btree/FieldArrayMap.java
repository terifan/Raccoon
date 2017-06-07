package org.terifan.raccoon.btree;

import java.io.IOException;
import java.util.ArrayList;
import org.terifan.raccoon.core.ArrayMap;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.FieldReader;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class FieldArrayMap extends ArrayMap
{
	private ArrayList<FieldDescriptor> mFields;


	public FieldArrayMap(ArrayList<FieldDescriptor> aFields, int aCapacity)
	{
		super(aCapacity);
		
		mFields = aFields;
	}


	public FieldArrayMap(ArrayList<FieldDescriptor> aFields, byte[] aBuffer)
	{
		super(aBuffer);
		
		mFields = aFields;
	}


	public FieldArrayMap(ArrayList<FieldDescriptor> aFields, byte[] aBuffer, int aOffset, int aCapacity)
	{
		super(aBuffer, aOffset, aCapacity);
		
		mFields = aFields;
	}


	@Override
	public int indexOf(byte[] aKey)
	{
		try
		{
			ByteArrayBuffer in = new ByteArrayBuffer(aKey);
			Comparable[] keyValues = new Comparable[mFields.size()];

			for (int i = 0; i < keyValues.length; i++)
			{
				keyValues[i] = (Comparable)FieldReader.readField(mFields.get(i), in, true);
			}

			int low = 0;
			int high = mEntryCount - 1;

			while (low <= high)
			{
				int mid = (low + high) >>> 1;

				int cmp = compare(mFields, keyValues, mBuffer, mStartOffset + readKeyOffset(mid), readKeyLength(mid));

				if (cmp > 0)
				{
					low = mid + 1;
				}
				else if (cmp < 0)
				{
					high = mid - 1;
				}
				else
				{
					return mid; // key found
				}
			}

			return -(low + 1); // key not found
		}
		catch (IOException | ClassNotFoundException e)
		{
			throw new IllegalArgumentException(e);
		}
	}


	private int compare(ArrayList<FieldDescriptor> aFields, Comparable[] aKeyValues, byte[] aBufferB, int aOffsetB, int aLengthB) throws IOException, ClassNotFoundException
	{
		ByteArrayBuffer in = new ByteArrayBuffer(aBufferB).position(aOffsetB).limit(aLengthB);

		for (int i = 0; i < aFields.size(); i++)
		{
			int result = compare(aKeyValues[i], FieldReader.readField(aFields.get(i), in, true));

			if (result != 0)
			{
				return result;
			}	
		}
		
		return 0;
	}


	private int compare(Comparable aValue0, Object aValue1)
	{
		return aValue0.compareTo(aValue1);
	}
}
