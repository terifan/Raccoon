package resources.entities;

import java.io.Serializable;
import org.terifan.raccoon.Key;


public class _ArrayK1 implements Serializable
{
	@Key public byte[] _id;
	public byte[] value1;
	public byte[][] value2;
	public byte[][][] value3;


	public _ArrayK1()
	{
	}


	public _ArrayK1(byte[] aId, byte[] aValue1, byte[][] aValue2, byte[][][] aValue3)
	{
		this._id = aId;
		this.value1 = aValue1;
		this.value2 = aValue2;
		this.value3 = aValue3;
	}
}