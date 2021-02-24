package resources.entities;

import java.io.Serializable;
import org.terifan.raccoon.annotations.Id;


public class _BooleansK1 implements Serializable
{
	@Id public byte[] _id;
	public boolean[] value1;
	public boolean[][] value2;
	public boolean[][][] value3;


	public _BooleansK1()
	{
	}


	public _BooleansK1(byte[] aId, boolean[] aValue1, boolean[][] aValue2, boolean[][][] aValue3)
	{
		this._id = aId;
		this.value1 = aValue1;
		this.value2 = aValue2;
		this.value3 = aValue3;
	}
}