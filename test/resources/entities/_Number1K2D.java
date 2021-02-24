package resources.entities;

import java.io.Serializable;
import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Id;


public class _Number1K2D implements Serializable
{
	@Discriminator public int _disc1;
	@Discriminator public int _disc2;
	@Id public int _id;
	public String name;


	public _Number1K2D()
	{
	}


	public _Number1K2D(int aDisc1, int aDisc2, int aId, String aName)
	{
		_disc1 = aDisc1;
		_disc2 = aDisc2;
		_id = aId;
		name = aName;
	}


	@Override
	public String toString()
	{
		return "_Number1K2D{" + "_disc1=" + _disc1 + ", _disc2=" + _disc2 + ", _id=" + _id + ", name=" + name + '}';
	}
}
