package resources.entities;

import java.util.Calendar;


public class _Object1K
{
	public String _name;
	public Calendar calendar;


	public _Object1K()
	{
	}


	public _Object1K(String aName)
	{
		_name = aName;
	}


	public _Object1K(String aName, Calendar aCalendar)
	{
		_name = aName;
		calendar = aCalendar;
	}


	@Override
	public String toString()
	{
		return _name + ", " + calendar;
	}
}