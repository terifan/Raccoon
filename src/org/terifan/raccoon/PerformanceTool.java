package org.terifan.raccoon;

import java.util.ArrayDeque;
import org.terifan.ganttchart.GanttChart;


public class PerformanceTool implements OpenParam
{
	private final GanttChart mChart;
	
	private ArrayDeque<String> mStack;
	
	
	public PerformanceTool(GanttChart aChart)
	{
		mChart = aChart;
		mStack = new ArrayDeque<>();
	}
	
	
	public boolean enter(Object aCaller, String aTag, String aDescription)
	{
		if (mChart != null)
		{
			mStack.add(aTag);

			mChart.enter(aDescription);
		}
		
		return true;
	}
	
	
	public boolean exit(Object aCaller, String aTag)
	{
		if (mChart != null)
		{
			mChart.exit();

			return mStack.removeLast().equals(aTag);
		}

		return true;
	}
	
	
	public boolean tick(String aDescription)
	{
		if (mChart != null)
		{
			mChart.tick(aDescription);
		}

		return true;
	}
}
