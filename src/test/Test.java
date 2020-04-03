package test;

import java.awt.Dimension;
import org.terifan.ganttchart.GanttChart;
import org.terifan.ganttchart.SimpleGanttWindow;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.PerformanceTool;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			GanttChart chart = new GanttChart();

			new SimpleGanttWindow(chart).show();

			long t = System.nanoTime();

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION, new TableParam(1, 1, 0), new PerformanceTool(chart)))
			{
				for (int i = 0; i < 1000; i++)
				{
					db.save(new MyEntity(i, -i, "01234567890123456789"));
				}

				db.commit();
			}
			
			System.out.println((System.nanoTime() - t)/1000000.0);

//			try (Database db = new Database(blockDevice, OpenOption.OPEN))
//			{
//				db.list(MyEntity.class).forEach(System.out::println);
//			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	static class MyEntity
	{
		@Key Integer id1;
		@Key Integer id2;
		String name;
		Dimension dim;

		public MyEntity()
		{
		}

		public MyEntity(Integer aId1, Integer aId2, String aName)
		{
			this.id1 = aId1;
			this.id2 = aId2;
			this.name = aName;
			dim = new Dimension(aId1,aId2);
		}


		@Override
		public String toString()
		{
			return "MyEntity{" + "id1=" + id1 + ", id2=" + id2 + ", name=" + name + ", dim=" + dim + '}';
		}
	}
}
