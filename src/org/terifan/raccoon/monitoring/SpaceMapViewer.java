package org.terifan.raccoon.monitoring;

import java.awt.Color;
import java.awt.Graphics;
import javax.swing.JPanel;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.managed.RangeMap;


public class SpaceMapViewer extends JPanel
{
	private final static long serialVersionUID = 1L;

	private transient Database mDatabase;


	public SpaceMapViewer(Database aDatabase)
	{
		mDatabase = aDatabase;
	}


	@Override
	protected void paintComponent(Graphics aGraphics)
	{
		ManagedBlockDevice blockDevice = (ManagedBlockDevice)mDatabase.getBlockDevice();
		RangeMap rangeMap = blockDevice.getRangeMap();

		int S = 7;

		for (int y = 0, i = 0; y < 100; y++)
		{
			for (int x = 0; x < 100; x++, i++)
			{
				aGraphics.setColor(rangeMap.isFree(i, 1) ? Color.GREEN : Color.BLUE);
				aGraphics.fillRect(x*S, y*S, S-1, S-1);
			}
		}
	}
}
