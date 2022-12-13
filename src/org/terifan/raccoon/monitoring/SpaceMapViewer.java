package org.terifan.raccoon.monitoring;

import java.awt.Color;
import java.awt.Graphics;
import javax.swing.JPanel;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.managed.RangeMap;


public class SpaceMapViewer extends JPanel
{
	private final static long serialVersionUID = 1L;

	private transient RaccoonDatabase mDatabase;


	public SpaceMapViewer(RaccoonDatabase aDatabase)
	{
		mDatabase = aDatabase;
	}


	@Override
	protected void paintComponent(Graphics aGraphics)
	{
//		ManagedBlockDevice blockDevice = (ManagedBlockDevice)mDatabase.getBlockDevice();
//		RangeMap rangeMap = blockDevice.getRangeMap();
//
//		int S = 7;
//
//		for (int y = 0, i = 0; y < 100; y++)
//		{
//			for (int x = 0; x < 100; x++, i++)
//			{
//				aGraphics.setColor(rangeMap.isFree(i, 1) ? Color.GREEN : Color.BLUE);
//				aGraphics.fillRect(x * S, y * S, S - 1, S - 1);
//			}
//		}
//
//		for (Table table : mDatabase.getTables())
//		{
//
//		}
	}
}

/*
package org.terifan.raccoon.monitoring;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.managed.RangeMap;


public class SpaceMapViewer extends DatabaseMonitorPanel
{
	private final static long serialVersionUID = 1L;

	private BufferedImage mImage;
	private int S = 7;


	public SpaceMapViewer(Database aDatabase)
	{
		super(aDatabase);

		mImage = new BufferedImage(S * 100, S * 100, BufferedImage.TYPE_INT_ARGB);
	}


	@Override
	protected void paintComponent(Graphics aGraphics)
	{
		super.paintComponent(aGraphics);

		aGraphics.drawImage(mImage, 0, 0, this);
	}


	@Override
	public void updateView()
	{
		Graphics2D g = mImage.createGraphics();

		ManagedBlockDevice blockDevice = (ManagedBlockDevice)mDatabase.getBlockDevice();
		RangeMap rangeMap = blockDevice.getRangeMap();

		for (int y = 0, i = 0; y < 100; y++)
		{
			for (int x = 0; x < 100; x++, i++)
			{
				g.setColor(rangeMap.isFree(i, 1) ? Color.GREEN : Color.BLUE);
				g.fillRect(x * S, y * S, S - 1, S - 1);
			}
		}

		g.dispose();

		repaint();
	}
}

*/
