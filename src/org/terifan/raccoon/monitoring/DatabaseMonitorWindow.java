package org.terifan.raccoon.monitoring;

import java.util.HashMap;
import javax.swing.JFrame;
import javax.swing.JTabbedPane;
import org.terifan.raccoon.RaccoonDatabase;


public class DatabaseMonitorWindow extends JFrame
{
	private final static long serialVersionUID = 1L;
	private JTabbedPane mTabbedPane;
	private HashMap<RaccoonDatabase, DatabaseMonitorPanel> mPanels;


	public DatabaseMonitorWindow()
	{
		mTabbedPane = new JTabbedPane();
		mPanels = new HashMap<>();

		super.add(mTabbedPane);
		super.setSize(1024, 768);
		super.setLocationRelativeTo(null);
		super.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		super.setVisible(true);
	}


	public MonitorInstance attach(RaccoonDatabase aDatabase)
	{
		DatabaseMonitorPanel panel = new DatabaseMonitorPanel(aDatabase);
		mPanels.put(aDatabase, panel);
		mTabbedPane.add(""+aDatabase, panel);
		return new MonitorInstance(this, aDatabase);
	}


	public void detach(RaccoonDatabase aDatabase)
	{
		mTabbedPane.remove(mPanels.remove(aDatabase));
	}


	void update()
	{
		repaint();
	}
}



/*
package org.terifan.raccoon.monitoring;

import java.util.HashMap;
import javax.swing.JFrame;
import javax.swing.JTabbedPane;
import org.terifan.raccoon.Database;


public class DatabaseMonitorWindow extends JFrame
{
	private final static long serialVersionUID = 1L;
	private JTabbedPane mTabbedPane;
	private HashMap<Database, DatabaseMonitorPanel> mPanels;
	private int mCounter;


	public DatabaseMonitorWindow()
	{
		mTabbedPane = new JTabbedPane();
		mPanels = new HashMap<>();

		super.add(mTabbedPane);
		super.setSize(1024, 768);
		super.setLocationRelativeTo(null);
		super.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		super.setVisible(true);
	}


	public MonitorInstance attach(Database aDatabase)
	{
		DatabaseMonitorPanel panel = new SpaceMapViewer(aDatabase);

		mPanels.put(aDatabase, panel);
		mTabbedPane.add("" + ++mCounter, panel);

		return new MonitorInstance(this, aDatabase);
	}


	public void detach(Database aDatabase)
	{
		mPanels.get(aDatabase).setDatabase(null);
//		mTabbedPane.remove(mPanels.remove(aDatabase));
	}


	void update()
	{
		for (DatabaseMonitorPanel panel : mPanels.values())
		{
			panel.updateView();
		}
	}
}

*/