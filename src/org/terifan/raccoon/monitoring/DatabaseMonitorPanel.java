package org.terifan.raccoon.monitoring;

import java.awt.BorderLayout;
import javax.swing.JPanel;
import org.terifan.raccoon.Database;


public class DatabaseMonitorPanel extends JPanel
{
	private final static long serialVersionUID = 1L;
	private transient Database mDatabase;


	public DatabaseMonitorPanel(Database aDatabase)
	{
		mDatabase = aDatabase;

		super.setLayout(new BorderLayout());
		super.add(new SpaceMapViewer(mDatabase), BorderLayout.CENTER);
	}


	public Database getDatabase()
	{
		return mDatabase;
	}


	public void setDatabase(Database aDatabase)
	{
		mDatabase = aDatabase;
	}
}

/*
package org.terifan.raccoon.monitoring;

import java.awt.BorderLayout;
import javax.swing.JPanel;
import org.terifan.raccoon.Database;


public abstract class DatabaseMonitorPanel extends JPanel
{
	private final static long serialVersionUID = 1L;

	protected transient Database mDatabase;


	public DatabaseMonitorPanel(Database aDatabase)
	{
		mDatabase = aDatabase;

		super.setLayout(new BorderLayout());
	}


	public Database getDatabase()
	{
		return mDatabase;
	}


	void setDatabase(Database aDatabase)
	{
		mDatabase = aDatabase;
	}


	abstract void updateView();
}

*/