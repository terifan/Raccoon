package org.terifan.raccoon.monitoring;

import org.terifan.raccoon.RaccoonDatabase;


public class MonitorInstance implements AutoCloseable
{
	private DatabaseMonitorWindow mDatabaseMonitorWindow;
	private RaccoonDatabase mDatabase;


	MonitorInstance(DatabaseMonitorWindow aDatabaseMonitorWindow, RaccoonDatabase aDatabase)
	{
		mDatabaseMonitorWindow = aDatabaseMonitorWindow;
		mDatabase = aDatabase;
	}


	@Override
	public void close() throws Exception
	{
		mDatabaseMonitorWindow.detach(mDatabase);
		mDatabaseMonitorWindow = null;
	}


	public void update()
	{
		mDatabaseMonitorWindow.update();
	}
}
