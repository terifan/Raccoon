package org.terifan.raccoon.monitoring;

import org.terifan.raccoon.Database;


public class MonitorInstance implements AutoCloseable
{
	private DatabaseMonitorWindow mDatabaseMonitorWindow;
	private Database mDatabase;


	MonitorInstance(DatabaseMonitorWindow aDatabaseMonitorWindow, Database aDatabase)
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
