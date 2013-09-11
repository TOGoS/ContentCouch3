package togos.ccouch3.download;

import java.util.Iterator;

import togos.ccouch3.TaskCounter;

public class Downloader
{
	public static int downloadMain( Iterator<String> args )
		throws InterruptedException
	{
		TaskCounter<Object> taskCounter = new TaskCounter<Object>();
		
		taskCounter.waitUntilEmpty();
		return 0;
	}
}
