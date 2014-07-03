package strongmail.eventloader;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;


public class InitLoader {

	
	public static void main(String args[]){
		 
		 try{
			System.out.println("Init Loader main called\n\n");
			
			String interval = args[0];
			int triggerInterval = interval != null ? Integer.valueOf(interval) : 5;
			 
		    SchedulerFactory sf = new StdSchedulerFactory();
		    Scheduler scheduler = sf.getScheduler();
		    scheduler.start();
		 
		    JobDetail job = JobBuilder.newJob(DataLoaderScheduledJob.class)
				    .withIdentity("eventloader", "eventloaderjob")
				    .build();

		    Trigger trigger = TriggerBuilder.newTrigger()
		    	    .withIdentity("eventloader", "eventloadertrigger")
		    	    .startNow()
		    	    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
		    	            .withIntervalInMinutes(triggerInterval)
		    	            .repeatForever())
		    	    .build();

	        scheduler.scheduleJob(job, trigger);
		 }
		 catch(Exception e){
			 System.out.println(e.getMessage());
		 }
		
	}

}
