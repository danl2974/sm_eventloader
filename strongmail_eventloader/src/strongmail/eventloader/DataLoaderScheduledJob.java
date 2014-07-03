package strongmail.eventloader;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;


public class DataLoaderScheduledJob implements Job {

	
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
	  if (!checkIfExecuting()){
		try{
		  System.out.println("\n\nJOB STARTED AT: " + new Date().toString());
		  LogFileParser lfp = new LogFileParser();
		  lfp.process();		  
		  System.out.println("\n\nJOB ENDED AT: " + new Date().toString());
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
		
	    }
	  
	    else{
	    	System.out.println("JOB SKIPPED");
	      }
	        
       }
	
	
	 private boolean checkIfExecuting(){
	    	
	    	Scheduler scheduler;
	    	int count = 0;
			try {
				scheduler = new StdSchedulerFactory().getScheduler();
	    	    List<JobExecutionContext> exejobs = scheduler.getCurrentlyExecutingJobs();
	    	    
	    	    for (JobExecutionContext jec : exejobs){
	    		          if(jec.getJobDetail().getKey().getName() == "eventloader"){
                                count ++;
	    		          }
	    	          }
	    	
			} catch (SchedulerException e) {
				System.out.println("Scheduler Exception" + e.getMessage());
				return false;
			}
	    	
			if(count > 1){
	        	System.out.println("Datalogloader still executing. Skipping Job");
	        	return true;
			}
			else{
				return false;
			}
			
	    	
	    }
		

}


