package strongmail.eventloader;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;



public class LogFileParser {
		
	private static final String LOG_PATH = "/usr/local/share/eventdrop/event.log";
	
	private FileReader fr;
	private BufferedReader br;
	
	private Jedis redis;

	public LogFileParser(){
		    try{
			this.fr = new FileReader(LOG_PATH);
		    this.br = new BufferedReader(fr);
		    }catch(Exception e){System.out.println("Constructor Exception: " + e.getMessage());}

	}
		
	public void process(){
		
		LogDataLoader.loadData(new EventRecordHealthAdvisor());
		
		/*
	
		ArrayList<EventRecordHealthAdvisor> recordList = new ArrayList<EventRecordHealthAdvisor>();
		String progress = ProgressRegister.readLast();
		System.out.println("Record progress " + progress);
		
		try{

		   String sLine;
		   while ((sLine = this.br.readLine()) != null){
			   
			   String [] fields = sLine.split("::");
			   EventRecordHealthAdvisor eventRecord = new EventRecordHealthAdvisor();
			   eventRecord.logLine = sLine;
			   eventRecord.timestamp = Timestamp.valueOf(fields[0]);
			   //eventRecord.timestamp = fields[0];
			   eventRecord.emailAddress = fields[1];
			   eventRecord.mailingId = fields[2];
			   eventRecord.campaignId = fields[3];
			   recordList.add(eventRecord);
			   
			   if(progress.indexOf(sLine) >= 0)
			   {
				   recordList.clear();
				   System.out.println("recordList.clear() called");
			   }

		   }
		   System.out.println("recordList size " +  String.valueOf(recordList.size()) );
		   if (recordList.size() > 0){
			   
			   int lastIndex = -1;
			   try{
			    	for (int i = 0; i < recordList.size(); i++)
			    	{
			    		LogDataLoader.loadData(recordList.get(i));
			    		lastIndex = i;
			    	}
			    	
			      }
			      catch(Exception ex){System.out.println("Exception calling uploads: " + ex.getMessage());}
			   
			     ProgressRegister.writeLast(recordList.get(lastIndex).logLine);
		   }
		        	      

		  }
		  catch(Exception ex){System.out.println("Exception Processing Logs: " + ex.getMessage());}

           System.out.println("Records Processed: " + recordList.size());

*/
        } 

		
	private static boolean checkProgress(String line, String progress){

		int found = line.indexOf(progress);
		if (found > 0){
			
			return true;
		}
		else{
			
		    return false;
		}
	} 
	
	
	
	//jedis.exists(mtaId) -------- use in process methods
	
	private HashMap<String,String> fetchFromRedis(String mtaId){
		
		HashMap<String,String> hm = new HashMap<String,String>();
		
		try{
		List<String> hmVals  = this.redis.hmget(mtaId, new String[] {"templateId","uuid"});
		hm.put("templateId", hmVals.get(0));
		hm.put("uuid", hmVals.get(1));
		return hm;
		}
		catch (Exception e){
			System.out.println("Exception fetchFromRedis: " + e.getMessage());
			return hm;
			}
		
	}	
	
	private static HashMap<String,String> fetchFromRedis(Jedis redis, String mtaId){
		
		HashMap<String,String> hm = new HashMap<String,String>();
		
		try{
		List<String> hmVals  = redis.hmget(mtaId, new String[] {"templateId","uuid"});
		hm.put("templateId", hmVals.get(0));
		hm.put("uuid", hmVals.get(1));
		return hm;
		}
		catch (Exception e){
			System.out.println("Exception fetchFromRedis: " + e.getMessage());
			return hm;
			}
		
	}
	
	
    private static void addToRedis(String mtaId, HashMap<String,String> embeddedTemplateUuid)
    {
        try{	
           Jedis jedis = new Jedis("localhost");
           jedis.hmset(mtaId, embeddedTemplateUuid);
           jedis.expire(mtaId, 86400);
           }
        catch (Exception e){
           System.out.println("Exception addToRedis: " + e.getMessage());
        }
      
    }
    

	
}

