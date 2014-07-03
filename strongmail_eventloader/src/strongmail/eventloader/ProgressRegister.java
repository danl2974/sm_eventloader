package strongmail.eventloader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class ProgressRegister {
	

		
	public static String readLast(){
		
		StringBuilder sb = new StringBuilder();
		
		try{
			File file = new File("/usr/local/share/eventdrop/progress.log");
			if (!file.exists()) {
				file.createNewFile();
			}
			FileReader fr = new FileReader(file.getAbsoluteFile());
			BufferedReader br = new BufferedReader(fr);
			String sLine;
			while ((sLine = br.readLine()) != null) {
				sb.append(sLine);
			}
			br.close();
		}
		catch(Exception e){
		  	
		}
		System.out.println("Last Update Read: " + sb.toString());
		return sb.toString();
		
	}
	
	public static boolean writeLast(String progressInfo){
		
		try{
			File file = new File("/usr/local/share/eventdrop/progress.log");
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile(), false);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(progressInfo);
			bw.close();
			System.out.println("Last Update Write: " + progressInfo);
			
			return true;
		}
		catch(Exception e){
			return false;
		}
		
	}
	
	


}
