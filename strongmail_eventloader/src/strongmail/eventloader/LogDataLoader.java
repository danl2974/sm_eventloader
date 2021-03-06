package strongmail.eventloader;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;


public class LogDataLoader {
	

	//private static String mssqlConnection = "jdbc:sqlserver://10.157.95.101:1433;database=StrongMail;user=StrongMail;password=Str0ngMai!;";
	private static String mssqlConnection = "jdbc:sqlserver://k22cep04af.database.windows.net:1433;database=email_engine;user=emailengine@k22cep04af;password=!Welco2200;encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
	private static String dbhost = "jdbc:sqlserver://10.157.95.101:1433;databaseName=EmailDataWarehouse";
	private static String dbusername = "StrongMail";
	private static String dbpassword = "Str0ngMai!";
	

	
	static public void loadData(EventRecordHealthAdvisor er){
		
		CallableStatement callableStatement = null;
		Connection con = null;
		
		try{
		  //String sproc = "EXECUTE StrongMail.dbo.HealthAdvisor_EmailRequestEvent_Add ?,?,?,?";
		  //String insert = "INSERT INTO [EmailDataWarehouse].[Email].[MailingInfo] ( [MailingId],[Mailing],[DepartmentId],[Department] ) VALUES( ?, ?, ?, ? )";
		  //String sproc = "{call dbo.HealthAdvisor_EmailRequestEvent_Add(?,?,?,?)}";
		  //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		  //Class.forName("net.sourceforge.jtds.jdbc.Driver");
		  //con = DriverManager.getConnection(dbhost, dbusername, dbpassword);
		  //con = DriverManager.getConnection(mssqlConnection);
			
		   Class.forName("org.postgresql.Driver");
  	       String jdbcFormat = String.format("jdbc:%s://%s:%s/%s", "postgresql", "64.213.211.240", 5432, "postgres");
  	       con = DriverManager.getConnection(jdbcFormat, "postgres", "str0ngmail");
  	       String select = "select * from mailing order by modified_time desc";
 		   Statement stmt = con.createStatement();
 		   ResultSet rs = stmt.executeQuery(select);
 		   while (rs.next()) {
 			  
			  System.out.println("RS " + rs.getString("name"));
			  
		  }
		  
		  
		  
		  /*
		  String query = "select * from [dbo].[HealthAdvisor_EmailRequestEvent]";
		  Statement stmt = con.createStatement();
		  System.out.println("inside loadData before Query");
		  ResultSet rs = stmt.executeQuery(query);
		  System.out.println("inside loadData after Query "); 
		  System.out.println(String.format("sql warning code %d", rs.getStatement().getWarnings().getErrorCode()) );
		  System.out.println("RS object " + String.valueOf( rs.getMetaData().getColumnCount() ) );
		  
		  while (rs.next()) {
		  
			  System.out.println("RS " + rs.getString("EMAIL") + rs.getString("MAILINGID"));
			  
		  }
		  */
		  /*
		  System.out.println("connection prodname " + con.getMetaData().getDatabaseProductVersion() + " " + String.valueOf(con.getMetaData().getCatalogs().getMetaData().getColumnCount()));
		  PreparedStatement pstmt = con.prepareStatement(insert);
		  //pstmt.setTimestamp(1, er.timestamp);
		  pstmt.setInt(1, 99999);
		  pstmt.setString(2, "Mailing000");
		  pstmt.setInt(3, 99999);
		  pstmt.setString(4, "Department000");
	      int callresult = pstmt.executeUpdate();
	      con.commit();
	      System.out.println("call " + String.format("%s", callresult));
		  */
		  /*
		  callableStatement = con.prepareCall(sproc);
		      
		      callableStatement.setTimestamp(1, er.timestamp);
		      //callableStatement.setString(1, er.timestamp);
		      callableStatement.setString(2, er.emailAddress);
		      callableStatement.setString(3, er.mailingId);
		      callableStatement.setString(4, er.campaignId);
		      int callresult = callableStatement.executeUpdate();
		      con.commit();
		      
		      */
		      /*
		      ResultSet rs = con.getMetaData().getProcedures(null, null, null);
		      while (rs.next()) {
		    	  System.out.println("Procedures " + rs.getString("PROCEDURE_NAME"));
		      }
		      */
		      
		      //System.out.println("call sproc " + String.format("%s %s %s %s %s", String.valueOf(callresult), String.valueOf(er.timestamp),  er.emailAddress,  er.mailingId, er.campaignId ));
		}
		catch(SQLException sqle){
			System.out.println(String.format("SQL EXCEPTION: loadData %s %d %s", sqle.getLocalizedMessage(), sqle.getErrorCode(), sqle.getSQLState() ) );
			sqle.printStackTrace();
		}
		catch(Exception e){
			System.out.println("EXCEPTION: loadData " + e.getMessage());
		}
                finally{
                     if (callableStatement != null){
                       try {
						callableStatement.close();
					    } catch (SQLException e) {}
                      }
                     if (con != null){
                       try {
						con.close();
					    } catch (SQLException e) {}
                      }

                  }
             
	}
	

	
	
}
