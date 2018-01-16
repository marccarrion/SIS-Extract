package com.starfish.sisintegration;

import java.util.Date;

// Colleague
import edu.fresno.uniobjects.*;
import edu.fresno.uniobjects.data.*;
import edu.fresno.uniobjects.exceptions.*;

import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniDictionary;
import asjava.uniobjects.UniFileException;
import asjava.uniobjects.UniSessionException;
import asjava.uniclientlibs.UniString;

import asjava.uniobjects.UniCommandException;

public class TestColleageRPC {
        static UniDataConnection uniData;
   
        public static void runQuery(String query) throws Exception { 
                     System.out.println("Running: " + query);
                     System.out.println("START " + new Date());
                     String response = uniData.query(query);
                     System.out.println("END " + new Date());
                     System.out.println(response);
		     System.out.println("Colleague connection test completed: SUCCESS");
                     System.out.println();
                     System.out.println();
                     System.out.println();
        }
  
	public static void main(String[] args) {
                String username = args[0];
                String password = args[1];
                String connection = args[2];
                String home = args[3];
                String query1 = "SELECT STUDENT.ACAD.CRED WITH STC.CURRENT.STATUS EQ 'A' SAVING UNIQUE STC.PERSON.ID";
                String query2 = "SELECT STUDENT.ACAD.CRED WITH EVAL 'TRANS(\"TERMS\",STC.TERM,TERM.START.DATE,\"X\")' GT EVAL 'DATE()-14610' AND EVAL 'TRANS(\"TERMS\",STC.TERM,TERM.END.DATE,\"X\")' LT EVAL 'DATE()+365' AND ( STC.CURRENT.STATUS EQ 'A' OR STC.CURRENT.STATUS EQ 'N' OR STC.CURRENT.STATUS = 'D' ) SAVING UNIQUE STC.PERSON.ID";
                // String query2 = "SELECT STUDENT.ACAD.CRED WITH EVAL 'TRANS(\"TERMS\",STC.TERM,TERM.START.DATE,\"X\")' GT EVAL 'DATE()-14610' AND ( STC.CURRENT.STATUS EQ 'A' OR STC.CURRENT.STATUS EQ 'N' OR STC.CURRENT.STATUS = 'D' ) SAVING UNIQUE STC.PERSON.ID";
                String query3_1 = "GET.LIST STARFISH.TRANSCRIPT.HISTORY";
                String query3_2 = "SELECT STUDENT.ACAD.CRED SAVING UNIQUE STC.COURSE.SECTION";

                try {
                     uniData = new UniDataConnection(username, password, connection, home);
                     uniData.connect();
      
                     System.out.println("Using UniData SDK version " + uniData.UniJava().getVersionNumber());
                     System.out.println("Connection number " + uniData.UniJava().getNumSessions() + " of " + uniData.UniJava().getMaxSessions());
      
                     String response;
                     response = uniData.query("CLEARSELECT");
                     runQuery(query1); 
                     response = uniData.query("CLEARSELECT");
                     runQuery(query2); 
                     response = uniData.query("CLEARSELECT");
                     runQuery(query3_1); 
                     runQuery(query3_2); 

                } catch (Exception e) {
                     System.out.println("ERROR " + new Date());
                     System.out.println("   ERROR:UniCommandException:" + e.getMessage() + "\n");
		     System.out.println("Colleague connection test completed: ERROR");
                }
         }
}
