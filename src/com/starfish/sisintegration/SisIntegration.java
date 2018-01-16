package com.starfish.sisintegration;

import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.io.File;
import java.io.FilenameFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Arrays;
import java.util.Map;
import java.util.Iterator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import java.io.FileWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
// Java 1.8 
// java.util.Base64
import org.apache.commons.codec.binary.Base64;

import java.security.*;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;


import org.apache.log4j.Logger;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

// display time and date using toString()
import java.util.Date;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;

import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;

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

import oracle.jdbc.OracleDriver;


public class SisIntegration implements Job {
        final static Logger log = Logger.getLogger(SisIntegration.class.getName());

        static String[] params_1 = { "-verbose", "-encrypt", "-help", "-testdb", "-schedule", "-onetime", "-validate"};
        static String[] params_2 = { "-config" };
        static UniDataConnection uniData;
	static Connection dbConn = null;
        static String passwordDecoded = null;
	static Statement dbStmt = null;
	static ResultSet dbResultset = null;
        static Boolean dbSuccess = null;
        static Properties properties = new Properties();
        // static final String NEW_LINE_SEPARATOR = "\n";
        // private static final String ALGO = "AES";
        private static final byte[] keyValue = new byte[] { 'T', 'h', 'e', 'B', 
                                                            'e', 's', 't', 'S', 
                                                            'e', 'c', 'r','e', 
                                                            't', 'K', 'e', 'y' };
    
        public static void createFile(String termStr, String fileStr) throws Exception {
             String fileGenStr = properties.getProperty(fileStr + ".class");
             // System.out.println(fileGenStr + "!!!!!!!!!!!!!!!!!!!!");
            log.info("Creating File " + fileStr );
            log.debug("Using " + fileGenStr + " for " + fileStr);

             FileGenerator fileGen;
             if (fileGenStr==null) {
                   fileGenStr = "com.starfish.sisintegration.BasicFileGenerator";
                   // System.out.println("MCC 06282016 Using basic file generator");
                   BasicFileGenerator test_bfg = new BasicFileGenerator();
                   // System.out.println("MCC 06282016 Direct creation works fine");
                   Class c = Class.forName(fileGenStr);
                   // System.out.println("MCC 06282016 without newInstance()");
                   fileGen = (FileGenerator)(c.newInstance());
                   // System.out.println("MCC 06282016 after newInstance()");
             }
             // System.out.println("MCC 06282016 selecting the class " + fileGenStr );
             fileGen = (FileGenerator)(Class.forName(fileGenStr).newInstance());
             // System.out.println("MCC 06282016 class selected " + fileGen.getClass().getName());
             // System.out.println(fileGen.getClass().getName());
             // System.out.println("SIS property: " + properties.getProperty("SIS"));
             log.debug("Using SIS " + properties.getProperty("SIS"));
             if (properties.getProperty("SIS").equals("Colleague")) {
                // System.out.println("MCC 06282016 using colleague?");
                fileGen.createFile(uniData, termStr, fileStr, properties); 
             } else {
                // System.out.println("MCC 06282016 Passing dbConn to the subroutine");
                fileGen.createFile(dbConn, termStr, fileStr, properties); 
             }
             // System.out.println("MCC 06282016 out of create file subroutine");
            log.info("File " + fileStr + " Finished");
        }
             
	public static void main(String[] args) throws SQLException, IOException, Exception, SchedulerException {
                HashMap argshm = new HashMap();
                for (int i = 0; i < args.length; i++) {
                    switch (args[i].charAt(0)) {
                        case '-':
                            if (args[i].length() < 2)
                               throw new IllegalArgumentException("Not a valid argument: "+args[i]);
                            else {
                               if ( args[i].equals("-help") ) {
                                   System.out.println("SIS Integration");
                                   System.out.println("Usage:");
                                   System.out.println("  java -cp $CP com.starfish.sisintegration.SisIntegration <params>");
                                   System.out.println("Params:");
                                   System.out.println("  -help: Show this message");
                                   System.out.println("  -verbose: Add extra System.out log");
                                   System.out.println("  -encrypt: Encrypt the password provided to use in the config xml to connect to the DB");
                                   System.out.println("  -testdb: Connect to the DB and check validation script");
                                   System.out.println("  -config <filename>: Use the filename instead of sisconfig.xml for configuration");
                                   System.out.println("  -schedule: Run the integration on the scheduled intervals");
                                   System.out.println("  -onetime: Run the integration one time only (not implemented)");
                                   System.exit(1);
                               }
                               if ( args[i].equals("-config") ) {
                                   if ( args.length-1 == i)
                                      throw new IllegalArgumentException("Expected arg after: "+args[i]);
                                   argshm.put(args[i], args[i+1]);
                                   i++;
                               } else {
                                   argshm.put(args[i], "");
                               }
                            }
                            break;
                        default:
                            // arg
                            argshm.put(args[i],"");
                            break;
                    }
                }

		try {
			String filename = (String)argshm.get("-config");
                        if ( filename==null ) filename = "sisconfig.xml";
			File file = new File(filename);
			FileInputStream fileInput = new FileInputStream(file);
                        // Properties properties = new Properties();
			properties.loadFromXML(fileInput);
			fileInput.close();

			file = new File(properties.getProperty("AdapterConfig"));
			fileInput = new FileInputStream(file);
			properties.loadFromXML(fileInput);
			fileInput.close();

                        if ( argshm.get("-verbose")!=null ) {
			   Enumeration enuKeys = properties.keys();
			   while (enuKeys.hasMoreElements()) {
				String key = (String) enuKeys.nextElement();
				String value = properties.getProperty(key);
				if (!key.equals("DBPassword") && !key.equals("LicenseKey")) System.out.println(key + ": " + value); 
				else System.out.println(key + ": " + "***********************"); 
                           }
                        }
			   Enumeration enuKeys = properties.keys();
			   while (enuKeys.hasMoreElements()) {
				String key = (String) enuKeys.nextElement();
				String value = properties.getProperty(key);
				if (!key.equals("DBPassword") && !key.equals("LicenseKey")) log.debug(key + ": " + value); 
				else log.debug(key + ": " + "***********************"); 
                           }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
                        log.error(e);
		} catch (IOException e) {
			e.printStackTrace();
                        log.error(e);
		}

                String license = properties.getProperty("LicenseKey");
                byte[] licenseArr = license.getBytes();
                byte[] licenseArr16 = Arrays.copyOfRange(licenseArr, 0, 16);
                // Execute only based on params, and then exit, just to get the key initially 
                if ( argshm.get("-encrypt")!=null )
		    System.out.println("-------- Generate Key  ------");
                String password = properties.getProperty("DBPassword");
                String passwordEnc1 = Utils.encrypt(password,keyValue);
                String passwordEnc = Utils.encrypt(passwordEnc1,licenseArr16);
                String passwordDec1 = Utils.decrypt(passwordEnc,licenseArr16);
                String passwordDec = Utils.decrypt(passwordDec1,keyValue);
       
                /* Show only based on parameters */
                if ( argshm.get("-encrypt")!=null ) {
                    System.out.println("Plain Text : " + password);
                    System.out.println("Encrypted Text : " + passwordEnc);
                    System.out.println("Decrypted Text : " + passwordDec);
                    System.exit(1);
                }
  

                if (properties.getProperty("SIS").equals("Colleague")) {
                      String passwordEncoded = properties.getProperty("DBPassword");
                      String passwordAux = Utils.decrypt(passwordEncoded,licenseArr16);
                      String passwordDecoded = Utils.decrypt(passwordAux,keyValue);
                      uniData = new UniDataConnection(properties.getProperty("DBUsername"),
                                                     passwordDecoded,
                                                     properties.getProperty("DBConnection"),
                                                     properties.getProperty("DBHome"));

                      uniData.connect();
                      log.info("Using UniData SDK version " + uniData.UniJava().getVersionNumber());
                      log.info("Connection number " + uniData.UniJava().getNumSessions() + " of " + uniData.UniJava().getMaxSessions());
// TODO
                      String response;
                      response = uniData.query("CLEARSELECT");
                      response = uniData.query(properties.getProperty("ValidateConnection"));
                      log.debug(response);
		      log.info("Colleague connection test completed.");
                      response = uniData.query("CLEARSELECT");
                      response = uniData.query(properties.getProperty("ValidateAccess"));
                      log.debug(response);
                      response = uniData.query("CLEARSELECT");
 		      log.info("Colleague access test completed.");

                } else {
		   log.info("-------- Testing JDBC Connectivity  ------");
 /*
		   try {
			// Returns the Class object associated with the class
			Class.forName(properties.getProperty("DBDriver"));
		   } catch (ClassNotFoundException exception) {
			System.out.println("Driver Class Not found Exception: " + exception.toString());
			return;
		   }
*/
                   // This DB connection may be needed at the Job level, not the whole app 
		   System.out.println("JDBC Driver Successfully Registered!");
		   try {
		        DriverManager.setLoginTimeout(5);
                        DriverManager.registerDriver(new OracleDriver());
			// Attempts to establish a connection
			// here DB name: localhost, sid: crunchify
			// dbConn = DriverManager.getConnection("jdbc:h2:~/test/db/TEST", "sa", "sa");
			// dbConn = DriverManager.getConnection("jdbc:h2:ssl://localhost:8955/~/test/db/TEST", "sa", "sa");
                        // "jdbc:h2:ssl://localhost:9092/~/test/db/TEST", "sa", "sa"
                        String passwordEncoded = properties.getProperty("DBPassword");
		        // System.out.println("-------- " + passwordEncoded + " ------ " + licenseArr16 + " ----------");
                        String passwordAux = Utils.decrypt(passwordEncoded,licenseArr16);
                        // String passwordDecoded = Utils.decrypt(passwordAux,keyValue);
                        passwordDecoded = Utils.decrypt(passwordAux,keyValue);
                        if ( argshm.get("-verbose")!=null ) {
                              System.out.println("Decrypted Text : " + passwordDecoded);
                        }
		        // System.out.println("-------- " + passwordDecoded + " ------");
                        // System.exit(1);
// MCC 20170201 This was commented out, we need it back in, and we need a way to recreate it if dropped
                        System.out.println("----------------- FIRST CONNECTION ----------------------------");
			dbConn = DriverManager.getConnection(properties.getProperty("DBConnection"),
			                                     properties.getProperty("DBUsername"),
			                                     passwordDecoded);
		   } catch (SQLException e) {
			log.error("Connection Failed! Check output console");
			log.error(e);
                        e.printStackTrace();
			return;
		   }

		   // Creates a Statement object for sending SQL statements to the database
		   dbStmt = dbConn.createStatement();
 
		   // Executes the given SQL statement, which returns a single ResultSet object
		   dbResultset = dbStmt.executeQuery(properties.getProperty("ValidateConnection"));
		   if (dbResultset.next()) {
			log.debug("Current Time: " + dbResultset.getDate(1).toString());
		   } else {
			throw new SQLException("Can NOT retrieve Current Time");
		   }
		   log.info("JDBC connection test completed.");
 
		   // Executes the given SQL commands which don't return anything
                   try {
		      dbSuccess = dbStmt.execute(properties.getProperty("ValidateAccess"));
                   } catch (SQLException e) {
                      log.error(e);
                      e.printStackTrace();
	              throw new SQLException("Can NOT validate proper access");
                   }
		   log.info("JDBC access test completed.");
                }


                if ( argshm.get("-testdb")!=null )
                       System.exit(1);

                // Run Single execution or configure Quartz here
                // specify the job' s details..
                JobDetail job = JobBuilder.newJob(SisIntegration.class)
                                          .withIdentity("SisIntegration")
                                          .build();
                
		Trigger trigger; 
                if ( argshm.get("-schedule")!=null ) {
                         // specify the running period of the job
                         // http://quartz-scheduler.org/api/2.2.0/org/quartz/CronExpression.html
                         log.info("Setting the followin schedule: " + properties.getProperty("Schedule"));
                         trigger = TriggerBuilder.newTrigger()
                                                 .withSchedule(CronScheduleBuilder.cronSchedule((String)properties.getProperty("Schedule")))
                                                 .build();

                // } else if ( argshm.get("-onetime") != null ) {
                } else { // Default to onetime
                         // Need to find a way to stop the single execution
                         log.info("Configure a simple execution");
                         trigger = TriggerBuilder.newTrigger().build();
                }
                // Pass properties
                job.getJobDataMap().put("Properties", properties);
                job.getJobDataMap().put("Parameters", argshm);
                
                //schedule the job
                SchedulerFactory schFactory = new StdSchedulerFactory();
                Scheduler sch = schFactory.getScheduler();
                sch.start();
                sch.scheduleJob(job, trigger);
                log.info("Scheduling ready to run.....");
   }

   public void execute(JobExecutionContext jExeCtx) throws JobExecutionException {
      Date date = new Date();

      JobDataMap data = jExeCtx.getJobDetail().getJobDataMap();
      Properties properties = (Properties)data.get("Properties");
      HashMap argshm = (HashMap)data.get("Parameters");

      log.debug("TestJob run successfully... " + date.toString());
      try {
                // Create Temporary tables
		log.info("Create Temporary Tables");
                String[] tablesArr=properties.getProperty("CreateTemporaryTables").split(",");
                log.debug("Count of tables = " + tablesArr.length);
                if ( argshm.get("-verbose")!=null )  {
                    System.out.println("Count of tables = " + tablesArr.length);
                }
   
                // Move this to a JOB that can be scheduled using Quartz 
                for(String tableStr : tablesArr) {
		     // Executes the given SQL commands
                     log.debug("START " + new Date());
                     try {
// TODO
                        if (properties.getProperty("SIS").equals("Colleague")) {
                           String queryStr = properties.getProperty("CreateTemporary." + tableStr);
                           String[] queriesArr=queryStr.split("\n");
                           String[] steps = new String[100];
                           Boolean inLoop = false;
                           String loopFile = null;
                           String loopKey = null;
                           String loopSize = null;
                           int stepsCount = 0;
                           Map<String,String> idFields = new HashMap<String, String>();
                           for(String indQueryStr : queriesArr) {
                               System.out.println(indQueryStr);
                               if (indQueryStr.startsWith("<< LOOP PERSON")) {
                                  String[] stepsArr=queryStr.split(" ");
                                  loopFile = stepsArr[4];
                                  loopKey = stepsArr[5];
                                  loopSize = stepsArr[6];
                                  inLoop = true; 
                                  idFields.put(loopKey, loopKey);
                               } else if ( indQueryStr.startsWith("<< END LOOP >>")) {
                                  log.debug("Running the loop now " + loopFile + " " + loopKey + " " + loopSize);
                                  inLoop = false;
                                  List<FieldSet> sets = uniData.getFields(loopFile, idFields);
                                  int bufferSize = Integer.parseInt(loopSize);
                                  int index = 0;
                                  int iterations = 0;
                                  StringBuffer ids = new StringBuffer();
                                  if (sets == null) {
                                       log.debug("No data returned.");
                                  } else {
                                     Iterator<FieldSet> iter = sets.iterator();
                                     while(iter.hasNext() && iterations < Utils.MAX_ITERATIONS ) {
                                         index++;
                                         FieldSet set = iter.next();
                                         String id = Utils.getData(set, loopKey, null);
                                         // System.out.println("ID: " + id);
                                         if (id != null) ids.append("'" + id + "' ");
                                         if (index >= bufferSize) {
                                             iterations++;
                                             Utils.concatHistory(uniData, steps, "IDs", ids, "STARFISH.TRANSCRIPT.HISTORY", properties);
                                             index = 0;
                                             ids = new StringBuffer();
                                         }
                                     }
                                     if (!ids.toString().equals("")) {
                                        iterations++;
                                        Utils.concatHistory(uniData, steps, "IDs", ids, "STARFISH.TRANSCRIPT.HISTORY", properties);
                                     }
                                  }
                               }else {
                                  if (inLoop) {
                                     log.debug("Adding to the loop" + indQueryStr);
                                     steps[stepsCount++] = indQueryStr;
                                  } else {
                                     try {
                                        String response = uniData.query(Utils.replaceVariables(indQueryStr,tableStr,properties));
                                        log.debug(response);
                                     } catch (UniCommandException e) {
                                          log.error("   ERROR:UniCommandException:" + e.getErrorCode() + " " + e.getMessage() + "\n");
                                          // System.out.println(new Date());
                                     }
                                  }
                               }
                           }
                        } else {
                           String queryStr = Utils.replaceVariables(properties.getProperty("CreateTemporary." + tableStr), tableStr, properties);
		           // dbSuccess = dbStmt.execute(queryStr);
                           try {
		              dbSuccess = dbStmt.execute(queryStr);
                              log.debug("dbSuccess");
                           } catch ( Exception sqlre ) {
                              if (( dbConn != null) && (dbConn.isClosed())) {
                                // MCC 20170201
                                log.info("Connection was dropped, reconnecting....");
                                System.out.println("----------------- RECONNECTING !!!! ----------------------------");
			        dbConn = DriverManager.getConnection(properties.getProperty("DBConnection"),
			                                             properties.getProperty("DBUsername"),
			                                             passwordDecoded);
		                dbStmt = dbConn.createStatement();
                                dbSuccess = dbStmt.execute(properties.getProperty("ValidateAccess"));
		                dbSuccess = dbStmt.execute(queryStr);
                              } else {
                                 throw sqlre;
                              }
                           }
                           dbResultset = dbStmt.executeQuery("select count(*) from " + tableStr); 
                           dbResultset.next();
                           int count = dbResultset.getInt(1);
                           log.debug(tableStr + " Created: " + count + " records");
                        }
                     } catch (SQLException e) {
                        log.error(e);
                        e.printStackTrace();
	                throw new SQLException("Can NOT create temporary table: " + tableStr);
                     }
                     // log.info("END temporary tab" + new Date());
                } 

                // Create all files
		log.info("-------- Create Starfish Files  ------");
                String[] filesArr=properties.getProperty("Files").split(",");
                log.debug("Count of files = " + filesArr.length);
                if ( argshm.get("-verbose")!=null ) 
                    System.out.println("Count of files = " + filesArr.length);
    
                for(String fileStr : filesArr) {
                     if (properties.getProperty(fileStr + ".generate").equals("1")) {
                        log.info("Create Starfish file " + fileStr );
                        if (properties.getProperty("SIS").equals("Colleague")) {
                           createFile(null, fileStr);
		        } else {
                           if ( properties.getProperty(fileStr + ".term_based").equals("1") ) {
                              String termsStr = properties.getProperty(fileStr + ".distinct_terms");
                              dbResultset = dbStmt.executeQuery(termsStr);
                              while (dbResultset.next()) {
                                   String termStr = dbResultset.getString(1);
                                   createFile(termStr, fileStr);
                              }
                           } else {
                              // System.out.println("MCC 0628106 Creating new file " + fileStr + " calling subroutine");
                              createFile(null, fileStr);
                           }
                        }
                     } else {
		        log.info("Skipping Starfish file " + fileStr );
                     }
                } 

                // Move files from Temp to Final
                File src = new File(properties.getProperty("TempExtractLocation"));
                File dest = new File(properties.getProperty("ExtractLocation"));
                String files[] = src.list();
    		for (String file : files) {
    		   //construct the src and dest file structure
    		   File srcFile = new File(src, file);
    		   File destFile = new File(dest, file + ".temp");
                   // Files.copy(srcFile.toPath(), destFile.toPath());
                   Files.move(srcFile.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                   log.info("File " + file + " moved to " + dest);
    		}

                // create new filename filter
                FilenameFilter fileNameFilter = new FilenameFilter() {
                   @Override
                   public boolean accept(File dir, String name) {
                       if (name.endsWith(".temp")) { return true; }
                       return false;
                   }
                };

                // rename files
                String tempfiles[] = dest.list(fileNameFilter);
    		for (String file : tempfiles) {
    		   //construct the src and dest file structure
    		   File srcFile2 = new File(dest, file);
    		   File destFile2 = new File(dest, file.replace(".temp",""));
                   if (destFile2.exists()) { destFile2.delete(); }
                   srcFile2.renameTo(destFile2);
    		}
                log.info("All files renamed");

                if ( argshm.get("-validate")!=null ) {
                   // Force execution of Validate
                   String[] val = properties.getProperty("Validate").split(";");
                   // String[] envp = new String[0];
                   String[] envp = null;
                   Process child = Runtime.getRuntime().exec(val[1],envp,new File(val[0]));
                   // Get output stream to write from it
                   OutputStream out = child.getOutputStream();
                   // Wait 10 minutes
                   try {
                       Thread.sleep(100 * 60 * 10);
                   } catch (InterruptedException e) {
                       e.printStackTrace();
                   }
                   // This does not close the window, we need to find another solution
                   child.destroy();
                }
      } catch (SQLException s) { 
  	  s.printStackTrace();
      } catch (IOException io) { 
  	  io.printStackTrace();
      } catch (Exception e) { 
  	  e.printStackTrace();
      }


      // MCC closing the DB so it does not interfere with later executions
      try {
          if (( dbConn != null) && !(dbConn.isClosed())) {
            dbConn.close();
          } 
      } catch (Exception e) {
            e.printStackTrace();
      }
   }
 
}
