package com.starfish.sisintegration;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;

import java.util.Properties;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import java.io.StringReader;

import edu.fresno.uniobjects.*;

// Rhino
import org.mozilla.javascript.*;

public class BasicFileGenerator implements FileGenerator {

        public BasicFileGenerator() {
           System.out.println("A new version of the Basic File Generator has been created");
        }

        public void createFile(Connection dbConn, String termStr, String fileStr, Properties properties) throws Exception {
                 System.out.println("Starting new file");
                 System.out.println(new Date());
	         Statement dbStmt = dbConn.createStatement();
                 if ( termStr!=null ) properties.setProperty(fileStr + ".current_term",termStr);
                                 else properties.setProperty(fileStr + ".current_term","");
                 String queryStr = Utils.replaceVariables(properties.getProperty(fileStr + ".template"), fileStr, properties);
                 String filenameStr = properties.getProperty(fileStr + ".filename");

                 // Trying to increase the number of rows pulled for every fetch
                 int fetch_rows = 1000;
                 dbStmt.setFetchSize(fetch_rows);

                 ResultSet dbResultset = dbStmt.executeQuery(queryStr);
                 System.out.println("Finished execution " + new Date());

                 String header = properties.getProperty(fileStr + ".fields");
                 String[] fieldsArr = header.split(",");
                 Object[] fieldsArrObj = (Object [])header.split(",");
                 System.out.println(header);
    
                 // Setting up the CSV file
                 if ( termStr!=null ) filenameStr = termStr + "_" + filenameStr;
                 FileWriter fileWriter = null;
                 CSVPrinter csvFilePrinter = null;
                 CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(properties.getProperty("NewLine",Utils.NEW_LINE_SEPARATOR));
                 // CSVFormat csvFileFormat = CSVFormat.DEFAULT;
                 //initialize FileWriter object
                 fileWriter = new FileWriter(new File(properties.getProperty("TempExtractLocation"),filenameStr));
                 BufferedWriter bf = new BufferedWriter(fileWriter, 1024 * 8 * 4);
                 //initialize CSVPrinter object
                 // csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
                 csvFilePrinter = new CSVPrinter(bf, csvFileFormat);
                 //Create CSV file header
                 csvFilePrinter.printRecord(fieldsArrObj);
    
                 int i=0;
                 int j=0;
                 String voidStr = new String();
                 long time1=System.currentTimeMillis();
                 long time2;
                 long time_its = 0;
                 long time_total = 0;
                 long time_total2 = 0;
                 List dataRecord = new ArrayList();
                 while (dbResultset.next()) {
                     time2=System.currentTimeMillis();
                     time_total = time_total + (time2 - time1);  
                     time_its++;
                     dataRecord.clear();
                     for(String fieldStr : fieldsArr) {
                         String value = dbResultset.getString(fieldStr);
                         dataRecord.add(value);
                         if (dbResultset.wasNull()) value=voidStr;
                         if (i < 50) System.out.print(value + ",");
                     }
                     if (i++ < 50) System.out.println();
                     if (j++ > 1000) { 
                        System.out.print('.');
                        j=0;
                     }
                     csvFilePrinter.printRecord(dataRecord);
                     time1=System.currentTimeMillis(); 
                     time_total2 = time_total2 + (time1 - time2);  
                 }
                 System.out.println(new Date());
                 if ( time_its == 0 ) time_its = 1;
                 System.out.println("CSV file was created successfully !!! " + time_total + " " + time_total2 + " " + time_its + " " + (time_total / time_its));
                 bf.flush();
                 bf.close();
                 // fileWriter.flush();
                 // fileWriter.close();
                 csvFilePrinter.close();
                 // close cursor
                 dbResultset.close();
                 dbStmt.close();
            }

        public void createFileDELETE(UniDataConnection ud, String termStr, String fileStr, Properties properties) throws Exception {
// String sample = "SAMPLE 10000";
String sample = "";

// TODO add replace variables to the with command and the fields command
String header = properties.getProperty(fileStr + ".header", "");
String aux_header = properties.getProperty(fileStr + ".__header", null);
String fields = properties.getProperty(fileStr + ".fields", "");
System.out.println(fields);
fields = fields.replace("\\,","}}");
System.out.println(fields);
String query = properties.getProperty(fileStr + ".template", "");
String filenameStr = properties.getProperty(fileStr + ".filename", "");
String file = properties.getProperty(fileStr + ".colleagueFile", "");
String with = properties.getProperty(fileStr + ".with", "");
String testfile= properties.getProperty(fileStr + ".testfile");
String script= properties.getProperty(fileStr + ".script");

// System.out.println(header);
// System.out.println(fields);

                 System.out.println("Starting new file");
                 System.out.println(new Date());

                 Utils.runStep(ud, "CLEARSELECT");
                 
                 Object[] fieldsArrObj = (Object [])header.split(",");
                 if ( aux_header != null) header = header + "," + aux_header;
                 String[] headerArr = header.split(",");
                 String[] fieldsArr = fields.split(",");
                 System.out.println(header);

                 Map<String,String> fields_map = new HashMap<String, String>();
                 int field_i = 0;
                 String fieldsQuery = new String();
                 for(String fieldStr : headerArr) {
                            // fields_map.put(fieldStr, fieldStr);
                            fields_map.put(fieldStr, fieldsArr[field_i].replace("}}",","));
                            fieldsQuery = fieldsQuery + " " + fieldsArr[field_i++].replace("}}",",") + " COL.HDG  \"" + fieldStr + "\"";
                 }

                 // Setting up the CSV file
                 // if ( termStr!=null ) filenameStr = termStr + "_" + filenameStr;
                 FileWriter fileWriter = null;
                 CSVPrinter csvFilePrinter = null;
                 CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(properties.getProperty("NewLine",Utils.NEW_LINE_SEPARATOR));
                 // CSVFormat csvFileFormat = CSVFormat.DEFAULT;
                 //initialize FileWriter object
                 fileWriter = new FileWriter(new File(properties.getProperty("TempExtractLocation"),filenameStr));
                 BufferedWriter bf = new BufferedWriter(fileWriter, 1024 * 8 * 4);
                 //initialize CSVPrinter object
                 // csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
                 csvFilePrinter = new CSVPrinter(bf, csvFileFormat);
                 //Create CSV file header
                 csvFilePrinter.printRecord(fieldsArrObj);

                 int i=0;
                 int j=0;
                 String voidStr = new String();
                 long time1=System.currentTimeMillis();
                 long time2;
                 long time_its = 0;
                 long time_total = 0;
                 long time_total2 = 0;

                 System.out.println(query);
                 Utils.runStep(ud, query);
                 System.out.println("The first GET LIST is done");

/* How can we change this? or just ignore the end: UDT.OPTIONS n {ON | OFF} */
                 System.out.println("Next is: LIST " + file + " " + fieldsQuery + " " + with + " TOXML" + " " + sample);
                 String response = Utils.runStep(ud, "LIST " + file + " " + fieldsQuery + " " + with + " TOXML" + " " + sample);
                 System.out.println("The second LIST is done");
                 if (testfile != null) {
                       PrintWriter out = new PrintWriter(testfile + "_full");
                       out.println(response);
                       out.close();
                 } 
                 // response = response.replaceAll("The following record ids do not exist:","");
                 int end = response.indexOf("</ROOT>");
                 System.out.println("The end is at " + end);
                 if (end > 0) response = response.substring(0, end+7);
                 response = response.replaceAll("The following record ids do not exist:","");
                 if (testfile != null) {
                       PrintWriter out = new PrintWriter(testfile);
                       out.println(response);
                       out.close();
                 } 

                 StringReader sr = new StringReader(response);

                 DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                 DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                 Document doc = dBuilder.parse(new InputSource(sr));

                 List dataRecord = new ArrayList();
                 NodeList nList = doc.getElementsByTagName(file);
                 if (nList.getLength() == 0)
                      System.out.println("No data returned.");

                 System.out.println("Going to parse the XML");
                 time2=System.currentTimeMillis();
                 for (int temp = 0; temp < nList.getLength(); temp++) {
                     Node nNode = nList.item(temp);
                     if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) nNode;
/* Need to do something with the _MV and _MS fields: Ideally some toString on the list... but if not, some other logic
<ROOT>
<COURSES _ID = "12081" integration_id = "12081" course_id = "ACR-5" course_name = "Electrical Applications" course_cip_code = "150501" modified_ts = "SF_NULL_VALUE" min_credit_hours = "4.00" status = "A" catalog_year = "SF_NULL_VALUE" org_integration_id = "SF_NULL_VALUE">
  <CRS.LEVELS_MV>
    <CRS.LEVELS_MS Level = "C"/>
  </CRS.LEVELS_MV>
  <CRS.LEVELS_MV>
    <CRS.LEVELS_MS Level = "Y"/>
  </CRS.LEVELS_MV>
  <CRS.DESC_MV>
    <CRS.DESC_MS description = "The course focuses on basic electrical theory and"/>
  </CRS.DESC_MV>
  <CRS.DESC_MV>
    <CRS.DESC_MS description = "training in installing, servicing,"/>
  </CRS.DESC_MV>
*/
                        /* TODO Running my extra command for each record... before I even try to write them down */
                        Context cx = Context.enter();
                        try {
                             Scriptable scope = cx.initStandardObjects();
                             for(String fieldStr : headerArr) {
                                 String value = eElement.getAttribute(fieldStr);
                                 if (value == null || value.equals("")) {
                                      // System.out.println("No attribute found, let's check if it is a multivalue " + fieldStr + " " + value);
                                      // String s = fields_map.get(fieldStr) + "_MV/" + fields_map.get(fieldStr) + "_MS";
                                      // String s = fields_map.get(fieldStr) + "_MS";
                                      String s = "*";
                                      NodeList subList = eElement.getElementsByTagName(s);
                                      // System.out.println("No attribute found, let's check if it is a multivalue " + subList.getLength() + " " + s);
                                      int isFirst = 1;
                                      for (int s_temp = 0; s_temp < subList.getLength(); s_temp++) {
                                         Node subNode = subList.item(s_temp);
                                         // System.out.println("subNode " + subNode);
                                         if (subNode.getNodeType() == Node.ELEMENT_NODE) {
                                              Element subElement = (Element)subNode;
                                              if ( subElement.getTagName().endsWith("_MS") && subElement.hasAttribute(fieldStr) ) {
                                                  // System.out.println("VALUE: " + fieldStr + " " + subElement.getAttribute(fieldStr));
                                                  // System.out.println(subElement);
                                                  if ( isFirst == 1 )
                                                     value =  subElement.getAttribute(fieldStr);
                                                  else 
                                                     value = value + "}}" + subElement.getAttribute(fieldStr);
                                                  isFirst = 0;
                                              }
                                         }
                                      }
                                 }
                                 if (value != null && value.equals("SF_NULL_VALUE")) value=voidStr;
                                 scope.put(fieldStr,scope,value);
                                 // if (fieldStr.equals("status")) System.out.println("Status before: " + value);
                                 // if (value == null) value=voidStr;
                             }
                             if ( script != null ) {
                                  // System.out.println("Executing Script: " + script);
                                  Object result = cx.evaluateString(scope, script, fileStr, 1, null);
                             }
                             for(String fieldStr : headerArr) {
                                 if ( !fieldStr.startsWith("__")) {
                                       // scope.get("a",scope)
                                       String value = scope.get(fieldStr,scope).toString();
                                       dataRecord.add(value);
                                       // if (fieldStr.equals("status")) System.out.println("Status after: " + value);
                                       if (i < 50) System.out.print(value + ",");
                                 }
                             }
                             if (i++ < 50) System.out.println();
                             if (j++ > 1000) {
                                System.out.print('.');
                                j=0;
                             }
                             csvFilePrinter.printRecord(dataRecord);
                             dataRecord.clear();
                        } finally {
                             Context.exit();
                        }
                        time1=System.currentTimeMillis();
                        time_total2 = time_total2 + (time1 - time2);
                     }
                 }
                 time_total = time_total + (time2 - time1);
                 time_its++;
                 if ( time_its == 0 ) time_its = 1;
                 System.out.println("CSV file was created successfully !!! " + time_total + " " + time_total2 + " " + time_its + " " + (time_total / time_its));
                 bf.flush();
                 bf.close();
                 // fileWriter.flush();
                 // fileWriter.close();
                 csvFilePrinter.close();
        }

        public void createFile(UniDataConnection ud, String termStr, String fileStr, Properties properties) throws Exception {
// String sample = "SAMPLE 10000";
String sample = "";

// TODO add replace variables to the with command and the fields command
String header = properties.getProperty(fileStr + ".header", "");
String aux_header = properties.getProperty(fileStr + ".__header", null);
String fields = properties.getProperty(fileStr + ".fields", "");
System.out.println(fields);
fields = fields.replace("\\,","}}");
System.out.println(fields);
String query = properties.getProperty(fileStr + ".template", "");
String filenameStr = properties.getProperty(fileStr + ".filename", "");
String file = properties.getProperty(fileStr + ".colleagueFile", "");
String with = properties.getProperty(fileStr + ".with", "");
String testfile= properties.getProperty(fileStr + ".testfile");
String script= properties.getProperty(fileStr + ".script");

// System.out.println(header);
// System.out.println(fields);

                 System.out.println("Starting new file");
                 System.out.println(new Date());

                 Utils.runStep(ud, "CLEARSELECT");
                 
                 Object[] fieldsArrObj = (Object [])header.split(",");
                 if ( aux_header != null) header = header + "," + aux_header;
                 String[] headerArr = header.split(",");
                 String[] fieldsArr = fields.split(",");
                 System.out.println(header);

                 Map<String,String> fields_map = new HashMap<String, String>();
                 int field_i = 0;
                 String fieldsQuery = new String();
                 for(String fieldStr : headerArr) {
                            // fields_map.put(fieldStr, fieldStr);
                            fields_map.put(fieldStr, fieldsArr[field_i].replace("}}",","));
                            fieldsQuery = fieldsQuery + " " + fieldsArr[field_i++].replace("}}",",") + " COL.HDG  \"" + fieldStr + "\"";
                 }

                 // Setting up the CSV file
                 // if ( termStr!=null ) filenameStr = termStr + "_" + filenameStr;
                 FileWriter fileWriter = null;
                 CSVPrinter csvFilePrinter = null;
                 CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(properties.getProperty("NewLine",Utils.NEW_LINE_SEPARATOR));
                 // CSVFormat csvFileFormat = CSVFormat.DEFAULT;
                 //initialize FileWriter object
                 fileWriter = new FileWriter(new File(properties.getProperty("TempExtractLocation"),filenameStr));
                 BufferedWriter bf = new BufferedWriter(fileWriter, 1024 * 8 * 4);
                 //initialize CSVPrinter object
                 // csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
                 csvFilePrinter = new CSVPrinter(bf, csvFileFormat);
                 //Create CSV file header
                 csvFilePrinter.printRecord(fieldsArrObj);

                 System.out.println(query);
                 Utils.runStep(ud, query);
                 System.out.println("The first GET LIST is done");

/* How can we change this? or just ignore the end: UDT.OPTIONS n {ON | OFF} */
                 System.out.println("Next is: LIST " + file + " " + fieldsQuery + " " + with + " TOXML" + " " + sample);
                 String response = Utils.runStep(ud, "LIST " + file + " " + fieldsQuery + " " + with + " TOXML" + " " + sample);
                 System.out.println("The second LIST is done");
                 if (testfile != null) {
                       PrintWriter out = new PrintWriter(testfile + "_full");
                       out.println(response);
                       out.close();
                 } 
				 
                 // response = response.replaceAll("The following record ids do not exist:","");
                 int end = response.indexOf("</ROOT>");
                 System.out.println("The end is at " + end);
                 if (end > 0) response = response.substring(0, end+7);
                 response = response.replaceAll("The following record ids do not exist:","");
                 if (testfile != null) {
                       PrintWriter out = new PrintWriter(testfile);
                       out.println(response);
                       out.close();
                 } 

		 ArrayList<ArrayList> allRecords = processXMLResponse(file, response, headerArr, script, fileStr);
		 printAllRecords(allRecords, csvFilePrinter);
				 
                 // Add a loop to write into the CSV 
                 bf.flush();
                 bf.close();
                 // fileWriter.flush();
                 // fileWriter.close();
                 csvFilePrinter.close();
        }
				 
        public static ArrayList<ArrayList> processXMLResponse(String file, String response, String[] headerArr, String script, String fileStr) throws Exception {
                 ArrayList allRecords = new ArrayList();
                 long time1=System.currentTimeMillis();
                 long time2;
                 long time_its = 0;
                 long time_total = 0;
                 long time_total2 = 0;
                 int i=0;
                 int j=0;	 
                 String voidStr = new String();
				 
                 StringReader sr = new StringReader(response);

                 DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                 DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                 Document doc = dBuilder.parse(new InputSource(sr));

                 List dataRecord = new ArrayList();
                 NodeList nList = doc.getElementsByTagName(file);
                 if (nList.getLength() == 0)
                      System.out.println("No data returned.");

                 System.out.println("Going to parse the XML");
                 time2=System.currentTimeMillis();
                 for (int temp = 0; temp < nList.getLength(); temp++) {
                     Node nNode = nList.item(temp);
                     if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) nNode;
                        Context cx = Context.enter();
                        try {
                             Scriptable scope = cx.initStandardObjects();
                             for(String fieldStr : headerArr) {
                                 String value = eElement.getAttribute(fieldStr);
                                 if (value == null || value.equals("")) {
                                      String s = "*";
                                      NodeList subList = eElement.getElementsByTagName(s);
                                      int isFirst = 1;
                                      for (int s_temp = 0; s_temp < subList.getLength(); s_temp++) {
                                         Node subNode = subList.item(s_temp);
                                         if (subNode.getNodeType() == Node.ELEMENT_NODE) {
                                              Element subElement = (Element)subNode;
                                              if ( subElement.getTagName().endsWith("_MS") && subElement.hasAttribute(fieldStr) ) {
                                                  if ( isFirst == 1 )
                                                     value =  subElement.getAttribute(fieldStr);
                                                  else 
                                                     value = value + "}}" + subElement.getAttribute(fieldStr);
                                                  isFirst = 0;
                                              }
                                         }
                                      }
                                 }
                                 if (value != null && value.equals("SF_NULL_VALUE")) value=voidStr;
                                 scope.put(fieldStr,scope,value);
                             }
                             if ( script != null ) {
                                  Object result = cx.evaluateString(scope, script, fileStr, 1, null);
                             }
                             for(String fieldStr : headerArr) {
                                 if ( !fieldStr.startsWith("__")) {
                                       String value = scope.get(fieldStr,scope).toString();
                                       dataRecord.add(value);
                                       if (i < 50) System.out.print(value + ",");
                                 }
                             }
                             if (i++ < 50) System.out.println();
                             if (j++ > 1000) {
                                System.out.print('.');
                                j=0;
                             }
		   	     // return in a list, not print to CSV YET!
                             // csvFilePrinter.printRecord(dataRecord);
			     // System.out.println(allRecords.toString());
			     allRecords.add(dataRecord);
                             dataRecord = new ArrayList();
                             // dataRecord.clear();
                        } finally {
                             Context.exit();
                        }
                        time1=System.currentTimeMillis();
                        time_total2 = time_total2 + (time1 - time2);
                     }
                 }
                 time_total = time_total + (time2 - time1);
                 time_its++;
                 if ( time_its == 0 ) time_its = 1;
                 System.out.println("CSV file was created successfully !!! " + time_total + " " + time_total2 + " " + time_its + " " + (time_total / time_its));
                 System.out.println(allRecords.size());
                 return allRecords;
	 }

	 public static void printAllRecords(ArrayList<ArrayList> records, CSVPrinter csvFilePrinter) throws Exception {
		 System.out.println("ADDING A LOOP HERE TO ACTUALLY WRITE THE FILE FROM THE LIST! "  + records.size());
                 Iterator it = records.iterator(); 
                 while(it.hasNext()){
                       ArrayList dataRecord = (ArrayList)it.next();
                       // System.out.println(dataRecord.toString());
                       csvFilePrinter.printRecord(dataRecord);
                 }
	 }
				
}
