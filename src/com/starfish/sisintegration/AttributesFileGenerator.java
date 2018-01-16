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

import java.util.Date;

import edu.fresno.uniobjects.*;

public class AttributesFileGenerator implements FileGenerator {

        public AttributesFileGenerator() {
        }

        public void createFile(Connection dbConn, String termStr, String fileStr, Properties properties) throws Exception {
                 System.out.println("WE ARE EXECUTING THE CUSTOM ONE");
                 System.out.println(new Date());

	         Statement dbStmt = dbConn.createStatement();
                 if ( termStr!=null ) properties.setProperty(fileStr + ".current_term",termStr);
                                 else properties.setProperty(fileStr + ".current_term","");
      
                 String attrArr[] = properties.getProperty(fileStr + ".list").split(",");
                 StringBuilder sb = new StringBuilder(256); 
                 boolean isFirst = true;
                 for(String attrStr : attrArr) {
                        System.out.println(attrStr);
                        if (!isFirst) {
                            sb.append(" union ");
                        } else { 
                            isFirst = false;
                        }
                        sb.append(Utils.replaceVariables(properties.getProperty(fileStr + "." + attrStr + ".template"),fileStr,properties)); 
                        System.out.println(sb.toString());
                 }
                 properties.setProperty(fileStr + ".list.unions", sb.toString());

                 String queryStr = Utils.replaceVariables(properties.getProperty(fileStr + ".template"), fileStr, properties);
                 String filenameStr = properties.getProperty(fileStr + ".filename");
                 ResultSet dbResultset = dbStmt.executeQuery(queryStr);
                 String header = properties.getProperty(fileStr + ".fields");
                 String[] fieldsArr = header.split(",");
                 Object[] fieldsArrObj = (Object [])header.split(",");
                 System.out.println(header);
    
                 // Setting up the CSV file
                 if ( termStr!=null ) filenameStr = termStr + "_" + filenameStr;
                 FileWriter fileWriter = null;
                 CSVPrinter csvFilePrinter = null;
                 CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(Utils.NEW_LINE_SEPARATOR);
                 //initialize FileWriter object
                 fileWriter = new FileWriter(new File(properties.getProperty("TempExtractLocation"),filenameStr));
                 //initialize CSVPrinter object
                 csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
                 //Create CSV file header
                 csvFilePrinter.printRecord(fieldsArrObj);
  
                 int i=0; 
                 int j=0; 
                 String voidStr = new String(); 
                 List dataRecord = new ArrayList();
                 while (dbResultset.next()) {
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
                 }
                 System.out.println(new Date());
                 System.out.println("CSV file was created successfully !!!");
                 fileWriter.flush();
                 fileWriter.close();
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

                 String[] attributesArr = aux_header.split(",");
                 ArrayList<ArrayList> allRecords = BasicFileGenerator.processXMLResponse(file, response, headerArr, script, fileStr);
                 ArrayList<ArrayList> explodedRecords = new ArrayList();
                 Iterator it = allRecords.iterator(); 
                 while(it.hasNext()){
                       ArrayList dataRecord = (ArrayList)it.next();
                       int i=4;
                       for(String attrStr : attributesArr) {
                            if (dataRecord.get(i) != null && !dataRecord.get(i).equals("")) { 
                               ArrayList newRecord = new ArrayList();
                               newRecord.add(dataRecord.get(0));
                               newRecord.add(attrStr);
                               newRecord.add(dataRecord.get(i));
                               newRecord.add(dataRecord.get(3));
                               explodedRecords.add(newRecord);
                            }
                            i++;
                       }
                 }
                 BasicFileGenerator.printAllRecords(explodedRecords, csvFilePrinter);
                                 
                 // Add a loop to write into the CSV 
                 bf.flush();
                 bf.close();
                 // fileWriter.flush();
                 // fileWriter.close();
                 csvFilePrinter.close();

        }

}
