package com.starfish.sisintegration;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedWriter;

import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Arrays;
import java.util.Iterator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.util.Stack;
import java.util.HashMap;
import java.util.TreeMap;

import com.starfish.sisintegration.bt.*;

import edu.fresno.uniobjects.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import java.io.StringReader;

// Rhino
import org.mozilla.javascript.*;

public class RequisitesFileGenerator implements FileGenerator {

        public RequisitesFileGenerator() {
        }

        public void createFile(Connection dbConn, String termStr, String fileStr, Properties properties) throws Exception {
                 String prepend = properties.getProperty(fileStr + ".id_prepend");
                 int[] and_or_id = new int[2];
                 // Integer and_id = new Integer(0);
                 // Integer or_id = new Integer(0);
                 and_or_id[0]=0; // and
                 and_or_id[1]=0; // or

	         Statement dbStmt = dbConn.createStatement();
                 if ( termStr!=null ) properties.setProperty(fileStr + ".current_term",termStr);
                                 else properties.setProperty(fileStr + ".current_term","");
                 String queryStr = Utils.replaceVariables(properties.getProperty(fileStr + ".template"), fileStr, properties);
                 String filenameStr = properties.getProperty(fileStr + ".filename");
                 ResultSet dbResultset = dbStmt.executeQuery(queryStr);
                 String header = properties.getProperty(fileStr + ".fields");
                 String agheader = properties.getProperty(fileStr + ".agfields");
                 String[] fieldsArr = header.split(",");
                 Object[] fieldsArrObj = (Object [])header.split(",");
                 Object[] agFieldsArrObj = (Object [])agheader.split(",");
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
                 csvFilePrinter.printRecord(agFieldsArrObj);
  
                 // BT bt = new BT();
                 // BTNode btnode = new BTNode(); 
                 // BTNode btconnector = new BTNode(); 
                 Stack stack = new Stack(); 
                 String previousSection = new String();
                 while (dbResultset.next()) {
// System.out.println("At the beginning of the loop " + stack.toString());
                     /* The prerequisites are not dumped to CSV directly, we need to use a Stack to create a binary tree
                        And then dump the Tree into the data format
                     */
                     // List dataRecord = new ArrayList();
                     HashMap hm = new HashMap(); 

                     String recordstr = "";
                     for(String fieldStr : fieldsArr) {
                         String value = dbResultset.getString(fieldStr);
                         // dataRecord.add(value);
                         hm.put(fieldStr,value);
                         if (dbResultset.wasNull()) value=new String();
                         recordstr = recordstr + "," + value ;
                     }
                   
                     // Is this a new record?
                     String currentSection = (String)hm.get("section_integration_id");
                     if (previousSection == null || previousSection.equals("") ) previousSection = currentSection;
                     if (!currentSection.equals(previousSection)) {
                         // System.out.println(currentSection + " --- " + previousSection);
                         System.out.println("Need to empty the stack and create the data for the CSV for the section");
                         System.out.println(stack);
                         Object o = stack.pop();
                         BT t;
                         if ( o instanceof BTNode ) {
                            t = new BT();
                            t.insert((BTNode)o);
                         } else {
                            t = (BT)o;
                         }
                         t.toCNF();
                         System.out.println("Normalized form " + t);
                         and_or_id[0]=0; // and
                         and_or_id[1]=0; // or
                         printTree(t.getRoot(), csvFilePrinter, and_or_id, previousSection, prepend);
                         if (!stack.empty()) {
                             System.out.println("THE STACK SHOULD BE EMPTY NOW");
                             stack = new Stack();
                         }
                         System.out.println();
                         System.out.println();
                     } 
                     previousSection = currentSection;

                     System.out.println(recordstr);

                     String lparen = (String)hm.get("lparen");
                     String rparen = (String)hm.get("rparen");
                     String connector = (String)hm.get("connector");
                     if ( lparen == null ) lparen = "";
                     if ( rparen == null ) rparen = "";
                     if ( connector == null ) connector = "";

                     String data_str = (String)hm.get("test") + '.' + 
                                       (String)hm.get("testscore") + '.' +
                                       (String)hm.get("prereq_integration_id") + '.' +
                                       (String)hm.get("min_grde");

                     // Case 1: Data, no connector, no parenthesis
                     //         This is the first record, do a push in the stack
                     // if ( connector.equals("") && lparen.equals("") && rparen.equals("") ) 
                     if ( connector.equals("") ) {
// System.out.println("CASE 1: Pushing the first record and maybe a parenthesis");
                         BTNode data = new BTNode( null, (String)hm.get("concurrency_ind"),
                                                         (String)hm.get("test"),
                                                         (String)hm.get("testscore"),
                                                         (String)hm.get("prereq_integration_id"),
                                                         (String)hm.get("min_grde"));
                         if ( lparen.equals("(") && rparen.equals("") ) stack.push("(");
// System.out.println("push a btnode " + data);
                         stack.push(data);
                     // Case 2: Data, with a connector, and no parenthesis, or two parenthesis (cancel each other)
                     //         Pull last tree from the stack and push the new tree with the operation
                     } else if ( !connector.equals("") && 
                             ( ( lparen.equals("(") && rparen.equals(")") )
                            || ( lparen.equals("") && rparen.equals("") ) ) ) {
// System.out.println("CASE 2: Data with connector");
                            BTNode op = new BTNode(connector, null, null, null, null, null);
                            BTNode data = new BTNode( null, (String)hm.get("concurrency_ind"),
                                                            (String)hm.get("test"),
                                                            (String)hm.get("testscore"),
                                                            (String)hm.get("prereq_integration_id"),
                                                            (String)hm.get("min_grde"));
                            Object lbranch = stack.pop();
                            if ( lbranch instanceof BTNode ) {
                                 BT bt = new BT();
                                 bt.insert(op);
                                 bt.insert(op,data);
                                 bt.insert(op,(BTNode)lbranch);
// System.out.println("1.push a bt " + bt);
// bt.preorder(op);
// System.out.println();
// bt.preorder();
// System.out.println();

                                 stack.push(bt);
                            } 
                            if ( lbranch instanceof BT ) {
                                 BT bt = new BT();
                                 bt.insert(op);
                                 bt.insert(op,data);
                                 bt.insert(op,((BT)lbranch).getRoot());
// System.out.println("2.push a bt " + bt);
// bt.preorder(op);
// System.out.println();
// bt.preorder();
// System.out.println();
                                 stack.push(bt);
                            } 
                            if ( lbranch instanceof String ) {
System.out.println("IS THIS AN ERROR?");
                               // parenthesis???
                            }
                     // Case 3: Data, with a connector, and opening parenthesis
                     //         Push the pieces so we can pull them back when the parenthesis closes
                     } else if ( !connector.equals("") && lparen.equals("(") && rparen.equals("") ) {
// System.out.println("CASE 3: Data with connector and open parenthesis");
                                 BTNode op = new BTNode(connector, null, null, null, null, null);
                                 BTNode data = new BTNode( null, (String)hm.get("concurrency_ind"),
                                                                 (String)hm.get("test"),
                                                                 (String)hm.get("testscore"),
                                                                 (String)hm.get("prereq_integration_id"),
                                                                 (String)hm.get("min_grde"));
                                 stack.push(op); 
                                 stack.push("(");
                                 stack.push(data);
                     // Case 4: Data, with a connector, and closing parenthesis
                     //         Pull the pieces so we can push a new tree
                     //         - create data
                     //         - pull lbranch
                     //         - pull op
                     //         - push new tree
                     //         - pull new tree
                     //         - pull lparent
                     //         - pull operator
                     //         - pull rbranch
                     //         - push new tree 
                     } else if ( !connector.equals("") && lparen.equals("") && rparen.equals(")") ) {
// System.out.println("CASE 4: Data with connector and close parenthesis");
                                 BTNode op = new BTNode(connector, null, null, null, null, null);
                                 BTNode data = new BTNode( null, (String)hm.get("concurrency_ind"),
                                                                 (String)hm.get("test"),
                                                                 (String)hm.get("testscore"),
                                                                 (String)hm.get("prereq_integration_id"),
                                                                 (String)hm.get("min_grde"));
                                 Object lbranch = stack.pop();
                                 if ( lbranch instanceof BTNode ) {
                                      BT bt = new BT();
                                      bt.insert(op);
                                      bt.insert(op,data);
                                      bt.insert(op,(BTNode)lbranch);
// System.out.println("push a bt " + bt);
                                      stack.push(bt);
                                 }
                                 if ( lbranch instanceof BT ) {
                                      BT bt = new BT();
                                      bt.insert(op);
                                      bt.insert(op,data);
                                      bt.insert(op,((BT)lbranch).getRoot());
// System.out.println("push a bt " + bt);
                                      stack.push(bt);
                                 }
// System.out.println("Am I in the next op?");
// System.out.println(stack);
                                 BT rbranch = (BT)stack.pop();
// System.out.println("pulled from tree " + rbranch);
                                 String lparent = (String)stack.pop();
                                 // This may be a null if this was the first parenthesis, we need to capture that
                                 if (!stack.empty()) {
                                   op = (BTNode)stack.pop();
// System.out.println("got it");
                                   lbranch = stack.pop();
                                   if ( lbranch instanceof BTNode ) {
                                        BT bt = new BT();
                                        bt.insert(op);
                                        bt.insert(op,((BT)rbranch).getRoot());
                                        bt.insert(op,(BTNode)lbranch);
                                        stack.push(bt);
                                   }
                                   if ( lbranch instanceof BT ) {
                                        BT bt = new BT();
                                        bt.insert(op);
                                        bt.insert(op,((BT)rbranch).getRoot());
                                        bt.insert(op,((BT)lbranch).getRoot());
                                        stack.push(bt);
                                   }
                                 } else { 
                                   // System.out.println(rbranch.toString());
                                   // Adding the tree, so we can empty it later
                                   stack.push(rbranch);
                                 }
                     } 
                     // csvFilePrinter.printRecord(dataRecord);
                     // System.out.println("At the end of the loop " + stack.toString());
                 }
                 System.out.println("Need to empty the stack for the last element and create the data for the CSV for the section");
                 System.out.println(stack);
                 Object o = stack.pop();
                 BT t;
                 if ( o instanceof BTNode ) {
                      t = new BT();
                      t.insert((BTNode)o);
                 } else {
                      t = (BT)o;
                 }
                 t.toCNF();
                 System.out.println("Normalized form " + t);
                 and_or_id[0]=0; // and
                 and_or_id[1]=0; // or
                 printTree(t.getRoot(), csvFilePrinter, and_or_id, previousSection, prepend);
                 System.out.println("CSV file was created successfully !!!");
                 fileWriter.flush();
                 fileWriter.close();
                 csvFilePrinter.close();
            }

            public void printTree(BTNode node, CSVPrinter csvFilePrinter, int[] and_or_id, String section, String prepend) throws Exception {
                     if (node == null) return;
                     if (node.isAND()) { 
                         printTree(node.getLeft(), csvFilePrinter, and_or_id, section, prepend);
                         and_or_id[0] = and_or_id[0] + 1;
                         and_or_id[1] = 0;
                         printTree(node.getRight(), csvFilePrinter, and_or_id, section, prepend);
                     } else if (node.isOR()) { 
                         printTree(node.getLeft(), csvFilePrinter, and_or_id, section, prepend);
                         and_or_id[1] = and_or_id[1] + 1;
                         printTree(node.getRight(), csvFilePrinter, and_or_id, section, prepend);
                     } else {
                         // System.out.println(section + " group " + and + " " + or );
                         List dataRecord = new ArrayList();
                         dataRecord.add(prepend + node.getID());
                         dataRecord.add(section);
                         if ( node.getPrereqIntegrationID() != null) 
                            dataRecord.add("course");
                         else if ( node.getTest() != null) 
                            dataRecord.add("test");
                         else
                            dataRecord.add("");
                         if ( node.getPrereqIntegrationID() != null) 
                            dataRecord.add(node.getPrereqIntegrationID());
                         else if ( node.getTest() != null) 
                            dataRecord.add(node.getTestScore());
                         else
                            dataRecord.add("");
                         dataRecord.add(and_or_id[0]);
                         dataRecord.add(and_or_id[1]);
                         dataRecord.add(node.getTest());
                         // MCC 20151207 if we are dealing with a pure grade, use >= but if it contains letters, use =
                         // dataRecord.add(">="); // Always assumed to greater or equal
                         if ( node.getPrereqIntegrationID() != null ) 
                            dataRecord.add("="); 
                         else
                            if (node.getTestScore() != null && node.getTestScore().matches("-?\\d+(\\.\\d+)?"))
                                dataRecord.add(">="); 
                            else
                                dataRecord.add("="); 
                         dataRecord.add(node.getConcurrency()); 
                         if ( node.getPrereqIntegrationID() != null) 
                            dataRecord.add(node.getMinGrade());
                         // else if ( node.getTest() != null) 
                         //    dataRecord.add(node.getTestScore());
                         else
                            dataRecord.add("");

                         System.out.println(dataRecord);
                         csvFilePrinter.printRecord(dataRecord);
                     } 
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

						 ArrayList<ArrayList> allRecords = BasicFileGenerator.processXMLResponse(file, response, headerArr, script, fileStr);
                                                 ArrayList courseReqs = new ArrayList();
                                                 // Before printing we want to add more records to the list
                                                 Iterator it = allRecords.iterator();
                                                 while(it.hasNext()){
                                                       ArrayList dataRecord = (ArrayList)it.next();
                                                       getRequirements(courseReqs, ud, (String)dataRecord.get(0), (String)dataRecord.get(1), headerArr,script,fileStr,properties);
                                                 }
						 // BasicFileGenerator.printAllRecords(allRecords, csvFilePrinter);
						 BasicFileGenerator.printAllRecords(courseReqs, csvFilePrinter);
				 
				                 // Add a loop to write into the CSV 
				                 bf.flush();
				                 bf.close();
				                 // fileWriter.flush();
				                 // fileWriter.close();
				                 csvFilePrinter.close();
		}


	        public void getRequirements(ArrayList courseReqs, UniDataConnection ud, String prereq, String course, String[] headerArr, String script, String fileStr, Properties properties) throws Exception {
                          // System.out.println("Running for: " + prereq);
                          Utils.runStep(ud, "CLEARSELECT");
                          // prereq_id,course_integration_id,prereq_type,prereq_value,and_id,or_id,prereq_test_id,prereq_operator,prereq_concurrent_flag,min_grade
                          // String response = Utils.runStep(ud, "LIST ACAD.REQMTS ACR.TOP.REQMT.BLOCK WITH @ID = '" + prereq + "' TOXML");
                          String query = new String("LIST ACAD.REQMT.BLOCKS EVAL '" + prereq + "' COL.HDG 'prereq_id' "  
                                                                        + " EVAL '" + course + "' COL.HDG 'course_integration_id' " 
                                                                        + " EVAL 'course' COL.HDG 'prereq_type' " 
                                                                        + " EVAL 'SF_NULL_VALUE' COL.HDG 'prereq_value' " 
                                                                        + " ACRB.COURSES COL.HDG 'and_id' " 
                                                                        + " ACRB.FROM.COURSES COL.HDG 'or_id' "  
                                                                        + " EVAL 'SF_NULL_VALUE' COL.HDG 'prereq_test_id' " 
                                                                        + " EVAL 'SF_NULL_VALUE' COL.HDG 'prereq_operator' " 
                                                                        // MCC TODO use the prereq_concurrent_flag from the block, passed to this query
                                                                        + " EVAL 'SF_NULL_VALUE' COL.HDG 'prereq_concurrent_flag' " 
                                                                        // + " ACRB.MIN.GRADE COL.HDG 'min_grade' "  
                                                                        + " EVAL 'OCONV(TRANS(\"GRADES\", ACRB.MIN.GRADE, GRD.VALUE, \"X\"), \"MR5\")' COL.HDG 'min_grade' "
                                                                        + " ACRB.SUBBLOCKS "
                                                                        + " ACRB.PRINTED.SPEC "
                                                                        + " EVAL 'TRANS(\"GRADES\", ACRB.MIN.GRADE, GRD.GRADE, \"X\")' COL.HDG 'grade' "
                                                                        + " EVAL 'TRANS(\"GRADES\", ACRB.MIN.GRADE, GRD.COMPARISON.GRADE, \"X\")' COL.HDG 'compgrade' "
                                                                        + " EVAL 'TRANS(\"GRADES\", ACRB.MIN.GRADE, GRD.VALUE, \"X\")' COL.HDG 'value' "
                                                                   + " WITH ACRB.ACAD.REQMT = '" + prereq + "' TOXML");
                          // System.out.println(query);
                          String response = Utils.runStep(ud, query);
                          int end = response.indexOf("</ROOT>");
                          if (end > 0) response = response.substring(0, end+7);
                          response = response.replaceAll("The following record ids do not exist:","");

			  String testfile = properties.getProperty(fileStr + ".testfile");
                          PrintWriter out = new PrintWriter(testfile.replace("xml", prereq + ".xml" ));
                          out.println(response);
                          out.close();
                          if ( response.trim().equals("") ) {
                             System.out.println("EMPTY FILE, POTENTIAL ISSUE WITH THE QUERY");
                          } else {
			     ArrayList<ArrayList> allRecords = BasicFileGenerator.processXMLResponse("ACAD.REQMT.BLOCKS", response, headerArr, script, fileStr);
                             int i_and = 1;
                             int i_or = 1;
                             int i_index = 1;
                             Iterator it = allRecords.iterator();
                             while(it.hasNext()){
                                    ArrayList dataRecord = (ArrayList)it.next();
                                    // No recursion needed?, all info is in the record now
                                    // ArrayList courseReqs = getRequirements(dataRecord.get(0));
                                    String andCourses_s = (String)dataRecord.get(4);
                                    if ( andCourses_s != null && !(andCourses_s.equals(""))) {
                                       // System.out.println(" And courses " + andCourses_s);
                                       String[] andCourses = andCourses_s.split("}}");
                                       for (String a : andCourses ) { 
                                            // System.out.println(a);
                                            ArrayList newRecord = new ArrayList();
                                            newRecord.add(dataRecord.get(0) + "_" + i_index++);
                                            newRecord.add(dataRecord.get(1));
                                            newRecord.add(dataRecord.get(2));
                                            newRecord.add(a);
                                            newRecord.add((new Integer(i_and)).toString());
                                            newRecord.add((new Integer(i_or)).toString());
                                            newRecord.add(dataRecord.get(6));
                                            if ( dataRecord.get(9) != null && !(dataRecord.get(9).equals("")) )
                                                newRecord.add("=");
                                            else 
                                                newRecord.add("");
                                            newRecord.add(dataRecord.get(8));
                                            newRecord.add(dataRecord.get(9));
                                            courseReqs.add(newRecord);
                                       }
                                    }
                                    i_and++;
                                    String orCourses_s = (String)dataRecord.get(5);
                                    if ( orCourses_s != null && !(orCourses_s.equals(""))) {
                                       // System.out.println(" Or courses " + orCourses_s);
                                       String[] orCourses = orCourses_s.split("}}");
                                       for (String a : orCourses ) { 
                                            // System.out.println(a);
                                            ArrayList newRecord = new ArrayList();
                                            newRecord.add(dataRecord.get(0) + "_" + i_index++);
                                            newRecord.add(dataRecord.get(1));
                                            newRecord.add(dataRecord.get(2));
                                            newRecord.add(a);
                                            newRecord.add((new Integer(i_and)).toString());
                                            newRecord.add((new Integer(i_or++)).toString());
                                            newRecord.add(dataRecord.get(6));
                                            if ( dataRecord.get(9) != null && !(dataRecord.get(9).equals("")) )
                                                newRecord.add("=");
                                            else 
                                                newRecord.add("");
                                            newRecord.add(dataRecord.get(8));
                                            newRecord.add(dataRecord.get(9));
                                            courseReqs.add(newRecord);
                                       }
                                    }
                                    i_and++;
                             }
                       }
                }
}
