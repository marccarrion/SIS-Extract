package com.starfish.sisintegration.bt;

public class BTNode {    

     private static int ids;

     BTNode left, right;
     String connector;
     String concurrency;
     String test;
     String testscore;
     String prereq_integration_id; 
     String min_grde;
     int node_id;
 
     public BTNode() {
         node_id = BTNode.ids++;
         left = null;
         right = null;
         connector = null;
         concurrency = null;
         test = null;
         testscore = null;
         prereq_integration_id = null; 
         min_grde = null;
     }

     public BTNode(String p_connector, String p_concurrency, String p_test, String p_testscore, String p_prereq_integration_id, String p_min_grde) {
         node_id = BTNode.ids++;
         left = null;
         right = null;
         connector = p_connector;
         concurrency = p_concurrency;
         test = p_test;
         testscore = p_testscore;
         prereq_integration_id = p_prereq_integration_id;
         min_grde = p_min_grde;
     }

     public BTNode(BTNode a) {
         node_id = BTNode.ids++;
         if ( a.left != null ) left = new BTNode(a.left);
         if ( a.right != null ) right = new BTNode(a.right);
         connector = a.connector;
         concurrency = a.concurrency;
         test = a.test;
         testscore = a.testscore;
         prereq_integration_id = a.prereq_integration_id;
         min_grde = a.min_grde;
     }
     
     public void setLeft(BTNode n) {
         left = n;
     }

     public void setRight(BTNode n) {
         right = n;
     }

     public BTNode getLeft() {
         return left;
     }

     public BTNode getRight() {
         return right;
     }

     public String toString() {
         // Does not show concurrency
         if ( connector != null && connector.equals("A") )
            return "AND";
         else if ( connector != null && connector.equals("O") )
            return "OR";
         else if ( test != null )
            return "Test:" + test  + ":" + testscore; 
         else if ( prereq_integration_id != null )
            return prereq_integration_id  + ":" + min_grde;
         else
            return "void"; 
         /* 
         if ( connector != null )
            return "(-" + node_id + ":" + connector + "-)"; 
         else if ( test != null )
            return "(-" + node_id + ":" + test  + "." + testscore  + "-)"; 
         else if ( prereq_integration_id != null )
            return "(-" + node_id + ":" + prereq_integration_id  + "." + min_grde + "-)";
         else
            return "(-" + node_id + ": void-)"; 
         */
     }

     public void setData (String p_connector, String p_concurrency, String p_test, String p_testscore, String p_prereq_integration_id, String p_min_grde) {
         connector = p_connector;
         concurrency = p_concurrency;
         test = p_test;
         testscore = p_testscore;
         prereq_integration_id = p_prereq_integration_id;
         min_grde = p_min_grde;
     }

     public String getConnector() {
         return connector;
     }     

     public String getConcurrency() {
         return concurrency;
     }     

     public String getTest() {
         return test;
     }     

     public String getTestScore() {
         return testscore;
     }     

     public String getPrereqIntegrationID() {
         return prereq_integration_id;
     }     

     public String getMinGrade() {
         return min_grde;
     }     

     public int getID() {
         return node_id;
     }     

     public Boolean isAND() {
         return connector!=null && connector.equals("A");
     }

     public Boolean isOR() {
         return connector!=null && connector.equals("O");
     }

     public Boolean equals(BTNode node) {
          Boolean equal = true;
          if ( node == null ) return false;

          if ( this.connector == null && node.connector != null ) return false;
          if ( this.connector != null && node.connector == null ) return false;
          if ( this.connector != null ) equal = connector.equals(node.connector);

          if ( this.concurrency == null && node.concurrency != null ) return false;
          if ( this.concurrency != null && node.concurrency == null ) return false;
          if ( this.concurrency != null ) equal = concurrency.equals(node.concurrency);

          if ( this.test == null && node.test != null ) return false;
          if ( this.test != null && node.test == null ) return false;
          if ( this.test != null ) equal = equal && test.equals(node.test);

          if ( this.testscore == null && node.testscore != null ) return false;
          if ( this.testscore != null && node.testscore == null ) return false;
          if ( this.testscore != null ) equal = equal && testscore.equals(node.testscore);

          if ( this.prereq_integration_id == null && node.prereq_integration_id != null ) return false;
          if ( this.prereq_integration_id != null && node.prereq_integration_id == null ) return false;
          if ( this.prereq_integration_id != null ) equal = equal && prereq_integration_id.equals(node.prereq_integration_id);

          if ( this.min_grde == null && node.min_grde != null ) return false;
          if ( this.min_grde != null && node.min_grde == null ) return false;
          if ( this.min_grde != null ) equal = equal && min_grde.equals(node.min_grde);

          if ( this.left == null && node.left != null ) return false;
          if ( this.left != null && node.left == null ) return false;
          if ( this.left != null ) equal = equal && left.equals(node.left);

          if ( this.right == null && node.right != null ) return false;
          if ( this.right != null && node.right == null ) return false;
          if ( this.right != null ) equal = equal && right.equals(node.right);

          return equal;
     }

 }
