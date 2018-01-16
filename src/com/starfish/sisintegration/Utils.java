package com.starfish.sisintegration;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Arrays;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.ArrayList;

// Java 1.8 
// java.util.Base64
import org.apache.commons.codec.binary.Base64;

import java.security.*;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;

// Colleague
import edu.fresno.uniobjects.*;
import edu.fresno.uniobjects.data.*;

public class Utils {
        static final String NEW_LINE_SEPARATOR = "\r\n";
        private static final String ALGO = "AES";
        public static final int MAX_ITERATIONS  = 100000;
    
        private static Key generateKey(byte[] keyValue) throws Exception {
             Key key = new SecretKeySpec(keyValue, ALGO);
             return key;
        }

        public static String encrypt(String Data, byte[] keyValue) throws Exception {
             Key key = generateKey(keyValue);
             Cipher c = Cipher.getInstance(ALGO);
             c.init(Cipher.ENCRYPT_MODE, key);
             byte[] encVal = c.doFinal(Data.getBytes());
             String encryptedValue = new Base64().encodeBase64String(encVal);
             return encryptedValue;
        }

        public static String decrypt(String encryptedData, byte[] keyValue) throws Exception {
             Key key = generateKey(keyValue);
             Cipher c = Cipher.getInstance(ALGO);
             c.init(Cipher.DECRYPT_MODE, key);
             byte[] decodedValue = new Base64().decodeBase64(encryptedData);
             byte[] decValue = c.doFinal(decodedValue);
             String decryptedValue = new String(decValue);
             return decryptedValue;
        }

        public static String replaceVariables(String queryStr, String fileStr, Properties properties) {
             String newQueryStr = queryStr;
             Pattern pattern = Pattern.compile("\\{\\{(.*?)\\}\\}");
             Matcher matcher = pattern.matcher(queryStr);
             while(matcher.find()) {
                             String found = matcher.group(1);
                             // System.out.println("looking for variable: " + found);
                             String replaceStr = properties.getProperty(fileStr + "." + found);
                             // TODO Add error messaging to variable not found
                             // System.out.println("2. " + fileStr + "-" + found + "-" + replaceStr);
                             if (found.startsWith("_")) {
                                replaceStr = "'" + replaceStr.replace(",","','") + "'";
                             }
                             newQueryStr = newQueryStr.replace("{{"+found+"}}", replaceStr);
             }
             // System.out.println("New Query: " + newQueryStr);
             return newQueryStr;
        }

        public static void concatHistory(UniDataConnection ud, String[] steps, String propertyName, StringBuffer ids, String fileStr, Properties properties) throws Exception {
                   // System.out.println(steps);
                   // System.out.println(ids);
                   String result = new String();
                   properties.setProperty(fileStr + "." + propertyName, ids.toString());
                   // System.out.println(properties.getProperty(fileStr + "." + propertyName, "not found"));
                   for (String step : steps) { 
                        if (step != null) {
                           step = replaceVariables(step, fileStr, properties);
                           // System.out.println(step);
                           result = runStep(ud, step);
                           // System.out.println("Concat step : " + result);
                        }
                   }
                   System.out.println("Concat finished : " + result);
        }

        public static String runStep(UniDataConnection ud, String command) throws Exception {
           String response = ud.query(command);
           return response;
        }

        public static String getData(FieldSet set, String field, String default_str) {
           if (set.getFieldByName(field) != null) {
                if (set.getFieldByName(field).getData() != null) {
                       return set.getFieldByName(field).getData();
                }
           }
           return default_str;
        }

}
