package com.starfish.sisintegration;

import java.io.File;
import java.io.FileWriter;

import java.util.Properties;
import java.util.Arrays;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.List;
import java.util.ArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import edu.fresno.uniobjects.*;

public interface FileGenerator {
        public void createFile(Connection dbConn, String termStr, String fileStr, Properties properties) 
               throws Exception;

        public void createFile(UniDataConnection ud, String termStr, String fileStr, Properties properties) 
               throws Exception;
}
