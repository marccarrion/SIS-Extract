package com.starfish.sisintegration;
 
// import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;
        
       // display time and date using toString()
 
public class TestJob implements Job {
 
   // private Logger log = Logger.getLogger(TestJob.class);
 
   public void execute(JobExecutionContext jExeCtx) throws JobExecutionException {
      Date date = new Date();
      System.out.println("TestJob run successfully... " + date.toString());
      // log.debug("TestJob run successfully...");
   }
 
}

