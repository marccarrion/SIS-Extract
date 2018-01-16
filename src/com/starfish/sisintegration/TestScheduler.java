package com.starfish.sisintegration;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
 
import com.starfish.sisintegration.TestJob;
 
/**
* @author onlinetechvision.com
* @since 17 Sept 2011
* @version 1.0.0
*
*/
public class TestScheduler {
   public static void main(String[] args) throws Exception {
 
      try {
 
         // specify the job' s details..
         JobDetail job = JobBuilder.newJob(TestJob.class)
                                   .withIdentity("testJob")
                                   .build();
 
         // specify the running period of the job
         // http://quartz-scheduler.org/api/2.2.0/org/quartz/CronExpression.html
         // String schedule = "0 20 14 ?  * MON,TUE,WED,THU,FRI";
         String schedule = "0 20,25,32,36,47,50,51,58 14 ?  * MON,TUE,WED,THU,FRI";
/*
         Trigger trigger = TriggerBuilder.newTrigger()
                                         .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                                                                            .withIntervalInSeconds(30)
                                                                            .repeatForever())
                                          .build();
*/
         Trigger trigger = TriggerBuilder.newTrigger()
                                         .withSchedule(CronScheduleBuilder.cronSchedule(new CronExpression(schedule)))
                                         .build();
 
         //schedule the job
         SchedulerFactory schFactory = new StdSchedulerFactory();
         Scheduler sch = schFactory.getScheduler();
         sch.start();
         sch.scheduleJob(job, trigger);
 
      } catch (SchedulerException e) {
         e.printStackTrace();
      }
   }
}
