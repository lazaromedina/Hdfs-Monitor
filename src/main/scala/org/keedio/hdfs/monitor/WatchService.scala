package org.keedio.hdfs.monitor

import org.apache.commons.vfs2.impl.DefaultFileMonitor
import org.apache.commons.vfs2.{FileListener, FileObject, FileChangeEvent, VFS}
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit, Executors}


/**
 * Created by luislazaro on 4/9/15.
 * lalazaro@keedio.com
 * Keedio
 */

class WatchService(csvDir: String, refresh: Int, start: Int, regex: Regex) {

      private val fsManager = VFS.getManager
      private val pathForMonitor: FileObject = fsManager.resolveFile(csvDir)

      //list of susbcribers(observers) for changes in pathForMonitor
      private val listeners: ListBuffer[StateListener] = new ListBuffer[StateListener]

      //observer for changes to a file
      private val fileListener = new FileListener {
            override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit = fireEvent(fileChangeEvent)

            override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = fireEvent(fileChangeEvent)

            override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = fireEvent(fileChangeEvent)
      }

      //Thread based polling file system monitor with a 1 second delay.
      private val defaultMonitor: DefaultFileMonitor = new DefaultFileMonitor(fileListener)
      defaultMonitor.addFile(pathForMonitor)
      defaultMonitor.setRecursive(true)
      defaultMonitor.setDelay(secondsToMiliseconds(refresh))

      // the number of threads to keep in the pool, even if they are idle
      private val corePoolSize = 1
      private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize)
      //Creates and executes a one-shot action that becomes enabled after the given delay
      private val tasks: ScheduledFuture[_] = scheduler.schedule(
            getTaskToSchedule(),
            start,
            TimeUnit.SECONDS
      )

      /**
       * call this method whenever you want to notify the event listeners about a particular event
       */
      def fireEvent(fileChangeEvent: FileChangeEvent): Unit = {
            listeners foreach (_.statusReceived(fileChangeEvent))
      }

      /**
       * Add element to list of registered listeners
       * @param listener
       */
      def addEventListener(listener: StateListener): Unit = {
            listener +=: listeners
      }

      /**
       * Remove element from list of registered listeners
       * @param listener
       */
      def removeEventListener(listener: StateListener): Unit = {
            listeners.find(_ == listener) match {
                  case Some(listener) => {
                        listeners.remove(listeners.indexOf(listener))
                  }
                  case None => ()
            }
      }

      /**
       *
       * auxiliar for using seconds where miliseconds is requiered
       * @param seconds
       * @return
       */
      //FIXME:not sure this is right becacuse may change all Int to Long ???
      implicit def secondsToMiliseconds(seconds: Int): Long = {
            seconds * 10 ^ (-3)
      }


      /**
       * Make a method runnable and schedule for one-shot
       * @return
       */
      def getTaskToSchedule(): Runnable = {
            new Runnable {
                  override def run(): Unit = {
                        defaultMonitor.start()
                  }
            }
      }
}
