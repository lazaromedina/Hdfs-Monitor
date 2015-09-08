package org.keedio.hdfs.monitor

import org.apache.commons.vfs2.impl.DefaultFileMonitor
import org.apache.commons.vfs2.provider.hdfs.HdfsFileSystemConfigBuilder
import org.apache.commons.vfs2._
import org.junit.Test

/**
 * Created by luislazaro on 8/9/15.
 * lalazaro@keedio.com
 * Keedio
 */
class DefaultMonitorHDFSTest {

    @Test
    def testApiFileMonitorHDFSFileSystem(): Unit = {
        val fsManager = VFS.getManager

        val opts: FileSystemOptions = new FileSystemOptions()
        val hdfsBuilder = HdfsFileSystemConfigBuilder.getInstance()
        hdfsBuilder.setConfigName(opts, "hdfsSystem")

        //just accesing hdfs system
        val fileObject = fsManager.resolveFile("hdfs://10.129.135.140:8020/tmp", opts)
        val children: Array[FileObject] = fileObject.getChildren
        children.foreach(f => println(f.getName.getBaseName))

        //val file1 = fsManager.resolveFile("hdfs://10.129.135.140:8020/tmp/fichero3_hdfs.txt", opts)

        //monitoring
        val listendir: FileObject = fsManager.resolveFile("hdfs://10.129.135.140:8020/tmp", opts)

        val fm = new DefaultFileMonitor(new FileListener {
            override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit = println("file deleted")

            override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = println("file changed")

            override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = println("file created")
        })

        fm.setRecursive(false)
        fm.addFile(listendir)
        fm.setDelay(0) //if not set or set to 0 seconds, file changed is not fired so it is not detected.
        fm.start()
        Thread.sleep(10000)

        //                try {
        //                    file1.createFile()
        //                    //Thread.sleep(1000)
        //                    file1.delete()
        //                } catch {
        //                    case e: IOException => println("I/O: ", e)
        //                }

        fm.stop()
    }

}
