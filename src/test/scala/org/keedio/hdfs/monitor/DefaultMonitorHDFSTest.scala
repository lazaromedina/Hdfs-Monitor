package org.keedio.hdfs.monitor

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl.DefaultFileMonitor
import org.apache.commons.vfs2.provider.hdfs.HdfsFileSystemConfigBuilder
import org.junit.Test




/**
 * Created by luislazaro on 8/9/15.
 * lalazaro@keedio.com
 * Keedio
 */
class DefaultMonitorHDFSTest {

    /**
     * @see https://commons.apache.org/proper/commons-vfs/filesystems.html
     */
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

        //monitoring
        val listendir: FileObject = fsManager.resolveFile("hdfs://10.129.135.140:8020/tmp", opts)

        val fm = new DefaultFileMonitor(new FileListener {
            override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit = println("file deleted")

            override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = println("file changed")

            override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = println("file created")
        })

        fm.setRecursive(true)
        fm.addFile(listendir)
        fm.setDelay(0) //if not set or set to 0 seconds, file changed is not fired so it is not detected.
        fm.start()
        Thread.sleep(10000)
        fm.stop()

    }

    @Test
    def testMiniDFSCluster():Unit = {
        MiniDFSCluster
    }


}
