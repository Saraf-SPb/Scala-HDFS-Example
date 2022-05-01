import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.io.InputStream
import java.net.URI
import scala.util.matching.Regex

object main extends App {
  val conf = new Configuration()
  val CoreSitePath = new Path("core-site.xml")
  val HDFSSitePath = new Path("hdfs-site.xml")
  val basePath = "hdfs://localhost:9000"
  val sourcePath: Path = new Path("/stage")
  val targetPath = new Path("/ods")
  val fileNamePattern = "part-0000.csv"
  val fileNameFilterPattern = ".*part-\\d+.csv$"
  private val fileSystem = FileSystem.get(new URI(basePath), conf)

  conf.addResource(CoreSitePath)
  conf.addResource(HDFSSitePath)

  createFolder(s"$basePath$targetPath")

  val status = fileSystem.listStatus(sourcePath)
  status.foreach(x => dataMotion(x.getPath))

  def dataMotion(path: Path): Unit = {
    val folderNameTarget = path.toString.replace(sourcePath.toString, targetPath.toString)
    createFolder(folderNameTarget)
    val out = fileSystem.create(new Path(s"$folderNameTarget/$fileNamePattern"))
    val b = new Array[Byte](1024)

    val fileList = fileSystem.listStatus(path)
    fileList
      .filter(x => fileNameFilter(x.getPath))
      .foreach(x => {
        val in: InputStream = fileSystem.open(x.getPath)
        var numBytes = in.read(b)

        while (numBytes > 0) {
          out.write(b, 0, numBytes)
          numBytes = in.read(b)
        }
        in.close()
      })

    out.close()
  }

  def fileNameFilter(fileName: Path): Boolean = {
    val pattern: Regex = fileNameFilterPattern.r
    pattern.unapplySeq(fileName.toString).isDefined
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}
