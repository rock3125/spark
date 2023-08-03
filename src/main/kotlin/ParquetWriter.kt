import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import java.io.BufferedOutputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

/**
 * create a file writer for Parquet file output
 */
object ParquetWriter {

    fun outputFile(fileName: String, ioBufferSize: Int = 16384): OutputFile {
        return object : OutputFile {
            override fun create(blockSizeHint: Long): PositionOutputStream {
                return makePositionOutputStream(Paths.get(fileName), ioBufferSize, false)
            }
            override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
                return makePositionOutputStream(Paths.get(fileName), ioBufferSize, true)
            }
            override fun supportsBlockSize(): Boolean {
                return false
            }
            override fun defaultBlockSize(): Long {
                return 0
            }
        }
    }

    private fun makePositionOutputStream(file: Path, ioBufSize: Int, trunc: Boolean): PositionOutputStream {
        val  output: OutputStream =
                BufferedOutputStream(Files.newOutputStream(file, StandardOpenOption.CREATE, if (trunc) StandardOpenOption.TRUNCATE_EXISTING else StandardOpenOption.APPEND), ioBufSize)
        return object : PositionOutputStream(){
            private var position: Long = 0L
            override fun write(b: Int){
                output.write(b)
                position++
            }
            override fun write(b: ByteArray){
                output.write(b)
                position += b.size.toLong()
            }
            override fun write(b: ByteArray, off: Int, len: Int) {
                output.write(b, off, len)
                position += len.toLong()
            }
            override fun flush(){
                output.flush()
            }
            override fun close(){
                output.close()
            }
            override fun getPos(): Long{
                return position
            }
        }
    }


}

