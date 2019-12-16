package spark.scala;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetReader {

  public static void main(String... args) throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/output/part-00000-5d4228dc-f39e-4e76-877f-efd406195a30-c000.snappy.parquet");
    InputFile inputFile = HadoopInputFile.fromPath(path, conf);
    ObjectMapper objectMapper = new ObjectMapper();
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

    MessageType schema = reader.getFooter().getFileMetaData().getSchema();

    // Row Group
    reader.getRowGroups().stream().forEach(rowGroup -> {
      try {

      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    System.out.println(objectMapper.writeValueAsString(reader.getFooter()));
  }
}
