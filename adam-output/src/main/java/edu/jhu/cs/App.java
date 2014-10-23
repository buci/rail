package edu.jhu.cs;

import htsjdk.samtools.*;
import org.apache.hadoop.fs.Path;
import org.bdgenomics.adam.converters.SAMRecordConverter;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.formats.avro.AlignmentRecord;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        final String inputFileName = args[0].trim();
        final String outputFileName = args[1].trim();

        final SAMFileReader samReader = new SAMFileReader(new File(inputFileName), null, true);
        samReader.setValidationStringency(ValidationStringency.LENIENT);
        final SequenceDictionary seqDict = SequenceDictionary.fromSAMHeader(samReader.getFileHeader());
        final RecordGroupDictionary rgDict = RecordGroupDictionary.fromSAMHeader(samReader.getFileHeader());

        final SAMRecordConverter samRecordConverter = new SAMRecordConverter();
        final AvroParquetWriter parquetWriter = new AvroParquetWriter<AlignmentRecord>(new Path(outputFileName), AlignmentRecord.SCHEMA$, CompressionCodecName.GZIP, 128 * 1024 * 1024, 1 * 1024 * 1024, true);

        int i = 0;

        final Set<String> startEndSet = new HashSet<String>();

        for (SAMRecord samRecord : samReader) {
            final AlignmentRecord convert = samRecordConverter.convert(samRecord, seqDict, rgDict);
            parquetWriter.write(convert);
            String startEndString = convert.getStart() + ":" + convert.getEnd();
            startEndSet.add(startEndString);
            i++;
            if (i % 10000 == 0) {
                System.out.println("wrote " + i + " records.");
            }
        }
        samReader.close();
        parquetWriter.close();
        System.out.println("wrote " + i + " records.");
    }
}
