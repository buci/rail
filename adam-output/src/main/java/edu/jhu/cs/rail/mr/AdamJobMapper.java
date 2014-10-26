package edu.jhu.cs.rail.mr;

import edu.jhu.cs.rail.util.RearrangeSAMFields;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMLineParser;
import htsjdk.samtools.SAMRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bdgenomics.adam.converters.SAMRecordConverter;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.formats.avro.AlignmentRecord;

import java.io.IOException;

/**
 * Mapper class.
 * This class is responsible for parsing SAMRecord from tab-delimited SAM text and emit AlignmentRecord.
 */
public class AdamJobMapper extends Mapper<LongWritable, Text, LongWritable, AvroWrapper<AlignmentRecord>> {
    private final LongWritable mapperEmitKey = new LongWritable();
    private final SAMFileHeader samFileHeader = new SAMFileHeader(); //Using default SAMFileHeader.
    private final SAMLineParser samLineParser = new SAMLineParser(samFileHeader);
    private final SequenceDictionary seqDict = SequenceDictionary.fromSAMHeader(samFileHeader);
    private final RecordGroupDictionary rgDict = RecordGroupDictionary.fromSAMHeader(samFileHeader);
    private final SAMRecordConverter samRecordConverter = new SAMRecordConverter();

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        // Input from SAM file has some one extra field at the beginning and few fields are out of order. Rearranging them.
        final String reArrangedRecord = RearrangeSAMFields.reArragneField(value.toString());

        final SAMRecord samRrecord = samLineParser.parseLine(reArrangedRecord);
        if (samRrecord.getAlignmentStart() == 0) {
            // If read is unaligned, we are discarding it.
            return;
        }
        final AlignmentRecord record = samRecordConverter.convert(samRrecord, seqDict, rgDict);
        mapperEmitKey.set(record.getStart());
        context.write(mapperEmitKey, new AvroWrapper<AlignmentRecord>(record));
    }
}
