package edu.jhu.cs.rail.util;

/**
 * There are minor difference in input and standard ordering of fields in a SAMRecord. Rearranging the fields in order.
 */
public class RearrangeSAMFields {

    // This is the order of the fields in data generated in previous step.
    private static final int SAMPLE = 0; // This is not being used.
    private static final int RNAME = 1;
    private static final int POS = 2;
    private static final int QNAME = 3;
    private static final int FLAG = 4;
    private static final int MAPQ = 5;
    private static final int CIGAR = 6;
    private static final int RNEXT = 7;
    private static final int PNEXT = 8;
    private static final int TLEN = 9;
    private static final int SEQ = 10;
    private static final int QUAL = 11;
    private static final int NUM_REQUIRED_FIELDS = 12;

    // Map-reduce jobs are thread-safe thus static sb will not be any issue.
    private static final StringBuilder sb = new StringBuilder();

    public static String reArragneField(final String samString) {
        sb.setLength(0);

        final String[] splits = samString.split("\t");
        sb.append(splits[QNAME]).append("\t");
        sb.append(splits[FLAG]).append("\t");
        sb.append(splits[RNAME]).append("\t");
        sb.append(splits[POS]).append("\t");
        sb.append(splits[MAPQ]).append("\t");
        sb.append(splits[CIGAR]).append("\t");
        sb.append(splits[RNEXT]).append("\t");
        sb.append(splits[PNEXT]).append("\t");
        sb.append(splits[TLEN]).append("\t");
        sb.append(splits[SEQ]).append("\t");
        sb.append(splits[QUAL]).append("\t");
        for (int i = NUM_REQUIRED_FIELDS; i < splits.length; i++) {
            sb.append(splits[i]).append("\t");
        }
        return sb.toString();
    }
}
