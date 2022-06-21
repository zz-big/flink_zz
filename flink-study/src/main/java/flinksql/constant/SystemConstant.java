package flinksql.constant;

import java.util.regex.Pattern;

public class SystemConstant {

    public final static String COMMENT_SYMBOL = "--";

    public final static String SEMICOLON = ";";

    public final static String LINE_FEED = "\n";

    public final static String SPACE = "";

    public static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

    public final static String JARVERSION = "lib/flinksql_test.jar";

    public static final String QUERY_JOBID_KEY_WORD = "job-submitted-success:";

    public static final String QUERY_JOBID_KEY_WORD_BACKUP = "Job has been submitted with JobID";

    public static final Long DEFAULT_CHECKPOINT_TIMEOUT = 1000 * 60 * 10L;

    public static final String NUMBER_DATE_TIME = "yyyyMMdd";

}
