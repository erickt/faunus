package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.util.List;

public class FaunusTool extends Configured implements Tool {
    public static final Logger logger = Logger.getLogger(FaunusTool.class);

    private final FaunusGraph graph;
    private final List<Job> jobs;

    public FaunusTool(FaunusGraph graph, List<Job> jobs) {
        this.graph = graph;
        this.jobs = jobs;
    }

    @Override
    public int run(String[] args) throws Exception {

        String script = null;
        boolean showHeader = true;

        if (args.length == 2) {
            script = args[0];
            showHeader = Boolean.valueOf(args[1]);
        }

        if (showHeader) {
            logger.info("Faunus: Graph Analytics Engine");
            logger.info("        ,");
            logger.info("    ,   |\\ ,__");
            logger.info("    |\\   \\/   `\\");
            logger.info("    \\ `-.:.     `\\");
            logger.info("     `-.__ `\\/\\/\\|");
            logger.info("        / `'/ () \\");
            logger.info("      .'   /\\     )");
            logger.info("   .-'  .'| \\  \\__");
            logger.info(" .'  __(  \\  '`(()");
            logger.info("/_.'`  `.  |    )(");
            logger.info("         \\ |");
            logger.info("          |/");
        }

        if (null != script && !script.isEmpty()) {
            logger.info("Generating job chain: " + script);
        }

        logger.info("Compiled to " + this.jobs.size() + " MapReduce job(s)");

        FaunusJobControl faunusJobControl = new FaunusJobControl(graph, jobs);
        faunusJobControl.run();

        if (faunusJobControl.getFailedJobs().size() == 0) {
            return 0;
        } else {
            return -1;
        }
    }
}
