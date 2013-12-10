package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.formats.FormatTools;
import com.thinkaurelius.faunus.formats.JobConfigurationFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class FaunusJobControl implements Runnable {

    public static final Logger logger = Logger.getLogger(FaunusJobControl.class);

    private final FaunusGraph graph;
    private final LinkedList<Job> jobsInProgress = new LinkedList<>();
    private Job runningJob = null;
    private final LinkedList<Job> successfulJobs = new LinkedList<>();
    private final LinkedList<Job> failedJobs = new LinkedList<>();

    public FaunusJobControl(FaunusGraph graph, Collection<Job> jobs) {
        this.graph = graph;
        jobsInProgress.addAll(jobs);
    }

    private List<Job> toList(LinkedList<Job> jobs) {
        ArrayList<Job> retv = new ArrayList<>();
        synchronized (jobs) {
            retv.addAll(jobs);
        }
        return retv;
    }

    public List<Job> getJobsInProgress() {
        return toList(jobsInProgress);
    }

    synchronized public Job getRunningJob() {
        return runningJob;
    }

    public List<Job> getSuccessfulJobs() {
        return toList(successfulJobs);
    }

    public List<Job> getFailedJobs() {
        return toList(failedJobs);
    }

    synchronized public boolean allFinished() {
        return jobsInProgress.isEmpty();
    }

    @Override
    public void run() {
        try {
            final FileSystem hdfs = FileSystem.get(this.graph.getConf());
            if (this.graph.getOutputLocationOverwrite() && hdfs.exists(this.graph.getOutputLocation())) {
                hdfs.delete(this.graph.getOutputLocation(), true);
            }

            int jobCount = this.jobsInProgress.size();
            final String jobPath = this.graph.getOutputLocation().toString() + "/" + Tokens.JOB;

            Iterator<Job> it = jobsInProgress.iterator();
            for (int i = 0; i < jobCount; ++i) {
                synchronized (this) {
                    runningJob = it.next();

                    try {
                        ((JobConfigurationFormat) (FormatTools.getBaseOutputFormatClass(runningJob).newInstance())).updateJob(runningJob);
                    } catch (final Exception e) {
                    }

                    logger.info("Executing job " + (i + 1) + " out of " + jobCount + ": " + runningJob.getJobName());
                    logger.info("Job data location: " + jobPath + "-" + i);

                    runningJob.submit();
                }

                boolean success = runningJob.waitForCompletion(true);

                synchronized (this) {
                    if (success) {
                        successfulJobs.add(runningJob);
                    } else {
                        failedJobs.add(runningJob);
                    }

                    it.remove();
                    runningJob = null;
                }

                if (i > 0) {
                    final Path path = new Path(jobPath + "-" + (i - 1));
                    // delete previous intermediate graph data
                    for (final FileStatus temp : hdfs.globStatus(new Path(path.toString() + "/" + Tokens.GRAPH + "*"))) {
                        hdfs.delete(temp.getPath(), true);
                    }
                    // delete previous intermediate graph data
                    for (final FileStatus temp : hdfs.globStatus(new Path(path.toString() + "/" + Tokens.PART + "*"))) {
                        hdfs.delete(temp.getPath(), true);
                    }
                }
                if (!success) {
                    logger.error("Faunus job error -- remaining MapReduce jobs have been canceled");
                    break;
                }
            }

        } catch (Throwable t) {
            logger.error("Error while trying to run jobs.", t);
            // Mark all the jobs as failed because we got something bad.
            failAllJobs(t);
        }
    }

    synchronized private void failAllJobs(Throwable t) {
        String message = "Unexpected System Error Occurred " + StringUtils.stringifyException(t);

        if (runningJob != null) {
            try {
                runningJob.killJob();
            } catch (IOException e) {
                logger.error("Error trying to clean up " + runningJob.getJobName(), e);
            } finally {
                failedJobs.add(runningJob);
                runningJob = null;
            }
        }
    }
}
