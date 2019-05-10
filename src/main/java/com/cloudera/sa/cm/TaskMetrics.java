package com.cloudera.sa.cm;

import java.util.HashSet;
import java.util.Set;

/**
 * All possible metrics for a Impala Query used here.
 */
public class TaskMetrics {
    private double maxMemoryGb;
    private double duration;
    private double maxDuration;
    private double admissionDurtaion;
    private double maxAdmissionDurtaion;
    private long totalInputBuytes;
    private long totalOutputBytes;
    private long maxInputBytes;
    private long maxOutputBytes;
    private Set<String> fileFormats;
    private Set<String> queues;
    private Set<String> users;

    public TaskMetrics() {
        maxMemoryGb = 0;
        duration = 0;
        maxDuration = 0;
        admissionDurtaion = 0;
        maxAdmissionDurtaion = 0;
        totalInputBuytes = 0;
        totalOutputBytes = 0;
        maxInputBytes = 0;
        maxOutputBytes = 0;
        fileFormats = new HashSet<>();
        queues = new HashSet<>();
        users = new HashSet<>();
    }

    /**
     * Get the max memory used.
     * @return Max memory used.
     */
    public double getMaxMemoryGb() {
        return maxMemoryGb;
    }

    /**
     * Get the total duration.
     * @return Total duration.
     */
    public double getDuration() {
        return duration;
    }

    /**
     * Get the total wait time.
     * @return Total wait time.
     */
    public double getAdmissionDurtaion() {
        return admissionDurtaion;
    }

    /**
     * Get the total input bytes.
     * @return Total input bytes.
     */
    public long getTotalInputBuytes() {
        return totalInputBuytes;
    }

    /**
     * Get total output bytes
     * @return Total output bytes.
     */
    public long getTotalOutputBytes() {
        return totalOutputBytes;
    }

    /**
     * Get max input bytes for statements.
     * @return Max input bytes for on statement.
     */
    public long getMaxInputBytes() {
        return maxInputBytes;
    }

    /**
     * Get max output bytes for statements.
     * @return Max output bytes for statements.
     */
    public long getMaxOutputBytes() {
        return maxOutputBytes;
    }

    /**
     * Get seen file formats.
     * @return File formats.
     */
    public Set<String> getFileFormats() {
        return fileFormats;
    }

    /**
     * Get all queues.
     * @return Queues.
     */
    public Set<String> getQueues() {
        return queues;
    }

    /**
     * Get all users.
     * @return All users.
     */
    public Set<String> getUsers() {
        return users;
    }

    /**
     * Get max duration of statements.
     * @return Max duration of statements.
     */
    public double getMaxDuration() {
        return maxDuration;
    }

    /**
     * Get max wait time of statements.
     * @return Max wait time of statements.
     */
    public double getMaxAdmissionDurtaion() {
        return maxAdmissionDurtaion;
    }

    /**
     * Add memory to the metrice.
     * @param memory Memory in GB.
     */
    public void updateMemoryGb(double memory) {
        maxMemoryGb = Math.max(maxMemoryGb, memory);
    }

    /**
     * Add duration to the metrics.
     * @param newDuration Duration in seconds.
     */
    public void updateDuration(double newDuration) {
        duration += newDuration;
        maxDuration = Math.max(maxDuration, newDuration);
    }

    /**
     * Add admission wait time to the metrics.
     * @param admissionWait Admission wait time to the metrics.
     */
    public void updateAdmissionWait(double admissionWait) {
        admissionDurtaion += admissionWait;
        maxAdmissionDurtaion = Math.max(maxAdmissionDurtaion, admissionWait);
    }

    /**
     * Add input bytes to the metrics.
     * @param inputBytes Input bytes to the metrics.
     */
    public void updateInputBytes(long inputBytes) {
        totalInputBuytes += inputBytes;
        maxInputBytes = Math.max(maxInputBytes, inputBytes);
    }

    /**
     * Add output bytes to the metrics.
     * @param outputBytes Output bytes to the metrics.
     */
    public void updateOutputBytes(long outputBytes) {
        totalOutputBytes += outputBytes;
        maxOutputBytes = Math.max(maxOutputBytes, outputBytes);
    }

    /**
     * Add input format to the metrics
     * @param inputFormat Input format.
     */
    public void addInputFormat(String inputFormat) {
        fileFormats.add(inputFormat);
    }

    /**
     * Add queue to the metrics.
     * @param queue Queue to the metrics.
     */
    public void addQueue(String queue) {
        queues.add(queue);
    }

    /**
     * Add user to the metrics.
     * @param user User to the metrics.
     */
    public void addUser(String user) {
        users.add(user);
    }

    /**
     * Merge information in the two metrics.
     * @param task Another metrics.
     */
    public void updateMetrics(TaskMetrics task) {
        maxMemoryGb = Math.max(maxMemoryGb, task.maxMemoryGb);
        duration += task.duration;
        maxDuration = Math.max(maxDuration, task.maxDuration);
        admissionDurtaion += task.admissionDurtaion;
        maxAdmissionDurtaion = Math.max(maxAdmissionDurtaion, task.maxAdmissionDurtaion);;
        totalInputBuytes += task.totalInputBuytes;
        totalOutputBytes += task.totalOutputBytes;
        maxInputBytes = Math.max(maxInputBytes, task.maxInputBytes);
        maxOutputBytes = Math.max(maxOutputBytes, task.maxOutputBytes);
        fileFormats.addAll(task.fileFormats);
        queues.addAll(task.queues);
        users.addAll(task.users);
    }

}
