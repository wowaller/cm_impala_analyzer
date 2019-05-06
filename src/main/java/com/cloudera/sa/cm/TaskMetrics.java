package com.cloudera.sa.cm;

import java.util.HashSet;
import java.util.Set;

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
    }

    public double getMaxMemoryGb() {
        return maxMemoryGb;
    }

    public double getDuration() {
        return duration;
    }

    public double getAdmissionDurtaion() {
        return admissionDurtaion;
    }

    public long getTotalInputBuytes() {
        return totalInputBuytes;
    }

    public long getTotalOutputBytes() {
        return totalOutputBytes;
    }

    public long getMaxInputBytes() {
        return maxInputBytes;
    }

    public long getMaxOutputBytes() {
        return maxOutputBytes;
    }

    public Set<String> getFileFormats() {
        return fileFormats;
    }

    public Set<String> getQueues() {
        return queues;
    }

    public double getMaxDuration() {
        return maxDuration;
    }

    public double getMaxAdmissionDurtaion() {
        return maxAdmissionDurtaion;
    }

    public void updateMemoryGb(double memory) {
        maxMemoryGb = Math.max(maxMemoryGb, memory);
    }

    public void updateDuration(double newDuration) {
        duration += newDuration;
        maxDuration = Math.max(maxDuration, newDuration);
    }

    public void updateAdmissionWait(double admissionWait) {
        admissionDurtaion += admissionWait;
        maxAdmissionDurtaion = Math.max(maxAdmissionDurtaion, admissionWait);
    }

    public void updateInputBytes(long inputBytes) {
        totalInputBuytes += inputBytes;
        maxInputBytes = Math.max(maxInputBytes, inputBytes);
    }

    public void updateOutputBytes(long outputBytes) {
        totalOutputBytes += outputBytes;
        maxOutputBytes = Math.max(maxOutputBytes, outputBytes);
    }

    public void addInputFormat(String inputFormat) {
        fileFormats.add(inputFormat);
    }

    public void addQueue(String queue) {
        queues.add(queue);
    }

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
    }

}
