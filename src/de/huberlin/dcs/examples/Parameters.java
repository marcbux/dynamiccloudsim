package de.huberlin.dcs.examples;

import java.util.Random;

public class Parameters {
	
	public static boolean outputDatacenterEvents = false;
	public static boolean outputWorkflowGraph = false;
	public static boolean outputVmPerformanceLogs = false;
	
	// experiment parameters
	public enum Scheduler {
		STATIC_ROUND_ROBIN,
		HEFT,
		JOB_QUEUE,
		LATE,
		C3
	}
	public static Scheduler scheduler = Scheduler.JOB_QUEUE;
	public static int numberOfRuns = 1000;

	// Heterogeneity parameters
	public static double cpuHeterogeneityCV = 0.4;
	public static double ioHeterogeneityCV = 0.15;
	public static double bwHeterogeneityCV = 0.2;
	
	// Dynamicity Parameters
	public static double baselineChangesPerHour = 0.5;
	public static double cpuDynamicsCV = 0.054;
	public static double ioDynamicsCV = 0.033;
	public static double bwDynamicsCV = 0.04;
	public static double cpuNoiseCV = 0.028;
	public static double ioNoiseCV = 0.007;
	public static double bwNoiseCV = 0.01;
	
	// straggler parameters
	public static double likelihoodOfStraggler = 0.015;
	public static double stragglerPerformanceCoefficient = 0.5;
	
	// the probability for a task to end in failure instead of success once it's execution time has passed
	public static double likelihoodOfFailure = 0.002;
	public static double runtimeFactorInCaseOfFailure = 20d;
	
	// the coefficient of variation for information that is typically not available in real-world scenarios
	// e.g., Task progress scores, HEFT runtime estimates
	public static double distortionCV = 0d;
	
	public static long seed = 130;
	public static Random numGen = new Random(seed);
	
	public static void parseParameters(String[] args) {
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].compareTo("-" + "outputVmPerformanceLogs") == 0) {
				outputVmPerformanceLogs = Boolean.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "scheduler") == 0) {
				scheduler = Scheduler.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "numberOfRuns") == 0) {
				numberOfRuns = Integer.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "heterogeneityCV") == 0) {
				cpuHeterogeneityCV = ioHeterogeneityCV = bwHeterogeneityCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "cpuHeterogeneityCV") == 0) {
				cpuHeterogeneityCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "ioHeterogeneityCV") == 0) {
				ioHeterogeneityCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "bwHeterogeneityCV") == 0) {
				bwHeterogeneityCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "baselineChangesPerHour") == 0) {
				baselineChangesPerHour = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "baselineCV") == 0) {
				cpuDynamicsCV = ioDynamicsCV = bwDynamicsCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "cpuDynamicsCV") == 0) {
				cpuDynamicsCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "ioDynamicsCV") == 0) {
				ioDynamicsCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "bwDynamicsCV") == 0) {
				bwDynamicsCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "noiseCV") == 0) {
				cpuNoiseCV = ioNoiseCV = bwNoiseCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "cpuNoiseCV") == 0) {
				cpuNoiseCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "ioNoiseCV") == 0) {
				ioNoiseCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "bwNoiseCV") == 0) {
				bwNoiseCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "likelihoodOfStraggler") == 0) {
				likelihoodOfStraggler = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "stragglerPerformanceCoefficient") == 0) {
				stragglerPerformanceCoefficient = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "likelihoodOfFailure") == 0) {
				likelihoodOfFailure = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "runtimeFactorInCaseOfFailure") == 0) {
				runtimeFactorInCaseOfFailure = Double.valueOf(args[++i]);
			}
		}
		
	}
	
}
