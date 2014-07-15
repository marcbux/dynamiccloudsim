package de.huberlin.wbi.dcs;

import java.util.Random;

import de.huberlin.wbi.dcs.examples.Parameters;

public class DynamicModel {
	
	private double previousTime;
	private final Random numGen;
	
	private double miBaselineChangesPerHour;
	private double ioBaselineChangesPerHour;
	private double bwBaselineChangesPerHour;
	
	private double miBaselineCV;
	private double ioBaselineCV;
	private double bwBaselineCV;
	
	private double miNoiseCV;
	private double ioNoiseCV;
	private double bwNoiseCV;
	
	private double miCurrentBaseline;
	private double ioCurrentBaseline;
	private double bwCurrentBaseline;
	
	public DynamicModel(double baselineChangesPerHour, double cpuDynamicsCV, double ioDynamicsCV, double bwDynamicsCV, double cpuNoiseCV, double ioNoiseCV, double bwNoiseCV) {
		this.previousTime = 0;
		this.numGen = Parameters.numGen;
		
		this.miBaselineChangesPerHour = this.ioBaselineChangesPerHour = this.bwBaselineChangesPerHour = baselineChangesPerHour;
		this.miBaselineCV = cpuDynamicsCV;
		this.ioBaselineCV = ioDynamicsCV;
		this.bwBaselineCV = bwDynamicsCV;
		this.miNoiseCV = cpuNoiseCV;
		this.ioNoiseCV = ioNoiseCV;
		this.bwNoiseCV = bwNoiseCV;
		
		changeMiBaseline();
		changeIoBaseline();
		changeBwBaseline();
	}
	
	public DynamicModel(double baselineChangesPerHour, double baselineCV, double noiseCV) {
		this(baselineChangesPerHour, baselineCV, baselineCV, baselineCV, noiseCV, noiseCV, noiseCV);
	}
	
	private void changeMiBaseline() {
		miCurrentBaseline = 0;
		while (miCurrentBaseline <= 0) {
			miCurrentBaseline = 1 + numGen.nextGaussian() * miBaselineCV;
		}
	}
	
	private void changeIoBaseline() {
		ioCurrentBaseline = 0;
		while (ioCurrentBaseline <= 0) {
			ioCurrentBaseline = 1 + numGen.nextGaussian() * ioBaselineCV;
		}
	}
	
	private void changeBwBaseline() {
		bwCurrentBaseline = 0;
		while (bwCurrentBaseline <= 0) {
			bwCurrentBaseline = 1 + numGen.nextGaussian() * bwBaselineCV;
		}
	}
	
	public void updateBaselines (double timespan) {
		double timespanInHours = timespan / (60 * 60);

		// Assuming exponential distribution
		double chanceOfMiChange = 1 - Math.pow(Math.E, - (timespanInHours * miBaselineChangesPerHour));
		double chanceOfIoChange = 1 - Math.pow(Math.E, - (timespanInHours * ioBaselineChangesPerHour));
		double chanceOfBwChange = 1 - Math.pow(Math.E, - (timespanInHours * bwBaselineChangesPerHour));
		
		if (numGen.nextDouble() <= chanceOfMiChange) {
			changeMiBaseline();
		}
		if (numGen.nextDouble() <= chanceOfIoChange) {
			changeIoBaseline();
		}
		if (numGen.nextDouble() <= chanceOfBwChange) {
			changeBwBaseline();
		}
	}
	
	public double nextMiCoefficient() {
		double nextMiCoefficient = 0;
		while (nextMiCoefficient <= 0) {
			nextMiCoefficient = miCurrentBaseline + numGen.nextGaussian() * miNoiseCV * miCurrentBaseline;
		}
		return nextMiCoefficient;
	}
	
	public double nextIoCoefficient() {
		double nextIoCoefficient = 0;
		while (nextIoCoefficient <= 0) {
			nextIoCoefficient = ioCurrentBaseline + numGen.nextGaussian() * ioNoiseCV * ioCurrentBaseline;
		}
		return nextIoCoefficient;
	}

	public double nextBwCoefficient() {
		double nextBwCoefficient = 0;
		while (nextBwCoefficient <= 0) {
			nextBwCoefficient = bwCurrentBaseline + numGen.nextGaussian() * bwNoiseCV * bwCurrentBaseline;
		}
		return nextBwCoefficient;
	}
	
	public double getPreviousTime() {
		return previousTime;
	}
	
	public void setPreviousTime(double previousTime) {
		this.previousTime = previousTime;
	}

}
