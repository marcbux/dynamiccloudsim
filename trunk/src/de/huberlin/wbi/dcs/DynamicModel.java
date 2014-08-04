package de.huberlin.wbi.dcs;

import java.util.Random;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import de.huberlin.wbi.dcs.examples.Parameters;

public class DynamicModel {

	private double previousTime;
	private final Random numGen;

	private double miCurrentBaseline;
	private double ioCurrentBaseline;
	private double bwCurrentBaseline;

	public DynamicModel() {
		this.previousTime = 0;
		this.numGen = Parameters.numGen;

		changeMiBaseline();
		changeIoBaseline();
		changeBwBaseline();
	}

	private void changeMiBaseline() {
		double mean = 1d;
		double dev = Parameters.cpuDynamicsCV;
		ContinuousDistribution dist = Parameters.getDistribution(Parameters.cpuDynamicsDistribution, mean, Parameters.cpuDynamicsAlpha, Parameters.cpuDynamicsBeta, dev, Parameters.cpuDynamicsShape, Parameters.cpuDynamicsLocation, Parameters.cpuDynamicsShift, Parameters.cpuDynamicsMin, Parameters.cpuDynamicsMax, Parameters.cpuDynamicsPopulation);
		miCurrentBaseline = 0;
		while (miCurrentBaseline <= 0) {
			miCurrentBaseline = dist.sample();
		}
	}

	private void changeIoBaseline() {
		double mean = 1d;
		double dev = Parameters.ioDynamicsCV;
		ContinuousDistribution dist = Parameters.getDistribution(Parameters.ioDynamicsDistribution, mean, Parameters.ioDynamicsAlpha, Parameters.ioDynamicsBeta, dev, Parameters.ioDynamicsShape, Parameters.ioDynamicsLocation, Parameters.ioDynamicsShift, Parameters.ioDynamicsMin, Parameters.ioDynamicsMax, Parameters.ioDynamicsPopulation);
		ioCurrentBaseline = 0;
		while (ioCurrentBaseline <= 0) {
			ioCurrentBaseline = dist.sample();
		}
	}

	private void changeBwBaseline() {
		double mean = 1d;
		double dev = Parameters.bwDynamicsCV;
		ContinuousDistribution dist = Parameters.getDistribution(Parameters.bwDynamicsDistribution, mean, Parameters.bwDynamicsAlpha, Parameters.bwDynamicsBeta, dev, Parameters.bwDynamicsShape, Parameters.bwDynamicsLocation, Parameters.bwDynamicsShift, Parameters.bwDynamicsMin, Parameters.bwDynamicsMax, Parameters.bwDynamicsPopulation);
		bwCurrentBaseline = 0;
		while (bwCurrentBaseline <= 0) {
			bwCurrentBaseline = dist.sample();
		}
	}

	public void updateBaselines(double timespan) {
		double timespanInHours = timespan / (60 * 60);

		// Assuming exponential distribution
		double chanceOfMiChange = 1 - Math.pow(Math.E,
				-(timespanInHours * Parameters.cpuBaselineChangesPerHour));
		double chanceOfIoChange = 1 - Math.pow(Math.E,
				-(timespanInHours * Parameters.ioBaselineChangesPerHour));
		double chanceOfBwChange = 1 - Math.pow(Math.E,
				-(timespanInHours * Parameters.bwBaselineChangesPerHour));

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
		double mean = miCurrentBaseline;
		double dev = Parameters.cpuNoiseCV
				* miCurrentBaseline;
		ContinuousDistribution dist = Parameters.getDistribution(Parameters.cpuNoiseDistribution, mean, Parameters.cpuNoiseAlpha, Parameters.cpuNoiseBeta, dev, Parameters.cpuNoiseShape, Parameters.cpuNoiseLocation, Parameters.cpuNoiseShift, Parameters.cpuNoiseMin, Parameters.cpuNoiseMax, Parameters.cpuNoisePopulation);
		double nextMiCoefficient = 0;
		while (nextMiCoefficient <= 0) {
			nextMiCoefficient = dist.sample();
		}
		return nextMiCoefficient;
	}

	public double nextIoCoefficient() {
		double mean = ioCurrentBaseline;
		double dev = Parameters.ioNoiseCV
				* ioCurrentBaseline;
		ContinuousDistribution dist = Parameters.getDistribution(Parameters.ioNoiseDistribution, mean, Parameters.ioNoiseAlpha, Parameters.ioNoiseBeta, dev, Parameters.ioNoiseShape, Parameters.ioNoiseLocation, Parameters.ioNoiseShift, Parameters.ioNoiseMin, Parameters.ioNoiseMax, Parameters.ioNoisePopulation);
		double nextIoCoefficient = 0;
		while (nextIoCoefficient <= 0) {
			nextIoCoefficient = dist.sample();
		}
		return nextIoCoefficient;
	}

	public double nextBwCoefficient() {
		double mean = bwCurrentBaseline;
		double dev = Parameters.bwNoiseCV
				* bwCurrentBaseline;
		ContinuousDistribution dist = Parameters.getDistribution(Parameters.bwNoiseDistribution, mean, Parameters.bwNoiseAlpha, Parameters.bwNoiseBeta, dev, Parameters.bwNoiseShape, Parameters.bwNoiseLocation, Parameters.bwNoiseShift, Parameters.bwNoiseMin, Parameters.bwNoiseMax, Parameters.bwNoisePopulation);
		double nextBwCoefficient = 0;
		while (nextBwCoefficient <= 0) {
			nextBwCoefficient = dist.sample();
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
