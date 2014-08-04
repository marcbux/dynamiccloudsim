package de.huberlin.wbi.dcs.distributions;

import java.util.Random;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

public class NormalDistribution implements ContinuousDistribution {

	/** The num gen. */
	private final Random numGen;

	/** The mean. */
	private final double mean;

	/** The dev. */
	private final double dev;

	public NormalDistribution(Random seed, double mean, double dev) {
		if (dev <= 0.0) {
			throw new IllegalArgumentException("Deviation must be greater than 0.0");
		}

		numGen = seed;
		this.mean = mean;
		this.dev = dev;
	}

	/**
	 * Instantiates a new lognormal distr.
	 * 
	 * @param mean the mean
	 * @param dev the dev
	 */
	public NormalDistribution(double mean, double dev) {
		if (dev <= 0.0) {
			throw new IllegalArgumentException("Deviation must be greater than 0.0");
		}

		numGen = new Random(System.currentTimeMillis());
		this.mean = mean;
		this.dev = dev;
	}

	@Override
	public double sample() {
		return numGen.nextGaussian() * dev + mean;
	}

}
