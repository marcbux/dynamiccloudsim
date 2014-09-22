package de.huberlin.wbi.dcs;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.UtilizationModel;

public class HeterogeneousCloudlet extends Cloudlet {
	
	private long mi = 0;
	private long io = 0;
	private long bw = 0;
	
	private static long totalMi = 0;
	private static long totalIo = 0;
	private static long totalBw = 0;
	
	private List<File> inputFiles;
	
	public HeterogeneousCloudlet(
			final int cloudletId,
			final long mi,
			final long io,
			final long bw,
			final int pesNumber,
			final long cloudletFileSize,
			final long cloudletOutputSize,
			final UtilizationModel utilizationModelCpu,
			final UtilizationModel utilizationModelRam,
			final UtilizationModel utilizationModelBw) {
		super(
				cloudletId,
				0,
				pesNumber,
				cloudletFileSize,
				cloudletOutputSize,
				utilizationModelCpu,
				utilizationModelRam,
				utilizationModelBw,
				true);
		incMi(mi > 0 ? mi : 1);
		incIo(io);
		incBw(bw);
		inputFiles = new ArrayList<>();
	}
	
	private void updateLength() {
		setCloudletLength(mi + io + bw);
	}
	
	public long getBw() {
		return bw;
	}
	
	public long getIo() {
		return io;
	}
	
	public long getMi() {
		return mi;
	}
	
	public void incBw(long bw) {
		this.bw += bw;
		totalBw += bw;
		updateLength();
	}
	
	public void incIo(long io) {
		this.io += io;
		totalIo += io;
		updateLength();
	}
	
	public void incMi(long mi) {
		this.mi += mi;
		totalMi += mi;
		updateLength();
	}
	
	public static long getTotalBw() {
		return totalBw;
	}
	
	public static long getTotalIo() {
		return totalIo;
	}
	
	public static long getTotalMi() {
		return totalMi;
	}
	
	public List<File> getInputFiles() {
		return inputFiles;
	}
	
	public void addInputFile(File file) {
		inputFiles.add(file);
	}
	
}
