package de.huberlin.dcs;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;

public class HeterogeneousCloudlet extends Cloudlet {
	
	private long mi;
	private long io;
	private long bw;
	
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
				mi + io + bw,
				pesNumber,
				cloudletFileSize,
				cloudletOutputSize,
				utilizationModelCpu,
				utilizationModelRam,
				utilizationModelBw,
				true);
		setMi(mi);
		setIo(io);
		setBw(bw);
	}
	
	@Override
	public long getCloudletTotalLength() {
		return getCloudletLength();
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
	
	public void setBw(long bw) {
		this.bw = bw;
	}
	
	public void setIo(long io) {
		this.io = io;
	}
	
	public void setMi(long mi) {
		this.mi = mi;
	}
	
}
