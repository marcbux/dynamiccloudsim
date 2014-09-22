package de.huberlin.wbi.dcs;

import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.ResCloudlet;

import de.huberlin.wbi.dcs.examples.Parameters;

public class ResHeterogeneousCloudlet extends ResCloudlet {

	private double localIo;
	private double lengthCoeff;
	private HeterogeneousCloudlet cloudlet;
	private DynamicVm vm;

	public ResHeterogeneousCloudlet(HeterogeneousCloudlet cloudlet, DynamicVm vm) {
		super(cloudlet);
		this.cloudlet = cloudlet;
		this.vm = vm;
		setLocalIo();
	}

	public void setLocalIo(double localIo) {
		this.localIo = localIo;
		lengthCoeff = (cloudlet.getCloudletTotalLength() - localIo)
				/ cloudlet.getCloudletTotalLength();
	}
	
	public void setLocalIo() {
		Host host = vm.getHost();
		long local = 0, global = 0;
		for (File file : cloudlet.getInputFiles()) {
			global += file.getSize();
			if (host instanceof DynamicHost) {
				DynamicHost dHost = (DynamicHost) host;
				if (dHost.containsFile(file)) {
					local += file.getSize();
				}
			}
		}
		if (global > 0) {
			double coeff = (double) local / (double) global;
			setLocalIo(coeff * cloudlet.getIo());
		} else {
			setLocalIo(0);
		}
	}

	public double getLocalIo() {
		return localIo;
	}

	@Override
	public long getCloudletLength() {
		return (Parameters.considerDataLocality) ? Math.max(0,
				(long) (super.getCloudletLength() * lengthCoeff)) : super
				.getCloudletLength();
	}

	@Override
	public long getCloudletTotalLength() {
		return (Parameters.considerDataLocality) ? Math.max(0,
				(long) (super.getRemainingCloudletLength() * lengthCoeff))
				: super.getRemainingCloudletLength();
	}

	@Override
	public long getRemainingCloudletLength() {
		return (Parameters.considerDataLocality) ? Math.max(0,
				(long) (super.getRemainingCloudletLength() * lengthCoeff))
				: super.getRemainingCloudletLength();
	}

	@Override
	public void updateCloudletFinishedSoFar(long miLength) {
		super.updateCloudletFinishedSoFar((Parameters.considerDataLocality) ? (long) (miLength / lengthCoeff)
				: miLength);
	}
}
