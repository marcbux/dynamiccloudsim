package de.huberlin.wbi.dcs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import de.huberlin.wbi.dcs.provisioners.BwProvisionerFull;

public class DynamicHost extends Host {
	
	/** The I/O capacity of this host (in byte per second). */
	private long io;
	
	/** The amount of compute units this host provides per Pe. */
	private double numberOfCusPerPe;
	
	private double mipsPerPe;
	
	private static long totalMi;
	private static long totalIo;
	private static long totalBw;
	
	private Set<File> localFiles;
	
	public DynamicHost(
			int id,
			int ram,
			long bandwidth,
			long io,
			long storage,
			double numberOfCusPerPe,
			int numberOfPes,
			double mipsPerPe) {	
		super(
				id,
				new RamProvisionerSimple(ram),
				new BwProvisionerFull(bandwidth),
				storage,
				new ArrayList<Pe>(),
				null
		);
		setIo(io);
		setMipsPerPe(mipsPerPe);
		setNumberOfCusPerPe(numberOfCusPerPe);
		List<Pe> peList = new ArrayList<Pe>();
		for (int i = 0; i < numberOfPes; i++) {
			peList.add(new Pe(i, new PeProvisionerSimple(mipsPerPe)));
		}
		setPeList(peList);
		setVmScheduler(new VmSchedulerTimeShared(peList));
		setFailed(false);
		localFiles = new HashSet<>();
	}
	
	@Override
	public boolean vmCreate(Vm vm) {
		if (vm instanceof DynamicVm) {
			DynamicVm dVm = (DynamicVm)vm;
			dVm.setMips((dVm.getNumberOfCusPerPe() / getNumberOfCusPerPe()) * getMipsPerPe());
			dVm.setBw(getBw());
			dVm.setIo(getIo());
			totalMi += dVm.getMips();
			totalIo += dVm.getIo();
			totalBw += dVm.getBw();
		}
		return super.vmCreate(vm);
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

	public long getIo() {
		return io;
	}
	
	public double getMipsPerPe() {
		return mipsPerPe;
	}
	
	public double getNumberOfCusPerPe() {
		return numberOfCusPerPe;
	}
	
	public void setIo(long io) {
		this.io = io;
	}
	
	public void setMipsPerPe(double mipsPerPe) {
		this.mipsPerPe = mipsPerPe;
	}
	
	public void setNumberOfCusPerPe(double numberOfCusPerPe) {
		this.numberOfCusPerPe = numberOfCusPerPe;
	}
	
	public void addFile(File file) {
		localFiles.add(file);
	}
	
	public boolean containsFile(File file) {
		return localFiles.contains(file);
	}
	
}
