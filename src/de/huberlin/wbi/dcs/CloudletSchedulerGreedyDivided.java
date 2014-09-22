package de.huberlin.wbi.dcs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.core.predicates.PredicateTime;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.Task;

public class CloudletSchedulerGreedyDivided extends CloudletSchedulerTimeShared {

	private double miPerVMPe;

	/** The Vm on which this scheduler operates */
	private DynamicVm vm;

	private HashMap<Integer, Integer> cloudletIdToTaskSlot;
	private HashSet<Integer> occupiedTaskSlots;

	// index 0: unassigned (available resources)
	// index i: assigned to task slot i
	private ArrayList<Double> mips;
	private ArrayList<Double> iops;
	private ArrayList<Double> bwps;

	public CloudletSchedulerGreedyDivided() {
		super();
		cloudletIdToTaskSlot = new HashMap<Integer, Integer>();
		occupiedTaskSlots = new HashSet<Integer>();
		mips = new ArrayList<Double>(0);
		iops = new ArrayList<Double>(0);
		bwps = new ArrayList<Double>(0);
		mips.add(0d);
		iops.add(0d);
		bwps.add(0d);
	}

	// Cloudlets utilize Resources in greedy fashion (take as many resources as
	// possible)
	// Other Cloudlets have to take what's left
	// There is no "fair" distribution of resources which is easy to compute
	// Cloudlets with 1 Pe can't utilize more than 1 Pe on the Vm (obviously)
	@Override
	public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
		setCurrentMipsShare(mipsShare);
		double timeSpan = currentTime - getPreviousTime();
		computeAvailableResources(mipsShare);

		// (1) update computation of all Cloudlets running on this VM
		List<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
		double nextEvent = Double.MAX_VALUE;
		for (ResCloudlet rcl : getCloudletExecList()) {
			double estimatedFinishTime = Double.MAX_VALUE;
			double assignedMips = assignResources(rcl);

			rcl.updateCloudletFinishedSoFar((long) (assignedMips * timeSpan * 1000000));
			rcl.getCloudlet().setCloudletFinishedSoFar(
					rcl.getCloudlet().getCloudletTotalLength()
							- rcl.getRemainingCloudletLength());

			if (assignedMips > 0) {
				estimatedFinishTime = currentTime
						+ rcl.getRemainingCloudletLength() / assignedMips;
			}
			if (estimatedFinishTime - currentTime < 0.1) {
				estimatedFinishTime = currentTime + 0.1;
			}

			long remainingLength = rcl.getRemainingCloudletLength();
			if (remainingLength == 0) {// finished: remove from the list
				toRemove.add(rcl);
			} else {
				if (estimatedFinishTime < nextEvent) {
					nextEvent = estimatedFinishTime;
				}
			}
		}

		if (Parameters.outputVmPerformanceLogs) {
			dumpCurrentResourceAssignmentsToLog(getPreviousTime());
			dumpCurrentResourceAssignmentsToLog(currentTime);
		}

		// (2) remove finished cloudlets
		for (ResCloudlet rcl : toRemove) {
			int taskSlot = cloudletIdToTaskSlot.get(rcl.getCloudletId());
			occupiedTaskSlots.remove(taskSlot);
			mips.set(taskSlot, 0d);
			iops.set(taskSlot, 0d);
			bwps.set(taskSlot, 0d);
			cloudletFinish(rcl);
			if (rcl.getCloudlet() instanceof Task) {
				Task task = (Task) rcl.getCloudlet();
				if (task.isScheduledToFail()) {
					try {
						task.setCloudletStatus(Cloudlet.FAILED);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		getCloudletExecList().removeAll(toRemove);

		if (getCloudletExecList().size() == 0) {
			setPreviousTime(currentTime);
			computeAvailableResources(mipsShare);
			return 0.0;
		}

		// (3) update VM performance (involves removal of next event in the
		// future queue previously scheduled by this scheduler)
		if (vm != null) {
			vm.getHost().getDatacenter()
					.cancelEvent(new PredicateTime(nextEvent, 0.001));
		}
		vm.updatePerformanceCoefficients();
		computeAvailableResources(mipsShare);

		// (4) estimate finish time of cloudlets, fed upwards to VM, Host, and
		// Datacenter
		// to decide, when to generate the next event
		nextEvent = Double.MAX_VALUE;
		for (ResCloudlet rcl : getCloudletExecList()) {
			double estimatedFinishTime = Double.MAX_VALUE;
			double assignedMips = assignResources(rcl);

			if (assignedMips > 0) {
				estimatedFinishTime = currentTime
						+ rcl.getRemainingCloudletLength() / assignedMips;
			}
			if (estimatedFinishTime - currentTime < 0.1) {
				estimatedFinishTime = currentTime + 0.1;
			}

			if (estimatedFinishTime < nextEvent) {
				nextEvent = estimatedFinishTime;
			}
		}

		setPreviousTime(currentTime);
		return nextEvent;
	}

	@Override
	public Cloudlet cloudletCancel(int cloudletId) {
		Cloudlet cl = super.cloudletCancel(cloudletId);

		if (cl != null) {
			int taskSlot = cloudletIdToTaskSlot.get(cl.getCloudletId());
			occupiedTaskSlots.remove(taskSlot);
			mips.set(taskSlot, 0d);
			iops.set(taskSlot, 0d);
			bwps.set(taskSlot, 0d);
		}

		return cl;
	}

	private void dumpCurrentResourceAssignmentsToLog(double currentTime) {
		BufferedWriter log = vm.getPerformanceLog();
		if (!(mips.size() <= vm.getTaskSlots())) {
			try {
				log.write(Double.toString(currentTime / 60));
				// log.write(Double.toString(currentTime));
				for (int i = mips.size() - 1; i >= 0; i--) {
					log.write("," + mips.get(i));
				}
				for (int i = iops.size() - 1; i >= 0; i--) {
					log.write("," + iops.get(i));
				}
				for (int i = bwps.size() - 1; i >= 0; i--) {
					log.write("," + bwps.get(i));
				}
				log.write("\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void computeAvailableResources(List<Double> mipsShare) {
		mips.set(0, getTotalMips(mipsShare) * vm.getCurrentMiCoefficient());
		iops.set(0, vm.getIo() * vm.getCurrentIoCoefficient());
		bwps.set(0, vm.getBw() * vm.getCurrentBwCoefficient());
		miPerVMPe = mips.get(0) / currentCPUs;
	}

	private double assignResources(ResCloudlet rcl) {
		Cloudlet cl = rcl.getCloudlet();

		// (a) compute, which of the resources MI, IO, and BW constitutes the
		// bottleneck,
		// according to currently available resources

		double totalMi = rcl.getCloudletTotalLength();
		double miSeconds = 0;
		double totalIo = 0;
		double ioSeconds = 0;
		double totalBw = 0;
		double bwSeconds = 0;

		if (cl instanceof HeterogeneousCloudlet) {
			HeterogeneousCloudlet dcl = (HeterogeneousCloudlet) cl;
			ResHeterogeneousCloudlet rdcl = (ResHeterogeneousCloudlet) rcl;
			totalMi = dcl.getMi();
			miSeconds = (totalMi > 0) ? Double.MAX_VALUE : 0;
			totalIo = dcl.getIo();
			if (Parameters.considerDataLocality) {
				totalIo -= rdcl.getLocalIo();
			}
			ioSeconds = (totalIo > 0) ? Double.MAX_VALUE : 0;
			totalBw = dcl.getBw();
			bwSeconds = (totalBw > 0) ? Double.MAX_VALUE : 0;
			if (iops.get(0) > 0) {
				ioSeconds = totalIo / iops.get(0);
			}
			if (bwps.get(0) > 0) {
				bwSeconds = totalBw / bwps.get(0);
			}
		}

		double maxMi = Math.min(mips.get(0), rcl.getNumberOfPes() * miPerVMPe);
		if (maxMi > 0) {
			miSeconds = totalMi / maxMi;
		}

		// (b) compute, how many MIPS will be assigned to this task, based on
		// the previously
		// determined bottleneck
		int taskSlot = cloudletIdToTaskSlot.get(rcl.getCloudletId());

		if (totalMi > 0 && miSeconds >= Math.max(ioSeconds, bwSeconds)) {
			mips.set(taskSlot,
					Math.min(mips.get(0), rcl.getNumberOfPes() * miPerVMPe));
			iops.set(taskSlot, totalIo * mips.get(0) / totalMi);
			bwps.set(taskSlot, totalBw * mips.get(0) / totalMi);
		} else if (totalIo > 0 && ioSeconds >= Math.max(miSeconds, bwSeconds)) {
			iops.set(taskSlot, iops.get(0));
			mips.set(taskSlot, totalMi * iops.get(0) / totalIo);
			bwps.set(taskSlot, totalBw * iops.get(0) / totalIo);
		} else if (totalBw > 0 && bwSeconds >= Math.max(miSeconds, ioSeconds)) {
			bwps.set(taskSlot, bwps.get(0));
			mips.set(taskSlot, totalMi * bwps.get(0) / totalBw);
			iops.set(taskSlot, totalIo * bwps.get(0) / totalBw);
		}

		mips.set(0, mips.get(0) - mips.get(taskSlot));
		iops.set(0, iops.get(0) - iops.get(taskSlot));
		bwps.set(0, bwps.get(0) - bwps.get(taskSlot));

		return mips.get(taskSlot) + iops.get(taskSlot) + bwps.get(taskSlot);
	}

	private double getTotalMips(List<Double> mipsShare) {
		double capacity = 0.0;
		int cpus = 0;
		for (Double mips : mipsShare) {
			capacity += mips;
			if (mips > 0.0) {
				cpus++;
			}
		}
		currentCPUs = cpus;

		return capacity;
	}

	@Override
	public double cloudletResume(int cloudletId) {
		boolean found = false;
		int position = 0;

		// look for the cloudlet in the paused list
		for (ResCloudlet rcl : getCloudletPausedList()) {
			if (rcl.getCloudletId() == cloudletId) {
				found = true;
				break;
			}
			position++;
		}

		if (found) {
			ResCloudlet rgl = getCloudletPausedList().remove(position);
			rgl.setCloudletStatus(Cloudlet.INEXEC);
			getCloudletExecList().add(rgl);

			double estimatedFinishTime = Double.MAX_VALUE;
			double assignedResources = assignResources(rgl);

			if (assignedResources > 0) {
				estimatedFinishTime = CloudSim.clock()
						+ rgl.getRemainingCloudletLength() / assignedResources;
			}

			return estimatedFinishTime;
		}

		return 0.0;
	}

	@Override
	public double cloudletSubmit(Cloudlet cloudlet, double fileTransferTime) {
		ResCloudlet rcl = (cloudlet instanceof HeterogeneousCloudlet) ? new ResHeterogeneousCloudlet(
				(HeterogeneousCloudlet) cloudlet, vm) : new ResCloudlet(
				cloudlet);

		rcl.setCloudletStatus(Cloudlet.INEXEC);
		for (int i = 0; i < cloudlet.getNumberOfPes(); i++) {
			rcl.setMachineAndPeId(0, i);
		}

		getCloudletExecList().add(rcl);
		int i = 1;
		while (occupiedTaskSlots.contains(i)) {
			i++;
		}
		cloudletIdToTaskSlot.put(rcl.getCloudletId(), i);
		occupiedTaskSlots.add(i);
		if (i >= mips.size()) {
			mips.add(0d);
			iops.add(0d);
			bwps.add(0d);
		}

		double estimatedTimeToFinish = Double.MAX_VALUE;
		double assignedResources = assignResources(rcl);

		if (assignedResources > 0) {
			estimatedTimeToFinish = rcl.getCloudletLength() / assignedResources;
		}
		if (estimatedTimeToFinish < 0.1) {
			estimatedTimeToFinish = 0.1;
		}

		return estimatedTimeToFinish;
	}

	@Override
	public double getTotalCurrentAvailableMipsForCloudlet(ResCloudlet rcl,
			List<Double> mipsShare) {
		double availableMiTemp = mips.get(0);
		double availableIoTemp = iops.get(0);
		double availableBwTemp = bwps.get(0);
		double assignedResources = assignResources(rcl);
		mips.set(0, availableMiTemp);
		iops.set(0, availableIoTemp);
		bwps.set(0, availableBwTemp);
		return assignedResources;
	}

	public DynamicVm getVm() {
		return vm;
	}

	public void setVm(DynamicVm vm) {
		this.vm = vm;
	}

}
