package de.huberlin.wbi.dcs.workflow;

import org.cloudbus.cloudsim.UtilizationModel;

import de.huberlin.wbi.dcs.HeterogeneousCloudlet;

public class Task extends HeterogeneousCloudlet implements Comparable<Task> {
	
	private final String name;
	
	private final String params;
	
	private int nDataDependencies;
	
	private Workflow workflow;
	
	private int depth;
	
	private static int maxDepth;
	
	private boolean speculativeCopy;
	
	private boolean destinedToFail;
	
	public Task(
			final String name,
			final String params,
			final Workflow workflow,
			final int userId,
			final int cloudletId,
			final long miLength,
			final long ioLength,
			final long bwLength,
			final int pesNumber,
			final long cloudletFileSize,
			final long cloudletOutputSize,
			final UtilizationModel utilizationModelCpu,
			final UtilizationModel utilizationModelRam,
			final UtilizationModel utilizationModelBw) {
		super(
				cloudletId,
				miLength,
				ioLength,
				bwLength,
				pesNumber,
				cloudletFileSize,
				cloudletOutputSize,
				utilizationModelCpu,
				utilizationModelRam,
				utilizationModelBw);
		this.name = name;
		this.params=params;
		this.workflow = workflow;
		this.setUserId(userId);
		this.depth = 0;
		destinedToFail = false;
		speculativeCopy = false;
	}
	
	public Task(Task task) {
		this(task.getName(),
				task.getParams(),
				task.getWorkflow(),
				task.getUserId(),
				task.getCloudletId(),
				task.getMi(),
				task.getIo(),
				task.getBw(),
				task.getNumberOfPes(),
				task.getCloudletFileSize(),
				task.getCloudletOutputSize(),
				task.getUtilizationModelCpu(),
				task.getUtilizationModelRam(),
				task.getUtilizationModelBw());
	}
	
	public String getName() {
		return name;
	}
	
	public String getParams() {
		return params;
	}
	
	public String toString() {
		return getName();
	}
	
	public void incNDataDependencies() {
		nDataDependencies++;
	}
	
	public void decNDataDependencies() {
		nDataDependencies--;
	}
	
	public boolean readyToExecute() {
		return nDataDependencies == 0;
	}
	
	public Workflow getWorkflow() {
		return workflow;
	}
	
	public static int getMaxDepth() {
		return maxDepth;
	}
	
	public int getDepth() {
		return depth;
	}
	
	public void setDepth(int depth) {
		this.depth = depth;
		if (depth > maxDepth) {
			maxDepth = depth;
		}
	}
	
	@Override
	public int compareTo(Task o) {
		return (this.getDepth() == o.getDepth()) ? Double.compare(this.getCloudletId(), o.getCloudletId()) : Double.compare(this.getDepth(), o.getDepth());
	}
	
	@Override
    public boolean equals(Object arg0) {
		return ((Task)arg0).getCloudletId() == getCloudletId();
	}
	
	@Override
    public int hashCode() {
		return getCloudletId();
    }

	public void setScheduledToFail(boolean scheduledToFail) {
		this.destinedToFail = scheduledToFail;
	}
	
	public boolean isScheduledToFail() {
		return destinedToFail;
	}
	
	public void setSpeculativeCopy(boolean speculativeCopy) {
		this.speculativeCopy = speculativeCopy;
	}
	
	public boolean isSpeculativeCopy() {
		return speculativeCopy;
	}
	
}
