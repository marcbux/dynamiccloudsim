package de.huberlin.wbi.dcs.workflow;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.HeterogeneousCloudlet;
import de.huberlin.wbi.dcs.examples.Parameters;

public class Task extends HeterogeneousCloudlet implements Comparable<Task> {
	
	private final String name;
	
	private final String params;
	
	private int nDataDependencies;
	
	private Workflow workflow;
	
	private int depth;
	
	private static int maxDepth;
	
	// LATE parameters
	private double progressScore;
	private double timeTaskHasBeenRunning;
	private double progressRate;
	private double estimatedTimeToCompletion;
	
	// HEFT parameters
	private double upwardRank;
	
	private boolean speculativeCopy;
	private boolean scheduledToFail;
	
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
		scheduledToFail = false;
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

	public void setUpwardRank(double upwardRank) {
		this.upwardRank = upwardRank;
	}
	
	public double getUpwardRank() {
		return upwardRank;
	}
	
	private void computeProgressScore() {
		double actualProgressScore = (double)(getCloudletFinishedSoFar()) / (double)(getCloudletLength());
		// the distortion is higher if task is really close to finish or just started recently
		double distortionIntensity = 1d - Math.abs(1d - actualProgressScore * 2d);
		double distortion = Parameters.numGen.nextGaussian() * Parameters.distortionCV * distortionIntensity;
		double perceivedProgressScore = actualProgressScore + distortion;
		progressScore = (perceivedProgressScore > 1) ? 0.99 : ((perceivedProgressScore < 0) ? 0.01 : perceivedProgressScore);
	}
	
	private void computeProgressRate() {
		computeProgressScore();
		timeTaskHasBeenRunning = CloudSim.clock() - getExecStartTime();
		progressRate = (timeTaskHasBeenRunning == 0) ? Double.MAX_VALUE : getProgressScore() / timeTaskHasBeenRunning;
	}
	
	public void computeEstimatedTimeToCompletion() {
		computeProgressRate();
		estimatedTimeToCompletion = (getProgressRate() == 0) ? Double.MAX_VALUE : (1d - getProgressScore()) / getProgressRate();
	}
	
	public double getProgressScore() {
		return progressScore;
	}
	
	public double getProgressRate() {
		return progressRate;
	}
	
	public double getEstimatedTimeToCompletion() {
		return estimatedTimeToCompletion;
	}

	public void setScheduledToFail(boolean scheduledToFail) {
		this.scheduledToFail = scheduledToFail;
	}
	
	public boolean isScheduledToFail() {
		return scheduledToFail;
	}
	
	public void setSpeculativeCopy(boolean speculativeCopy) {
		this.speculativeCopy = speculativeCopy;
	}
	
	public boolean isSpeculativeCopy() {
		return speculativeCopy;
	}
	
}
