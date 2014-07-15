package de.huberlin.wbi.dcs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.examples.Parameters;

public class DynamicVm extends Vm {
	
	private long io;
	
	private double numberOfCusPerPe;
	
	private DynamicModel dynamicModel;
	
	private double currentMiCoefficient;
	private double currentIoCoefficient;
	private double currentBwCoefficient;
	
	private double previousTime;
	
	private BufferedWriter performanceLog;
	private int taskSlots;
	
	public DynamicVm(
			int id,
			int userId,
			double numberOfCusPerPe,
			int numberOfPes,
			int ram,
			long storage,
			String vmm,
			CloudletScheduler cloudletScheduler,
			DynamicModel dynamicModel,
			String performanceLogFileName,
			int taskSlots) {
		super(id, userId, -1, numberOfPes, ram, -1, storage, vmm, cloudletScheduler);
		setNumberOfCusPerPe(numberOfCusPerPe);
		setDynamicModel(dynamicModel);
		setCoefficients();
		previousTime = CloudSim.clock();
		this.taskSlots = taskSlots;
		if (Parameters.outputVmPerformanceLogs) {
			try {
				performanceLog = new BufferedWriter(new FileWriter(new File(performanceLogFileName)));
				performanceLog.write("time");
				String[] resources = {"mips", "iops", "bwps"};
				for (String resource : resources) {
					for (int i = 0; i < taskSlots; i++) {
						performanceLog.write("," + resource + " task slot " + i);
					}
					performanceLog.write("," + resource + " unassigned");
				}
				performanceLog.write("\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (cloudletScheduler instanceof CloudletSchedulerGreedyDivided) {
			CloudletSchedulerGreedyDivided cloudletSchedulerGreedyDivided = (CloudletSchedulerGreedyDivided)cloudletScheduler;
			cloudletSchedulerGreedyDivided.setVm(this);
		}
	}
	
	public DynamicVm(
			int id,
			int userId,
			double numberOfCusPerPe,
			int numberOfPes,
			int ram,
			long storage,
			String vmm,
			CloudletScheduler cloudletScheduler,
			DynamicModel dynamicModel,
			String performanceLogFileName) {
		this(id, userId, numberOfCusPerPe, numberOfPes, ram, storage, vmm, cloudletScheduler, dynamicModel, performanceLogFileName, 1);
	}
	
	public void updatePerformanceCoefficients () {
		double currentTime = CloudSim.clock();
		double timespan = currentTime - getPreviousTime();
		setPreviousTime(currentTime);
		dynamicModel.updateBaselines(timespan);
		setCoefficients();
	}
	
	private void setCoefficients() {
		setCurrentMiCoefficient(dynamicModel.nextMiCoefficient());
		setCurrentIoCoefficient(dynamicModel.nextIoCoefficient());
		setCurrentBwCoefficient(dynamicModel.nextBwCoefficient());
	}
	
	public void setMips(double mips) {
		super.setMips(mips);
	}
	
	public long getIo() {
		return io;
	}
	
	public double getNumberOfCusPerPe() {
		return numberOfCusPerPe;
	}
	
	public void setIo(long io) {
		this.io = io;
	}
	
	public void setNumberOfCusPerPe(double numberOfCusPerPe) {
		this.numberOfCusPerPe = numberOfCusPerPe;
	}
	
	public DynamicModel getDynamicModel() {
		return dynamicModel;
	}
	
	public void setDynamicModel(DynamicModel dynamicModel) {
		this.dynamicModel = dynamicModel;
	}
	
	public double getCurrentBwCoefficient() {
		return currentBwCoefficient;
	}
	
	public double getCurrentIoCoefficient() {
		return currentIoCoefficient;
	}
	
	public double getCurrentMiCoefficient() {
		return currentMiCoefficient;
	}
	
	private void setCurrentBwCoefficient(double currentBwCoefficient) {
		this.currentBwCoefficient = currentBwCoefficient;
	}
	
	private void setCurrentIoCoefficient(double currentIoCoefficient) {
		this.currentIoCoefficient = currentIoCoefficient;
	}
	
	private void setCurrentMiCoefficient(double currentMiCoefficient) {
		this.currentMiCoefficient = currentMiCoefficient;
	}
	
	public double getPreviousTime() {
		return previousTime;
	}
	
	public void setPreviousTime(double previousTime) {
		this.previousTime = previousTime;
	}
	
	public BufferedWriter getPerformanceLog() {
		return performanceLog;
	}
	
	public void closePerformanceLog() {
		if (Parameters.outputVmPerformanceLogs) {
			try {
				performanceLog.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public int getTaskSlots() {
		return taskSlots;
	}
}
