package de.huberlin.dcs.examples;
import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.dcs.CloudletSchedulerGreedyDivided;
import de.huberlin.dcs.DynamicHost;
import de.huberlin.dcs.DynamicModel;
import de.huberlin.dcs.DynamicVm;
import de.huberlin.dcs.VmAllocationPolicyRandom;
import de.huberlin.dcs.workflow.Task;
import de.huberlin.dcs.workflow.Workflow;
import de.huberlin.dcs.workflow.io.MontageTraceFileReader;
import de.huberlin.dcs.workflow.scheduler.*;

public class MontageWorkflowExampleSmall {
	
	private static String traceFile = "examples/montage.m17.1.trace";
	
	// datacenter params
	private static long bwpsPerPe = 256;
	private static long iopsPerPe = 20 * 1024;
	
	private static int nOpteron270 = 200;
	private static int nCusPerCoreOpteron270 = 2;
	private static int nCoresOpteron270 = 4;
	private static int mipsPerCoreOpteron270 = 174;
	
	private static int nOpteron2218 = 200;
	private static int nCusPerCoreOpteron2218 = 2;
	private static int nCoresOpteron2218 = 4;
	private static int mipsPerCoreOpteron2218 = 247;
	
	private static int nXeonE5430 = 100;
	private static int nCusPerCoreXeonE5430 = 2;
	private static int nCoresXeonE5430 = 8;
	private static int mipsPerCoreXeonE5430 = 355;
	
	// vm params
	private static int nVms = 8;
	private static int taskSlotsPerVm = 1;
	
	private static double numberOfCusPerPe = 1;
	private static int numberOfPes = 1;
	private static int ram = (int)(1.7 * 1024);
	
	public static void main (String[] args) {
		
		double totalRuntime = 0d;
		Parameters.parseParameters(args);
		
		try {
			for (int i = 0; i < Parameters.numberOfRuns; i++) {
				if (!Parameters.outputDatacenterEvents) {
					Log.disable();
				}
				// First step: Initialize the CloudSim package
				int num_user = 1;   // number of grid users
				Calendar calendar = Calendar.getInstance();
				boolean trace_flag = false;  // mean trace events
				CloudSim.init(num_user, calendar, trace_flag);
				
				// Second step: Create Datacenter
				createDatacenter("Datacenter");
				
				// Third step: Create Scheduler and submit workflow
				WorkflowScheduler scheduler = null;
				switch (Parameters.scheduler) {
					case STATIC_ROUND_ROBIN:	scheduler = new StaticRoundRobinScheduler("StaticRoundRobinScheduler", taskSlotsPerVm);
												break;
					case LATE:					scheduler = new LATEScheduler("LATEScheduler", taskSlotsPerVm);
												break;
					case HEFT:					scheduler = new HEFTScheduler("HEFTScheduler", taskSlotsPerVm);
												break;
					case JOB_QUEUE:				scheduler = new GreedyQueueScheduler("GreedyQueueScheduler", taskSlotsPerVm);
												break;
					default:					scheduler = new C3("C3", taskSlotsPerVm);
												break;
				}
				
				// Fourth step: Create VMs
				List<Vm> vmlist = createVMList(scheduler.getId(), i);
				scheduler.submitVmList(vmlist);
				
				// Fifth step: Create Cloudlets and send them to Scheduler
				MontageTraceFileReader logFileReader = new MontageTraceFileReader(new File(traceFile), true, true, ".*jpg");
				Workflow alignmentWorkflow = logFileReader.parseLogFile(scheduler.getId());
				if (Parameters.outputWorkflowGraph) {
					alignmentWorkflow.visualize(1920,1200);
				}
				scheduler.submitWorkflow(alignmentWorkflow);
				
				// Sixth step: Starts the simulation
				CloudSim.startSimulation();
				CloudSim.stopSimulation();
				
				totalRuntime += scheduler.getRuntime();
				System.out.println(scheduler.getRuntime() / 60);
			}
			
			Log.printLine("Average runtime in minutes: " + totalRuntime / Parameters.numberOfRuns / 60);
			Log.printLine("Total Workload: " + Task.getTotalMi() + "mi " + Task.getTotalIo() + "io " + Task.getTotalBw() + "bw");
			Log.printLine("Total VM Performance: " + DynamicHost.getTotalMi() + "mips " + DynamicHost.getTotalIo() + "iops " + DynamicHost.getTotalBw() + "bwps");
			Log.printLine("minimum minutes (quotient): " + Task.getTotalMi() / DynamicHost.getTotalMi() / 60 + " " + Task.getTotalIo() / DynamicHost.getTotalIo() / 60 + " " + Task.getTotalBw() / DynamicHost.getTotalBw() / 60);		
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
		
	}
	
	private static List<Vm> createVMList(int userId, int run) {

		//Creates a container to store VMs. This list is passed to the broker later
		LinkedList<Vm> list = new LinkedList<Vm>();

		//VM Parameters
		long storage = 10000;
		String vmm = "Xen";
		
		//create VMs
		Vm[] vm = new DynamicVm[nVms];

		for(int i = 0; i < nVms; i++){
			DynamicModel dynamicModel = new DynamicModel(Parameters.baselineChangesPerHour, Parameters.cpuDynamicsCV, Parameters.ioDynamicsCV, Parameters.bwDynamicsCV, Parameters.cpuNoiseCV, Parameters.ioNoiseCV, Parameters.bwNoiseCV);
			vm[i] = new DynamicVm(i, userId, numberOfCusPerPe, numberOfPes, ram, storage, vmm, new CloudletSchedulerGreedyDivided(), dynamicModel, "output/run_" + run + "_vm_" + i + ".csv", taskSlotsPerVm);
			list.add(vm[i]);
		}

		return list;
	}
	
	// all numbers in 1000 (e.g. kb/s)
	private static Datacenter createDatacenter(String name) {
		Random numGen; numGen = Parameters.numGen;
		List<DynamicHost> hostList = new ArrayList<DynamicHost>();
		int hostId = 0;
		long storage = 1024 * 1024;
		
		int ram = (int)(2 * 1024 * nCusPerCoreOpteron270 * nCoresOpteron270);
		for (int i = 0; i < nOpteron270; i++) {
			long bwps = 0;
			while (bwps <= 0) {
				bwps = (long)((1 + numGen.nextGaussian() * Parameters.bwHeterogeneityCV) * bwpsPerPe);
			}
			long iops = 0;
			while (iops <= 0) {
				iops = (long)((1 + numGen.nextGaussian() * Parameters.ioHeterogeneityCV) * iopsPerPe);
			}
			long mips = 0;
			while (mips <= 0) {
				mips = (long)((1 + numGen.nextGaussian() * Parameters.cpuHeterogeneityCV) * mipsPerCoreOpteron270);
			}
			if (numGen.nextDouble() < Parameters.likelihoodOfStraggler) {
				bwps *= Parameters.stragglerPerformanceCoefficient;
				iops *= Parameters.stragglerPerformanceCoefficient;
				mips *= Parameters.stragglerPerformanceCoefficient;
			}
			hostList.add(
				new DynamicHost(
					hostId++,
					ram,
					bwps,
					iops,
					storage,
					nCusPerCoreOpteron270,
					nCoresOpteron270,
					mips
				)
			);
		}
		
		ram = (int)(2 * 1024 * nCusPerCoreOpteron2218 * nCoresOpteron2218);
		for (int i = 0; i < nOpteron2218; i++) {
			long bwps = 0;
			while (bwps <= 0) {
				bwps = (long)((1 + numGen.nextGaussian() * Parameters.bwHeterogeneityCV) * bwpsPerPe);
			}
			long iops = 0;
			while (iops <= 0) {
				iops = (long)((1 + numGen.nextGaussian() * Parameters.ioHeterogeneityCV) * iopsPerPe);
			}
			long mips = 0;
			while (mips <= 0) {
				mips = (long)((1 + numGen.nextGaussian() * Parameters.cpuHeterogeneityCV) * mipsPerCoreOpteron2218);
			}
			if (numGen.nextDouble() < Parameters.likelihoodOfStraggler) {
				bwps *= Parameters.stragglerPerformanceCoefficient;
				iops *= Parameters.stragglerPerformanceCoefficient;
				mips *= Parameters.stragglerPerformanceCoefficient;
			}
			hostList.add(
				new DynamicHost(
					hostId++,
					ram,
					bwps,
					iops,
					storage,
					nCusPerCoreOpteron2218,
					nCoresOpteron2218,
					mips
				)
			);
		}
		
		ram = (int)(2 * 1024 * nCusPerCoreXeonE5430 * nCoresXeonE5430);
		for (int i = 0; i < nXeonE5430; i++) {
			long bwps = 0;
			while (bwps <= 0) {
				bwps = (long)((1 + numGen.nextGaussian() * Parameters.bwHeterogeneityCV) * bwpsPerPe);
			}
			long iops = 0;
			while (iops <= 0) {
				iops = (long)((1 + numGen.nextGaussian() * Parameters.ioHeterogeneityCV) * iopsPerPe);
			}
			long mips = 0;
			while (mips <= 0) {
				mips = (long)((1 + numGen.nextGaussian() * Parameters.cpuHeterogeneityCV) * mipsPerCoreXeonE5430);
			}
			if (numGen.nextDouble() < Parameters.likelihoodOfStraggler) {
				bwps *= Parameters.stragglerPerformanceCoefficient;
				iops *= Parameters.stragglerPerformanceCoefficient;
				mips *= Parameters.stragglerPerformanceCoefficient;
			}
			hostList.add(
				new DynamicHost(
					hostId++,
					ram,
					bwps,
					iops,
					storage,
					nCusPerCoreXeonE5430,
					nCoresXeonE5430,
					mips
				)
			);
		}

		String arch = "x86";
		String os = "Linux";
		String vmm = "Xen";
		double time_zone = 10.0;
		double cost = 3.0;
		double costPerMem = 0.05;
		double costPerStorage = 0.001;
		double costPerBw = 0.0;
		LinkedList<Storage> storageList = new LinkedList<Storage>();

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem,
				costPerStorage, costPerBw);

		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicyRandom(hostList, Parameters.seed++), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

}
