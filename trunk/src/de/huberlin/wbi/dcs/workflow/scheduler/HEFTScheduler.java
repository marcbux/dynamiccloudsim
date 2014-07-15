package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.TaskUpwardRankComparator;
import de.huberlin.wbi.dcs.workflow.Workflow;

// currently assumes no data transfer times (similar to CloudSim)
// and is provided with runtime estimates per VM as if there were only one taskslot per Vm
public class HEFTScheduler extends StaticRoundRobinScheduler {
	
	Map<Integer, Double> readyTimePerTask;
	Map<Integer, TreeSet<Double>> freeTimeSlotStartsPerVm;
	Map<Integer, HashMap<Double, Double>> freeTimeSlotLengthsPerVm;
	
	Map<Integer, Double> distortedMiPerTask;
	Map<Integer, Double> distortedIoPerTask;
	Map<Integer, Double> distortedBwPerTask;
	
	public HEFTScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		readyTimePerTask = new HashMap<Integer, Double>();
		freeTimeSlotStartsPerVm = new HashMap<Integer, TreeSet<Double>>();
		freeTimeSlotLengthsPerVm = new HashMap<Integer, HashMap<Double, Double>>();
		distortedMiPerTask = new HashMap<Integer, Double>();
		distortedIoPerTask = new HashMap<Integer, Double>();
		distortedBwPerTask = new HashMap<Integer, Double>();
	}

	@Override
	protected void submitCloudlets() {
		super.registerVms();
		for (int vmId : vms.keySet()) {
			assignedTasks.put(vmId, new ArrayList<Task>());
			TreeSet<Double> occupiedTimeSlotStarts = new TreeSet<Double>();
			occupiedTimeSlotStarts.add(0d);
			freeTimeSlotStartsPerVm.put(vmId, occupiedTimeSlotStarts);
			HashMap<Double, Double> freeTimeSlotLengths = new HashMap<Double, Double>();
			freeTimeSlotLengths.put(0d, Double.MAX_VALUE);
			freeTimeSlotLengthsPerVm.put(vmId, freeTimeSlotLengths);
		}
		
		for (Workflow workflow : workflows) {
			List<Task> tasks = workflow.getSortedTasks();
			
			// distort runtime estimates
			for (Task task : tasks) {
				distortedMiPerTask.put(task.getCloudletId(), Math.max(0d, task.getMi() + task.getMi() * Parameters.numGen.nextGaussian() * Parameters.distortionCV));
				distortedIoPerTask.put(task.getCloudletId(), Math.max(0d, task.getIo() + task.getIo() * Parameters.numGen.nextGaussian() * Parameters.distortionCV));
				distortedBwPerTask.put(task.getCloudletId(), Math.max(0d, task.getBw() + task.getBw() * Parameters.numGen.nextGaussian() * Parameters.distortionCV));
			}
			
			// compute upward ranks of all tasks
			for (int i = tasks.size() - 1; i >= 0; i--) {
				Task task = tasks.get(i);
				readyTimePerTask.put(task.getCloudletId(), 0d);
				double maxSuccessorRank = 0;
				for (DataDependency outgoingEdge : workflow.getGraph().getOutEdges(task)) {
					Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
					if (child.getUpwardRank() > maxSuccessorRank) {
						maxSuccessorRank = child.getUpwardRank();
					}
				}
				
				double averageComputationCost = 0;
				for (Vm vm : vms.values()) {
					double miSeconds = distortedMiPerTask.get(task.getCloudletId()) / (vm.getNumberOfPes() * vm.getMips());
					double ioSeconds = 0;
					if (vm instanceof DynamicVm) {
						DynamicVm dVm = (DynamicVm) vm;
						ioSeconds = distortedIoPerTask.get(task.getCloudletId()) / dVm.getIo();
					}
					double bwSeconds = distortedBwPerTask.get(task.getCloudletId()) / vm.getBw();
					averageComputationCost += Math.max(Math.max(miSeconds, ioSeconds), bwSeconds);
				}
				averageComputationCost /= vms.size();
				
				// note that the upward rank of a task will always be greater than that of its successors
				task.setUpwardRank(averageComputationCost + maxSuccessorRank);
			}
			
			// Phase 1: Task Prioritizing (sort by decreasing order of rank)
			Collections.sort(tasks, new TaskUpwardRankComparator());
			
			// Phase 2: Processor Selection
			for (Task task : tasks) {
				double readyTime = readyTimePerTask.get(task.getCloudletId());
				
				Vm bestVm = null;
				double bestVmFreeTimeSlotActualStart = Double.MAX_VALUE;
				double bestFinish = Double.MAX_VALUE;
				
				for (Vm vm : vms.values()) {
					double miSeconds = distortedMiPerTask.get(task.getCloudletId()) / (vm.getNumberOfPes() * vm.getMips());
					double ioSeconds = 0;
					if (vm instanceof DynamicVm) {
						DynamicVm dVm = (DynamicVm) vm;
						ioSeconds = distortedIoPerTask.get(task.getCloudletId()) / (double)dVm.getIo();
					}
					double bwSeconds = distortedBwPerTask.get(task.getCloudletId()) / (double)vm.getBw();
					double computationCost = Math.max(Math.max(miSeconds, ioSeconds), bwSeconds);
					
					// the readytime of this task will have been set by now, as all predecessor tasks have a higher upward rank and thus have been assigned to a vm already
					TreeSet<Double> freeTimeSlotStarts = freeTimeSlotStartsPerVm.get(vm.getId());
					HashMap<Double, Double> freeTimeSlotLengths = freeTimeSlotLengthsPerVm.get(vm.getId());
					
					SortedSet<Double> freeTimeSlotStartsAfterReadyTime = (freeTimeSlotStarts.floor(readyTime) != null) ? freeTimeSlotStarts.tailSet(freeTimeSlotStarts.floor(readyTime)) : freeTimeSlotStarts.tailSet(freeTimeSlotStarts.ceiling(readyTime));
					
					for (double freeTimeSlotStart : freeTimeSlotStartsAfterReadyTime) {
						double freeTimeSlotActualStart = Math.max(readyTime, freeTimeSlotStart);
						if (freeTimeSlotActualStart + computationCost > bestFinish) break;
						double freeTimeSlotLength = freeTimeSlotLengths.get(freeTimeSlotStart);
						if (freeTimeSlotActualStart > freeTimeSlotStart) freeTimeSlotLength -= freeTimeSlotActualStart - freeTimeSlotStart;
						if (computationCost < freeTimeSlotLength) {
							bestVm = vm;
							bestVmFreeTimeSlotActualStart = freeTimeSlotActualStart;
							bestFinish = freeTimeSlotActualStart + computationCost;
						}
					}
				}
				
				// assign task to vm
				assignedTasks.get(bestVm.getId()).add(task);
				Log.printLine(CloudSim.clock() + ": " + getName() + ": Assigning Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \""  + " to VM # " + bestVm.getId());
				
				// update readytime of all successor tasks
				for (DataDependency outgoingEdge : workflow.getGraph().getOutEdges(task)) {
					Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
					if (bestFinish > readyTimePerTask.get(child.getCloudletId())) {
						readyTimePerTask.put(child.getCloudletId(), bestFinish);
					}
				}
				
				double timeslotStart = freeTimeSlotStartsPerVm.get(bestVm.getId()).floor(bestVmFreeTimeSlotActualStart);
				double timeslotLength = freeTimeSlotLengthsPerVm.get(bestVm.getId()).get(timeslotStart);
				double diff = bestVmFreeTimeSlotActualStart - timeslotStart;
				// add time slots before and after
				if (bestVmFreeTimeSlotActualStart > timeslotStart) {
					freeTimeSlotLengthsPerVm.get(bestVm.getId()).put(timeslotStart, diff);
				} else {
					freeTimeSlotStartsPerVm.get(bestVm.getId()).remove(timeslotStart);
					freeTimeSlotLengthsPerVm.get(bestVm.getId()).remove(timeslotStart);
				}
				
				double computationCost = bestFinish - bestVmFreeTimeSlotActualStart;
				double actualTimeSlotLength = timeslotLength - diff;
				if (computationCost < actualTimeSlotLength) {
					freeTimeSlotStartsPerVm.get(bestVm.getId()).add(bestFinish);
					freeTimeSlotLengthsPerVm.get(bestVm.getId()).put(bestFinish, actualTimeSlotLength - computationCost);
				}
				
			}

		}
		super.submitTasks();
	}

}
