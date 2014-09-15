package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;

// currently assumes no data transfer times (similar to CloudSim)
// and is provided with runtime estimates per VM as if there were only one taskslot per Vm
public class HEFTScheduler extends StaticRoundRobinScheduler {
	
	public class TaskUpwardRankComparator implements Comparator<Task> {
		
		@Override
		public int compare(Task task1, Task task2) {
			return Double.compare(upwardRanks.get(task2), upwardRanks.get(task1));
		}

	}

	Map<Task, Double> readyTimePerTask;
	Map<Vm, TreeSet<Double>> freeTimeSlotStartsPerVm;
	Map<Vm, Map<Double, Double>> freeTimeSlotLengthsPerVm;

	Map<Task, Double> distortedMiPerTask;
	Map<Task, Double> distortedIoPerTask;
	Map<Task, Double> distortedBwPerTask;
	
	Map<Task, Double> upwardRanks;

	public HEFTScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		readyTimePerTask = new HashMap<>();
		freeTimeSlotStartsPerVm = new HashMap<>();
		freeTimeSlotLengthsPerVm = new HashMap<>();
		distortedMiPerTask = new HashMap<>();
		distortedIoPerTask = new HashMap<>();
		distortedBwPerTask = new HashMap<>();
		upwardRanks = new HashMap<>();
	}

	@Override
	public void reschedule(Collection<Task> tasks, Collection<Vm> vms) {
		for (Vm vm : vms) {
			if (!readyTasks.containsKey(vm)) {
				Queue<Task> q = new LinkedList<>();
				readyTasks.put(vm, q);

				TreeSet<Double> occupiedTimeSlotStarts = new TreeSet<>();
				occupiedTimeSlotStarts.add(0d);
				freeTimeSlotStartsPerVm.put(vm, occupiedTimeSlotStarts);
				Map<Double, Double> freeTimeSlotLengths = new HashMap<>();
				freeTimeSlotLengths.put(0d, Double.MAX_VALUE);
				freeTimeSlotLengthsPerVm.put(vm, freeTimeSlotLengths);
			}
		}

		List<Task> sortedTasks = new ArrayList<>(tasks);
		Collections.sort(sortedTasks);

		// distort runtime estimates
		for (Task task : sortedTasks) {
			distortedMiPerTask.put(
					task,
					Math.max(0d, task.getMi() + task.getMi()
							* Parameters.numGen.nextGaussian()
							* Parameters.distortionCV));
			distortedIoPerTask.put(
					task,
					Math.max(0d, task.getIo() + task.getIo()
							* Parameters.numGen.nextGaussian()
							* Parameters.distortionCV));
			distortedBwPerTask.put(
					task,
					Math.max(0d, task.getBw() + task.getBw()
							* Parameters.numGen.nextGaussian()
							* Parameters.distortionCV));
		}

		// compute upward ranks of all tasks
		for (int i = sortedTasks.size() - 1; i >= 0; i--) {
			Task task = sortedTasks.get(i);
			readyTimePerTask.put(task, 0d);
			double maxSuccessorRank = 0;
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph()
					.getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph()
						.getDest(outgoingEdge);
				if (upwardRanks.get(child) > maxSuccessorRank) {
					maxSuccessorRank = upwardRanks.get(child);
				}
			}

			double averageComputationCost = 0;
			for (Vm vm : vms) {
				double miSeconds = distortedMiPerTask.get(task)
						/ (vm.getNumberOfPes() * vm.getMips());
				double ioSeconds = 0;
				if (vm instanceof DynamicVm) {
					DynamicVm dVm = (DynamicVm) vm;
					ioSeconds = distortedIoPerTask.get(task) / dVm.getIo();
				}
				double bwSeconds = distortedBwPerTask.get(task) / vm.getBw();
				averageComputationCost += Math.max(
						Math.max(miSeconds, ioSeconds), bwSeconds);
			}
			averageComputationCost /= vms.size();

			// note that the upward rank of a task will always be greater than
			// that of its successors
			upwardRanks.put(task, averageComputationCost + maxSuccessorRank);
		}

		// Phase 1: Task Prioritizing (sort by decreasing order of rank)
		Collections.sort(sortedTasks, new TaskUpwardRankComparator());

		// Phase 2: Processor Selection
		for (Task task : sortedTasks) {
			double readyTime = readyTimePerTask.get(task);

			Vm bestVm = null;
			double bestVmFreeTimeSlotActualStart = Double.MAX_VALUE;
			double bestFinish = Double.MAX_VALUE;

			for (Vm vm : vms) {
				double miSeconds = distortedMiPerTask.get(task)
						/ (vm.getNumberOfPes() * vm.getMips());
				double ioSeconds = 0;
				if (vm instanceof DynamicVm) {
					DynamicVm dVm = (DynamicVm) vm;
					ioSeconds = distortedIoPerTask.get(task)
							/ (double) dVm.getIo();
				}
				double bwSeconds = distortedBwPerTask.get(task)
						/ (double) vm.getBw();
				double computationCost = Math.max(
						Math.max(miSeconds, ioSeconds), bwSeconds);

				// the readytime of this task will have been set by now, as all
				// predecessor tasks have a higher upward rank and thus have
				// been assigned to a vm already
				TreeSet<Double> freeTimeSlotStarts = freeTimeSlotStartsPerVm
						.get(vm);
				Map<Double, Double> freeTimeSlotLengths = freeTimeSlotLengthsPerVm
						.get(vm);

				SortedSet<Double> freeTimeSlotStartsAfterReadyTime = (freeTimeSlotStarts
						.floor(readyTime) != null) ? freeTimeSlotStarts
						.tailSet(freeTimeSlotStarts.floor(readyTime))
						: freeTimeSlotStarts.tailSet(freeTimeSlotStarts
								.ceiling(readyTime));

				for (double freeTimeSlotStart : freeTimeSlotStartsAfterReadyTime) {
					double freeTimeSlotActualStart = Math.max(readyTime,
							freeTimeSlotStart);
					if (freeTimeSlotActualStart + computationCost > bestFinish)
						break;
					double freeTimeSlotLength = freeTimeSlotLengths
							.get(freeTimeSlotStart);
					if (freeTimeSlotActualStart > freeTimeSlotStart)
						freeTimeSlotLength -= freeTimeSlotActualStart
								- freeTimeSlotStart;
					if (computationCost < freeTimeSlotLength) {
						bestVm = vm;
						bestVmFreeTimeSlotActualStart = freeTimeSlotActualStart;
						bestFinish = freeTimeSlotActualStart + computationCost;
					}
				}
			}

			// assign task to vm
			schedule.put(task, bestVm);
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": Assigning Task # " + task.getCloudletId() + " \""
					+ task.getName() + " " + task.getParams() + " \""
					+ " to VM # " + bestVm.getId());

			// update readytime of all successor tasks
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph()
					.getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph()
						.getDest(outgoingEdge);
				if (bestFinish > readyTimePerTask.get(child)) {
					readyTimePerTask.put(child, bestFinish);
				}
			}

			double timeslotStart = freeTimeSlotStartsPerVm.get(bestVm).floor(
					bestVmFreeTimeSlotActualStart);
			double timeslotLength = freeTimeSlotLengthsPerVm.get(bestVm).get(
					timeslotStart);
			double diff = bestVmFreeTimeSlotActualStart - timeslotStart;
			// add time slots before and after
			if (bestVmFreeTimeSlotActualStart > timeslotStart) {
				freeTimeSlotLengthsPerVm.get(bestVm).put(timeslotStart, diff);
			} else {
				freeTimeSlotStartsPerVm.get(bestVm).remove(timeslotStart);
				freeTimeSlotLengthsPerVm.get(bestVm).remove(timeslotStart);
			}

			double computationCost = bestFinish - bestVmFreeTimeSlotActualStart;
			double actualTimeSlotLength = timeslotLength - diff;
			if (computationCost < actualTimeSlotLength) {
				freeTimeSlotStartsPerVm.get(bestVm).add(bestFinish);
				freeTimeSlotLengthsPerVm.get(bestVm).put(bestFinish,
						actualTimeSlotLength - computationCost);
			}

		}

	}
}
