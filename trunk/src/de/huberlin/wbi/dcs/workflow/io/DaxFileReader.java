package de.huberlin.wbi.dcs.workflow.io;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.ParameterException;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;

import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.parser.Parser;
import edu.isi.pegasus.planner.parser.dax.DAX2CDAG;
import edu.isi.pegasus.planner.parser.dax.DAXParser;

public class DaxFileReader extends LogFileReader {

	@Override
	protected void fillDataStructures(int userId, String filePath,
			boolean fileNames, boolean kernelTime, Workflow workflow) {
		try {
			Set<String> tasks = new HashSet<>();

			PegasusProperties properties = PegasusProperties
					.nonSingletonInstance();
			PegasusBag bag = new PegasusBag();
			bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

			LogManager logger = LogManagerFactory
					.loadSingletonInstance(properties);
			logger.logEventStart("DaxWorkflow", "", "");
			logger.setLevel(2);

			bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);

			DAXParser daxParser = DAXParserFactory.loadDAXParser(bag,
					"DAX2CDAG", filePath);
			((Parser) daxParser).startParser(filePath);
			ADag dag = (ADag) ((DAX2CDAG) daxParser.getDAXCallback())
					.getConstructedObject();

			Queue<String> jobQueue = new LinkedList<>();
			for (Object rootNode : dag.getRootNodes()) {
				jobQueue.add((String) rootNode);
			}

			UtilizationModel utilizationModel = new UtilizationModelFull();

			while (!jobQueue.isEmpty()) {
				String jobId = jobQueue.remove();
				Job job = dag.getSubInfo(jobId);

				String taskName = job.getTXName();
				String taskId = job.getID();
				tasks.add(taskId);
				String params = job.getArguments();
				long cloudletLength = (long) (Math.abs(job.getRuntime()) * 1000);

				long bwLength = 0;
				int pesNumber = 1;
				long inputSize = 0;
				long outputSize = 0;

				for (Object input : job.getInputFiles()) {
					PegasusFile file = (PegasusFile) input;
					String name = file.getLFN();
					int size = (int) (Math.abs(file.getSize()) + 0.5d);
					if (size > 0) {
						inputSize += size;
						if (!fileNameToFile.containsKey(name)) {
							fileNameToFile.put(name, new File(name, size));
							fileNameToConsumingTaskIds.put(name,
									new ArrayList<Long>());
						}
						fileNameToConsumingTaskIds.get(name).add(
								(long) cloudletId);
					}
				}

				for (Object output : job.getOutputFiles()) {
					PegasusFile file = (PegasusFile) output;
					String name = file.getLFN();
					int size = (int) (Math.abs(file.getSize()) + 0.5d);
					if (size > 0) {
						outputSize += size;
						if (!fileNameToFile.containsKey(name)) {
							fileNameToFile.put(name, new File(name, size));
							fileNameToConsumingTaskIds.put(name,
									new ArrayList<Long>());
						}
						fileNameToProducingTaskId.put(name, (long) cloudletId);
					}
				}

				for (Object child : dag.getChildren(jobId)) {
					String childId = (String) child;
					if (!tasks.contains(childId)
							&& tasks.containsAll(dag.getParents(childId)))
						jobQueue.add(childId);
				}

				long ioLength = (inputSize + outputSize) / 1024;

				Task task = new Task(taskName, params, workflow, userId,
						(int) cloudletId, cloudletLength, ioLength, bwLength,
						pesNumber, inputSize, outputSize, utilizationModel,
						utilizationModel, utilizationModel);

				taskIdToTask.put((long)cloudletId, task);
				cloudletId++;
			}

		} catch (ParameterException e) {
			e.printStackTrace();
		}
	}
}
