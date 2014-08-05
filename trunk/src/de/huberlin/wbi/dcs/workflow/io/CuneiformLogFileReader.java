package de.huberlin.wbi.dcs.workflow.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.ParameterException;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

public class CuneiformLogFileReader extends LogFileReader {

	@Override
	protected void fillDataStructures(int userId, String filePath,
			boolean fileNames, boolean kernelTime, Workflow workflow) {
		try (BufferedReader reader = new BufferedReader(
				new FileReader(filePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				JsonReportEntry e = new JsonReportEntry(line);
				Task task = null;
				if (e.hasInvocId()) {
					task = createOrGetTask(e.getInvocId(), e.getTaskName(),
							workflow, userId);
				}
				switch (e.getKey()) {
				case JsonReportEntry.KEY_INVOC_TIME:
					task.incMi(e.getValueJsonObj().getLong("realTime"));
					break;
				case JsonReportEntry.KEY_FILE_SIZE_STAGEIN:
					String fileName = e.getFile();
					long fileSize = Long.parseLong(e.getValueRawString())  / 1024;
					if (fileSize > 0) {
						createOrGetFile(fileName, (int)fileSize);
						task.incIo(fileSize);
						fileNameToConsumingTaskIds.get(fileName).add(
								e.getInvocId());
					}
					break;
				case JsonReportEntry.KEY_FILE_SIZE_STAGEOUT:
					fileName = e.getFile();
					fileSize = Long.parseLong(e.getValueRawString()) / 1024;
					if (fileSize > 0) {
						createOrGetFile(e.getFile(), (int)fileSize);
						task.incIo(fileSize);
						fileNameToProducingTaskId.put(fileName, e.getInvocId());
					}
					break;
//				case HiwayDBI.KEY_INVOC_TIME_STAGEIN:
//					task.setBw(task.getBw()
//							+ e.getValueJsonObj().getLong("realTime"));
//					break;
//				case HiwayDBI.KEY_INVOC_TIME_STAGEOUT:
//					task.setBw(task.getBw()
//							+ e.getValueJsonObj().getLong("realTime"));
//					break;
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private Task createOrGetTask(long taskId, String taskName,
			Workflow workflow, int userId) {
		if (!taskIdToTask.containsKey(taskId)) {
			Task task = new Task(taskName, "", workflow, userId, cloudletId++,
					0, 0, 0, 1, 0, 0, utilizationModel, utilizationModel,
					utilizationModel);
			taskIdToTask.put(taskId, task);
		}
		return taskIdToTask.get(taskId);
	}

	private File createOrGetFile(String fileName, int fileSize) {
		if (!fileNameToFile.containsKey(fileName)) {
			File file;
			try {
				file = new File(fileName, fileSize);
				fileNameToFile.put(fileName, file);
				fileNameToConsumingTaskIds.put(fileName, new ArrayList<Long>());
			} catch (ParameterException e) {
				e.printStackTrace();
			}
		}
		return fileNameToFile.get(fileName);
	}

}
