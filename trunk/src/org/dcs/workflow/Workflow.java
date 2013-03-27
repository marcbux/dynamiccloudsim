package org.dcs.workflow;

import java.util.*;

import javax.swing.JFrame;

import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.ParameterException;

import edu.uci.ics.jung.algorithms.layout.*;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position;

public class Workflow {
	
	private Graph<Task, DataDependency> workflow;
	
	private int[] breadth;
	
	public Workflow() {
		workflow = new DirectedSparseMultigraph<Task, DataDependency>();
	}
	
	public void addTask(Task task) {
		workflow.addVertex(task);
	}
	
	public void addFile(File file, Task taskGeneratingThisFile, List<Task> tasksRequiringThisFile) {
		try {
			int depth = taskGeneratingThisFile.getDepth() + 1;
			for (Task t : tasksRequiringThisFile) {
				workflow.addEdge(new DataDependency(file), taskGeneratingThisFile, t);
				t.incNDataDependencies();
				setDepth(t, depth);
			}
		} catch (ParameterException e) {
			e.printStackTrace();
		}
	}
	
	private void computeWidthArray() {
		breadth = new int[Task.getMaxDepth()+1];
		for (Task t : workflow.getVertices()) {
			int depth = t.getDepth();
			breadth[depth]++;
		}
	}
	
	private void setDepth(Task task, int depth) {
		if (task.getDepth() < depth) {
			task.setDepth(depth);
			for (Task successor : workflow.getSuccessors(task)) {
				setDepth(successor, depth + 1);
			}
		}
	}
	
	public Graph<Task, DataDependency> getGraph() {
		return workflow;
	}
	
	public Collection<Task> getTasks() {
		return workflow.getVertices();
	}
	
	public List<Task> getSortedTasks() {
		List<Task> tasks = new ArrayList<Task>(getTasks());
		Collections.sort(tasks);
		return tasks;
	}
	
	public Collection<Task> getChildren(Task task) {
		return workflow.getSuccessors(task);
	}
	
	public List<Task> getSortedChildren (Task task) {
		List<Task> children = new ArrayList<Task>(getChildren(task));
		Collections.sort(children);
		return children;
	}
	
	public int getNTasks() {
		return workflow.getVertexCount();
	}
	
	@Override
	public String toString() {
		return workflow.toString();
	}
	
	public void visualize(int width, int height) {
		computeWidthArray();
		JFrame jf = new JFrame();
		StaticLayout<Task, DataDependency> sl = new StaticLayout<Task, DataDependency>(workflow, new TaskPositionInWorkflowTransformer(width, height, breadth));
		VisualizationViewer<Task, DataDependency> vv = new VisualizationViewer<Task, DataDependency>(sl);
		vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller<Task>());
		vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller<DataDependency>());
		vv.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR);
		jf.getContentPane().add(vv);
		jf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		jf.pack();
		jf.setVisible(true);
	}
}
