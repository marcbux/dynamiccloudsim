package de.huberlin.wbi.dcs.workflow;

import java.awt.Point;
import java.awt.geom.Point2D;
import org.apache.commons.collections15.Transformer;

public class TaskPositionInWorkflowTransformer implements Transformer<Task, Point2D> {
	
	private int width;
	private int height;
	private int[] breadth;
	private int[] breadthIndexes;
	
	public TaskPositionInWorkflowTransformer(int width, int height, int[] breadth) {
		super();
		this.width = width - 40;
		this.height = height - 40;
		this.breadth = breadth;
		breadthIndexes = new int[Task.getMaxDepth()+1];
	}
	
	@Override
	public Point2D transform(Task arg0) {
		Task task = arg0;
		double depth = task.getDepth();
		
		double xcontigs = breadth[task.getDepth()];
		double ycontigs = Task.getMaxDepth() + 1;
		
		int xpos = (int)(((width * breadthIndexes[task.getDepth()]++ / xcontigs) + (width * breadthIndexes[task.getDepth()] / xcontigs)) / 2d) + 20;
		int ypos = (int)(((height * depth / ycontigs) + (height * (depth + 1) / ycontigs)) / 2d) + 20;
		return new Point(xpos, ypos);
	}

}
