package de.huberlin.wbi.dcs.workflow;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Paint;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.util.*;

import javax.imageio.ImageIO;
//import javax.swing.JFrame;

import org.apache.commons.collections15.Transformer;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.ParameterException;

import edu.uci.ics.jung.algorithms.layout.*;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.VisualizationImageServer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
//import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
//import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position;

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
	
//	public List<Task> getSortedTasks() {
//		List<Task> tasks = new ArrayList<Task>(getTasks());
//		Collections.sort(tasks);
//		return tasks;
//	}
	
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
		Dimension size = new Dimension(width, height);

		StaticLayout<Task, DataDependency> sl = new StaticLayout<Task, DataDependency>(workflow, new TaskPositionInWorkflowTransformer(width, height, breadth));
		sl.setSize(size);
		VisualizationViewer<Task, DataDependency> vv = new VisualizationViewer<Task, DataDependency>(sl);
		vv.setPreferredSize(size); 
		
		// Transformer maps the vertex number to a vertex property
        Transformer<Task,Paint> vertexColor = new Transformer<Task,Paint>() {
            public Paint transform(Task task) {
            	switch (task.getDepth()) {
            		case 2: return new Color(31,73,125);   // index
            		case 3: return new Color(228,108,10);  // align
            		case 4: return new Color(119,147,60);  // samtools view
            		case 5: return new Color(244,238,19);  // samtools sort
            		case 6: return new Color(96,74,123);   // samtools merge
            		case 7: return new Color(96,74,123);   // samtools merge
            		case 8: return new Color(149,55,53);   // pileup
            		case 9: return new Color(49,133,156);  // varscan
            		case 10: return new Color(20,137,89);  // diff
//            		case 6: return new Color(96,74,123);
//            		case 7: return new Color(96,74,123);
            		default: return new Color(127,127,127); // stage in / out
            	}
            }
        };
        Transformer<Task,Shape> vertexSize = new Transformer<Task,Shape>(){
            public Shape transform(Task task){
                Ellipse2D circle = new Ellipse2D.Double(-15, -15, 30, 30);
                if (breadth[task.getDepth()] > 25) {
                	circle = new Ellipse2D.Double(-5, -15, 10, 30);
                }
                if (breadth[task.getDepth()] > 250) {
                	circle = new Ellipse2D.Double(-2, -15, 4, 30);
                }
                return circle;
            }
        };
        Transformer<Task,Stroke> vertexStroke = new Transformer<Task,Stroke>(){
            public Stroke transform(Task i){
                return new BasicStroke(0f);
            }
        };
		
		// to file
		VisualizationImageServer<Task, DataDependency> vis = new VisualizationImageServer<Task, DataDependency>(vv.getGraphLayout(), vv.getGraphLayout().getSize());
		vis.setPreferredSize(size);
		vis.getRenderContext().setVertexFillPaintTransformer(vertexColor);
		vis.getRenderContext().setVertexShapeTransformer(vertexSize);
		vis.getRenderContext().setVertexStrokeTransformer(vertexStroke);
//		vis.getRenderContext().setVertexLabelTransformer(new ToStringLabeller<Task>());
//		vis.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller<DataDependency>());
//		vis.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR);
		BufferedImage image = (BufferedImage) vis.getImage(new Point2D.Double(vv.getGraphLayout().getSize().getWidth() / 2,
			    vv.getGraphLayout().getSize().getHeight() / 2), new Dimension(vv.getGraphLayout().getSize()));
		try {
			ImageIO.write(image, "png", new java.io.File("test.png"));
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		// to frame
//      vv.getRenderContext().setVertexFillPaintTransformer(vertexColor);
//      vv.getRenderContext().setVertexShapeTransformer(vertexSize);
//      vv.getRenderContext().setVertexStrokeTransformer(vertexStroke);
//		vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller<Task>());
//		vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller<DataDependency>());
//		vv.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR);
//		JFrame jf = new JFrame();
//		jf.getContentPane().add(vv);
//		jf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//		jf.pack();
//		jf.setVisible(true);
	}
}
