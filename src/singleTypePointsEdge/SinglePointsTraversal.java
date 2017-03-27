package singleTypePointsEdge;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.graphdb.traversal.Uniqueness;

import system.Queries;
import system.GraphDbTraversal;

/**
 * SinglePointsTraversal class
 * 
 * @author ksemer
 */
public class SinglePointsTraversal extends GraphDbTraversal {

	// adjacency relationship
	private RelationshipType adjRelation;

	/**
	 * Constructor
	 * 
	 * @param graphdb
	 */
	public SinglePointsTraversal(GraphDatabaseService graphdb) {
		super(graphdb);

		// load graph types
		loadTypes();
	}

	protected String pathTraversalAtLeast(Node src, Node trg, List<Integer> times, int k) {

		String path = "";
		Map<String, BitSet> inter = new HashMap<>();
		BitSet interval = new BitSet();

		for (int t : times)
			interval.set(t);

		for (Path p : graphdb.traversalDescription().breadthFirst().relationships(adjRelation, Direction.BOTH)
				.uniqueness(Uniqueness.NODE_PATH).evaluator(new Evaluator() {
					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						Node u = path.lastRelationship().getStartNode();
						Node v = path.lastRelationship().getEndNode();
						BitSet edge_lifespan;

						String key = u.getId() + "_" + v.getId();

						if ((edge_lifespan = inter.get(key)) == null) {
							edge_lifespan = new BitSet();
							inter.put(key, edge_lifespan);

							int counter = 0;

							for (int i = 0; i < times.size(); i++) {

								int[] timeInstances = (int[]) path.lastRelationship().getProperty("time_instances");

								if (Arrays.binarySearch(timeInstances, times.get(i)) >= 0) {
									edge_lifespan.set(times.get(i));
									counter++;
								}

								if (times.size() - 1 - i + counter < k)
									return Evaluation.EXCLUDE_AND_PRUNE;
							}
						} else if (edge_lifespan.cardinality() < k) {
							return Evaluation.EXCLUDE_AND_PRUNE;
						} else if (path.endNode().equals(trg)) {
							return Evaluation.INCLUDE_AND_PRUNE;
						}

						return Evaluation.INCLUDE_AND_CONTINUE;
					}

				}).traverse(src)) {

			if (p.endNode().equals(trg)) {
				BitSet I = (BitSet) interval.clone();
				String key;

				for (Relationship rel_ : p.relationships()) {
					key = rel_.getStartNode().getId() + "_" + rel_.getEndNode().getId();
					I.and(inter.get(key));

					if (I.cardinality() < k)
						break;
				}

				if (I.cardinality() >= k) {
					if (Queries.simplePath)
						path += Paths.simplePathToString(p);
					else
						path += Paths.pathToString(p, pathPrinter);
					return path;
				}
			}
		}

		return path;
	}

	protected String pathTraversalDisj(Node src, Node trg, int time_instance) {

		String path = "";

		for (Path p : graphdb.traversalDescription().breadthFirst().relationships(adjRelation, Direction.BOTH)
				.uniqueness(Uniqueness.NODE_GLOBAL).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						int[] timeInstances = (int[]) path.lastRelationship().getProperty("time_instances");

						if (Arrays.binarySearch(timeInstances, time_instance) < 0)
							return Evaluation.EXCLUDE_AND_PRUNE;

						return Evaluation.INCLUDE_AND_CONTINUE;
					}
				}).traverse(src)) {

			if (p.endNode().equals(trg)) {
				if (Queries.simplePath)
					path += Paths.simplePathToString(p);
				else
					path += Paths.pathToString(p, pathPrinter);
				return path;
			}
		}

		return path;
	}

	protected String pathTraversalConj(Node src, Node trg, List<Integer> times) {

		String path = "";
		Map<String, Boolean> prune = new HashMap<>();

		for (Path p : graphdb.traversalDescription().breadthFirst().relationships(adjRelation, Direction.BOTH)
				.uniqueness(Uniqueness.NODE_PATH).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						Node u = path.lastRelationship().getStartNode();
						Node v = path.lastRelationship().getEndNode();
						Boolean isPruned;

						String key = u.getId() + "_" + v.getId();

						if ((isPruned = prune.get(key)) == null) {

							int[] timeInstances = (int[]) path.lastRelationship().getProperty("time_instances");

							for (int time_instance : times) {
								if (Arrays.binarySearch(timeInstances, time_instance) < 0) {
									prune.put(key, true);
									return Evaluation.EXCLUDE_AND_PRUNE;
								}
							}

							prune.put(key, false);
						} else if (isPruned) {
							return Evaluation.EXCLUDE_AND_PRUNE;
						}

						return Evaluation.INCLUDE_AND_CONTINUE;
					}
				}).traverse(src)) {

			if (p.endNode().equals(trg)) {
				if (Queries.simplePath)
					path += Paths.simplePathToString(p);
				else
					path += Paths.pathToString(p, pathPrinter);
				return path;
			}
		}

		return path;
	}

	protected boolean reachabilityBFS(Node src, Node trg, int time_instance) {

		for (Node currentNode : graphdb.traversalDescription().breadthFirst().relationships(adjRelation, Direction.BOTH)
				.uniqueness(Uniqueness.NODE_GLOBAL).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						int[] timeInstances = (int[]) path.lastRelationship().getProperty("time_instances");

						if (Arrays.binarySearch(timeInstances, time_instance) < 0)
							return Evaluation.EXCLUDE_AND_PRUNE;

						return Evaluation.INCLUDE_AND_CONTINUE;
					}
				}).traverse(src).nodes()) {

			if (currentNode.equals(trg))
				return true;
		}

		return false;
	}

	protected void loadTypes() {

		try (Transaction tx = graphdb.beginTx()) {

			for (RelationshipType rel : graphdb.getAllRelationshipTypes()) {

				if (rel.name().equals("" + RelTypes.EXISTS))
					continue;

				adjRelation = rel;
			}

			for (IndexDefinition x : graphdb.schema().getIndexes()) {
				System.out.println(x.getPropertyKeys() + "\t" + x.getLabel());
			}

			tx.success();
		}
	}
}