package singleTypeIntervalEdge;

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
 * SingleIntervalTraversal class
 * 
 * @author ksemer
 */
public class SingleIntervalTraversal extends GraphDbTraversal {

	// adjacency relationship
	private RelationshipType adjRelation;

	/**
	 * Constructor
	 * 
	 * @param graphdb
	 */
	public SingleIntervalTraversal(GraphDatabaseService graphdb) {
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

						int[] tstart = (int[]) path.lastRelationship().getProperty("tstart");
						int[] tend = (int[]) path.lastRelationship().getProperty("tend");
						String[] interval = null;
						int start, end, j_ = 0;

						Node u = path.lastRelationship().getStartNode();
						Node v = path.lastRelationship().getEndNode();
						BitSet edge_lifespan;

						String key = u.getId() + "_" + v.getId();

						if ((edge_lifespan = inter.get(key)) == null) {
							edge_lifespan = new BitSet();
							inter.put(key, edge_lifespan);

							for (int i = 0; i < Queries.intervals.length; i++) {
								interval = Queries.intervals[i].split("-");
								start = Integer.parseInt(interval[0]);
								end = Integer.parseInt(interval[1]);

								for (int j = j_; j < tstart.length; j++) {

									if (tend[j] < start)
										continue;

									if (tstart[j] > end)
										break;

									if (start >= tstart[j] && end <= tend[j]) {
										edge_lifespan.set(start, end + 1);
										j_ = j;
										break;
									}
								}
							}

							if (edge_lifespan.cardinality() < k)
								return Evaluation.EXCLUDE_AND_PRUNE;
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

						int[] tstart = (int[]) path.lastRelationship().getProperty("tstart");
						int[] tend = (int[]) path.lastRelationship().getProperty("tend");

						for (int i = 0; i < tstart.length; i++) {
							if (tstart[i] <= time_instance && time_instance <= tend[i])
								return Evaluation.INCLUDE_AND_CONTINUE;
							else if (tstart[i] > time_instance)
								return Evaluation.EXCLUDE_AND_PRUNE;
						}

						return Evaluation.EXCLUDE_AND_PRUNE;
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

						int[] tstart = (int[]) path.lastRelationship().getProperty("tstart");
						int[] tend = (int[]) path.lastRelationship().getProperty("tend");
						String[] interval = null;
						int start, end, j_ = 0;

						Node u = path.lastRelationship().getStartNode();
						Node v = path.lastRelationship().getEndNode();
						Boolean isPruned;

						String key = u.getId() + "_" + v.getId();

						if ((isPruned = prune.get(key)) == null) {

							for (int i = 0; i < Queries.intervals.length; i++) {
								interval = Queries.intervals[i].split("-");
								start = Integer.parseInt(interval[0]);
								end = Integer.parseInt(interval[1]);

								for (int j = j_; j < tstart.length; j++) {

									if (tend[j] < start) {
										if (j + 1 == tstart.length) {
											prune.put(key, true);
											return Evaluation.EXCLUDE_AND_PRUNE;
										}
										continue;
									}

									if (tstart[j] > end || (tend[j] < end && tend[j] >= start)) {
										prune.put(key, true);
										return Evaluation.EXCLUDE_AND_PRUNE;
									}

									if (start >= tstart[j] && end <= tend[j]) {
										j_ = j;
										break;
									}
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

						int[] tstart = (int[]) path.lastRelationship().getProperty("tstart");
						int[] tend = (int[]) path.lastRelationship().getProperty("tend");

						for (int i = 0; i < tstart.length; i++) {
							if (tstart[i] <= time_instance && time_instance <= tend[i])
								return Evaluation.INCLUDE_AND_CONTINUE;
							else if (tstart[i] > time_instance)
								return Evaluation.EXCLUDE_AND_PRUNE;
						}

						return Evaluation.EXCLUDE_AND_PRUNE;
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