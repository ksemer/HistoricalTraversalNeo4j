package multiTypeEdge;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.google.common.collect.HashBasedTable;

import system.Queries;
import system.GraphDbTraversal;

/**
 * MultiTraversal class
 * 
 * @author ksemer
 */
public class MultiTraversal extends GraphDbTraversal {

	// all relationships
	private Map<Integer, RelationshipType> relationships;

	/**
	 * Constructor
	 * 
	 * @param graphdb
	 */
	public MultiTraversal(GraphDatabaseService graphdb) {
		super(graphdb);

		// load graph types
		loadTypes();
	}

	@Override
	protected String pathTraversalAtLeast(Node src, Node trg, List<Integer> times, int k) {

		String path = "";
		BitSet interval = new BitSet();
		HashBasedTable<Integer, Integer, BitSet> inter = HashBasedTable.create(100000, 100);

		for (int t : times)
			interval.set(t);

		for (Path p : graphdb.traversalDescription().breadthFirst().uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
				.evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						if (path.lastRelationship().isType(RelTypes.EXISTS))
							return Evaluation.EXCLUDE_AND_PRUNE;

						Node u = path.lastRelationship().getStartNode();
						Node v = path.lastRelationship().getEndNode();
						Node tmp;
						BitSet edge_lifespan;
						
						int u_id = Integer.parseInt("" + u.getProperty("id"));
						int v_id = Integer.parseInt("" + v.getProperty("id"));
						int tmp_;
						
						if (u_id > v_id) {
							tmp_ = u_id;
							u_id = v_id;
							v_id = tmp_;
						}


						if ((edge_lifespan = inter.get(u_id, v_id)) == null) {
							edge_lifespan = new BitSet();
							inter.put(u_id, v_id, edge_lifespan);

							if (u.getDegree() > v.getDegree()) {
								tmp = u;
								u = v;
								v = tmp;
							}

							int counter = 0;

							for (int i = 0; i < times.size(); i++) {

								Iterable<Relationship> rels = u.getRelationships(Direction.BOTH,
										relationships.get(times.get(i)));

								for (Relationship r : rels) {

									if (r.getOtherNode(u).equals(v)) {
										edge_lifespan.set(times.get(i));
										counter++;
										break;
									}
								}

								if (times.size() - 1 - i + counter < k)
									return Evaluation.EXCLUDE_AND_PRUNE;
							}

						} else if (edge_lifespan.cardinality() < k) {
							return Evaluation.EXCLUDE_AND_PRUNE;
						} else if (path.endNode().equals(trg)) {
							return Evaluation.INCLUDE_AND_PRUNE;
						}
						
						if (path.lastRelationship().getEndNode().equals(src))
							return Evaluation.EXCLUDE_AND_PRUNE;


						return Evaluation.INCLUDE_AND_CONTINUE;
					}

				}).traverse(src)) {				

			if (p.endNode().equals(trg)) {
				BitSet I = (BitSet) interval.clone();

				for (Relationship rel_ : p.relationships()) {
					int u_id = Integer.parseInt("" + rel_.getStartNode().getProperty("id"));
					int v_id = Integer.parseInt("" + rel_.getEndNode().getProperty("id"));
					int tmp;

					if (u_id > v_id) {
						tmp = u_id;
						u_id = v_id;
						v_id = tmp;
					}

					I.and(inter.get(u_id, v_id));

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

	@Override
	protected String pathTraversalDisj(Node src, Node trg, int t) {

		String path = "";

		for (Path p : graphdb.traversalDescription().breadthFirst().relationships(relationships.get(t), Direction.BOTH)
				.uniqueness(Uniqueness.NODE_GLOBAL).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

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

	@Override
	protected String pathTraversalConj(Node src, Node trg, List<Integer> times) {

		String path = "";

		for (Path p : graphdb.traversalDescription().breadthFirst()
				.relationships(relationships.get(times.get(0)), Direction.BOTH)
				.uniqueness(Uniqueness.RELATIONSHIP_GLOBAL).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						Node u = path.lastRelationship().getStartNode();
						Node v = path.lastRelationship().getEndNode();
						Node tmp;

						if (u.getDegree() > v.getDegree()) {
							tmp = u;
							u = v;
							v = tmp;
						}

						boolean found = false;

						for (int i = 1; i < times.size(); i++) {
							found = false;

							Iterable<Relationship> rels = u.getRelationships(Direction.BOTH,
									relationships.get(times.get(i)));

							for (Relationship r : rels) {
								if (r.getOtherNode(u).equals(v)) {
									found = true;
									break;
								}
							}

							if (!found)
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

	@Override
	protected List<Node> reachabilityGlobalBFSConj(Node src, List<Integer> times) {

		Set<Node> nodes = new HashSet<>();

		for (Path p : graphdb.traversalDescription().breadthFirst()
				.relationships(relationships.get(times.get(0)), Direction.BOTH)
				.uniqueness(Uniqueness.RELATIONSHIP_GLOBAL).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						Node u = path.lastRelationship().getStartNode();
						Node v = path.lastRelationship().getEndNode();
						Node tmp;

						if (u.getDegree() > v.getDegree()) {
							tmp = u;
							u = v;
							v = tmp;
						}

						boolean found = false;

						for (int i = 1; i < times.size(); i++) {
							found = false;

							Iterable<Relationship> rels = u.getRelationships(Direction.BOTH,
									relationships.get(times.get(i)));

							for (Relationship r : rels) {
								if (r.getOtherNode(u).equals(v)) {
									found = true;
									break;
								}
							}

							if (!found)
								return Evaluation.EXCLUDE_AND_PRUNE;

						}

						return Evaluation.INCLUDE_AND_CONTINUE;
					}

				}).traverse(src)) {

			nodes.add(p.endNode());
			nodes.add(p.startNode());
		}

		return new ArrayList<>(nodes);
	}

	@Override
	protected List<String> reachabilityGlobalBFSAtLeast(Node src, List<Integer> times, int k) {

		Set<String> pairs = new HashSet<>();
		Map<String, BitSet> inter = new HashMap<>();
		BitSet interval = new BitSet();

		for (int t : times)
			interval.set(t);

		for (Path p : graphdb.traversalDescription().breadthFirst().uniqueness(Uniqueness.NODE_PATH)
				.evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						if (path.lastRelationship().isType(RelTypes.EXISTS))
							return Evaluation.EXCLUDE_AND_PRUNE;

						Node u = path.lastRelationship().getStartNode();
						Node v = path.lastRelationship().getEndNode();
						Node tmp;
						BitSet edge_lifespan;

						int u_id = Integer.parseInt("" + u.getProperty("id"));
						int v_id = Integer.parseInt("" + v.getProperty("id"));
						int tmp_;

						if (u_id > v_id) {
							tmp_ = u_id;
							u_id = v_id;
							v_id = tmp_;
						}

						String key = u_id + "," + v_id;

						if ((edge_lifespan = inter.get(key)) == null) {
							edge_lifespan = new BitSet();
							inter.put(key, edge_lifespan);

							if (u.getDegree() > v.getDegree()) {
								tmp = u;
								u = v;
								v = tmp;
							}

							int counter = 0;

							for (int i = 0; i < times.size(); i++) {

								Iterable<Relationship> rels = u.getRelationships(Direction.BOTH,
										relationships.get(times.get(i)));

								for (Relationship r : rels) {

									if (r.getOtherNode(u).equals(v)) {
										edge_lifespan.set(times.get(i));
										counter++;
										break;
									}
								}

								if (times.size() - 1 - i + counter < k)
									return Evaluation.EXCLUDE_AND_PRUNE;
							}

						} else if (edge_lifespan.cardinality() < k) {
							return Evaluation.EXCLUDE_AND_PRUNE;
						}

						return Evaluation.INCLUDE_AND_CONTINUE;
					}

				}).traverse(src)) {

			BitSet I = (BitSet) interval.clone();
			String key = null;

			for (Relationship rel_ : p.relationships()) {
				int u_id = Integer.parseInt("" + rel_.getStartNode().getProperty("id"));
				int v_id = Integer.parseInt("" + rel_.getEndNode().getProperty("id"));
				int tmp;

				if (u_id > v_id) {
					tmp = u_id;
					u_id = v_id;
					v_id = tmp;
				}

				key = u_id + "," + v_id;

				I.and(inter.get(key));

				if (I.cardinality() < k)
					break;
				else
					pairs.add(key);
			}
		}

		return new ArrayList<>(pairs);
	}

	@Override
	protected boolean reachabilityBFS(Node src, Node trg, int t) {

		for (Node currentNode : graphdb.traversalDescription().breadthFirst()
				.relationships(relationships.get(t), Direction.BOTH).uniqueness(Uniqueness.NODE_GLOBAL).traverse(src)
				.nodes()) {

			if (currentNode.equals(trg))
				return true;
		}

		return false;
	}

	@Override
	protected List<Node> reachabilityGlobalBFS(Node src, int t) {
		List<Node> nodes = new ArrayList<>();

		for (Path currentNode : graphdb.traversalDescription().breadthFirst()
				.relationships(relationships.get(t), Direction.BOTH).uniqueness(Uniqueness.NODE_GLOBAL).traverse(src)) {
			nodes.add(currentNode.endNode());
		}

		return nodes;
	}

	@Override
	protected void loadTypes() {
		relationships = new HashMap<>();

		try (Transaction tx = graphdb.beginTx()) {

			for (RelationshipType rel : graphdb.getAllRelationshipTypes()) {

				if (rel.name().equals("" + RelTypes.EXISTS))
					continue;

				relationships.put(Integer.parseInt(rel.name()), rel);
			}

			for (IndexDefinition x : graphdb.schema().getIndexes()) {
				System.out.println(x.getPropertyKeys() + "\t" + x.getLabel());
			}

			tx.success();
		}
	}
}