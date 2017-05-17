package multiTypeEdge;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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

import system.Config;
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

	/******************************** Traversals ********************************/

	@Override
	protected boolean disjunctiveBFS(Node src, Node trg, Set<Integer> times) {

		for (int t : times) {
			for (Node currentNode : graphdb.traversalDescription().breadthFirst()
					.relationships(relationships.get(t), Direction.BOTH).uniqueness(Uniqueness.NODE_GLOBAL)
					.traverse(src).nodes()) {

				if (currentNode.equals(trg))
					return true;
			}
		}

		return false;
	}

	@Override
	protected boolean conjunctiveBFS(Node src, Node trg, Set<Integer> times) {
		boolean found;

		for (int t : times) {

			found = false;

			for (Node currentNode : graphdb.traversalDescription().breadthFirst()
					.relationships(relationships.get(t), Direction.BOTH).uniqueness(Uniqueness.NODE_GLOBAL)
					.traverse(src).nodes()) {

				if (currentNode.equals(trg)) {
					found = true;
					break;
				}
			}

			if (!found) {
				return false;
			}
		}

		return true;
	}

	@Override
	protected boolean atLeastKBFS(Node src, Node trg, Set<Integer> times_, int k) {
		int counter = 0;

		List<Integer> times = new ArrayList<>(times_);

		for (int i = 0; i < times.size(); i++) {
			for (Node currentNode : graphdb.traversalDescription().breadthFirst()
					.relationships(relationships.get(times.get(i)), Direction.BOTH).uniqueness(Uniqueness.NODE_GLOBAL)
					.traverse(src).nodes()) {

				if (currentNode.equals(trg)) {
					counter++;
					break;
				}
			}

			if (counter + times.size() - i <= k)
				return false;
		}

		return true;
	}

	/******************************** Paths ********************************/

	@Override
	protected String pathTraversalDisj(Node src, Node trg, Set<Integer> times) {

		String path = "";

		for (int t : times) {

			for (Path p : graphdb.traversalDescription().breadthFirst()
					.relationships(relationships.get(t), Direction.BOTH).uniqueness(Uniqueness.NODE_GLOBAL)
					.evaluator(new Evaluator() {

						@Override
						public Evaluation evaluate(final Path path) {

							if (path.length() == 0)
								return Evaluation.EXCLUDE_AND_CONTINUE;

							return Evaluation.INCLUDE_AND_CONTINUE;
						}
					}).traverse(src)) {

				if (p.endNode().equals(trg)) {
					if (Config.SIMPLE_PATH)
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
	protected String pathTraversalConj(Node src, Node trg, Set<Integer> times) {

		String path = "";
		List<Integer> times_ = new ArrayList<>(times);

		for (Path p : graphdb.traversalDescription().breadthFirst()
				.relationships(relationships.get(times_.get(0)), Direction.BOTH)
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
									relationships.get(times_.get(i)));

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
				if (Config.SIMPLE_PATH)
					path += Paths.simplePathToString(p);
				else
					path += Paths.pathToString(p, pathPrinter);
				return path;
			}
		}

		return path;
	}

	@Override
	protected String pathTraversalAtLeast(Node src, Node trg, Set<Integer> times, int k) {

		Map<Node, BitSet> traversedI = new HashMap<>();
		Queue<Deque<Node>> toBeTraversed = new LinkedList<Deque<Node>>();
		Queue<BitSet> intervalToBeChecked = new LinkedList<BitSet>();
		BitSet interval = new BitSet();
		String path = "";

		for (int t : times)
			interval.set(t);

		Deque<Node> p = new LinkedList<Node>(), p_;
		p.add(src);

		toBeTraversed.add(p);
		intervalToBeChecked.add(interval);
		traversedI.put(src, (BitSet) interval.clone());

		Node n, w;
		BitSet iQ, eI, life;

		while (!toBeTraversed.isEmpty()) {
			p = toBeTraversed.poll();
			n = p.getLast();
			iQ = intervalToBeChecked.poll();

			// for all the adjacencies
			for (Relationship rel : n.getRelationships(Direction.BOTH)) {

				if (rel.isType(RelTypes.EXISTS))
					continue;

				w = rel.getOtherNode(n);

				eI = getLifespan_(n, w, iQ);

				if (eI.cardinality() < k)
					continue;

				// trg node found
				if (w.equals(trg) && eI.cardinality() >= k)
					return p.toString();

				if ((life = traversedI.get(w)) == null) {

					// puts the traversed time for trg_node
					traversedI.put(w, eI);

					p_ = new LinkedList<Node>(p);
					p_.add(w);
					toBeTraversed.add(p_);
					intervalToBeChecked.add(eI);
				} else if (!refine(life, eI).isEmpty()) {
					p_ = new LinkedList<Node>(p);
					p_.add(w);
					toBeTraversed.add(p_);
					intervalToBeChecked.add(eI);
				}
			}
		}
		return path;
	}

	/******************************** Util ********************************/

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

	@Override
	protected BitSet getLifespan(Relationship rel, BitSet I) {
		// TODO (not needed)
		return null;
	}

	/**
	 * Get the times that u is connected with v in I
	 * 
	 * @param u
	 * @param v
	 * @param I
	 * @return
	 */
	private BitSet getLifespan_(Node u, Node v, BitSet I) {
		BitSet lifespan = new BitSet();
		int time_instance;

		for (Iterator<Integer> it = I.stream().iterator(); it.hasNext();) {
			time_instance = it.next();

			Iterable<Relationship> rels = u.getRelationships(Direction.BOTH, relationships.get(time_instance));

			for (Relationship r : rels) {

				if (r.getOtherNode(u).equals(v)) {
					lifespan.set(time_instance);
					break;
				}
			}
		}
		return lifespan;
	}

	@Override
	public List<RelationshipType> getAdjRelation() {
		return new ArrayList<>(relationships.values());
	}
}