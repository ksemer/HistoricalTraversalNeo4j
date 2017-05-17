package system;

import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * GraphDbTraversal Abstract Class
 * 
 * @author ksemer
 */
public abstract class GraphDbTraversal {
	// graph db instance
	protected GraphDatabaseService graphdb;

	public PathPrinter pathPrinter = new PathPrinter("name");

	/**
	 * RelationType for time index
	 * 
	 * @author ksemer
	 *
	 */
	public static enum RelTypes implements RelationshipType {
		EXISTS
	}

	public GraphDbTraversal(GraphDatabaseService graphdb) {
		this.graphdb = graphdb;
	}

	public abstract List<RelationshipType> getAdjRelation();

	/**
	 * Return as BitSet the lifespan of the given relationship intersected with
	 * I
	 * 
	 * @param rel
	 * @param I
	 * @return
	 */
	protected abstract BitSet getLifespan(Relationship rel, BitSet I);

	/**
	 * Load graph db types
	 */
	protected abstract void loadTypes();

	/**
	 * Connection src -> trg at least 1 time
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public boolean disjunctiveReachability(Node src, Node trg, Set<Integer> times) {

		if (Config.TIME_INDEX_ENABLED) {
			times.removeAll(checkLifespan(src, times));
			times.removeAll(checkLifespan(trg, times));
		}

		return disjunctiveBFS(src, trg, times);
	}

	/**
	 * Connection src -> trg in all times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public boolean conjunctiveReachability(Node src, Node trg, Set<Integer> times) {

		if (Config.TIME_INDEX_ENABLED && !(checkLifespan(src, times).isEmpty() && checkLifespan(trg, times).isEmpty()))
			return false;

		return conjunctiveBFS(src, trg, times);
	}

	/**
	 * Connection src -> trg at least k times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @param k
	 * @return
	 */
	public boolean atLeastReachability(Node src, Node trg, Set<Integer> times, int k) {

		if (Config.TIME_INDEX_ENABLED) {
			times.removeAll(checkLifespan(src, times));
			times.removeAll(checkLifespan(trg, times));

			if (times.size() < k)
				return false;
		}

		return atLeastKBFS(src, trg, times, k);
	}

	/**
	 * Path u -> v alive in at least 1 time
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public String disjunctivePath(Node src, Node trg, Set<Integer> times) {

		if (Config.TIME_INDEX_ENABLED) {
			times.removeAll(checkLifespan(src, times));
			times.removeAll(checkLifespan(trg, times));
		}

		String path = pathTraversalDisj(src, trg, times);

		if (path.isEmpty())
			return "no path exists";

		return path;
	}

	/**
	 * Path u -> v alive in all times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public String conjunctivePath(Node src, Node trg, Set<Integer> times) {

		String path = null;

		if (Config.TIME_INDEX_ENABLED && !(checkLifespan(src, times).isEmpty() && checkLifespan(trg, times).isEmpty()))
			return "no path exists";

		path = pathTraversalConj(src, trg, times);

		if (path.isEmpty())
			return "no path exists";

		return path;
	}

	/**
	 * Path u -> alive in at least k times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @param k
	 * @return
	 */
	public String atLeastPath(Node src, Node trg, Set<Integer> times, int k) {

		String path = "";

		if (Config.TIME_INDEX_ENABLED) {
			times.removeAll(checkLifespan(src, times));
			times.removeAll(checkLifespan(trg, times));

			if (times.size() < k)
				return "no path exists";
		}

		path = pathTraversalAtLeast(src, trg, times, k);

		if (!path.isEmpty())
			return path;

		return "no path exists";
	}

	/******************************** Traversals ********************************/

	protected boolean disjunctiveBFS(Node src, Node trg, Set<Integer> times) {

		Map<Node, BitSet> traversedI = new HashMap<>();
		Queue<Node> toBeTraversed = new LinkedList<Node>();
		Queue<BitSet> intervalToBeChecked = new LinkedList<BitSet>();
		BitSet interval = new BitSet();

		RelationshipType adjRelation = getAdjRelation().get(0);

		for (int t : times)
			interval.set(t);

		toBeTraversed.add(src);
		intervalToBeChecked.add(interval);
		traversedI.put(src, (BitSet) interval.clone());

		Node n, w;
		BitSet iQ, eI, life;

		while (!toBeTraversed.isEmpty()) {
			n = toBeTraversed.poll();
			iQ = intervalToBeChecked.poll();

			// for all the adjacencies
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {

				eI = getLifespan(rel, iQ);

				if (eI.isEmpty())
					continue;

				w = rel.getOtherNode(n);

				// trg node found
				if (w.equals(trg))
					return true;

				if ((life = traversedI.get(w)) == null) {

					// puts the traversed time for trg_node
					traversedI.put(w, eI);

					toBeTraversed.add(w);
					intervalToBeChecked.add(eI);
				} else if (!refine(life, eI).isEmpty()) {
					toBeTraversed.add(w);
					intervalToBeChecked.add(eI);
				}
			}
		}
		return false;
	}

	/**
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	protected boolean conjunctiveBFS(Node src, Node trg, Set<Integer> times) {

		Map<Node, BitSet> traversedI = new HashMap<>();
		Queue<Node> toBeTraversed = new LinkedList<Node>();
		Queue<BitSet> intervalToBeChecked = new LinkedList<BitSet>();
		BitSet interval = new BitSet(), R = new BitSet();

		RelationshipType adjRelation = getAdjRelation().get(0);

		for (int t : times)
			interval.set(t);

		toBeTraversed.add(src);
		intervalToBeChecked.add(interval);
		traversedI.put(src, (BitSet) interval.clone());

		Node n, w;
		BitSet iQ, eI, life;

		while (!toBeTraversed.isEmpty()) {
			n = toBeTraversed.poll();
			iQ = intervalToBeChecked.poll();

			// for all the adjacencies
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {

				eI = getLifespan(rel, iQ);

				if (eI.isEmpty())
					continue;

				w = rel.getOtherNode(n);

				// trg node found
				if (w.equals(trg)) {

					// R merge with I
					R.or(eI);

					if (R.equals(interval))
						return true;

					continue;
				}

				if ((life = traversedI.get(w)) == null) {

					// puts the traversed time for trg_node
					traversedI.put(w, eI);

					toBeTraversed.add(w);
					intervalToBeChecked.add(eI);
				} else if (!refine(life, eI).isEmpty()) {
					toBeTraversed.add(w);
					intervalToBeChecked.add(eI);
				}
			}
		}
		return false;
	}

	/**
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @param k
	 * @return
	 */
	protected boolean atLeastKBFS(Node src, Node trg, Set<Integer> times, int k) {

		Map<Node, BitSet> traversedI = new HashMap<>();
		Queue<Node> toBeTraversed = new LinkedList<Node>();
		Queue<BitSet> intervalToBeChecked = new LinkedList<BitSet>();
		BitSet interval = new BitSet(), R = new BitSet();

		RelationshipType adjRelation = getAdjRelation().get(0);

		for (int t : times)
			interval.set(t);

		toBeTraversed.add(src);
		intervalToBeChecked.add(interval);
		traversedI.put(src, (BitSet) interval.clone());

		Node n, w;
		BitSet iQ, eI, life;

		while (!toBeTraversed.isEmpty()) {
			n = toBeTraversed.poll();
			iQ = intervalToBeChecked.poll();

			// for all the adjacencies
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {

				eI = getLifespan(rel, iQ);

				if (eI.isEmpty())
					continue;

				w = rel.getOtherNode(n);

				// trg node found
				if (w.equals(trg)) {

					// R merge with I
					R.or(eI);

					if (R.cardinality() >= k)
						return true;

					continue;
				}

				if ((life = traversedI.get(w)) == null) {

					// puts the traversed time for trg_node
					traversedI.put(w, eI);

					toBeTraversed.add(w);
					intervalToBeChecked.add(eI);
				} else if (!refine(life, eI).isEmpty()) {
					toBeTraversed.add(w);
					intervalToBeChecked.add(eI);
				}
			}
		}
		return false;
	}

	/******************************** Paths ********************************/

	/**
	 * Path computation for disjunctive query
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	protected String pathTraversalDisj(Node src, Node trg, Set<Integer> times) {

		Map<Node, BitSet> traversedI = new HashMap<>();
		Queue<Deque<Node>> toBeTraversed = new LinkedList<Deque<Node>>();
		Queue<BitSet> intervalToBeChecked = new LinkedList<BitSet>();
		BitSet interval = new BitSet();
		String path = "";

		RelationshipType adjRelation = getAdjRelation().get(0);

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
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {

				eI = getLifespan(rel, iQ);

				if (eI.isEmpty())
					continue;

				w = rel.getOtherNode(n);

				// trg node found
				if (w.equals(trg))
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

	/**
	 * Path computation for conjunctive query
	 * 
	 * @param src
	 * @param trg
	 * @param rel
	 * @param times
	 * @return
	 */
	protected String pathTraversalConj(Node src, Node trg, Set<Integer> times) {

		Set<Node> traversed = new HashSet<>();
		Queue<Deque<Node>> toBeTraversed = new LinkedList<Deque<Node>>();
		BitSet interval = new BitSet(), eI;
		String path = "";

		RelationshipType adjRelation = getAdjRelation().get(0);

		for (int t : times)
			interval.set(t);

		Deque<Node> p = new LinkedList<Node>(), p_;
		p.add(src);

		toBeTraversed.add(p);
		traversed.add(src);

		Node n, w;

		while (!toBeTraversed.isEmpty()) {
			p = toBeTraversed.poll();
			n = p.getLast();

			// for all the adjacencies
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {

				eI = getLifespan(rel, interval);

				if (!eI.equals(interval))
					continue;

				w = rel.getOtherNode(n);

				// trg node found
				if (w.equals(trg))
					return p.toString();

				if (!traversed.contains(w)) {
					traversed.add(w);
					p_ = new LinkedList<Node>(p);
					p_.add(w);
					toBeTraversed.add(p_);
				}
			}
		}
		return path;
	}

	/**
	 * Path computation for at least query
	 * 
	 * @param src
	 * @param trg
	 * @param rel
	 * @param times
	 * @param k
	 * @return
	 */
	protected String pathTraversalAtLeast(Node src, Node trg, Set<Integer> times, int k) {

		Map<Node, BitSet> traversedI = new HashMap<>();
		Queue<Deque<Node>> toBeTraversed = new LinkedList<Deque<Node>>();
		Queue<BitSet> intervalToBeChecked = new LinkedList<BitSet>();
		BitSet interval = new BitSet();
		String path = "";

		RelationshipType adjRelation = getAdjRelation().get(0);

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
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {

				eI = getLifespan(rel, iQ);

				if (eI.isEmpty() || eI.cardinality() < k)
					continue;

				w = rel.getOtherNode(n);

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

	/********************************* Util ********************************/

	/**
	 * Return times where node n was not alive
	 * 
	 * @param n
	 * @param times
	 * @return
	 */
	private Set<Integer> checkLifespan(Node n, Set<Integer> times) {
		Set<Integer> t = new HashSet<>(times);

		for (Relationship rel : n.getRelationships(RelTypes.EXISTS, Direction.INCOMING)) {
			t.remove(Integer.parseInt(rel.getStartNode().getProperty("timeInstance").toString()));
		}

		return t;
	}

	/**
	 * Checks if pruning will be done for node that has been already traversed
	 * 
	 * @param INb
	 * @param I
	 * @return
	 */
	protected BitSet refine(BitSet INb, BitSet I) {

		for (Iterator<Integer> it = I.stream().iterator(); it.hasNext();) {
			int t = it.next();

			if (!INb.get(t)) {
				INb.set(t);
				it.remove();
			}
		}

		return I;
	}
}