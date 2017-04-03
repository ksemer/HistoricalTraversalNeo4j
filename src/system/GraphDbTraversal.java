package system;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

import multiTypeEdge.MultiTraversal;

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
	 * Return as BitSet the lifespan of the given relatioship
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
	 * Path computation for disjunctive query
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	protected abstract String pathTraversalDisj(Node src, Node trg, List<Integer> times);

	/**
	 * Path computation for conjunctive query
	 * 
	 * @param src
	 * @param trg
	 * @param rel
	 * @param times
	 * @return
	 */
	protected abstract String pathTraversalConj(Node src, Node trg, List<Integer> times);

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
	protected abstract String pathTraversalAtLeast(Node src, Node trg, List<Integer> times, int k);

	/**
	 * Connection src -> trg in all times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public boolean conjunctiveReachability(Node src, Node trg, Set<Integer> times) {

		if (!checkLifespan(src, times).isEmpty() || !checkLifespan(trg, times).isEmpty())
			return false;

		List<Integer> times_ = new ArrayList<>(times);
		Collections.sort(times_);
		
		return conjunctiveBFS(src, trg, times_);
	}

	/**
	 * Connection src -> trg at least 1 time
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public boolean disjunctiveReachability(Node src, Node trg, Set<Integer> times) {

		Set<Integer> t1, t2;

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);
		
		List<Integer> times_ = new ArrayList<>(t1);
		
		if (! (this instanceof MultiTraversal) )
			Collections.sort(times_);
		
		return disjunctiveBFS(src, trg, times_);
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
		Set<Integer> t1, t2;

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);

		if (t1.size() < k)
			return false;
		
		List<Integer> times_ = new ArrayList<>(t1);

		if (! (this instanceof MultiTraversal) )
			Collections.sort(times_);
		
		return atLeastKBFS(src, trg, times_, k);
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

		if (!checkLifespan(src, times).isEmpty() || !checkLifespan(trg, times).isEmpty())
			return "no path exists";

		List<Integer> times_ = new ArrayList<>(times);
		Collections.sort(times_);

		path = pathTraversalConj(src, trg, times_);

		if (path.isEmpty())
			return "no path exists";

		return path;
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

		Set<Integer> t1, t2;

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);
		
		List<Integer> times_ = new ArrayList<>(t1);
		
		if (! (this instanceof MultiTraversal) )
			Collections.sort(times_);
		
		String path = "no path exists";
		path = pathTraversalDisj(src, trg, times_);	
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

		Set<Integer> t1, t2;
		String path = "";

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);

		if (t1.size() < k)
			return "no path exists";

		List<Integer> times_ = new ArrayList<>(t1);
		Collections.sort(times_);

		path = pathTraversalAtLeast(src, trg, times_, k);

		if (!path.isEmpty())
			return path;

		return "no path exists";
	}
	
	/******************************** Traversals ********************************/

	protected boolean disjunctiveBFS(Node src, Node trg, List<Integer> times) {
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
		BitSet iQ, I, eI, life;

		while (!toBeTraversed.isEmpty()) {
			n = toBeTraversed.poll();
			iQ = intervalToBeChecked.poll();

			// for all the adjacencies
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {
				w = rel.getOtherNode(n);

				eI = getLifespan(rel, interval);

				I = (BitSet) iQ.clone();
				I.and(eI);

				if (I.isEmpty())
					continue;

				// trg node found
				if (w.equals(trg))
					return true;

				if ((life = traversedI.get(w)) == null) {

					// puts the traversed time for trg_node
					traversedI.put(w, (BitSet) I.clone());

					toBeTraversed.add(w);
					intervalToBeChecked.add((BitSet) I.clone());
				} else if (!refine(life, I)) {
					toBeTraversed.add(w);
					intervalToBeChecked.add((BitSet) I.clone());
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
	protected boolean conjunctiveBFS(Node src, Node trg, List<Integer> times) {

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
		BitSet iQ, I, eI, life;

		while (!toBeTraversed.isEmpty()) {
			n = toBeTraversed.poll();
			iQ = intervalToBeChecked.poll();

			// for all the adjacencies
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {
				w = rel.getOtherNode(n);

				eI = getLifespan(rel, interval);

				// join edge lifespan with iQ
				I = (BitSet) iQ.clone();
				I.and(eI);

				if (I.isEmpty())
					continue;

				// trg node found
				if (w.equals(trg)) {

					// R merge with I
					R.or(I);

					if (R.equals(interval))
						return true;
				}

				if ((life = traversedI.get(w)) == null) {

					// puts the traversed time for trg_node
					traversedI.put(w, (BitSet) I.clone());

					toBeTraversed.add(w);
					intervalToBeChecked.add((BitSet) I.clone());
				} else if (!refine(life, I)) {
					toBeTraversed.add(w);
					intervalToBeChecked.add((BitSet) I.clone());
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
	protected boolean atLeastKBFS(Node src, Node trg, List<Integer> times, int k) {

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
		BitSet iQ, I, eI, life;

		while (!toBeTraversed.isEmpty()) {
			n = toBeTraversed.poll();
			iQ = intervalToBeChecked.poll();

			// for all the adjacencies
			for (Relationship rel : n.getRelationships(Direction.BOTH, adjRelation)) {
				w = rel.getOtherNode(n);

				eI = getLifespan(rel, interval);

				// join edge lifespan with iQ
				I = (BitSet) iQ.clone();
				I.and(eI);

				if (I.isEmpty())
					continue;

				// trg node found
				if (w.equals(trg) && I.cardinality() >= k)
					return true;

				if ((life = traversedI.get(w)) == null) {

					// puts the traversed time for trg_node
					traversedI.put(w, (BitSet) I.clone());

					toBeTraversed.add(w);
					intervalToBeChecked.add((BitSet) I.clone());
				} else if (!refine(life, I)) {
					toBeTraversed.add(w);
					intervalToBeChecked.add((BitSet) I.clone());
				}
			}
		}
		return false;
	}
	
	/******************************** Util methods ********************************/

	/**
	 * Return times where node n was alive
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
	 * @param INb
	 * @param I
	 * @return
	 */
	protected boolean refine(BitSet INb, BitSet I) {
		for (int i = 0; i < I.length(); i++) {
			if (I.get(i) && !INb.get(i)) {
				// if pruning will not be done then update the traversedBitset with the new time instants
				INb.or(I);
				return false;
			}
		}
		return true;
	}
}