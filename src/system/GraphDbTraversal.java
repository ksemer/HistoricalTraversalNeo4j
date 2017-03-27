package system;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * GraphDbTraversal Abstract Class
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
	
	/**
	 * Reachability computation between src -> trg
	 * 
	 * @param src
	 * @param trg
	 * @param rel
	 * @return
	 */
	protected abstract boolean reachabilityBFS(Node src, Node trg, int t);
	
	/**
	 * Load graph db types
	 */
	protected abstract void loadTypes();
	
	/**
	 * Path computation for disjunctive query
	 * 
	 * @param src
	 * @param trg
	 * @param time_instance
	 * @return
	 */
	protected abstract String pathTraversalDisj(Node src, Node trg, int t);

	/**
	 * Path computation for conjunctive query
	 * 
	 * @param src
	 * @param trg
	 * @param rel
	 * @param times
	 * @return
	 */
	protected abstract String pathTraversalConj(Node src, Node trg, List<Integer> times_);
	
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
	protected abstract String pathTraversalAtLeast(Node src, Node trg, List<Integer> times_, int k);


	/**
	 * Connection src -> trg in all times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public boolean conjuctiveReachability(Node src, Node trg, Set<Integer> times) {

		if (!checkLifespan(src, times).isEmpty() || !checkLifespan(trg, times).isEmpty())
			return false;

		for (int t : times) {
			if (!reachabilityBFS(src, trg, t)) {
				return false;
			}
		}

		return true;
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

		for (int t : t1) {
			if (reachabilityBFS(src, trg, t)) {
				return true;
			}
		}

		return false;
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

		for (int t : t1) {
			if (reachabilityBFS(src, trg, t)) {
				if (--k == 0)
					return true;
			}
		}

		return true;
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
		String path = null;

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);

		for (int t : t1) {
			path = pathTraversalDisj(src, trg, t);

			if (!path.isEmpty())
				return path;
		}

		return "no path exists";
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

	/** Return times where node n was alive
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

	public boolean conjuctiveReachability(Set<Integer> times) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean disjunctiveReachability(Set<Integer> times) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean atLeastReachability(Set<Integer> times, int parseInt) {
		// TODO Auto-generated method stub
		return false;
	}

	public String disjunctivePath(Set<Integer> times) {
		// TODO Auto-generated method stub
		return null;
	}

	public String atLeastPath(Set<Integer> times, int parseInt) {
		// TODO Auto-generated method stub
		return null;
	}

	public String conjunctivePath(Set<Integer> times) {
		// TODO Auto-generated method stub
		return null;
	}
}